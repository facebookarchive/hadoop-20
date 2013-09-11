/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedAvatarFileSystem;
import org.apache.hadoop.raid.protocol.PolicyInfo;


/**
 * Traverses the directory tree and gets the desired FileStatus specified by
 * a given {@link DirectoryTraversal.Filter}. This class is not thread safe.
 */
public class DirectoryTraversal {

  static final public Log LOG =
    LogFactory.getLog(DirectoryTraversal.class);
  static final public FileStatus FINISH_TOKEN = new FileStatus();
  static final int OUTPUT_QUEUE_SIZE = 10000;

  final private FileSystem fs;
  final private DistributedAvatarFileSystem avatarFs;
  final private BlockingQueue<FileStatus> output;
  final private BlockingDeque<Path> directories;
  final private Filter filter;
  final private Processor[] processors;
  final private AtomicInteger totalDirectories;
  final private AtomicInteger activeThreads;
  final private boolean doShuffle;
  final private boolean allowStandby;
  private volatile boolean finished = false;

  // For enabling/disabling avatar.
  private static final boolean ENABLE_AVATAR_STANDBY = false;

  /**
   * Filters the elements to output
   */
  public interface Filter {
    boolean check(FileStatus f) throws IOException;
  }

  public DirectoryTraversal(Collection<Path> roots, FileSystem fs,
      Filter filter, int numThreads, boolean doShuffle)
      throws IOException {
    this(DirectoryTraversal.class.getSimpleName(), roots, fs, filter,
        numThreads, doShuffle);
  }

  public DirectoryTraversal(String friendlyName, Collection<Path> roots,
      FileSystem fs, Filter filter, int numThreads, boolean doShuffle)
      throws IOException {
    this(friendlyName, roots, fs, filter, numThreads, doShuffle, false,
        false);
  }
  
  public DirectoryTraversal(String friendlyName, Collection<Path> roots,
      FileSystem fs, Filter filter, int numThreads, boolean doShuffle, 
      boolean allowUseStandby)
      throws IOException {
    this(friendlyName, roots, fs, filter, numThreads, doShuffle, 
        allowUseStandby, false);
  }

  public DirectoryTraversal(String friendlyName, Collection<Path> roots,
      FileSystem fs, Filter filter, int numThreads, boolean doShuffle,
      boolean allowUseStandby, boolean checkLeafDir)
      throws IOException {
    this.output = new ArrayBlockingQueue<FileStatus>(OUTPUT_QUEUE_SIZE);
    this.directories = new LinkedBlockingDeque<Path>();
    this.fs = fs;
    if (ENABLE_AVATAR_STANDBY && allowUseStandby && fs instanceof DistributedAvatarFileSystem) {
    	avatarFs = (DistributedAvatarFileSystem) fs;
    } else {
    	avatarFs = null;
    }
    this.filter = filter;
    this.totalDirectories = new AtomicInteger(roots.size());
    this.processors = new Processor[numThreads];
    this.activeThreads = new AtomicInteger(numThreads);
    this.doShuffle = doShuffle;
    this.allowStandby = allowUseStandby;
    if (doShuffle) {
      List<Path> toShuffleAndAdd = new ArrayList<Path>();
      toShuffleAndAdd.addAll(roots);
      Collections.shuffle(toShuffleAndAdd);
      this.directories.addAll(toShuffleAndAdd);
    } else {
      this.directories.addAll(roots);
    }
    LOG.info("Starting with directories:" + roots.toString() +
        " numThreads:" + numThreads);
    if (roots.isEmpty()) {
      try {
        output.put(FINISH_TOKEN);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      return;
    }
    for (int i = 0; i < processors.length; ++i) {
      if (checkLeafDir) {
        processors[i] = new LeafDirectoryProcessor();
      } else {
        processors[i] = new Processor();
      }
      processors[i].setName(friendlyName + i);
    }
    for (int i = 0; i < processors.length; ++i) {
      processors[i].start();
    }
  }

  /**
   * Retrieves the next filtered element.
   * Returns {@link FINISH_TOKEN} when traversal is done. Calling this after
   * {@link FINISH_TOKEN} is returned is not allowed.
   */
  public FileStatus next() throws IOException {
    if (finished) {
      LOG.warn("Should not call next() after FINISH_TOKEN is obtained.");
      return FINISH_TOKEN;
    }
    FileStatus f = null;
    try {
      f = output.take();
    } catch (InterruptedException e) {
      finished = true;
      interruptProcessors();
      throw new IOException(e);
    }
    if (f == FINISH_TOKEN) {
      LOG.info("traversal is done. Returning FINISH_TOKEN");
      finished = true;
    }
    return f;
  }

  private void interruptProcessors() {
    for (Thread processor : processors) {
      if (processor != null) {
        processor.interrupt();
      }
    }
  }
  
  private class LeafDirectoryProcessor extends Processor {
    @Override
    protected void filterDirectory(Path dir, List<Path> subDirs,
        List<FileStatus> filtered) throws IOException {
      subDirs.clear();
      filtered.clear();
      if (dir == null) {
        return;
      }
      FileStatus[] elements;
      if (avatarFs != null) {
        elements = avatarFs.listStatus(dir, true);
      } else {
        elements = fs.listStatus(dir);
      }
      cache.clear();
      if (elements != null) {
        boolean isLeafDir = true;
        for (FileStatus element : elements) {
          if (element.isDir()) {
            subDirs.add(element.getPath());
            isLeafDir = false;
          }
        }
        if (isLeafDir && elements.length > 0) {
          FileStatus dirStat = avatarFs != null?
              avatarFs.getFileStatus(dir): 
              fs.getFileStatus(dir);
          if (filter.check(dirStat)) {
            filtered.add(dirStat);
          }
        }
      }
    }
  }

  private class Processor extends Thread {
    /* This cache is used to reduce the number of RPC calls, instead of running listLocatedStatus for each file, 
     * We run listLocatedStatus for files' parent path and cache the results. Because one processor processes the 
     * files under the same directory, only few RPC call is needed to get LocatedFileStatus of these files.
     * Please check PlacementMonitor.getLocatedFileStatus for more details.  
     */
    protected HashMap<String, LocatedFileStatus> cache;
    @Override
    public void run() {
      this.cache = PlacementMonitor.locatedFileStatusCache.get();
      List<Path> subDirs = new ArrayList<Path>();
      List<FileStatus> filtered = new ArrayList<FileStatus>();
      try {
        while (!finished && totalDirectories.get() > 0) {
          Path dir = null;
          try {
            dir = directories.poll(1000L, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            continue;
          }
          if (dir == null) {
            continue;
          }
          try {
            filterDirectory(dir, subDirs, filtered);
          } catch (Throwable ex) {
            LOG.error(getName() + " throws Throwable. Skip " + dir, ex);
            totalDirectories.decrementAndGet();
            continue;
          }
          int numOfDirectoriesChanged = -1 + subDirs.size();
          if (totalDirectories.addAndGet(numOfDirectoriesChanged) == 0) {
            interruptProcessors();
          }
          submitOutputs(filtered, subDirs);
        }
      } finally {
        // clear the cache to avoid memory leak
        cache.clear();
        PlacementMonitor.locatedFileStatusCache.remove();
        int active = activeThreads.decrementAndGet();
        if (active == 0) {
          while (true) {
            try {
              output.put(FINISH_TOKEN);
              break;
            } catch (InterruptedException e) {
            }
          }
        }
      }
    }

    protected void filterDirectory(Path dir, List<Path> subDirs,
        List<FileStatus> filtered) throws IOException {
      subDirs.clear();
      filtered.clear();
      if (dir == null) {
        return;
      }
      FileStatus[] elements;
      if (avatarFs != null) {
    	  elements = avatarFs.listStatus(dir, true);
      } else {
    	  elements = fs.listStatus(dir);
      }
      cache.clear();
      if (elements != null) {
        for (FileStatus element : elements) {
          if (filter.check(element)) {
            filtered.add(element);
          }
          if (element.isDir()) {
            subDirs.add(element.getPath());
          }
        }
      }
    }

    /**
     * Submit filtered result to output and directories. Will swallow interrupt
     * unless {@link finished} is set to true.
     */
    private void submitOutputs(List<FileStatus> filtered, List<Path> subDirs) {
      if (doShuffle) {
        Collections.shuffle(subDirs);
      }
      for (Path subDir : subDirs) {
        while (!finished) {
          try {
            directories.putFirst(subDir);
            break;
          } catch (InterruptedException e) {
          }
        }
      }
      for (FileStatus out : filtered) {
        while (!finished) {
          try {
            output.put(out);
            break;
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  public static DirectoryTraversal fileRetriever(
      List<Path> roots, FileSystem fs, int numThreads, boolean doShuffle,
      boolean allowUseStandby)
      throws IOException {
    Filter filter = new Filter() {
      @Override
      public boolean check(FileStatus f) throws IOException {
        return !f.isDir();
      }
    };
    return new DirectoryTraversal("File Retriever ", roots, fs, filter,
      numThreads, doShuffle, allowUseStandby);
  }

  public static DirectoryTraversal directoryRetriever(
      List<Path> roots, FileSystem fs, int numThreads, boolean doShuffle,
      boolean allowUseStandby, boolean checkLeafDir)
      throws IOException {
    Filter filter = new Filter() {
      @Override
      public boolean check(FileStatus f) throws IOException {
        return f.isDir();
      }
    };
    return new DirectoryTraversal("Directory Retriever ", roots, fs, filter,
      numThreads, doShuffle, allowUseStandby, checkLeafDir);
  }
  
  public static DirectoryTraversal directoryRetriever(
      List<Path> roots, FileSystem fs, int numThreads, boolean doShuffle,
      boolean allowUseStandby) throws IOException {
    return directoryRetriever(roots, fs, numThreads, 
              doShuffle, allowUseStandby, false);
  }

  public static DirectoryTraversal directoryRetriever(
      List<Path> roots, FileSystem fs, int numThreads, boolean doShuffle)
      throws IOException {
    return directoryRetriever(roots, fs, numThreads, doShuffle, false);
  }

  public static DirectoryTraversal raidFileRetriever(
      final PolicyInfo info, List<Path> roots, Collection<PolicyInfo> allInfos,
      Configuration conf, int numThreads, boolean doShuffle, boolean allowStandby)
      throws IOException {
    final RaidState.Checker checker = new RaidState.Checker(allInfos, conf);
    Filter filter = new Filter() {
      @Override
      public boolean check(FileStatus f) throws IOException {
        long now = RaidNode.now();
        if (f.isDir()) {
          return false;
        }
        RaidState state = checker.check(info, f, now, false);
        LOG.debug(f.getPath().toUri().getPath() + " : " + state);
        return state == RaidState.NOT_RAIDED_BUT_SHOULD;
      }
    };
    FileSystem fs = new Path(Path.SEPARATOR).getFileSystem(conf);
    return new DirectoryTraversal("Raid File Retriever ", roots, fs, filter,
      numThreads, doShuffle, allowStandby, false);
  }
  
  public static DirectoryTraversal raidLeafDirectoryRetriever(
      final PolicyInfo info, List<Path> roots, Collection<PolicyInfo> allInfos,
      final Configuration conf, int numThreads, boolean doShuffle,
      boolean allowStandby)
      throws IOException {
    final RaidState.Checker checker = new RaidState.Checker(allInfos, conf);
    final FileSystem fs = FileSystem.get(conf);
    Filter filter = new Filter() {
      @Override
      public boolean check(FileStatus f) throws IOException {
        long now = RaidNode.now();
        if (!f.isDir()) {
          return false;
        }
        List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(conf,
            fs, f.getPath());
        RaidState state = checker.check(info, f, now, false, lfs);
        if (LOG.isDebugEnabled()) {
          LOG.debug(f.getPath() + " : " + state);
        }
        return state == RaidState.NOT_RAIDED_BUT_SHOULD;
      }
    };
    return new DirectoryTraversal("Raid File Retriever ", roots, fs, filter,
      numThreads, doShuffle, allowStandby, true);
  }
}

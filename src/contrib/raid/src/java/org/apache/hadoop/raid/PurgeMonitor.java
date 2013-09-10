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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.RaidUtils.RaidInfo;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Periodically delete orphaned parity files.
 */
public class PurgeMonitor implements Runnable {
  public static final Log LOG = LogFactory.getLog(PurgeMonitor.class);
  public static final long PURGE_MONITOR_SLEEP_TIME_DEFAULT = 10000L;
  public static final String PURGE_MONITOR_SLEEP_TIME_KEY =
      "hdfs.raid.purge.monitor.sleep";

  volatile boolean running = true;

  private Configuration conf;
  private PlacementMonitor placementMonitor;
  private int directoryTraversalThreads;
  private boolean directoryTraversalShuffle;
  private long purgeMonitorSleepTime = PURGE_MONITOR_SLEEP_TIME_DEFAULT;

  AtomicLong entriesProcessed;
  private final RaidNode raidNode;
  private final static List<FileStatus> modifiedSource = 
      new ArrayList<FileStatus>();
  
  public PurgeMonitor(Configuration conf, 
                      PlacementMonitor placementMonitor,
                      final RaidNode raidNode) {
    this.conf = conf;
    this.placementMonitor = placementMonitor;
    this.directoryTraversalShuffle =
        conf.getBoolean(RaidNode.RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
    this.directoryTraversalThreads =
        conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
    this.purgeMonitorSleepTime = 
        conf.getLong(PURGE_MONITOR_SLEEP_TIME_KEY,
            PURGE_MONITOR_SLEEP_TIME_DEFAULT);
    this.entriesProcessed = new AtomicLong(0);
    this.raidNode = raidNode;
  }

  /**
   */
  public void run() {
    while (running) {
      try {
        doPurge();
      } catch (Exception e) {
        LOG.error("PurgeMonitor error ", e);
      } finally {
        LOG.info("Purge parity files thread continuing to run...");
      }
    }
  }

  /**
   * Traverse the parity destination directory, removing directories that
   * no longer existing in the source.
   * @throws IOException
   */
  private void purgeDirectories(FileSystem fs, Path root) throws IOException {
    DirectoryTraversal traversal =
        DirectoryTraversal.directoryRetriever(Arrays.asList(root), fs,
            directoryTraversalThreads, directoryTraversalShuffle);
    String prefix = root.toUri().getPath();
    
    FileStatus dir;
    while ((dir = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
      Path dirPath = dir.getPath();
      if (dirPath.toUri().getPath().endsWith(RaidNode.HAR_SUFFIX)) {
        continue;
      }
      String dirStr = dirPath.toUri().getPath();
      if (!dirStr.startsWith(prefix)) {
        continue;
      }
      entriesProcessed.incrementAndGet();
      String src = dirStr.replaceFirst(prefix, "");
      if (src.length() == 0) continue;
      Path srcPath = new Path(src);
      if (!fs.exists(srcPath)) {
        performDelete(fs, dirPath, true);
      }
    }
  }
  
  void doPurge() throws IOException, InterruptedException {
    entriesProcessed.set(0);
    while (running) {
    	Thread.sleep(purgeMonitorSleepTime);
      placementMonitor.startCheckingFiles();
      try {
        // purge the directories
        for (Codec c : Codec.getCodecs()) {
          Path parityPath = new Path(c.parityDirectory);
          FileSystem parityFs = parityPath.getFileSystem(conf);
          // One pass to purge directories that dont exists in the src.
          // This is cheaper than looking at parities.
          LOG.info("Check directory for codec: " + c.id + ", directory: " + c.parityDirectory);
          purgeDirectories(parityFs, parityPath);
        }
        
      	// purge the parities.
        for (Codec c : Codec.getCodecs()) {
          modifiedSource.clear();
          purgeCode(c);
          try {
            // re-generate the parity files for modified sources.
            if (modifiedSource.size() > 0) {
              LOG.info("re-generate parity files");
              PolicyInfo info = raidNode.determinePolicy(c);
              // check if we should raid the files/directories.
              for (Iterator<FileStatus> it = modifiedSource.iterator(); 
                      it.hasNext();) {
                FileStatus stat = it.next();
                if (!RaidNode.shouldRaid(conf, 
                    stat.getPath().getFileSystem(conf), stat, c)) {
                  it.remove();
                }
              }
              raidNode.raidFiles(info, modifiedSource);
            }
          } catch (Exception ex) {
            // ignore the error
            LOG.warn(ex.getMessage(), ex);
          }
        }
      } finally {
        placementMonitor.clearAndReport();
      }
    }
  }

  void purgeCode(Codec codec) throws IOException {
    Path parityPath = new Path(codec.parityDirectory);
    FileSystem parityFs = parityPath.getFileSystem(conf);
    PolicyInfo policy = raidNode == null ? null: raidNode.determinePolicy(codec);
    FileSystem srcFs = parityFs;  // Assume src == parity
    FileStatus stat = null;
    try {
      stat = parityFs.getFileStatus(parityPath);
    } catch (FileNotFoundException e) {}
    if (stat == null) return;

    LOG.info("Purging obsolete parity files for " + parityPath);
    DirectoryTraversal obsoleteParityFileRetriever =
      new DirectoryTraversal(
        "Purge File ",
        Collections.singletonList(parityPath),
        parityFs,
        new PurgeParityFileFilter(conf, codec, policy, srcFs, parityFs,
          parityPath.toUri().getPath(), placementMonitor, entriesProcessed),
        directoryTraversalThreads,
        directoryTraversalShuffle);
    FileStatus obsolete = null;
    while ((obsolete = obsoleteParityFileRetriever.next()) !=
              DirectoryTraversal.FINISH_TOKEN) {
      performDelete(parityFs, obsolete.getPath(), false);
    }

    if (!codec.isDirRaid) {
      DirectoryTraversal obsoleteParityHarRetriever =
        new DirectoryTraversal(
          "Purge HAR ",
          Collections.singletonList(parityPath),
          parityFs,
          new PurgeHarFilter(conf, codec, policy, srcFs, parityFs,
             parityPath.toUri().getPath(), placementMonitor, entriesProcessed),
          directoryTraversalThreads,
          directoryTraversalShuffle);
      while ((obsolete = obsoleteParityHarRetriever.next()) !=
                DirectoryTraversal.FINISH_TOKEN) {
        performDelete(parityFs, obsolete.getPath(), true);
      }
    }
  }

  static void performDelete(FileSystem fs, Path p, boolean recursive)
      throws IOException {
    DistributedFileSystem dfs = DFSUtil.convertToDFS(fs);
    boolean success = dfs.delete(p, recursive);
    if (success) {
      LOG.info("Purging " + p + ", recursive=" + recursive);
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).entriesPurged.inc();
    } else {
      LOG.error("Could not delete " + p);
    }
  }

  static class PurgeHarFilter implements DirectoryTraversal.Filter {
    Configuration conf;
    Codec codec;
    PolicyInfo policy;
    FileSystem srcFs;
    FileSystem parityFs;
    String parityPrefix;
    PlacementMonitor placementMonitor;
    AtomicLong counter;

    PurgeHarFilter(
        Configuration conf,
        Codec codec,
        PolicyInfo policy,
        FileSystem srcFs,
        FileSystem parityFs,
        String parityPrefix,
        PlacementMonitor placementMonitor,
        AtomicLong counter) {
      this.conf = conf;
      this.codec = codec;
      this.policy = policy;
      this.parityPrefix = parityPrefix;
      this.srcFs = srcFs;
      this.parityFs = parityFs;
      this.placementMonitor = placementMonitor;
      this.counter = counter;
    }

    @Override
    public boolean check(FileStatus f) throws IOException {
      if (f.isDir()) {
        String pathStr = f.getPath().toUri().getPath();
        if (pathStr.endsWith(RaidNode.HAR_SUFFIX)) {
          counter.incrementAndGet();
          try {
            float harUsedPercent =
              usefulHar(codec, policy, srcFs, parityFs, f.getPath(), 
                  parityPrefix, conf, placementMonitor);
            LOG.info("Useful percentage of " + pathStr + " " + harUsedPercent);
            // Delete the har if its usefulness reaches a threshold.
            if (harUsedPercent <= conf.getFloat("raid.har.usage.threshold", 0)) {
              return true;
            }
          } catch (IOException e) {
            LOG.warn("Error in har check ", e);
          }
        }
      }
      return false;
    }
  }
  
  static class PurgeParityFileFilter implements DirectoryTraversal.Filter {
    Configuration conf;
    Codec codec;
    PolicyInfo policy;
    FileSystem srcFs;
    FileSystem parityFs;
    String parityPrefix;
    PlacementMonitor placementMonitor;
    AtomicLong counter;
    final long minFileSize;

    PurgeParityFileFilter(
        Configuration conf,
        Codec codec,
        PolicyInfo policy,
        FileSystem srcFs,
        FileSystem parityFs,
        String parityPrefix,
        PlacementMonitor placementMonitor,
        AtomicLong counter) {
      this.conf = conf;
      this.codec = codec;
      this.policy = policy;
      this.parityPrefix = parityPrefix;
      this.srcFs = srcFs;
      this.parityFs = parityFs;
      this.placementMonitor = placementMonitor;
      this.counter = counter;
      this.minFileSize = conf.getLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY,
          RaidNode.MINIMUM_RAIDABLE_FILESIZE);
    }
    
    private void checkSrcDir(FileSystem srcFs, FileStatus dirStat) throws IOException {
    	if (!dirStat.isDir()) {
    		return;
    	}
    	
    	if (placementMonitor == null) {
				LOG.warn("PlacementMonitor is null, can not check the file.");
				return;
			}
    	
    	FileStatus[] files = srcFs.listStatus(dirStat.getPath());
    	for (FileStatus stat : files) {    		
    		// only check small unraided files.
    		if (stat.getLen() >= this.minFileSize) {
    			continue;
    		}
  			placementMonitor.checkSrcFile(srcFs, stat);
    	}
    }

    @Override
    public boolean check(FileStatus f) throws IOException {
      if (f.isDir()) return false;

      String pathStr = f.getPath().toUri().getPath();

      // Verify the parityPrefix is a prefix of the parityPath
      if (!pathStr.startsWith(parityPrefix)) return false;
      
      // Do not deal with parity HARs here.
      if (pathStr.indexOf(RaidNode.HAR_SUFFIX) != -1) return false;

      counter.incrementAndGet();
      String src = pathStr.replaceFirst(parityPrefix, "");

      Path srcPath = new Path(src);
      boolean shouldDelete = false;
      FileStatus srcStat = null;
      try {
        srcStat = srcFs.getFileStatus(srcPath);
      } catch (FileNotFoundException e) {
        // No such src file, delete the parity file.
        shouldDelete = true;
      }
      
      // check the src files of the directory-raid parity
      if (srcStat != null && codec.isDirRaid) {
      	checkSrcDir(srcFs, srcStat);
      }

      if (!shouldDelete) {
        try {
          if (existsBetterParityFile(codec, srcStat, conf)) {
            shouldDelete = true;
          }
          
          if (!shouldDelete) {
            if (srcStat.getModificationTime() != f.getModificationTime()
                && codec.isDirRaid) {
              modifiedSource.add(srcStat);
              LogUtils.logEvent(srcFs, srcPath, LOGTYPES.MODIFICATION_TIME_CHANGE,
                  LOGRESULTS.NONE, codec, null, null, codec.id);
              // delete the parity file after we finish the re-generation.
              return false;
            }
            
            ParityFilePair ppair =
                ParityFilePair.getParityFile(codec, srcStat, conf);
            if ( ppair == null ||
                 !parityFs.equals(ppair.getFileSystem()) ||
                 !pathStr.equals(ppair.getPath().toUri().getPath())) {
              shouldDelete = true;
            } else {
              // This parity file matches the source file.
              if (placementMonitor != null) {
                placementMonitor.checkFile(srcFs, srcStat,
                  ppair.getFileSystem(), ppair.getFileStatus(), codec, policy);
              }
            }
          }
        } catch (IOException e) {
          LOG.warn("Error during purging " + src, e);
        }
      }
      return shouldDelete;
    }
  }

  /**
   * Is there a parity file which has a codec with higher priority?
   */
  private static boolean existsBetterParityFile(
      Codec codec, FileStatus srcStat, Configuration conf) throws IOException {
    for (Codec c : Codec.getCodecs()) {
      if (c.priority > codec.priority) {
        ParityFilePair ppair = ParityFilePair.getParityFile(
            c, srcStat, conf);
        if (ppair != null) {
          return true;
        }
      }
    }
    return false;
  }

  //
  // Returns the number of up-to-date files in the har as a percentage of the
  // total number of files in the har.
  //
  protected static float usefulHar(
    Codec codec,
    PolicyInfo policy,
    FileSystem srcFs, FileSystem parityFs,
    Path harPath, String parityPrefix, Configuration conf,
    PlacementMonitor placementMonitor) throws IOException {

    HarIndex harIndex = HarIndex.getHarIndex(parityFs, harPath);
    Iterator<HarIndex.IndexEntry> entryIt = harIndex.getEntries();
    int numUseless = 0;
    int filesInHar = 0;
    while (entryIt.hasNext()) {
      HarIndex.IndexEntry entry = entryIt.next();
      filesInHar++;
      if (!entry.fileName.startsWith(parityPrefix)) {
        continue;
      }
      String src = entry.fileName.substring(parityPrefix.length());
      Path srcPath = new Path(src);
      FileStatus srcStatus = null;
      try {
        srcStatus = srcFs.getFileStatus(srcPath);
      } catch (FileNotFoundException e) {
        numUseless++;
        continue;
      }
      if (existsBetterParityFile(codec, srcStatus, conf)) {
        numUseless += 1;
        continue;
      }
      try {
        if (srcStatus == null) {
          numUseless++;
        } else if (entry.mtime != srcStatus.getModificationTime()) {
          numUseless++;
        } else {
          // This parity file in this HAR is good.
          if (placementMonitor != null) {
            // Check placement.
            placementMonitor.checkFile(
              srcFs, srcStatus,
              parityFs, harIndex.partFilePath(entry), entry, codec, policy);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Useful file " + entry.fileName);
          }
        }
      } catch (FileNotFoundException e) {
        numUseless++;
      }
    }
    if (filesInHar == 0) { return 0; }
    float uselessPercent = numUseless * 100.0f / filesInHar;
    return 100 - uselessPercent;
  }

  public String htmlTable() {
    return JspUtils.tableSimple(
            JspUtils.tr(
              JspUtils.td("Entries Processed") +
              JspUtils.td(":") +
              JspUtils.td(entriesProcessed.toString())));
  }
}


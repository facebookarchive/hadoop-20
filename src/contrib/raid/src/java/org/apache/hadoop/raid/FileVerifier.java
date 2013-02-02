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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 * Check for files that have a replication factor of less than 3
 * And which do not have a parity file created for them
 */
public class FileVerifier {

  private Configuration conf;
  private boolean directoryTraversalShuffle;
  private int directoryTraversalThreads;
  private ChecksumStore ckmStore;

  public FileVerifier(Configuration conf) throws IOException {
    this.conf = conf;
    this.directoryTraversalShuffle =
        conf.getBoolean(RaidNode.RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
    this.directoryTraversalThreads =
        conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
    this.ckmStore = RaidNode.createChecksumStore(conf, false);
    if (ckmStore == null) {
      throw new IOException("Checksum store is not available");
    }
  }
  
  public static class GetChecksumAction implements Runnable {
    final int BUFFER_SIZE = 1024*1024;
    FSDataInputStream stm = null;
    byte[] buffer = null;
    FileStatus f = null;
    long blockNum = 0;
    Long oldCkm = null;
    Block blk = null;
    String infoMessage = null;
    AtomicBoolean isBadFile = null; 
    AtomicInteger actionNum = null;
    GetChecksumAction(FileStatus f, int blockNum, FileSystem fs, Long oldCkm, 
        Block blk, AtomicBoolean newIsBadFile, AtomicInteger newActionNum)
            throws IOException {
      this.f = f;
      stm = fs.open(f.getPath());
      buffer = new byte[BUFFER_SIZE];
      this.oldCkm = oldCkm;
      this.blk = blk;
      this.blockNum = blockNum;
      stm.seek(blockNum * f.getBlockSize());
      isBadFile = newIsBadFile;
      actionNum = newActionNum;
    }
    
    public void run() {
      try {
        long startOffset = blockNum * f.getBlockSize();
        infoMessage = " block " + blockNum + " " + blk + " in file " + f.getPath();
        CRC32 crc = new CRC32();
        long toRead = Math.min(f.getBlockSize(), f.getLen() - startOffset);
        int pos = 0;
        while (pos < toRead) {
          int len = (int)Math.min(toRead - pos, BUFFER_SIZE);
          int numRead = stm.read(buffer, 0, len);
          if (numRead > 0) {
            crc.update(buffer, 0, numRead);
            pos += numRead;
          } else break;
        }
        if (pos != toRead) {
          System.err.println("Cannot fully read " + infoMessage);
          return;
        }
        System.err.println(blk + ":" + crc.getValue());
        if (crc.getValue() != oldCkm) {
          System.err.println("Mismatch : (old)" + oldCkm +
              " (new)" + crc.getValue() + " " + infoMessage);
          isBadFile.set(true);
        }
      } catch (IOException ioe) {
        System.err.println("Read exception: " + infoMessage + ioe.getMessage());
      } finally {
        actionNum.incrementAndGet();
        if (stm != null) {
          try {
            stm.close();
          } catch (IOException e) {
            System.err.println("Close exception: " + infoMessage + e.getMessage());
          }
        }
      }
    }
  }
  
  public void verifyFile(Path root, PrintStream out) throws IOException {
    FileSystem fs = root.getFileSystem(conf);
    List<Path> allPaths = Arrays.asList(root);
    DirectoryTraversal.Filter filter = new FileVerifyFilter(conf, ckmStore);
    boolean allowUseStandby = false;
    DirectoryTraversal traversal =
      new DirectoryTraversal("File Verifier Retriver ", allPaths, fs, filter,
        directoryTraversalThreads, directoryTraversalShuffle, allowUseStandby);
    FileStatus newFile;
    while ((newFile = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
      Path filePath = newFile.getPath();
      out.println(filePath.toUri().getPath());
    }
  }

  static class FileVerifyFilter implements DirectoryTraversal.Filter {
    Configuration conf;
    ChecksumStore ckmStore;
    final int THREAD_NUM = 5;
    ThreadFactory factory = null;
    
    
    FileVerifyFilter(Configuration conf, final ChecksumStore ckmStore)
        throws IOException {
      this.conf = conf;
      this.ckmStore = ckmStore;
      this.factory = new ThreadFactory() {
        final AtomicInteger numThreads = new AtomicInteger();
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("FileVerifyFilter-" + numThreads.getAndIncrement());
          return t;
        }
      };
    }
    
    @Override
    public boolean check(FileStatus f) throws IOException {
      if (f.isDir()) return false;
      DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
      LocatedBlocks lbs = fs.getLocatedBlocks(f.getPath(), 0, f.getLen());
      if (lbs.getLocatedBlocks().size() == 0) 
        return false;
      AtomicBoolean badFile = new AtomicBoolean(false);
      AtomicInteger actionNum = new AtomicInteger(0);
      ExecutorService executor = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM,
          0L, TimeUnit.MILLISECONDS, 
          new ArrayBlockingQueue<Runnable>(THREAD_NUM*10, true),
          factory);
      
      int expectedActions = 0;
      for (int i = 0; i < lbs.getLocatedBlocks().size(); i++) {
        Block blk = lbs.get(i).getBlock();
        Long ckm = null;
        synchronized(ckmStore) {
          ckm = ckmStore.getChecksum(blk);
        }
        if (ckm != null) {
          expectedActions++;
          executor.execute(new GetChecksumAction(f, i, fs, ckm, blk, badFile
              , actionNum));
        }
      }
      while (actionNum.get() < expectedActions) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          executor.shutdown();
          throw new IOException(ie);
        }
      }
      executor.shutdown();
      return badFile.get();
    }   
  }
}
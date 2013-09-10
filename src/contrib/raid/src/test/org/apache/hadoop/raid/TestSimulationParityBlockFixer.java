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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

public class TestSimulationParityBlockFixer extends TestSimulationBlockFixer {
  public void testParityHarBadBlockFixer() throws Exception {
    LOG.info("Test testParityHarBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1, "org.apache.hadoop.raid.BadXORCode",
        "org.apache.hadoop.raid.BadReedSolomonCode", "rs", true); 
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    // Parity file will have 7 blocks.
    long crc = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                               1, 20, blockSize);
    LOG.info("Created test files");
    
    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt(RaidNode.RAID_PARITY_HAR_THRESHOLD_DAYS_KEY, 0);
    localConf.set("raid.blockfix.classname",
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      Path harDirectory =
        new Path("/destraidrs/user/dhruba/raidtest/raidtest" +
                 RaidNode.HAR_SUFFIX);
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 1000 * 120) {
        if (fileSys.exists(harDirectory)) {
          break;
        }
        LOG.info("Waiting for har");
        Thread.sleep(1000);
      }
      assertEquals(true, fileSys.exists(harDirectory));
      cnode.stop(); cnode.join();
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      // Corrupt source blocks
      FileStatus stat = fileSys.getFileStatus(file1);
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
          dfs, file1.toUri().getPath(), 0, stat.getLen());
      int[] corruptBlockIdxs = new int[]{0};
      for (int idx: corruptBlockIdxs) {
        TestBlockFixer.corruptBlock(locs.get(idx).getBlock(),
            dfsCluster);
      }
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs,
          stat.getBlockSize());
  
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
  
      cnode = RaidNode.createRaidNode(null, localConf);
      start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Waiting for files to be fixed.");
        Thread.sleep(1000);
      }
  
      long checkCRC = RaidDFSUtil.getCRC(fileSys, file1);
      assertEquals("file not fixed", crc, checkCRC);
      // Verify the counters are right
      long expectedNumFailures = corruptBlockIdxs.length;
      assertEquals(expectedNumFailures,
          cnode.blockIntegrityMonitor.getNumBlockFixSimulationFailures());
      assertEquals(0,
          cnode.blockIntegrityMonitor.getNumBlockFixSimulationSuccess());
    } catch (Exception e) {
      LOG.info("Exception ", e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testParityHarBlockFix completed.");
  }
  
  public void testGoodParityBlockFixer() throws Exception {
    implSimulationBlockFixer(true, "xor", false);
    implSimulationBlockFixer(true, "rs", false);
  }
  
  public void testBadParityBlockFixer() throws Exception {
    implSimulationBlockFixer(false, "xor", false);
    implSimulationBlockFixer(false, "rs", false);
  }
  
  @Override
  public void testBadBlockFixerWithoutChecksums() throws Exception {
    implSimulationBlockFixer(false, "xor", false, false, true);
    implSimulationBlockFixer(false, "rs", false, false, true);
  }
  
  @Override
  public void testBadBlockFixerWithoutSimulation() throws Exception {
    implSimulationBlockFixer(false, "xor", false, true, false);
    implSimulationBlockFixer(false, "rs", false, true, false);
  }
  
  @Override
  public void testGoodBlockFixer() throws Exception {
  }
  
  @Override
  public void testBadBlockFixer() throws Exception {
  }
}

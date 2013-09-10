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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Priority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import java.util.HashSet;
import java.util.Set;

/*
 * In Smoke Test Runnable, raidnode will try to raid a small file 
 * and submit a small job to fix this file to verify blockfixing is working.
 * If not, it will shutdown the raidnode
 */
public class SmokeTestThread implements Callable<Boolean> {
  public static final Log LOG = LogFactory.getLog(SmokeTestThread.class);
  public static final String TEST_CODEC = "rs";
  public static final long NUM_SOURCE_BLOCKS = 3;
  public static final long BLOCK_SIZE = 512L;
  public static final long SLEEP_TIME = 3000L;
  public static final String SMOKE_TEST_TIMEOUT_KEY = 
      "raid.smoke.test.timeout";
  public static final long DEFAULT_SMOKE_TEST_TIME_OUT = 120000L;
  public long timeOut = DEFAULT_SMOKE_TEST_TIME_OUT;
  public IOException ioe = null;
  public FileSystem fileSys = null;
  public String testFileDirectory = "/test/";
  public String testFileBase = testFileDirectory + "smoketest";
  public Random rand = new Random();
  public DistRaidNode distRaidNode = null;
  public CRC32 checksum = new CRC32();
  public SmokeTestThread(RaidNode rn) {
    distRaidNode = (DistRaidNode)rn;
    timeOut = distRaidNode.getConf().getLong(SMOKE_TEST_TIMEOUT_KEY, 
        DEFAULT_SMOKE_TEST_TIME_OUT);
  }
  
  @Override 
  public Boolean call() throws Exception {
    Path testPath = null;
    try {
      fileSys = FileSystem.get(distRaidNode.getConf());
      // Create a small file with 3 blocks
      String testFile = testFileBase + rand.nextLong();
      testPath = new Path(testFile);
      if (fileSys.exists(testPath)) {
        fileSys.delete(testPath, true);
      }
      long blockSize = BLOCK_SIZE;
      FSDataOutputStream stm = fileSys.create(testPath, true,
          fileSys.getConf().getInt("io.file.buffer.size", 4096),
          (short)3, blockSize);
      // Write 3 blocks.
      byte[] b = new byte[(int)blockSize];
      for (int i = 0; i < NUM_SOURCE_BLOCKS; i++) {
        rand.nextBytes(b);
        stm.write(b);    
        checksum.update(b);
      }
      stm.close();
      LOG.info("[SMOKETEST] Created a test file: " + testFile + 
          " with CRC32 checksum " + checksum.getValue());
      PolicyInfo info = new PolicyInfo(testFile, distRaidNode.getConf());
      info.setCodecId(TEST_CODEC);
      info.setSrcPath(testFileDirectory);
      info.setShouldRaid(true);
      info.setProperty("modTimePeriod", "0");
      info.setProperty("targetReplication", "1");
      info.setProperty("metaReplication", "1");
      FileStatus stat = fileSys.getFileStatus(testPath);
      ArrayList<FileStatus> fstats = new ArrayList<FileStatus>();
      fstats.add(stat);
      // Raid it using rs
      DistRaid dr = DistRaidNode.raidFiles(distRaidNode.getConf(), distRaidNode.jobMonitor,
          fstats, info);
      LOG.info("[SMOKETEST] RS Raid test file: " + testFile);
      if (dr == null) {
        throw new IOException("Failed to sart a raiding job");
      }
      long startTime = System.currentTimeMillis(); 
      while (!dr.checkComplete() &&
             System.currentTimeMillis() - startTime < timeOut) {
        Thread.sleep(SLEEP_TIME);
      }
      if (!dr.checkComplete()) {
        throw new IOException("Failed to finish the raiding job in " + 
            (timeOut/1000) + " seconds");
      }
      if (!dr.successful()) {
        throw new IOException("Failed to raid the file " + testFile);
      }
      LOG.info("[SMOKETEST] Finish raiding test file: " + testFile);
      // Verify parity file exists
      Codec codec = Codec.getCodec(TEST_CODEC);
      Path parityPath = new Path(codec.getParityPrefix(),
          RaidNode.makeRelative(testPath));
      FileStatus parityStat = fileSys.getFileStatus(parityPath);
      long numParityBlocks = RaidNode.numBlocks(parityStat);
      long expectedNumParityBlocks = 
          RaidNode.numStripes(NUM_SOURCE_BLOCKS,
              codec.stripeLength) * codec.parityLength;
      if (numParityBlocks != expectedNumParityBlocks ||
          parityStat.getLen() != expectedNumParityBlocks * BLOCK_SIZE) {
        throw new IOException("[SMOKETEST] Parity file " + parityPath + 
            " has " + numParityBlocks + " blocks and " + parityStat.getLen() + 
            " bytes, but we expect " + expectedNumParityBlocks  + " blocks and " + 
            (expectedNumParityBlocks * BLOCK_SIZE) + " bytes");
      }
      LOG.info("[SMOKETEST] Verification of parity file " + parityPath + " succeeded");
      LocatedBlock[] blocks = new LocatedBlock[1];
      LocatedBlocks lbs = 
          ((DistributedFileSystem)fileSys).getLocatedBlocks(testPath, 0,
              Integer.MAX_VALUE);
      // Corrupt the first block
      blocks[0] = lbs.get(0);
      ((DistributedFileSystem)fileSys).getClient().reportBadBlocks(blocks);
      LOG.info("[SMOKETEST] Finish corrupting the first block " + 
          lbs.get(0).getBlock());
      // submit a job to "fix" it
      Set<String> jobFiles = new HashSet<String>();
      jobFiles.add(testFile);
      Job job = DistBlockIntegrityMonitor.startOneJob(
          (DistBlockIntegrityMonitor.Worker) distRaidNode.blockIntegrityMonitor.getCorruptionMonitor(),
          Priority.HIGH, jobFiles, System.currentTimeMillis(), new AtomicLong(0), 
          new AtomicLong(System.currentTimeMillis()), Integer.MAX_VALUE);
      startTime = System.currentTimeMillis();
      while (!job.isComplete() && System.currentTimeMillis() - startTime < timeOut) {
        Thread.sleep(SLEEP_TIME);
      }
      if (!job.isComplete()) {
        throw new IOException("Failed to finish the blockfixing job in " + 
            (timeOut/1000) + " seconds");
      }
      if (!job.isSuccessful()) {
        throw new IOException("Failed to fix the file " + testFile);
      }
      LOG.info("[SMOKETEST] Finish blockfixing test file: " + testFile);
      // wait for block is reported
      startTime = System.currentTimeMillis();
      while (((DistributedFileSystem)fileSys).getLocatedBlocks(testPath, 0,
          Integer.MAX_VALUE).get(0).isCorrupt() && 
          System.currentTimeMillis() - startTime < timeOut) {
        Thread.sleep(SLEEP_TIME);
      }
      CRC32 newChk = new CRC32();
      FSDataInputStream readStm = fileSys.open(testPath);
      int num = 0;
      while (num >= 0) {
        num = readStm.read(b);
        if (num < 0) {
          break;
        }
        newChk.update(b, 0, num);
      }
      stm.close();
      if (newChk.getValue() != checksum.getValue()) {
        throw new IOException("Fixed file's checksum " + newChk.getValue() +
            " != original one " + checksum.getValue());
      }
      LOG.info("[SMOKETEST] Verification of fixed test file: " + testFile);
      return true;
    } catch (IOException ex) {
      LOG.error("Get IOException in SmokeTestThread", ex);
      ioe = ex;
      return false;
    } catch (Throwable ex) {
      LOG.error("Get Error in SmokeTestThread", ex);
      ioe = new IOException(ex);
      return false;
    } finally {
      try {
        if (fileSys != null) {
          fileSys.delete(testPath, true);
        }
      } catch (IOException ioe) {
        LOG.error("Get error during deletion", ioe);
      }
    }
  }
}
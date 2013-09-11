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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.ParityFilePair;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.TestDirectoryRaidDfs;
import org.apache.hadoop.raid.Utils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestRaidDfs {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidDfs");
  final static int NUM_DATANODES = 3;
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;
  Codec codec;
  int stripeLength;
  
  private void mySetup(String erasureCode, int rsParityLength) 
      throws Exception {
    mySetup(erasureCode, rsParityLength, false, false);
  }
  
  private void mySetup(String erasureCode, int rsParityLength, 
      boolean isDirRaid) throws Exception {
    mySetup(erasureCode, rsParityLength, isDirRaid, false);
  }

  private void mySetup(
      String erasureCode, int rsParityLength, boolean isDirRaid, 
      boolean specialBufSize)
          throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    if (!specialBufSize) {
      conf.setInt("raid.encoder.bufsize", 128);
      conf.setInt("raid.decoder.bufsize", 128);
    } else {
      // set buffer size to be not dividable 
      conf.setInt("raid.encoder.bufsize", 127);
      conf.setInt("raid.decoder.bufsize", 127);
      conf.setInt("raid.encoder.max.buffer.size", 7);
      conf.setInt("raid.decoder.max.buffer.size", 7);
    }

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1,
        rsParityLength, "/destraid", "/destraidrs", false, isDirRaid);
    codec = Codec.getCodec(erasureCode);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // Reduce run time for the test.
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    conf.setInt("dfs.client.baseTimeWindow.waitOn.BlockMissingException", 10);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");

    conf.set("raid.server.address", "localhost:0");

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
  }

  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }
  
  private LocatedBlocks getBlockLocations(Path file, long length)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
    return RaidDFSUtil.getBlockLocations(
      dfs, file.toUri().getPath(), 0, length);
  }

  private LocatedBlocks getBlockLocations(Path file)
    throws IOException {
    FileStatus stat = fileSys.getFileStatus(file);
    return getBlockLocations(file, stat.getLen());
  }

  private DistributedRaidFileSystem getRaidFS() throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    return (DistributedRaidFileSystem)FileSystem.get(dfsUri, clientConf);
  }
  
  public static void waitForFileRaided(
      Log logger, FileSystem fileSys, Path file, Path destPath)
  throws IOException, InterruptedException {
    waitForFileRaided(logger, fileSys, file, destPath, (short)1);
  }
  
  public static void waitForReplicasReduction(FileSystem fileSys, Path srcPath,
      short targetReplication) throws IOException, InterruptedException {
    // Make sure datanode report block deletion fast enough
    assertEquals(fileSys.getConf().getLong("dfs.blockreport.intervalMsec",
        FSConstants.BLOCKREPORT_INTERVAL), 8000);
    FileStatus fstat = fileSys.getFileStatus(srcPath);
    FileStatus[] listStats = null;
    if (fstat.isDir()) {
      listStats = fileSys.listStatus(srcPath);
    } else {
      listStats = new FileStatus[]{fstat};
    }
    long start;
    for (FileStatus stat: listStats) {
      assertEquals("File " + stat.getPath() +
          " current repl: " + stat.getReplication() + " expected: " + 
          targetReplication, stat.getReplication(), targetReplication);
    }
    for (FileStatus stat: listStats) {
      start = System.currentTimeMillis();
      boolean reduced = false;
      while (System.currentTimeMillis() - start < 60000 && !reduced) {
        BlockLocation[] bls = fileSys.getFileBlockLocations(stat, 0L, stat.getLen());
        reduced = true;
        for (BlockLocation bl: bls) {
          if (bl.getHosts().length > targetReplication) {
            reduced = false;
            break;
          }
        }
        if (!reduced)
          Thread.sleep(1000);
      }
      assertTrue("Replicas of " + stat.getPath() + " are more than " + targetReplication,
          reduced);
    }
  }

  public static void waitForFileRaided(
    Log logger, FileSystem fileSys, Path file, Path destPath, short targetReplication)
  throws IOException, InterruptedException {
    FileStatus parityStat = null;
    String fileName = file.getName().toString();
    long startTime = System.currentTimeMillis();
    // wait till file is raided
    while (parityStat == null && System.currentTimeMillis() - startTime < 120000) {
      logger.info("Waiting for files to be raided.");
      try {
        FileStatus[] listPaths = fileSys.listStatus(destPath);
        if (listPaths != null) {
          for (FileStatus f : listPaths) {
            logger.info("File raided so far : " + f.getPath());
            String found = f.getPath().getName().toString();
            if (fileName.equals(found)) {
              parityStat = f;
              break;
            }
          }
        }
      } catch (FileNotFoundException e) {
        //ignore
      }
      Thread.sleep(1000);                  // keep waiting
    }

    while (true) {
      LocatedBlocks locations = null;
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      locations = RaidDFSUtil.getBlockLocations(
        dfs, file.toUri().getPath(), 0, parityStat.getLen());
      if (!locations.isUnderConstruction()) {
        break;
      }
      Thread.sleep(1000);
    }

    while (true) {
      FileStatus stat = fileSys.getFileStatus(file);
      if (stat.getReplication() == targetReplication) break;
      Thread.sleep(1000);
    }
  }
  
  public static void waitForDirRaided(
      Log logger, FileSystem fileSys, Path file, Path destPath) 
    throws IOException, InterruptedException {
    waitForDirRaided(logger, fileSys, file, destPath, (short)1);
  }

  public static void waitForDirRaided(
      Log logger, FileSystem fileSys, Path file, Path destPath, 
      short targetReplication) 
         throws IOException, InterruptedException {
    waitForDirRaided(logger, fileSys, file, destPath, 
        targetReplication, 90000);
  }

  public static void waitForDirRaided(
      Log logger, FileSystem fileSys, Path file, Path destPath, 
      short targetReplication, long waitMillis)
    throws IOException, InterruptedException {
    FileStatus parityStat = null;
    String fileName = file.getName().toString();
    long startTime = System.currentTimeMillis();
    FileStatus srcStat = fileSys.getFileStatus(file);
    // wait till file is raided
    while (parityStat == null &&
        System.currentTimeMillis() - startTime < waitMillis) {
      logger.info("Waiting for files to be raided.");
      try {
        FileStatus[] listPaths = fileSys.listStatus(destPath);
        if (listPaths != null) {
          for (FileStatus f : listPaths) {
            logger.info("File raided so far : " + f.getPath());
            String found = f.getPath().getName().toString();
            if (fileName.equals(found) &&
                srcStat.getModificationTime() == f.getModificationTime()) {
              parityStat = f;
              break;
            }
          }
        }
      } catch (FileNotFoundException e) {
        //ignore
      }
      Thread.sleep(1000);                  // keep waiting
    }
    assertTrue("Parity file is not generated", parityStat != null);
    assertEquals(srcStat.getModificationTime(), parityStat.getModificationTime());
    for (FileStatus stat: fileSys.listStatus(file)) {
      assertEquals(stat.getReplication(), targetReplication);
    }
  }
  
  private void corruptBlockAndValidate(Path srcFile, Path destPath,
    int[] listBlockNumToCorrupt, long blockSize, int numBlocks,
    MiniDFSCluster cluster)
  throws IOException, InterruptedException {
    RaidDFSUtil.cleanUp(fileSys, srcFile.getParent());
    fileSys.mkdirs(srcFile.getParent());
    int repl = 1;
    long crc = createTestFilePartialLastBlock(fileSys, srcFile, repl,
                  numBlocks, blockSize);
    long length = fileSys.getFileStatus(srcFile).getLen();

    if (codec.isDirRaid) {
      RaidNode.doRaid(conf, fileSys.getFileStatus(srcFile.getParent()),
      destPath, codec, new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
      false, repl, repl);
    } else {
      RaidNode.doRaid(conf, fileSys.getFileStatus(srcFile),
      destPath, codec, new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
      false, repl, repl);
    }

    // Delete first block of file
    for (int blockNumToCorrupt : listBlockNumToCorrupt) {
      LOG.info("Corrupt block " + blockNumToCorrupt + " of file " + srcFile);
      LocatedBlocks locations = getBlockLocations(srcFile);
      corruptBlock(srcFile, locations.get(blockNumToCorrupt).getBlock(),
            NUM_DATANODES, true, cluster);
    }

    // Validate
    DistributedRaidFileSystem raidfs = getRaidFS();
    assertTrue(validateFile(raidfs, srcFile, length, crc));
  }
 
  @Test
  public void testRaidDfsXorSpecialBufferSize() throws Exception {
    testRaidDfsXorCore(false, true);
  }
  
  @Test
  public void testRaidDfsDirXorSpecialBufferSize() throws Exception {
    testRaidDfsXorCore(true, true);
  }
  
  @Test
  public void testRaidDfsRsSpecialBufferSize() throws Exception {
    testRaidDfsRsCore(false, true);
  }
  
  @Test
  public void testRaidDfsDirRsSpecialBufferSize() throws Exception {
    testRaidDfsRsCore(true, true);
  }

  /**
   * Create a file, corrupt several blocks in it and ensure that the file can be
   * read through DistributedRaidFileSystem by ReedSolomon coding.
   */
  private void testRaidDfsRsCore(boolean isDirRaid, boolean specialBufSize)
      throws Exception {
    LOG.info("Test testRaidDfs started");

    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("rs", 3, isDirRaid, specialBufSize);

    int[][] corrupt = {{1, 2, 3}, {1, 4, 7}, {3, 6, 7}};
    try {
      for (int i = 0; i < corrupt.length; i++) {
        Path file = new Path("/user/dhruba/raidtest/file" + i);
        corruptBlockAndValidate(
            file, new Path("/destraidrs"), corrupt[i], blockSize, numBlocks,
            dfs);
      }
    } catch (Exception e) {
      LOG.info("testRaidDfs Exception " + e +
                StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testRaidDfs completed.");
  }
 
  @Test
  public void testRaidDfsRs() throws Exception {
    testRaidDfsRsCore(false, false);
  }
  
  @Test
  public void testRaidDfsDirRs() throws Exception {
    testRaidDfsRsCore(true, false);
  }
  
  /**
   * Test DistributedRaidFileSystem with relative path 
   */
  @Test
  public void testRelativePath() throws Exception {
    stripeLength = 3;
    mySetup("xor", 1);

    try {
      DistributedRaidFileSystem raidfs = getRaidFS();
      Path file = new Path(raidfs.getHomeDirectory(), "raidtest/file1");
      Path file1 = new Path("raidtest/file1");
      long crc = createTestFile(raidfs.getFileSystem(), file, 1, 8, 8192L);
      FileStatus stat = fileSys.getFileStatus(file);
      LOG.info("Created " + file + ", crc=" + crc + ", len=" + stat.getLen());

      byte[] filebytes = new byte[(int)stat.getLen()];
      // Test that readFully returns the correct CRC when there are no errors.
      FSDataInputStream stm = raidfs.open(file);
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
      stm.close();
      
      stm = raidfs.open(file1);
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
      stm.close();
    } finally {
      myTearDown();
    }
  }

  /**
   * Test DistributedRaidFileSystem.readFully()
   */
  private void testReadFullyCore(boolean isDirRaid) throws Exception {
    stripeLength = 3;
    mySetup("xor", 1, isDirRaid);

    try {
      Path file = new Path("/user/raid/raidtest/file1");
      long crc = createTestFile(fileSys, file, 1, 8, 8192L);
      FileStatus stat = fileSys.getFileStatus(file);
      LOG.info("Created " + file + ", crc=" + crc + ", len=" + stat.getLen());

      byte[] filebytes = new byte[(int)stat.getLen()];
      // Test that readFully returns the correct CRC when there are no errors.
      DistributedRaidFileSystem raidfs = getRaidFS();
      FSDataInputStream stm = raidfs.open(file);
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
      stm.close();
      
      // Generate parity.
      if (isDirRaid) {
        RaidNode.doRaid(conf, fileSys.getFileStatus(file.getParent()),
          new Path("/destraid"), codec, new RaidNode.Statistics(),
          RaidUtils.NULL_PROGRESSABLE,
          false, 1, 1);
      } else {
        RaidNode.doRaid(conf, fileSys.getFileStatus(file),
            new Path("/destraid"), codec, new RaidNode.Statistics(),
            RaidUtils.NULL_PROGRESSABLE,
            false, 1, 1);
      }
      int[] corrupt = {0, 4, 7}; // first, last and middle block
      for (int blockIdx : corrupt) {
        LOG.info("Corrupt block " + blockIdx + " of file " + file);
        LocatedBlocks locations = getBlockLocations(file);
        removeAndReportBlock((DistributedFileSystem)fileSys, 
            file, locations.get(blockIdx), dfs);
      }
      // Test that readFully returns the correct CRC when there are errors.
      stm = raidfs.open(file);
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
    } finally {
      myTearDown();
    }
  }
  
  @Test
  public void testReadFully() throws Exception {
    testReadFullyCore(false);
  }
  
  @Test
  public void testDirReadFully() throws Exception {
    testReadFullyCore(true);
  }

  @Test
  public void testSeek() throws Exception {
    stripeLength = 3;
    mySetup("xor", 1);

    try {
      Path file = new Path("/user/raid/raidtest/file1");
      long crc = createTestFile(fileSys, file, 1, 8, 8192L);
      FileStatus stat = fileSys.getFileStatus(file);
      LOG.info("Created " + file + ", crc=" + crc + ", len=" + stat.getLen());

      // Test that readFully returns the correct CRC when there are no errors.
      DistributedRaidFileSystem raidfs = getRaidFS();
      FSDataInputStream stm = raidfs.open(file);

      //Test end of file
      LOG.info("Seek to " + (stat.getLen()-1) + ", len=" + stat.getLen());
      stm.seek(stat.getLen()-1);
      assertEquals(stat.getLen()-1, stm.getPos());
      
      LOG.info("Seek to " + stat.getLen() + ", len=" + stat.getLen());
      stm.seek(stat.getLen());
      assertEquals(stat.getLen(), stm.getPos());

      // Should work.
      LOG.info("Seek to " + (stat.getLen()/2) + ", len=" + stat.getLen());
      stm.seek(stat.getLen()/2);
      assertEquals(stat.getLen()/2, stm.getPos());

      // Should work.
      LOG.info("Seek to " + stat.getLen() + ", len=" + stat.getLen());
      stm.seek(stat.getLen());
      assertEquals(stat.getLen(), stm.getPos());
      
      LOG.info("Seek to " + (stat.getLen()+1) + ", len=" + stat.getLen());
      boolean expectedExceptionThrown = false;
      try {
        stm.seek(stat.getLen() + 1);
      } catch (EOFException e) {
        expectedExceptionThrown = true;
      }
      assertTrue(expectedExceptionThrown);
    } finally {
      myTearDown();
    }
  }

  /**
   * Create a file, corrupt a block in it and ensure that the file can be
   * read through DistributedRaidFileSystem by XOR code.
   * @specialBufSize test raiding files with block size not dividable by
   * buffer size 
   */
  private void testRaidDfsXorCore(boolean isDirRaid, boolean specialBufSize)
      throws Exception {
    LOG.info("Test testRaidDfs started.");

    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("xor", 1, isDirRaid, specialBufSize);

    // heavy test!! try to corrupt every block to test the partial reading
    int[][] corrupt = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}}; 
    try {
      for (int i = 0; i < corrupt.length; i++) {
        Path file = new Path("/user/dhruba/raidtest/xor" + i);
        corruptBlockAndValidate(
            file, new Path("/destraid"), corrupt[i], blockSize, numBlocks, dfs);
      }
    } catch (Exception e) {
      LOG.info("testRaidDfs Exception " + e +
                StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testRaidDfs completed.");
  }
  
  @Test
  public void testRaidDfsXor() throws Exception {
    testRaidDfsXorCore(false, false);
  }
  
  @Test
  public void testRaidDfsDirXor() throws Exception {
    testRaidDfsXorCore(true, false);
  }
  
  @Test
  public void testRead() throws Exception {
    mySetup("xor", 1);
    try {
      Path p = new Path("/tmp/testRead." + System.currentTimeMillis());
      long crc = createTestFile(fileSys, p, 1, 8, 8192L);
      FileStatus stat = fileSys.getFileStatus(p);
      LOG.info("Created " + p + ", crc=" + crc + ", len=" + stat.getLen());

      // Test that readFully returns the correct CRC when there are no errors.
      DistributedRaidFileSystem raidfs = getRaidFS();
      FSDataInputStream stm = raidfs.open(p);
      // Should be able to read all bytes.
      for (int i = 0; i < stat.getLen(); i++) {
        int val = stm.read();
        if (val < 0) {
          LOG.error("Invalid value " + val + " at index " + i);
        }
        assertTrue(val >= 0);
      }
      stm.close();
      // One more read should return -1 to indicate EOF.
      assertEquals(-1, stm.read());
    } finally {
      myTearDown();
    }
  }

  @Test
  public void testZeroLengthFile() throws Exception {
    mySetup("xor", 1);
    try {
      Path p = new Path("/tmp/testZero." + System.currentTimeMillis());
      fileSys.create(p).close();
      FileStatus stat = fileSys.getFileStatus(p);
      LOG.info("Created " + p + ", len=" + stat.getLen());

      // Test that readFully returns the correct CRC when there are no errors.
      DistributedRaidFileSystem raidfs = getRaidFS();
      FSDataInputStream stm = raidfs.open(p);
      assertEquals(-1, stm.read());
    } finally {
      myTearDown();
    }
  }
  
  private void testTooManyErrorsDecodeXORCore(boolean isDirRaid)
      throws Exception {
    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("xor", 1, isDirRaid);
    try {
      int[] corrupt = {0,1}; // Two blocks in the same stripe is too much.
      Path file = new Path("/user/dhruba/raidtest/file1");
      boolean expectedExceptionThrown = false;
      try {
        corruptBlockAndValidate(
            file, new Path("/destraid"), corrupt, blockSize, numBlocks, dfs);
        // Should not reach.
      } catch (IOException e) {
        LOG.info("Expected exception caught" + e);
        expectedExceptionThrown = true;
      }
      assertTrue(expectedExceptionThrown);
    } finally {
      myTearDown();
    }
  }
  
  @Test
  public void testTooManyErrorsDecodeXOR() throws Exception {
    testTooManyErrorsDecodeXORCore(false);
  }
  
  @Test
  public void testTooManyErrorsDecodeDirXOR() throws Exception {
    testTooManyErrorsDecodeXORCore(true);
  }

  private void testTooManyErrorsDecodeRSCore(boolean isDirRaid)
      throws Exception {
    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("rs", 1, isDirRaid);
    try {
      int[] corrupt = {0,1}; // Two blocks in the same stripe is too much.
      Path file = new Path("/user/dhruba/raidtest/file2");
      boolean expectedExceptionThrown = false;
      try {
        corruptBlockAndValidate(
            file, new Path("/destraidrs"), corrupt, blockSize, numBlocks, dfs);
      } catch (IOException e) {
        LOG.info("Expected exception caught" + e);
        expectedExceptionThrown = true;
      }
      assertTrue(expectedExceptionThrown);
    } finally {
      myTearDown();
    }
  }
  
  @Test
  public void testTooManyErrorsDecodeRS() throws Exception {
    testTooManyErrorsDecodeRSCore(false);
  }
  
  @Test
  public void testTooManyErrorsDecodeDirRS() throws Exception {
    testTooManyErrorsDecodeRSCore(true);
  }

  private void testTooManyErrorsEncodeCore(boolean isDirRaid)
      throws Exception {
    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("xor", 1, isDirRaid);
    // Encoding with XOR should fail when even one block is corrupt.
    try {
      Path destPath = new Path("/destraid/user/dhruba/raidtest");
        Path file = new Path("/user/dhruba/raidtest/file1");
        int repl = 1;
        createTestFilePartialLastBlock(fileSys, file, repl, numBlocks, blockSize);

        int blockNumToCorrupt = 0;
        LOG.info("Corrupt block " + blockNumToCorrupt + " of file " + file);
        LocatedBlocks locations = getBlockLocations(file);
        removeAndReportBlock((DistributedFileSystem)fileSys,
            file, locations.get(blockNumToCorrupt),
            dfs);

      boolean expectedExceptionThrown = false;
      try {
        if (isDirRaid) {
          RaidNode.doRaid(conf, fileSys.getFileStatus(file.getParent()),
              destPath, codec, new RaidNode.Statistics(),
              RaidUtils.NULL_PROGRESSABLE, false, repl, repl);
        } else {
          RaidNode.doRaid(conf, fileSys.getFileStatus(file),
            destPath, codec, new RaidNode.Statistics(),
            RaidUtils.NULL_PROGRESSABLE, false, repl, repl);
        }
      } catch (IOException e) {
        LOG.info("Expected exception caught" + e);
        expectedExceptionThrown = true;
      }
      assertTrue(expectedExceptionThrown);
    } finally {
      myTearDown();
    }
  }
  
  @Test
  public void testTooManyErrorsEncode() throws Exception {
    testTooManyErrorsEncodeCore(false);
  }
  
  @Test
  public void testTooManyErrorsDirEncode() throws Exception {
    testTooManyErrorsEncodeCore(true);
  }

  private void testTooManyErrorsEncodeRSCore(boolean isDirRaid) throws Exception {
    long blockSize = 8192L;
    int numBlocks = 8;
    stripeLength = 3;
    mySetup("rs", 1, isDirRaid);
    // Encoding with RS should fail when even one block is corrupt.
    try {
      Path destPath = new Path("/destraidrs/user/dhruba/raidtest");
        Path file = new Path("/user/dhruba/raidtest/file2");
        int repl = 1;
        createTestFilePartialLastBlock(fileSys, file, repl, numBlocks, blockSize);

        int blockNumToCorrupt = 0;
        LOG.info("Corrupt block " + blockNumToCorrupt + " of file " + file);
        LocatedBlocks locations = getBlockLocations(file);
        removeAndReportBlock((DistributedFileSystem)fileSys,
            file, locations.get(blockNumToCorrupt),
            dfs);

      boolean expectedExceptionThrown = false;
      try {
        if (isDirRaid) {
          RaidNode.doRaid(conf, fileSys.getFileStatus(file.getParent()),
              destPath, codec, new RaidNode.Statistics(),
              RaidUtils.NULL_PROGRESSABLE, false, repl, repl);
        } else {
          RaidNode.doRaid(conf, fileSys.getFileStatus(file),
            destPath, codec, new RaidNode.Statistics(),
            RaidUtils.NULL_PROGRESSABLE, false, repl, repl);
        }
      } catch (IOException e) {
        expectedExceptionThrown = true;
        LOG.info("Expected exception caught" + e);
      }
      assertTrue(expectedExceptionThrown);
    } finally {
      myTearDown();
    }
  }
  
  @Test
  public void testTooManyErrorsEncodeRS() throws Exception {
    testTooManyErrorsEncodeRSCore(false);
  }
  
  @Test
  public void testTooManyErrorsEncodeDirRS() throws Exception {
    testTooManyErrorsEncodeRSCore(true);
  }

  //
  // creates a file and populate it with random data. Returns its crc.
  //
  public static long createTestFile(FileSystem fileSys, Path name, int repl,
                        int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    Random rand = new Random();
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
    // fill random data into file
    final byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }
  
  //
  // creates a file given a specific file size and it with random data.
  // Returns its crc.
  //
  public static long createTestFile(FileSystem fileSys, Path name, int repl,
                        long fileSize, long blockSize, int seed)
    throws IOException {
    CRC32 crc = new CRC32();
    Random rand = new Random(seed);
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blockSize);
    LOG.info("create file " + name + " size: " + fileSize + " blockSize: " + 
             blockSize + " repl: " + repl);
    // fill random data into file
    byte[] b = new byte[(int)blockSize];
    long numBlocks = fileSize / blockSize;
    for (int i = 0; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    long lastBlock = fileSize - numBlocks * blockSize;
    if (lastBlock > 0) {
      b = new byte[(int)lastBlock];
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }
  
  /**
   * Create a bunch of files under a directory srcDir.
   * all files' lengths are in fileSizes
   * all files' block sizes are in blockSizs
   * we will generate these files and put their checksum into crcs array
   * The seeds we use to generate files are stored in seeds array 
   */
  public static Path[] createTestFiles(Path srcDir, long[] fileSizes,
      long[] blockSizes, long[] crcs, int[] seeds,
      FileSystem fileSys, short repl)
          throws IOException {
    Path[] files = new Path[fileSizes.length];
    fileSys.mkdirs(srcDir);
    LOG.info("Create files under directory " + srcDir);
    Random rand = new Random();
    for (int i = 0; i < fileSizes.length; i++) {
      Path file = files[i] = new Path(srcDir, "file" + i);
      seeds[i] = rand.nextInt();
      crcs[i] = TestRaidDfs.createTestFile(fileSys, files[i], repl, fileSizes[i],
          blockSizes[i], seeds[i]);
      assertEquals("file size is not expected", fileSizes[i],
          fileSys.getFileStatus(file).getLen()); 
    }
    return files;
  }

  //
  // Creates a file with partially full last block. Populate it with random
  // data. Returns its crc.
  //
  public static long createTestFilePartialLastBlock(
      FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    Random rand = new Random();
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
    // Write whole blocks.
    byte[] b = new byte[(int)blocksize];
    for (int i = 1; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    // Write partial block.
    b = new byte[(int)blocksize/2 - 1];
    rand.nextBytes(b);
    stm.write(b);
    crc.update(b);

    stm.close();
    return crc.getValue();
  }

  static long bufferCRC(byte[] buf) {
    CRC32 crc = new CRC32();
    crc.update(buf, 0, buf.length);
    return crc.getValue();
  }

  //
  // validates that file matches the crc.
  //
  public static boolean validateFile(FileSystem fileSys, Path name, long length,
                                  long crc) 
    throws IOException {

    long numRead = 0;
    CRC32 newcrc = new CRC32();
    FSDataInputStream stm = fileSys.open(name);
    final byte[] b = new byte[4192];
    int num = 0;
    while (num >= 0) {
      num = stm.read(b);
      if (num < 0) {
        break;
      }
      numRead += num;
      newcrc.update(b, 0, num);
    }
    stm.close();

    if (numRead != length) {
      LOG.info("Number of bytes read " + numRead +
               " does not match file size " + length);
      return false;
    }

    LOG.info(" Newcrc " + newcrc.getValue() + " old crc " + crc);
    if (newcrc.getValue() != crc) {
      LOG.info("CRC mismatch of file " + name + ": " +
               newcrc.getValue() + " vs. " + crc);
      return false;
    }
    return true;
  }

  /*
   * The Data directories for a datanode
   */
  private static File[] getDataNodeDirs(int i, MiniDFSCluster cluster) throws IOException {
    File[] dir = new File[2];
    dir[0] = cluster.getBlockDirectory("data" + (2*i+1));
    dir[1] = cluster.getBlockDirectory("data" + (2*i+2));
    return dir;
  }
  
  
  public static void removeAndReportBlock(DistributedFileSystem blockDfs,
            Path filePath, int[] blockIdxs, MiniDFSCluster cluster) 
                throws IOException {
    FileStatus stat = blockDfs.getFileStatus(filePath);
    LocatedBlocks blocks = blockDfs.getLocatedBlocks(filePath, 
        0, stat.getLen());
    
    for (int blockIdx : blockIdxs) {
      removeAndReportBlock(blockDfs, filePath, blocks.get(blockIdx), cluster);
    }
  }
  
  /**
   * removes a specified block from MiniDFS storage and reports it as corrupt
   */
  public static void removeAndReportBlock(DistributedFileSystem blockDfs,
                                    Path filePath,
                                    LocatedBlock block,
                                    MiniDFSCluster cluster) 
    throws IOException {
    corruptBlock(filePath, block.getBlock(), 
        NUM_DATANODES, true, cluster);
   
    // report deleted block to the name node
    LocatedBlock[] toReport = { block };
    blockDfs.getClient().namenode.reportBadBlocks(toReport);

  }

  //
  // Delete/Corrupt specified block of file
  //
  public static void corruptBlock(Path file, Block blockNum,
                    int numDataNodes, boolean delete, MiniDFSCluster cluster)
    throws IOException {
    long id = blockNum.getBlockId();

    // Now deliberately remove/truncate data blocks from the block.
    int numDeleted = 0;
    int numCorrupted = 0;
    for (int i = 0; i < numDataNodes; i++) {
      File[] dirs = getDataNodeDirs(i, cluster);

      for (int j = 0; j < dirs.length; j++) {
        File[] blocks = dirs[j].listFiles();
        assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length >= 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          LOG.info("block file: " + blocks[idx]);
          if (blocks[idx].getName().startsWith("blk_" + id) &&
              !blocks[idx].getName().endsWith(".meta")) {
            if (delete) {
              blocks[idx].delete();
              LOG.info("Deleted block " + blocks[idx]);
              numDeleted++;
            } else {
              // Corrupt
              File f = blocks[idx];
              long seekPos = f.length()/2;
              RandomAccessFile raf = new RandomAccessFile(f, "rw");
              raf.seek(seekPos);
              int data = raf.readInt();
              raf.seek(seekPos);
              raf.writeInt(data+1);
              LOG.info("Corrupted block " + blocks[idx]);
              numCorrupted++;
            }
          }
        }
      }
    }
    assertTrue("Nothing corrupted or deleted",
              (numCorrupted + numDeleted) > 0);
  }

  public static void corruptBlock(Path file, Block blockNum,
                    int numDataNodes, long offset,
                    MiniDFSCluster cluster) throws IOException {
    long id = blockNum.getBlockId();

    // Now deliberately remove/truncate data blocks from the block.
    //
    for (int i = 0; i < numDataNodes; i++) {
      File[] dirs = getDataNodeDirs(i, cluster);
      
      for (int j = 0; j < dirs.length; j++) {
        File[] blocks = dirs[j].listFiles();
        assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length >= 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          if (blocks[idx].getName().startsWith("blk_" + id) &&
              !blocks[idx].getName().endsWith(".meta")) {
            // Corrupt
            File f = blocks[idx];
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(offset);
            int data = raf.readInt();
            raf.seek(offset);
            raf.writeInt(data+1);
            LOG.info("Corrupted block " + blocks[idx]);
          }
        }
      }
    }
  }
}

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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;

import junit.framework.TestCase;

public class TestDirectoryRaidEncoder extends TestCase { 
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestDirectoryRaidEncoder");
  final static int NUM_DATANODES = 3;
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }

  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  
  private void mySetup() throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.setInt("raid.encoder.bufsize", 128);
    conf.setInt("raid.decoder.bufsize", 128);
    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // Reduce run time for the test.
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    conf.setInt("dfs.client.baseTimeWindow.waitOn.BlockMissingException", 10);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");

    conf.set("raid.server.address", "localhost:0");
    //Don't allow empty file to be raid
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);

    FileSystem.setDefaultUri(conf, namenode);
  }
  
  private Codec loadTestCodecs(String erasureCode, int stripeLength, 
      boolean isDirRaid) throws Exception {
    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1,
        3, "/destraid", "/destraidrs", false, isDirRaid);
    return Codec.getCodec(erasureCode);
  }
  
  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }
  
  private boolean doRaid(Configuration conf, FileSystem fileSys, 
      Path srcPath, Codec codec) 
    throws IOException {
    return RaidNode.doRaid(conf, fileSys.getFileStatus(srcPath), 
        new Path(codec.parityDirectory), codec,
        new RaidNode.Statistics(),
        RaidUtils.NULL_PROGRESSABLE, false, 1, 1);
  }
  
  public void testAbnormalDirectory() throws Exception {
    mySetup();
    Codec codec = loadTestCodecs("xor", 4, true);
    try {
      Path sourceDir = new Path("/user/raid");
      Path parityFile = new Path("/destraid/user/raid");
      assertTrue(fileSys.mkdirs(sourceDir));
      LOG.info("Test non-leaf directory");
      assertFalse("Couldn't raid non-leaf directory ",
          doRaid(conf, fileSys, sourceDir.getParent(), codec));
      assertFalse(fileSys.exists(parityFile.getParent()));
      
      LOG.info("Test empty directory");
      assertFalse("Couldn't raid empty directory ", 
          doRaid(conf, fileSys, sourceDir, codec));
      assertFalse(fileSys.exists(parityFile));
      
      LOG.info("Test empty file in the directory");
      Path emptyFile = new Path(sourceDir, "emptyFile");
      TestRaidDfs.createTestFile(fileSys, emptyFile, 1, 0, 8192L);
      assertTrue(fileSys.exists(emptyFile));
      assertFalse("No raidable files in the directory",
          doRaid(conf, fileSys, sourceDir, codec));
      assertFalse(fileSys.exists(parityFile));
      
      LOG.info("Test not enough blocks in the directory");
      Path file1 = new Path(sourceDir, "file1");
      Path file2 = new Path(sourceDir, "file2");
      TestRaidDfs.createTestFile(fileSys, file1, 1, 1, 8192L);
      TestRaidDfs.createTestFile(fileSys, file2, 1, 1, 8192L);
      LOG.info("Created two files with two blocks in total");
      assertTrue(fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      assertFalse("Not enough blocks in the directory",
          doRaid(conf, fileSys, sourceDir, codec));
      assertFalse(fileSys.exists(parityFile));
      
    } finally {
      myTearDown();
    }
  }
  
  private void validateSingleFile(String code, FileSystem fileSys, 
      Path sourceDir, int stripeLength, int blockNum, boolean lastPartial)
          throws Exception {
    LOG.info("Test file with " + blockNum + " blocks and " +
          (lastPartial? "partial": "full") + " last block");
    Codec codec = loadTestCodecs(code, stripeLength, true);
    Path parityDir = new Path(codec.parityDirectory);
    RaidDFSUtil.cleanUp(fileSys, sourceDir);
    RaidDFSUtil.cleanUp(fileSys, parityDir);
    fileSys.mkdirs(sourceDir);
    
    Path file1 = new Path(sourceDir, "file1");
    if (!lastPartial) {
      TestRaidDfs.createTestFile(fileSys, file1, 2, blockNum, 8192L);
    } else {
      TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1, 2,
          blockNum, 8192L);
    }
    Path parityFile = RaidNode.getOriginalParityFile(parityDir, sourceDir);
    // Do directory level raid
    LOG.info("Create a directory-raid parity file " + parityFile);
    assertTrue("Cannot raid directory " + sourceDir, 
        doRaid(conf, fileSys, sourceDir, codec));
    assertEquals("Modification time should be the same", 
        fileSys.getFileStatus(sourceDir).getModificationTime(),
        fileSys.getFileStatus(parityFile).getModificationTime());
    assertEquals("Replica num of source file should be reduced to 1",
        fileSys.getFileStatus(file1).getReplication(), 1);
    assertEquals("Replica num of parity file should be reduced to 1",
        fileSys.getFileStatus(parityFile).getReplication(), 1);
    long dirCRC = RaidDFSUtil.getCRC(fileSys, parityFile);
    long dirLen = fileSys.getFileStatus(parityFile).getLen();
    // remove the parity dir
    RaidDFSUtil.cleanUp(fileSys, parityDir);
    codec = loadTestCodecs(code, stripeLength, false);
    Path parityFile1 = RaidNode.getOriginalParityFile(parityDir,
        file1);
    LOG.info("Create a file-raid parity file " + parityFile1);
    assertTrue("Cannot raid file " + file1, 
        doRaid(conf, fileSys, file1, codec));
    assertTrue("Parity file doesn't match when the file has " + blockNum + 
        " blocks ", 
        TestRaidDfs.validateFile(fileSys, parityFile1, dirLen, dirCRC));
  }

  public void testOneFileDirectory() throws Exception {
    mySetup();
    int stripeLength = 4;
    try {
      for (String code: RaidDFSUtil.codes) {
        LOG.info("testOneFileDirectory: Test code " + code);
        Codec codec = loadTestCodecs(code, stripeLength, true);
        Path sourceDir = new Path("/user/raid", code);
        assertTrue(fileSys.mkdirs(sourceDir));
        Path twoBlockFile = new Path(sourceDir, "twoBlockFile");;
        LOG.info("Test one file with 2 blocks");
        TestRaidDfs.createTestFile(fileSys, twoBlockFile, 2, 2, 8192L);
        assertTrue(fileSys.exists(twoBlockFile));
        assertFalse("Not enough blocks in the directory",
            RaidNode.doRaid(conf, fileSys.getFileStatus(sourceDir),
                new Path(codec.parityDirectory), codec,
                new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1));
        fileSys.delete(twoBlockFile, true);
        
        LOG.info("Test one file with blocks less than one stripe");
        validateSingleFile(code, fileSys, sourceDir, stripeLength, 3,
            false);
        validateSingleFile(code, fileSys, sourceDir, stripeLength, 3,
            true);
        LOG.info("Test one file with one stripe blocks");
        validateSingleFile(code, fileSys, sourceDir, stripeLength,
            stripeLength, false);
        validateSingleFile(code, fileSys, sourceDir, stripeLength,
            stripeLength, true);
        
        LOG.info("Test one file with more than one stripe blocks");
        validateSingleFile(code, fileSys, sourceDir, stripeLength,
            stripeLength + 2, false);
        validateSingleFile(code, fileSys, sourceDir, stripeLength,
            stripeLength + 2, true);
      }
    } finally {
      myTearDown();
    }
  }
  
  private void validateMultipleFiles(String code, FileSystem fileSys, 
      Path sourceDir, int stripeLength, long[] fileSizes, long blockSize,
      long singleFileBlockSize) throws Exception {
    long[] blockSizes = new long[fileSizes.length];
    for (int i = 0; i< fileSizes.length; i++)
      blockSizes[i] = blockSize;
    validateMultipleFiles(code, fileSys, sourceDir, stripeLength, fileSizes,
        blockSizes, singleFileBlockSize);
    
  }
  
  //
  // creates a file by grouping multiple files together
  // Returns its crc.
  //
  private long createDirectoryFile(FileSystem fileSys, Path name, int repl,
                        long[] fileSizes, long[] blockSizes, int[] seeds,
                        long blockSize) throws IOException {
    CRC32 crc = new CRC32();
    assert fileSizes.length == blockSizes.length;
    assert fileSizes.length == seeds.length;
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blockSize);
    byte[] zeros = new byte[(int)(blockSize)];
    for (int j = 0; j < zeros.length; j++) {
      zeros[j] = 0;
    }
    // fill random data into file
    for (int i = 0; i < fileSizes.length; i++) {
      assert blockSizes[i] <= blockSize;
      byte[] b = new byte[(int)blockSizes[i]];
      long numBlocks = fileSizes[i] / blockSizes[i];
      Random rand = new Random(seeds[i]);
      for (int j = 0; j < numBlocks; j++) {
        rand.nextBytes(b);
        stm.write(b);
        crc.update(b);
        int zeroLen = (int)(blockSize - blockSizes[i]);
        stm.write(zeros, 0, zeroLen);
        crc.update(zeros, 0, zeroLen);
      }
      long lastBlock = fileSizes[i] - numBlocks * blockSizes[i];
      if (lastBlock > 0) {
        b = new byte[(int)lastBlock];
        rand.nextBytes(b);
        stm.write(b);
        crc.update(b);
        if (i + 1 < fileSizes.length) {
          // Not last block of file, write zero
          int zeroLen = (int)(blockSize - lastBlock);
          stm.write(zeros, 0, zeroLen);
          crc.update(zeros, 0, zeroLen);
        }
      }
    }
    stm.close();
    return crc.getValue();
  }
  
  private void printFileCRC(FileSystem fs, Path file, long bufferSize)
      throws IOException {
    byte[] buffer = new byte[(int)bufferSize];
    FSDataInputStream stm = fs.open(file);
    StringBuilder sb = new StringBuilder();
    sb.append("CRC for file: " + file + " size " +
      fs.getFileStatus(file).getLen() + "\n");
    while (stm.read(buffer) >= 0) {
      CRC32 crc = new CRC32();
      crc.update(buffer);
      sb.append(" " + crc.getValue());
    }
    sb.append("\n");
    System.out.println(sb.toString());
    stm.close();
  }
  
  private void validateMultipleFiles(String code, FileSystem fileSys, 
      Path sourceDir, int stripeLength, long[] fileSizes, long[] blockSizes,
      long blockSize) throws Exception {
    assert fileSizes.length == blockSizes.length;
    Codec codec = loadTestCodecs(code, stripeLength, true);
    Path parityDir = new Path(codec.parityDirectory);
    RaidDFSUtil.cleanUp(fileSys, sourceDir);
    RaidDFSUtil.cleanUp(fileSys, parityDir);
    fileSys.mkdirs(sourceDir);
    
    LOG.info("Create files under directory " + sourceDir);
    Random rand = new Random();
    int[] seeds = new int[fileSizes.length];
    for (int i = 0; i < fileSizes.length; i++) {
      Path file = new Path(sourceDir, "file" + i);
      seeds[i] = rand.nextInt();
      TestRaidDfs.createTestFile(fileSys, file, 2, fileSizes[i],
          blockSizes[i], seeds[i]);
    }
    Path parityFile = RaidNode.getOriginalParityFile(parityDir, sourceDir);
    // Do directory level raid
    LOG.info("Create a directory-raid parity file " + parityFile);
    assertTrue("Cannot raid directory " + sourceDir, 
        doRaid(conf, fileSys, sourceDir, codec));
    this.printFileCRC(fileSys, parityFile, blockSize);
    long dirCRC = RaidDFSUtil.getCRC(fileSys, parityFile);
    long dirLen = fileSys.getFileStatus(parityFile).getLen();
    
    assertEquals("Modification time should be the same", 
        fileSys.getFileStatus(sourceDir).getModificationTime(),
        fileSys.getFileStatus(parityFile).getModificationTime());
    assertEquals("Replica num of parity file should be reduced to 1",
        fileSys.getFileStatus(parityFile).getReplication(), 1);
    for (int i = 0; i < fileSizes.length; i++) {
      Path file = new Path(sourceDir, "file" + i);
      assertEquals("Replica num of source file should be reduced to 1",
          fileSys.getFileStatus(file).getReplication(), 1);
    }
    // remove the source dir and parity dir
    RaidDFSUtil.cleanUp(fileSys, sourceDir);
    RaidDFSUtil.cleanUp(fileSys, parityDir);
    fileSys.mkdirs(sourceDir);
    codec = loadTestCodecs(code, stripeLength, false);
    Path file1 = new Path(sourceDir, "file1");
    Path parityFile1 = RaidNode.getOriginalParityFile(parityDir,
        file1);
    LOG.info("Create a source file " + file1);
    this.createDirectoryFile(fileSys, file1, 1, fileSizes, blockSizes, seeds,
        blockSize);
    LOG.info("Create a file-raid parity file " + parityFile1);
    assertTrue("Cannot raid file " + file1, 
        doRaid(conf, fileSys, file1, codec));
    this.printFileCRC(fileSys, parityFile1, blockSize);
    assertTrue("Parity file doesn't match", 
        TestRaidDfs.validateFile(fileSys, parityFile1, dirLen, dirCRC));
  }
  
  public void testSmallFileDirectory() throws Exception {
    mySetup();
    int stripeLength = 4;
    long blockSize = 8192L;
    try {
      for (String code: RaidDFSUtil.codes) {
        LOG.info("testSmallFileDirectory: Test code " + code);
        Path sourceDir = new Path("/user/raid");
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[]{1000L, 4000L, 1000L}, blockSize, 4096L);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[]{2000L, 3000L, 2000L, 3000L}, blockSize, 3072L);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[]{3000L, 3000L, 3000L, 3000L}, blockSize, 3072L);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[]{511L, 3584L, 3000L, 1234L, 512L, 1234L, 3000L,
            3234L, 511L}, blockSize, 3584L);
      }
    } finally {
      myTearDown();
    }
  }
  
  public void testIdenticalBlockSizeFileDirectory() throws Exception {
    mySetup();
    int stripeLength = 4;
    long blockSize = 8192L;
    try {
      for (String code: RaidDFSUtil.codes) {
        LOG.info("testIdenticalBlockSizeFileDirectory: Test code " + code);
        Path sourceDir = new Path("/user/raid");
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {1000L, blockSize, 2*blockSize, 4000L}, blockSize,
            blockSize);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {blockSize, 2*blockSize, 3*blockSize, 4*blockSize},
            blockSize, blockSize);
        int halfBlock = (int)blockSize/2;
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {blockSize + halfBlock, 2*blockSize + halfBlock,
                       3*blockSize + halfBlock, 4*blockSize + halfBlock},
            blockSize, blockSize);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {blockSize+1, 9*blockSize+1, 2*blockSize+1,
            3*blockSize+1}, blockSize, blockSize);
      }
    } finally {
      myTearDown();
    }
  }
  
  public void testDifferentBlockSizeFileDirectory() throws Exception {
    mySetup();
    int stripeLength = 3;
    long blockSize = 8192L;
    try {
      for (String code: RaidDFSUtil.codes) {
        LOG.info("testDifferentBlockSizeFileDirectory: Test code " + code);
        Path sourceDir = new Path("/user/raid");
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {1000, blockSize, 2*blockSize, 2*blockSize + 1},
            new long[] {blockSize, blockSize, 2*blockSize, blockSize},
            2*blockSize);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {blockSize, 2*blockSize, 3*blockSize, 4*blockSize},
            new long[] {blockSize, 2*blockSize, 3*blockSize, blockSize},
            3*blockSize);
        validateMultipleFiles(code, fileSys, sourceDir, stripeLength,
            new long[] {blockSize+1, 9*blockSize+1, 2*blockSize+1,
            blockSize+1}, new long[]{blockSize, 2*blockSize, 3*blockSize,
            blockSize}, 2*blockSize+512);
      }
    } finally {
      myTearDown();
    }
  }
}

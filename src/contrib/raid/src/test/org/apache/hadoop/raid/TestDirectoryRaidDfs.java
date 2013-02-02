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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.DirectoryStripeReader.BlockInfo;

public class TestDirectoryRaidDfs extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestDirectoryRaidDecoder");
  final static int NUM_DATANODES = 3;
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }
  final static long blockSize = 8192L;
  final static long halfBlock = blockSize / 2;
  final static long[][] fileSizes = {
    //Small file directory
    {2000L, 3000L, 2000L},
    {3000L, 3000L, 3000L},
    {511L, 3584L, 3000L, 1234L, 512L, 1234L, 3000L},
    //Identical Block Size File Directory
    {1000L, blockSize, 2*blockSize, 4000L},
    {blockSize, blockSize, 4*blockSize + 1},
    {blockSize + halfBlock, 2*blockSize + halfBlock,
      halfBlock},
    {blockSize+1, 7*blockSize+1, 2*blockSize+1,
      3*blockSize+1},
    //Different Block Size File Directory
    {1000, blockSize, 2*blockSize, 2*blockSize + 1},
    {blockSize, 2*blockSize, 3*blockSize, 4*blockSize},
    {blockSize+1, 9*blockSize+1, 2*blockSize+1,
      blockSize+1}
  };
  final static long[][] blockSizes = {
    // Small file directory
    {blockSize, blockSize, blockSize},
    {blockSize, blockSize, blockSize},
    {blockSize, blockSize, blockSize, blockSize, 
     blockSize, blockSize, blockSize},
    //Identical Block Size File Directory
    {blockSize, blockSize, blockSize, blockSize},
    {blockSize, blockSize, blockSize},
    {blockSize, blockSize, blockSize},
    {blockSize, blockSize, blockSize, blockSize},
    //Different Block Size File Directory
    {blockSize, blockSize, 2*blockSize, blockSize},
    {blockSize, 2*blockSize, 3*blockSize, blockSize},
    {blockSize, 2*blockSize, 3*blockSize,
      blockSize}
  };

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;
  Codec codec;
  int stripeLength;
   
  public static void setupStripeStore(Configuration conf, FileSystem fs) {
    final String STRIPE_STORE_DIR = new File(TEST_DIR,
        "stripe_store." + System.currentTimeMillis()).getAbsolutePath();
    conf.set(RaidNode.RAID_STRIPE_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalStripeStore");
    fs.getConf().set(RaidNode.RAID_STRIPE_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalStripeStore");
    conf.set(LocalStripeStore.LOCAL_STRIPE_STORE_DIR_KEY
        + "." + fs.getUri().getAuthority(), STRIPE_STORE_DIR);
    fs.getConf().set(LocalStripeStore.LOCAL_STRIPE_STORE_DIR_KEY
        + "." + fs.getUri().getAuthority(), STRIPE_STORE_DIR);
    RaidNode.createStripeStore(conf, true, null);
  }

  private void mySetup(
      String erasureCode, int rsParityLength) throws Exception {
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.setInt("raid.encoder.bufsize", 128);
    conf.setInt("raid.decoder.bufsize", 128);

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, rsParityLength, "/destraid",
        "/destraidrs", false, true);
    codec = Codec.getCodec(erasureCode);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // Reduce run time for the test.
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    conf.setInt("dfs.client.baseTimeWindow.waitOn.BlockMissingException", 10);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");

    conf.set("raid.server.address", "localhost:0");
    // Avoid datanode putting blocks under subdir directory. Corruptblock function
    // can only corrupt blocks under the current directory
    conf.setInt("dfs.datanode.numblocks", 1000);

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    setupStripeStore(conf, fileSys);
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    //Don't allow empty file to be raid
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
  }

  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }
 
  static private DistributedRaidFileSystem getRaidFS(FileSystem fileSys,
      Configuration conf)
      throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    return (DistributedRaidFileSystem)FileSystem.get(dfsUri, clientConf);
  }
  
  static public void corruptBlocksInDirectory(Configuration conf, 
      Path srcDir, long[] crcs, Integer[] listBlockNumToCorrupt,
      FileSystem fileSys, MiniDFSCluster cluster, 
      boolean validate, boolean reportBadBlocks) throws IOException {
    long[] lengths = new long[crcs.length];
    // Get all block Info;
    ArrayList<BlockInfo> blocks = new ArrayList<BlockInfo>();
    List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(conf,
        fileSys, srcDir);
    assertNotNull(lfs);
    for (int fid = 0; fid < lfs.size(); fid++) {
      FileStatus fsStat = lfs.get(fid);
      long numBlock = RaidNode.getNumBlocks(fsStat);
      for (int bid = 0; bid < numBlock; bid++) {
        blocks.add(new BlockInfo(fid, bid));
      }
      lengths[fid] = fsStat.getLen();
    }
    HashSet<Integer> affectedFiles = new HashSet<Integer>();
    HashSet<Integer> affectedBlocks = new HashSet<Integer>();
    // corrupt blocks
    for (int blockNumToCorrupt : listBlockNumToCorrupt) {
      if (blockNumToCorrupt >= blocks.size()) {
        continue;
      }
      BlockInfo bi = null;
      int blockIndex = blockNumToCorrupt;
      if (blockNumToCorrupt < 0) {
        blockIndex = blocks.size() + blockNumToCorrupt;
        if (blockIndex < 0) {
          continue;
        }
      }
      if (affectedBlocks.contains(blockIndex)) {
        continue;
      }
      affectedBlocks.add(blockIndex);
      bi = blocks.get(blockIndex);
      FileStatus srcFileFs = lfs.get(bi.fileIdx);
      Path srcFile = srcFileFs.getPath();
      LOG.info("Corrupt block " + bi.blockId + " of file " + 
        srcFile);
      LocatedBlocks locations = RaidDFSUtil.getBlockLocations(
        (DistributedFileSystem)fileSys, srcFile.toUri().getPath(),
        0L, srcFileFs.getLen());
      TestRaidDfs.corruptBlock(srcFile,
          locations.get(bi.blockId).getBlock(),
          NUM_DATANODES, true, cluster);
      if (reportBadBlocks) {
        cluster.getNameNode().reportBadBlocks(new LocatedBlock[]
            {locations.get(bi.blockId)});
      }
      affectedFiles.add(bi.fileIdx);
    }
    // validate files
    if (validate) {
      DistributedRaidFileSystem raidfs = getRaidFS(fileSys, conf);
      for (Integer fid: affectedFiles) {
        FileStatus stat = lfs.get(fid);
        assertTrue(TestRaidDfs.validateFile(raidfs, stat.getPath(),
            lengths[fid], crcs[fid]));
        // test readFully
        byte[] filebytes = new byte[(int)stat.getLen()];
        FSDataInputStream stm = raidfs.open(stat.getPath());
        stm.readFully(0, filebytes);
        CRC32 crc = new CRC32();
        crc.update(filebytes, 0, filebytes.length);
        assertEquals(crcs[fid], crc.getValue());
      }
    }
  }
  
  /**
   * Create a bunch of files, corrupt blocks in some of them and ensure that
   * corrupted files can be read through DistributedRaidFileSystem
   */
  public void testRaidDirDfs(String code, Integer[][] corrupt)
      throws Exception {
    LOG.info("Test testRaidDirDfs for " + code + " started.");
    stripeLength = 3;
    try {
      mySetup(code, 2);
      for (int i = 0; i < corrupt.length; i++) {
        Path srcDir= new Path("/user/dhruba/dir" + i);
        assert fileSizes.length == blockSizes.length;
        Codec curCodec = codec;
        for (int j = 0; j < fileSizes.length; j++) {
          LOG.info(" Test " + code + " at corrupt scenario " + i + 
              " files scenario " + j);
          assert fileSizes[j].length == blockSizes[j].length;
          long[] crcs = new long[fileSizes[j].length];
          int[] seeds = new int[fileSizes[j].length];
          Path parityDir = new Path(codec.parityDirectory);
          RaidDFSUtil.cleanUp(fileSys, srcDir);
          RaidDFSUtil.cleanUp(fileSys, parityDir);
          TestRaidDfs.createTestFiles(srcDir, fileSizes[j],
            blockSizes[j], crcs, seeds, fileSys, (short)1);
          assertTrue(RaidNode.doRaid(conf,
            fileSys.getFileStatus(srcDir),
            new Path(curCodec.parityDirectory), curCodec,
            new RaidNode.Statistics(),
            RaidUtils.NULL_PROGRESSABLE,
            false, 1, 1));
          corruptBlocksInDirectory(conf, srcDir,
            crcs, corrupt[i], fileSys, dfs, true, false);
          RaidDFSUtil.cleanUp(fileSys, srcDir);
          RaidDFSUtil.cleanUp(fileSys, new Path(curCodec.parityDirectory));
          RaidDFSUtil.cleanUp(fileSys, new Path("/tmp"));
        }
      }
    } catch (Exception e) {
      LOG.info("testRaidDirDfs Exception " + e.getMessage(), e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testRaidDirDfs for " + code + " completed.");
  }
  
  public void testXORRaidDirDfs() throws Exception{
    testRaidDirDfs("xor", 
        new Integer[][]{{0}, {1}, {2}, {-3}, {-2}, {-1}});
  }

  public void testTooManyErrorsDecode() throws Exception {
    LOG.info("testTooManyErrorsDecode start");
    long blockSize = 8192L;
    stripeLength = 3;
    mySetup("xor", 1);
    long[][] fsizes = {{2000L, 3000L, 2000L}, 
                       {blockSize + 1, blockSize + 1},
                       {2*blockSize, blockSize + blockSize/2}};
    long[][] bsizes = {{blockSize, blockSize, blockSize},
                       {blockSize, blockSize},
                       {2*blockSize, blockSize}};
    Integer[][] corrupts = {{0, 1}, {0, 2}, {1, 2}};
    try {
      for (String code: RaidDFSUtil.codes) {
        Codec curCodec = Codec.getCodec(code);
        Path srcDir = new Path("/user/dhruba/" + code);
        for (int i = 0; i < corrupts.length; i++) {
          for (int j = 0; j < fsizes.length; j++) {
            long[] crcs = new long[fsizes[j].length];
            int[] seeds = new int[fsizes[j].length];
            Path parityDir = new Path(codec.parityDirectory);
            RaidDFSUtil.cleanUp(fileSys, srcDir);
            RaidDFSUtil.cleanUp(fileSys, parityDir);
            TestRaidDfs.createTestFiles(srcDir, fsizes[j],
                bsizes[j], crcs, seeds, fileSys, (short)1);
            assertTrue(RaidNode.doRaid(conf,
                fileSys.getFileStatus(srcDir),
                new Path(curCodec.parityDirectory), curCodec,
                new RaidNode.Statistics(),
                RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1));
            boolean expectedExceptionThrown = false;
            try {
              corruptBlocksInDirectory(conf, srcDir,
                  crcs, corrupts[i], fileSys, dfs, true, false);
              // Should not reach.
            } catch (IOException e) {
              LOG.info("Expected exception caught" + e);
              expectedExceptionThrown = true;
            }
            assertTrue(expectedExceptionThrown);
          }
        }
      }
      LOG.info("testTooManyErrorsDecode complete");
    } finally {
      myTearDown();
    }
  }

  public void testTooManyErrorsEncode() throws Exception {
    LOG.info("testTooManyErrorsEncode complete");
    stripeLength = 3;
    mySetup("xor", 1);
    // Encoding should fail when even one block is corrupt.
    Random rand = new Random();
    try {
      for (String code: RaidDFSUtil.codes) {
        Codec curCodec = Codec.getCodec(code);
        Path srcDir = new Path("/user/dhruba/" + code);
        for (int j = 0; j < fileSizes.length; j++) {
          long[] crcs = new long[fileSizes[j].length];
          int[] seeds = new int[fileSizes[j].length];
          Path parityDir = new Path(codec.parityDirectory);
          RaidDFSUtil.cleanUp(fileSys, srcDir);
          RaidDFSUtil.cleanUp(fileSys, parityDir);
          TestRaidDfs.createTestFiles(srcDir, fileSizes[j],
              blockSizes[j], crcs, seeds, fileSys, (short)1);
          corruptBlocksInDirectory(conf, srcDir,
              crcs, new Integer[]{rand.nextInt() % 3},
              fileSys, dfs, false, false);
          boolean expectedExceptionThrown = false;
          try {
            RaidNode.doRaid(conf, fileSys.getFileStatus(srcDir),
                new Path(curCodec.parityDirectory), curCodec,
                new RaidNode.Statistics(),
                RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1);
            // Should not reach.
          } catch (IOException e) {
            LOG.info("Expected exception caught" + e);
            expectedExceptionThrown = true;
          }
          assertTrue(expectedExceptionThrown);
        }
      }
      LOG.info("testTooManyErrorsEncode complete");
    } finally {
      myTearDown();
    }
  }
}

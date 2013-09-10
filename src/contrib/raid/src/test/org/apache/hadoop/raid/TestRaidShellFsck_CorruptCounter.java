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
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.HarIndex;

public class TestRaidShellFsck_CorruptCounter extends TestCase{
  final static Log LOG =
      LogFactory.getLog("org.apache.hadoop.raid.TestRaidShellFsck_CorruptCounter");
  final static String TEST_DIR = 
      new File(System.
          getProperty("test.build.data", "build/contrib/raid/test/data")).
          getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml").
      getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 4;
  final static int STRIPE_BLOCKS = 3; // number of file blocks per stripe
  final static int PARITY_BLOCKS = 3; // number of file blocks per stripe

  final static int FILE_BLOCKS = 6; // number of blocks that file consists of
  final static short REPL = 1; // replication factor before raiding
  final static long BLOCK_SIZE = 8192L; // size of block in byte
  final static String DIR_PATH = "/user/rashmikv/raidtest";
  final static Path FILE_PATH0 =
      new Path("/user/rashmikv/raidtest/file0.test");
  final static Path FILE_PATH1 =
      new Path("/user/rashmikv/raidtest/file1.test");
  final static Path FILE_PATH2 =
      new Path("/user/rashmikv/raidtest/file2.test");
  final static String MONITOR_DIRS = "/,/user";
  /*to test RS:
   * Confirm that codec is set to "rs" in the setUp function
   */
  final static Path RAID_PATH = new Path("/raidrs/user/rashmikv/raidtest");
  final static String RAID_DIR = "/raidrs";
  final static String CODE_USED ="rs";


  final static String HAR_NAME = "raidtest_raid.har";

  Configuration conf = null;
  Configuration raidConf = null;
  Configuration clientConf = null;
  MiniDFSCluster cluster = null;
  DistributedFileSystem dfs = null;
  RaidNode rnode = null;


  RaidShell shell = null;
  String[] args = null;


  /**
   * creates a MiniDFS instance with a raided file in it
   */
  private void setUp(boolean doHar) throws IOException, ClassNotFoundException {
    final int timeBeforeHar;
    if (doHar) {
      timeBeforeHar = 0;
    } else {
      timeBeforeHar = -1;
    }


    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    Utils.loadTestCodecs(conf, 3, 1, 3, "/raid", "/raidrs");

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    // use local block fixer
    /*conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");*/
    conf.set("raid.blockfix.classname", 
        "org.apache.hadoop.raid.DistBlockIntegrityMonitor");

    conf.set("raid.server.address", "localhost:0");
    conf.set("mapred.raid.http.address", "localhost:0");

    conf.setInt("dfs.corruptfilesreturned.max", 500);

    conf.setBoolean("dfs.permissions", false);
    conf.set("raid.corruptfile.counter.dirs", MONITOR_DIRS);
    conf.setInt("raid.corruptfilecount.interval", 1000);

    conf.setLong("raid.blockfix.maxpendingjobs", 1L);


    cluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    cluster.waitActive();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    String namenode = dfs.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);

    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    //Change the codec name: rs of xor here:
    String str =
        "<configuration> " +
            "    <policy name = \"RaidTest1\"> " +
            "      <srcPath prefix=\"" + DIR_PATH + "\"/> " +
            "      <codecId>rs</codecId> " +
            "      <destPath> " + RAID_DIR + " </destPath> " +
            "      <property> " +
            "        <name>targetReplication</name> " +
            "        <value>1</value> " +
            "        <description>after RAIDing, decrease the replication " +
            "factor of a file to this value.</description> " +
            "      </property> " +
            "      <property> " +
            "        <name>metaReplication</name> " +
            "        <value>1</value> " +
            "        <description> replication factor of parity file</description> " +
            "      </property> " +
            "      <property> " +
            "        <name>modTimePeriod</name> " +
            "        <value>2000</value> " +
            "        <description>time (milliseconds) after a file is modified " + 
            "to make it a candidate for RAIDing</description> " +
            "      </property> ";

    if (timeBeforeHar >= 0) {
      str +=
          "      <property> " +
              "        <name>time_before_har</name> " +
              "        <value>" + timeBeforeHar + "</value> " +
              "        <description> amount of time waited before har'ing parity " +
              "files</description> " +
              "     </property> ";
    }

    str +=
        "    </policy>" +
            "</configuration>";

    fileWriter.write(str);
    fileWriter.close();

    createTestFile(FILE_PATH0);
    createTestFile(FILE_PATH1);
    createTestFile(FILE_PATH2);


    Path[] filePaths = { FILE_PATH0, FILE_PATH1, FILE_PATH2  };
    raidTestFiles(RAID_PATH, filePaths, doHar);

    clientConf = new Configuration(raidConf);
    clientConf.set("fs.hdfs.impl",
        "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl",
        "org.apache.hadoop.hdfs.DistributedFileSystem");

    // prepare shell and arguments
    shell = new RaidShell(clientConf);
    args = new String[3];
    args[0] = "-fsck";
    args[1] = DIR_PATH;
    args[2] = "-retNumStrpsMissingBlks";
    
    Runtime runtime = Runtime.getRuntime();
    runtime = spy(runtime);
    doNothing().when(runtime).exit(anyInt());
    FSEditLog.setRuntimeForTesting(runtime);
  }

  /**
   * Creates test file consisting of random data
   */
  private void createTestFile(Path filePath) throws IOException {
    Random rand = new Random();
    FSDataOutputStream stm = dfs.create(filePath, true, 
        conf.getInt("io.file.buffer.size",
            4096), REPL, BLOCK_SIZE);

    final byte[] b = new byte[(int) BLOCK_SIZE];
    for (int i = 0; i < FILE_BLOCKS; i++) {
      rand.nextBytes(b);
      stm.write(b);
    }
    stm.close();
    LOG.info("test file created");

  }

  /**
   * raids test file
   */
  private void raidTestFiles(Path raidPath, Path[] filePaths, boolean doHar)
      throws IOException, ClassNotFoundException {
    // create RaidNode
    raidConf = new Configuration(conf);
    raidConf.setInt(RaidNode.RAID_PARITY_HAR_THRESHOLD_DAYS_KEY, 0);
    raidConf.setInt("raid.blockfix.interval", 1000);

    raidConf.setLong("raid.blockfix.maxpendingjobs", 1L);

    // the RaidNode does the raiding inline (instead of submitting to MR node)
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    rnode = RaidNode.createRaidNode(null, raidConf);

    for (Path filePath: filePaths) {
      long waitStart = System.currentTimeMillis();
      boolean raided = false;

      Path parityFilePath = new Path(RAID_DIR, 
          filePath.toString().substring(1));
      FileStatus fileStat = dfs.getFileStatus(filePath);

      while (!raided) {
        try {
          FileStatus[] listPaths = dfs.listStatus(raidPath);
          if (listPaths != null) {
            if (doHar) {
              // case with HAR
              for (FileStatus f: listPaths) {
                if (f.getPath().toString().endsWith(".har")) {
                  // check if the parity file is in the index
                  final Path indexPath = new Path(f.getPath(), "_index");
                  final FileStatus indexFileStatus = 
                      dfs.getFileStatus(indexPath);
                  final HarIndex harIndex = 
                      new HarIndex(dfs.open(indexPath), indexFileStatus.getLen());
                  final HarIndex.IndexEntry indexEntry =
                      harIndex.findEntryByFileName(parityFilePath.toString());
                  if (indexEntry != null) {
                    LOG.info("raid file " + parityFilePath.toString() + 
                        " found in Har archive: " + 
                        f.getPath().toString() +
                        " ts=" + indexEntry.mtime);
                    raided = true;
                    break;
                  }
                }
              }

            } else {
              // case without HAR
              for (FileStatus f : listPaths) {
                Path found = new Path(f.getPath().toUri().getPath());
                if (parityFilePath.equals(found) ) {
                  ParityFilePair ppair =
                      ParityFilePair.getParityFile(Codec.getCodec(CODE_USED), fileStat, conf);
                  if (ppair != null) {
                    LOG.info("raid file found: " + ppair.getPath());
                    raided = true;
                  }
                  break;
                }
              }
            }
          }
        } catch (FileNotFoundException ignore) {
        }
        if (!raided) {
          if (System.currentTimeMillis() > waitStart + 40000L) {
            LOG.error("parity file not created after 40s");
            throw new IOException("parity file not HARed after 40s");
          } else {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
          }
        }
      }
    }

    rnode.stop();
    rnode.join();
    rnode = null;
    LOG.info("test file raided");    
  }

  /**
   * sleeps for up to 20s until the number of corrupt files 
   * in the file system is equal to the number specified
   */
  private void waitUntilCorruptFileCount(DistributedFileSystem dfs,
      int corruptFiles)
          throws IOException {
    int initialCorruptFiles = DFSUtil.getCorruptFiles(dfs).length;
    long waitStart = System.currentTimeMillis();
    while (DFSUtil.getCorruptFiles(dfs).length != corruptFiles) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {

      }

      if (System.currentTimeMillis() > waitStart + 20000L) {
        break;
      }
    }

    long waited = System.currentTimeMillis() - waitStart;

    int corruptFilesFound = DFSUtil.getCorruptFiles(dfs).length;
    if (corruptFilesFound != corruptFiles) {
      throw new IOException("expected " + corruptFiles + 
          " corrupt files but got " +
          corruptFilesFound);
    }
    LOG.info("Number of corrupted files: " + corruptFiles);
  }

  /**
   * removes a specified block from MiniDFS storage and reports it as corrupt
   */
  private void removeAndReportBlock(DistributedFileSystem blockDfs,
      Path filePath,
      LocatedBlock block) 
          throws IOException {
    TestRaidDfs.corruptBlock(filePath, block.getBlock(), 
        NUM_DATANODES, true, cluster);

    // report deleted block to the name node
    LocatedBlock[] toReport = { block };
    blockDfs.getClient().namenode.reportBadBlocks(toReport);

  }


  /**
   * removes a file block in the specified stripe
   */
  private void removeFileBlock(Path filePath, int stripe, int blockInStripe)
      throws IOException {
    LocatedBlocks fileBlocks = dfs.getClient().namenode.
        getBlockLocations(filePath.toString(), 0, FILE_BLOCKS * BLOCK_SIZE);
    if (fileBlocks.locatedBlockCount() != FILE_BLOCKS) {
      throw new IOException("expected " + FILE_BLOCKS + 
          " file blocks but found " + 
          fileBlocks.locatedBlockCount());
    }
    if (blockInStripe >= STRIPE_BLOCKS) {
      throw new IOException("blockInStripe is " + blockInStripe +
          " but must be smaller than " + STRIPE_BLOCKS);
    }
    LocatedBlock block = fileBlocks.get(stripe * STRIPE_BLOCKS + blockInStripe);
    removeAndReportBlock(dfs, filePath, block);
    LOG.info("removed file " + filePath.toString() + " block " +
        stripe * STRIPE_BLOCKS + " in stripe " + stripe);
  }

  private void removeParityBlock(Path filePath, int stripe) throws IOException {
    removeParityBlock(filePath, stripe, 0);
  }
  /**
   * removes a parity block in the specified stripe
   */
  private void removeParityBlock(Path filePath, int stripe, int blockInStripe) throws IOException {
    FileStatus fileStat = 
        filePath.getFileSystem(raidConf).getFileStatus(filePath);
    // find parity file
    ParityFilePair ppair =
        ParityFilePair.getParityFile(Codec.getCodec(CODE_USED), fileStat, conf);
    // System.err.println("Got the parityFilePair");
    String parityPathStr = ppair.getPath().toUri().getPath();
    // System.err.println("Path to parity"+parityPathStr);
    LOG.info("parity path: " + parityPathStr);
    FileSystem parityFS = ppair.getFileSystem();
    if (!(parityFS instanceof DistributedFileSystem)) {
      throw new IOException("parity file is not on distributed file system");
    }
    DistributedFileSystem parityDFS = (DistributedFileSystem) parityFS;


    // now corrupt the block corresponding to the stripe selected
    FileStatus parityFileStatus =
        parityDFS.getFileStatus(new Path(parityPathStr));
    long parityBlockSize = parityFileStatus.getBlockSize();
    long parityFileLength = parityFileStatus.getLen();
    long parityFileLengthInBlocks = (parityFileLength / parityBlockSize) + 
        (((parityFileLength % parityBlockSize) == 0) ? 0L : 1L);
    if (parityFileLengthInBlocks <= stripe) {
      throw new IOException("selected stripe " + stripe + 
          " but parity file only has " + 
          parityFileLengthInBlocks + " blocks");
    }
    if (parityBlockSize != BLOCK_SIZE) {
      throw new IOException("file block size is " + BLOCK_SIZE + 
          " but parity file block size is " + 
          parityBlockSize);
    }
    LocatedBlocks parityFileBlocks = parityDFS.getClient().namenode.
        getBlockLocations(parityPathStr, 0, parityFileLength);
    if (blockInStripe >= PARITY_BLOCKS) {
      throw new IOException("blockInStripe is " + blockInStripe +
          " but must be smaller than " + PARITY_BLOCKS);
    }
    LocatedBlock parityFileBlock = parityFileBlocks.get(stripe * PARITY_BLOCKS + blockInStripe);
    removeAndReportBlock(parityDFS, new Path(parityPathStr), parityFileBlock);
    LOG.info("removed parity file block/stripe " + stripe + " for " + filePath.toString());

  }

  /**
   * returns the data directories for a data node
   */
  private File[] getDataDirs(int datanode) throws IOException{
    File data_dir = new File(System.getProperty("test.build.data"), 
        "dfs/data/");
    File dir1 = new File(data_dir, "data"+(2 * datanode + 1));
    File dir2 = new File(data_dir, "data"+(2 * datanode + 2));
    if (!(dir1.isDirectory() && dir2.isDirectory())) {
      throw new IOException("data directories not found for data node " + 
          datanode + ": " + dir1.toString() + " " + 
          dir2.toString());
    }

    File[] dirs = new File[2];
    dirs[0] = new File(dir1, "current");
    dirs[1] = new File(dir2, "current");
    return dirs;
  }


  /**
   * checks fsck with no missing blocks
   */
  public void testClean() throws Exception {
    LOG.info("testClean");
    setUp(false);
    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();

    int limit= Codec.getCodec("rs").stripeLength+Codec.getCodec("rs").parityLength;
    long[] result2 = new long[limit];

    for (int i=0; i<limit;i++) {
      result2[i]=shell.getStrpMissingBlks("rs", i);
      assertTrue("New fsck should return 0, but returns " +
          Long.toString(result2[i]), result2[i] == 0);
    }

    assertTrue("fsck should return 0, but returns " +
        Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with missing block in file block but not in parity block
   */
  public void testFileBlockMissing() throws Exception {
    LOG.info("testFileBlockMissing");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 0, 0);

    //corrupting two source blocks in the same stripe
    removeFileBlock(FILE_PATH1, 0, 0);
    removeFileBlock(FILE_PATH1, 0, 2); 

    waitUntilCorruptFileCount(dfs, 2);

    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();

    int limit= Codec.getCodec("rs").stripeLength+Codec.getCodec("rs").parityLength;

    long[] result2 = new long[limit];
    for (int i=0; i<limit;i++) {
      System.err.println("Reading the resulting array");
      result2[i]=shell.getStrpMissingBlks("rs", i);
    }
    assertTrue("Assertion1: New fsck should return 1, but returns " + 
        Long.toString(result2[0]), result2[0] == 1);
    assertTrue("Assertion2: New fsck should return 1, but returns " + 
        Long.toString(result2[1]), result2[1] == 1);

    assertTrue("fsck should return 0, but returns " + 
        Integer.toString(result), result == 0);
  }

  /**
   * 
   * checks new fsck with missing blocks in both parity block and file block
   */
  public void testParityBlockMissing() throws Exception {
    LOG.info("testParityBlockMissing");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    //file0 has one source blocks and one parity blocks missing in stripe 0
    removeFileBlock(FILE_PATH0, 0, 2);
    removeParityBlock(FILE_PATH0, 0);
    //file1 has two source blocks missing in stripe0 and two parity blocks missing in stripe0
    removeFileBlock(FILE_PATH1, 0, 0);
    removeFileBlock(FILE_PATH1, 0, 1);
    removeParityBlock(FILE_PATH1, 0);
    removeParityBlock(FILE_PATH1, 0, 1);

    waitUntilCorruptFileCount(dfs, 4);
    //file2 has two source block missing
    removeFileBlock(FILE_PATH2, 1, 0);
    removeFileBlock(FILE_PATH2, 1, 1);

    assertEquals(0, ToolRunner.run(shell, args));
    int limit= Codec.getCodec("rs").stripeLength+Codec.getCodec("rs").parityLength;

    long[] result2 = new long[limit];
    for (int i=0; i<limit;i++) {
      result2[i]=shell.getStrpMissingBlks("rs", i);
    }
    assertTrue("Assertion1: New fsck should return 0, but returns " + 
        Long.toString(result2[0]), result2[0] == 0);
    assertTrue("Assertion2: New fsck should return 2, but returns " + 
        Long.toString(result2[1]), result2[1] == 2);
    assertTrue("Assertion2: New fsck should return 1, but returns " + 
        Long.toString(result2[3]), result2[3] == 1);

    int result = shell.getCorruptCount();

    assertTrue("fsck should return 0, but returns " +
        Integer.toString(result), result == 1);
  }

  /**
   * checks the new counters added in corruptFileCounter through the Raid node
   * (raid shell fsck is not run explicitly)
   */
  public void testCountersInCorruptFileCounter() 
      throws Exception {
    LOG.info("testCountersInCorruptFileCounter");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0); 
    //file 0 has one source missing (1+0) blocks missing
    removeFileBlock(FILE_PATH0, 1, 0);
    waitUntilCorruptFileCount(dfs, 1);
    //file 1 has three source and one parity block missing = (3+1) blocks missing
    removeFileBlock(FILE_PATH1, 0, 0);
    removeFileBlock(FILE_PATH1, 0, 1);
    removeFileBlock(FILE_PATH1, 0, 2);
    waitUntilCorruptFileCount(dfs, 2);
    removeParityBlock(FILE_PATH1, 0,0);
    waitUntilCorruptFileCount(dfs, 3);

    //File 2 has 3 source blocks and 1 parity blocks missing = (3+2) blocks missing
    removeFileBlock(FILE_PATH2, 1, 0);
    removeFileBlock(FILE_PATH2, 1, 1);
    removeFileBlock(FILE_PATH2, 1, 2);
    waitUntilCorruptFileCount(dfs, 4);

    removeParityBlock(FILE_PATH2, 1, 0);
    removeParityBlock(FILE_PATH2, 1, 2);

    waitUntilCorruptFileCount(dfs, 5);

    Configuration localConf = new Configuration(conf);
    localConf.setBoolean("raid.blockreconstruction.corrupt.disable", true);
    localConf.setInt("raid.blockfix.interval", 3000);
    localConf.set("raid.blockfix.classname", 
        "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);
    localConf.set("raid.corruptfile.counter.dirs", MONITOR_DIRS);
    ///user/rashmikv/raidtest,
    localConf.setInt("raid.corruptfilecount.interval", 2000);
    rnode = RaidNode.createRaidNode(null, localConf);
    Thread.sleep(8000);

    long result = rnode.getNumFilesWithMissingBlks();
    assertTrue("Result should be 3 but returns " +Long.toString(result), result==3);

    long[] result2 = rnode.getNumStrpWithMissingBlksRS();
    LOG.info("verify the missing blocks.");

    assertTrue("Result at index 0 should be 1 but got "+Long.toString(result2[0]), result2[0]==1);
    assertTrue("Result at index 1 should be 0 but got "+Long.toString(result2[1]), result2[1]==0);
    assertTrue("Result at index 2 should be 0 but got "+Long.toString(result2[2]), result2[2]==0);
    assertTrue("Result at index 3 should be 1 but got "+Long.toString(result2[3]), result2[3]==1);
    assertTrue("Result at index 4 should be 1 but got "+Long.toString(result2[4]), result2[4]==1);

    RaidNodeMetrics inst = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
    long result4=inst.numFilesWithMissingBlks.get();
    assertTrue("Number of files with missing blocks should be 3 but got "+Long.toString(result4),result4==3);
    long result5=inst.numStrpsOneMissingBlk.get();
    long result6=inst.numStrpsTwoMissingBlk.get();

    long result7=inst.numStrpsThreeMissingBlk.get();
    long result8=inst.numStrpsFourMissingBlk.get();

    long result9=inst.numStrpsFiveMoreMissingBlk.get();

    assertTrue("result 5 incorrect",result5==1);
    assertTrue("result 6 incorrect",result6==0);
    assertTrue("result 7 incorrect",result7==0);
    assertTrue("result 8 incorrect",result8==1);
    assertTrue("result 9 incorrect",result9==1);

    tearDown();
  }
  
  public void testRaidNodeMetricsNumFilesToFixDropped() 
      throws Exception {
    LOG.info("testRaidNodeMetricsNumFileToFixDropped");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);

    //file 0 has one source missing (1+0) blocks missing
    removeFileBlock(FILE_PATH0, 0, 0);

    waitUntilCorruptFileCount(dfs, 1);
    //file 1 has three source and one parity block missing = (1) blocks missing
    removeFileBlock(FILE_PATH1, 0, 0);
    waitUntilCorruptFileCount(dfs, 2);

    //File 2 has 3 source blocks and 1 parity blocks missing = (3+0) blocks missing
    removeFileBlock(FILE_PATH2, 1, 0);
    removeFileBlock(FILE_PATH2, 1, 1);
    removeFileBlock(FILE_PATH2, 1, 2);

    waitUntilCorruptFileCount(dfs, 3);

    Configuration localConf = new Configuration(conf);
    //disabling corrupt file counter
    localConf.setBoolean("raid.corruptfile.counter.disable", true);
    localConf.setInt(BlockIntegrityMonitor.BLOCKCHECK_INTERVAL, 2000);
    localConf.setLong(
        DistBlockIntegrityMonitor.RAIDNODE_BLOCK_FIX_SUBMISSION_INTERVAL_KEY,
        2000L);
    localConf.set("raid.blockfix.classname", 
        "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);
    localConf.set("raid.corruptfile.counter.dirs", "/user");
    localConf.setInt("raid.corruptfilecount.interval", 2000);
    rnode = RaidNode.createRaidNode(null, localConf);
    LOG.info("Checking Raid Node Metric numFilesToFixDropped") ;
    RaidNodeMetrics inst = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
    long startTime = System.currentTimeMillis();
    long result = 0;
    while (System.currentTimeMillis() - startTime < 60000 && result != 2) {
      result = inst.numFilesToFixDropped.get();
      Thread.sleep(1000);
    }
    LOG.info("Num files to fix dropped in raid node metrics is " + result);
    assertTrue("Number of files to fix dropped with missing blocks should be 2 but got "+Long.toString(result),result==2);
    tearDown();
  }

  @After
  public void tearDown() throws Exception {
    if (rnode != null) {
      rnode.stop();
      rnode.join();
      rnode = null;
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    dfs = null;

    LOG.info("Test cluster shut down");
  }


}


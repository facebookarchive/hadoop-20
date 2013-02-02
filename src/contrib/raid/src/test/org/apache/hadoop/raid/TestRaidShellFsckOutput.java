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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.TestDirectoryParityRegenerator.FakeBlockIntegerityMonitor;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRaidShellFsckOutput {

  final static Log LOG = LogFactory.getLog(TestRaidShellFsckOutput.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml")
      .getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  final static int STRIPE_BLOCKS = 3; // number of blocks per stripe
  final static short repl = 1; // replication factor before raiding
  final static long blockSize = 10240L; // size of block in byte
  final long[] fileSizes = new long[] { 2 * blockSize + blockSize / 2, // block
                                                                       // 0, 1,
                                                                       // 2
      7 * blockSize, // block 3, 4, 5, 6
      blockSize + blockSize / 2 + 1 }; // block 7, 8, 9, 10
  final long[] blockSizes = new long[] { blockSize, 2 * blockSize,
      blockSize / 2 };
  final Integer[] corruptBlockIdxs = new Integer[] { 0, 1, 3, 4, 5, 6, 7, 8, 9, 10};

  Configuration conf = null;
  Configuration raidConf = null;
  MiniDFSCluster cluster = null;
  DistributedFileSystem dfs = null;
  RaidNode rnode = null;

  public Configuration getRaidNodeConfig(Configuration conf) {
    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    // no block fixing here.
    localConf.set("raid.blockfix.classname",
        FakeBlockIntegerityMonitor.class.getName());
    return localConf;
  }

  private Path[] prepareTestData(Path dirPath, Codec codec) throws IOException {
    long[] crcs = new long[fileSizes.length];
    int[] seeds = new int[fileSizes.length];
    Path[] files = TestRaidDfs.createTestFiles(dirPath, fileSizes, blockSizes,
        crcs, seeds, dfs, repl);
    if (codec.isDirRaid) {
      FileStatus stat = dfs.getFileStatus(dirPath);
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, false, repl,
          repl);
      assertTrue(ParityFilePair.parityExists(stat, codec, raidConf));
    } else {
      for (Path file : files) {
        FileStatus stat = dfs.getFileStatus(file);
        RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
            new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, false,
            repl, repl);
        assertTrue(ParityFilePair.parityExists(stat, codec, raidConf));
      }
    }
    return files;
  }

  public BufferedReader runFsck(String dir, boolean listRecoverableFiles) 
      throws Exception {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(bout, true);
      RaidShell shell = new RaidShell(raidConf, ps);
      int res = ToolRunner.run(shell, 
          listRecoverableFiles ? 
          new String[] { "-fsck", dir, "-listrecoverablefiles"}
          : new String[] { "-fsck", dir});
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      shell.close();
      return new BufferedReader(new InputStreamReader(bin));
    } catch (Exception e) {
    }
    return null;
  }

  @Test
  public void testListRecoverableFiles() throws Exception {
    // raid some rs data
    Codec rsCodec = Codec.getCodec("rs");
    Path rsParent = new Path("/user/dikang/testraidrs");
    Path[] rsFiles = prepareTestData(rsParent, rsCodec);

    // raid some dir rs data
    Codec dirRsCodec = Codec.getCodec("dir-rs");
    Path dirRsParent = new Path("/user/dikang/testraiddirrs");
    Path[] dirRsFiles = prepareTestData(dirRsParent, dirRsCodec);

    BufferedReader reader = runFsck(rsParent.toUri().getPath(), true);
    assertNull("shoud have no corrupted files", reader.readLine());
    
    reader = runFsck(dirRsParent.toUri().getPath(), true);
    assertNull("shoud have no corrupted files", reader.readLine());
    
    // corrupt files
    TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, rsParent,
        new long[rsFiles.length], corruptBlockIdxs, dfs, cluster, false, true);
    
    TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirRsParent,
        new long[dirRsFiles.length], corruptBlockIdxs, dfs, cluster, false, true);
    
    int count = 0;
    String line;
    reader = runFsck(rsParent.toUri().getPath(), true);
    while ((line = reader.readLine()) != null) {
      LOG.info("Fsck Output: " + line);
      count ++;
    }
    
    reader = runFsck(dirRsParent.toUri().getPath(), true);
    while ((line = reader.readLine()) != null) {
      LOG.info("Fsck Output: " + line);
      count ++;
    }
    
    assertEquals("Should have 2 recoverable corrupted files", 2, count);
    
    count = 0;
    reader = runFsck(rsParent.toUri().getPath(), false);
    while ((line = reader.readLine()) != null) {
      LOG.info("Fsck Output: " + line);
      count ++;
    }
    
    reader = runFsck(dirRsParent.toUri().getPath(), false);
    while ((line = reader.readLine()) != null) {
      LOG.info("Fsck Output: " + line);
      count ++;
    }
    
    assertEquals("Should have 4 un-recoverable corrupted files", 4, count);
  }

  @Before
  public void setUp() throws IOException {
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    Utils.loadAllTestCodecs(conf, STRIPE_BLOCKS, 1, 2, "/destraid",
        "/destraidrs", "/destraiddir", "/destraiddirrs");

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    // use local block fixer
    conf.set("raid.blockfix.classname",
        "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");

    conf.set("raid.server.address", "localhost:0");
    conf.set("mapred.raid.http.address", "localhost:0");

    conf.setInt("dfs.corruptfilesreturned.max", 500);

    conf.setBoolean("dfs.permissions", false);

    cluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    cluster.waitActive();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    String namenode = dfs.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
    TestDirectoryRaidDfs.setupStripeStore(conf, dfs);
    raidConf = getRaidNodeConfig(conf);
  }

  @After
  public void tearDown() throws IOException {
    if (dfs != null) {
      dfs.close();
    }
    
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}

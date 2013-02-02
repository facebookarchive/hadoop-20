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
package org.apache.hadoop.raid.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.MissingParityFiles;
import org.apache.hadoop.raid.RaidShell;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.TestDirectoryRaidDfs;
import org.apache.hadoop.raid.TestRaidNode;
import org.apache.hadoop.mapred.MiniMRCluster;

public class TestParityVerifier extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestParityVerifier");
  final static Random rand = new Random();
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  Path root = null;
  Codec xorCodec = null;
  Codec dirxorCodec = null;
  List<FileStatus> xorBadFiles = null;
  List<FileStatus> dirxorBadFiles = null;

  private void createClusters() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    TestRaidNode.loadTestCodecs(conf, 3, 3, 1, 4);
    dfs = new MiniDFSCluster(conf, 3, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    //Don't allow empty file to be raid
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
    xorCodec = Codec.getCodec("xor");
    dirxorCodec = Codec.getCodec("dir-xor");
    
    Path srcDir = null;
    for (int j = 0; j < 2; j++) {
      srcDir = new Path("/dirxor/" + j);
      for (int i = 0 ;i < 5; i++) {
        Path newFile = new Path(srcDir, Integer.toString(i));
        TestRaidDfs.createTestFile(fileSys, newFile, 3, 1, 4096L);
      }
      RaidNode.doRaid(conf, fileSys.getFileStatus(srcDir),
          new Path(dirxorCodec.parityDirectory), dirxorCodec,
          new RaidNode.Statistics(),
          RaidUtils.NULL_PROGRESSABLE,
          false, 2, 2);
    }
    LOG.info("Reduce replication of odd number files under /dirxor/1 to 1");
    dirxorBadFiles = new ArrayList<FileStatus>();
    for (int i = 1; i<5; i+=2) {
      Path newFile = new Path(new Path("/dirxor/1"), Integer.toString(i));
      fileSys.setReplication(newFile, (short)1);
      dirxorBadFiles.add(fileSys.getFileStatus(newFile));
    }
    srcDir = new Path("/xor/3");
    for (int i = 0; i < 2; i++) {
      Path newFile = new Path(srcDir, Integer.toString(i));
      TestRaidDfs.createTestFile(fileSys, newFile,
          3, 3, 4096L);
      RaidNode.doRaid(conf, fileSys.getFileStatus(newFile), 
          new Path(xorCodec.parityDirectory), xorCodec, 
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, 
          false, 2, 2);
    }
    xorBadFiles = new ArrayList<FileStatus>();
    LOG.info("Reduce replication of file /xor/3/1 to 1");
    Path newFile = new Path(srcDir, Integer.toString(1));
    fileSys.setReplication(newFile, (short)1);
    xorBadFiles.add(fileSys.getFileStatus(newFile));
  }
  
  Set<String> getBadFiles(Codec codec) throws IOException {
    ParityVerifier pv = new ParityVerifier(conf, false, 
        2, codec);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bout, true);
    pv.verifyParities(new Path(codec.parityDirectory),
        ps);
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    BufferedReader r = new BufferedReader(new InputStreamReader(bin));
    String l = null;
    Set<String> badFiles = new HashSet<String>();
    while ((l = r.readLine()) != null) {
      badFiles.add(l);
    }
    return badFiles;
  }
  
  /**
   * Return number of files which doesn't match the replication
   */
  public int verifyRepl(List<FileStatus> files, short repl) throws IOException {
    int total = 0;
    for (FileStatus stat : files) {
      if (repl != stat.getReplication()) {
        total++;
      }
    }
    return total;
  }

  /**
   * Test whether source files have low replication are found
   */
  public void testBadParityFilesDetection() throws Exception {
    try {
      System.out.println("Start testParityVerifier");
      createClusters();      
      Set<String> badFiles = getBadFiles(xorCodec);
      assertEquals("only one xor parity is expected", 1, badFiles.size());
      assertEquals("/xor/3/1's parity should be bad",
          xorCodec.parityDirectory + "/xor/3/1", badFiles.iterator().next());
      verifyRepl(xorBadFiles, (short)1);
      badFiles = getBadFiles(dirxorCodec);
      assertEquals("only one dir xor parity is expected", 1, badFiles.size());
      assertEquals("/dirxor/1's parity should be bad",
          dirxorCodec.parityDirectory + "/dirxor/1", badFiles.iterator().next());
      verifyRepl(dirxorBadFiles, (short)1);
    } catch (Exception e) {
      LOG.info("Test testBadParityFilesDetection Exception ", e);
      throw e;
    } finally {
      dfs.shutdown();
    }
  }

  public void testBadParityFilesRestore() throws Exception {
    try {
      System.out.println("Start testBadParityFilesRestore");
      createClusters();
      FileStatus[] files = fileSys.listStatus(new Path("/xor/3"));
      List<FileStatus> lfs = Arrays.asList(files);
      assertEquals(xorBadFiles.size(), verifyRepl(lfs, (short)2));
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[5];
      args[0] = "-verifyParity";
      args[1] = "-repl";
      args[2] = "2";
      args[3] = "-restore";
      args[4] = xorCodec.parityDirectory;
      ToolRunner.run(shell, args);
      files = fileSys.listStatus(new Path("/xor/3"));
      lfs = Arrays.asList(files);
      assertEquals(0, verifyRepl(lfs, (short)2));
      
      args[4] = dirxorCodec.parityDirectory;
      files = fileSys.globStatus(new Path("/dirxor/*/*"));
      lfs = Arrays.asList(files);
      assertEquals(dirxorBadFiles.size(), verifyRepl(lfs, (short)2));
      ToolRunner.run(shell, args);
      files = fileSys.globStatus(new Path("/dirxor/*/*"));
      lfs = Arrays.asList(files);
      assertEquals(0, verifyRepl(lfs, (short)2));
      
    } catch (Exception e) {
      LOG.info("Test testBadParityFilesRestore Exception ", e);
      throw e;
    } finally {
      dfs.shutdown();
    }
  }
}
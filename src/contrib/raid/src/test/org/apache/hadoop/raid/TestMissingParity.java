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
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.MissingParityFiles;
import org.apache.hadoop.raid.RaidShell;
import org.apache.hadoop.mapred.MiniMRCluster;

public class TestMissingParity extends TestCase {
  
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");
  final static Random rand = new Random();

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;
  Path root = null;
  Set<String> allExpectedMissingFiles = null;

  private void createClusters(boolean local) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    Utils.loadTestCodecs(conf);

    // scan all policies once every 100 second
    conf.setLong("raid.policy.rescan.interval", 100 * 1000L);

    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    if (local) {
      conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    } else {
      conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    }

    // use local block fixer
    conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");

    conf.set("raid.server.address", "localhost:0");

    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;
    final int jobTrackerPort = 60050;

    dfs = new MiniDFSCluster(conf, 6, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();

    Path raidRoot = new Path(Codec.getCodec("xor").parityDirectory);
    root = raidRoot.getParent();
    String file1 = "/p1/f1.txt";
    String file2 = "/p1/f2.txt";
    String file3 = "/p2/f3.txt";
    String file4 = "/p2/f4.txt";
    Path fPath1 = new Path(root + file1);
    Path fPath2 = new Path(root + file2);
    Path fPath3 = new Path(root + file3);
    Path fPath4 = new Path(root + file4);
    Path rPath3 = new Path(raidRoot + file3);
    allExpectedMissingFiles = new HashSet<String>();
    allExpectedMissingFiles.add(fPath2.toUri().getPath());
    allExpectedMissingFiles.add(fPath3.toUri().getPath());
    allExpectedMissingFiles.add(fPath4.toUri().getPath());
    fileSys.create(fPath1, (short)3);
    fileSys.create(fPath2, (short)2);
    fileSys.create(fPath3, (short)2);
    fileSys.create(fPath4, (short)2);
    fileSys.create(rPath3, (short)2);
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
  }

  /**
   * Test whether MissingParityFiles selects the correct files
   * The files selected should be  /p1/f2.txt /p2/f3.txt and  /p2/f4.txt
   */
  public void testMissingRaidFiles() throws Exception {
    try {
      System.out.println("Test unraided files with < 3 replication");
      createClusters(true);
      MissingParityFiles mf1 = new MissingParityFiles(conf);
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(bout, true);
      mf1.findMissingParityFiles(root, ps);
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      BufferedReader r = new BufferedReader(new InputStreamReader(bin));
      String l = null;
      Set<String> missingFiles = new HashSet<String>();
      while ((l = r.readLine()) != null) {
        missingFiles.add(l);
      }
      System.out.println("Missing files count = " + missingFiles.size());
      assertEquals(allExpectedMissingFiles, missingFiles);
    } catch (Exception e) {
      LOG.info("Test testMissingRaidFiles Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      dfs.shutdown();
      mr.shutdown();
    }
  }

  /**
   * Test whether the raid shell is correct integrated with MissingParityFiles
   * Again the files selected should be /p1/f2.txt /p2/f3.txt and /p2/f4.txt
   */
  public void testRaidShell() throws Exception {
    try {
      System.out.println("Test shell unraided files with < 3 replication");
      createClusters(true);
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[2];
      args[0] = "-findMissingParityFiles";
      args[1] = root.toString();
      ToolRunner.run(shell, args);
    } catch (Exception e) {
      LOG.info("Test testRaidShell Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      dfs.shutdown();
      mr.shutdown();
    }
  }
}


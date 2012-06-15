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

import java.io.IOException;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;

public class TestAvatarFailover {
  final static Log LOG = LogFactory.getLog(TestAvatarFailover.class);

  private static final String FILE_PATH = "/dir1/dir2/myfile";
  private static final long FILE_LEN = 512L * 1024L;

  private Configuration conf;
  private MiniAvatarCluster cluster;
  private DistributedAvatarFileSystem dafs;
  private Path path;

  @BeforeClass
  public static void setUpStatic() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean federation) throws Exception {
    conf = new Configuration();
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null);
      dafs = cluster.getFileSystem();
    } else {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null, 1, true);
      dafs = cluster.getFileSystem(0);
    }
    
    path = new Path(FILE_PATH);    
    DFSTestUtil.createFile(dafs, path, FILE_LEN, (short)1, 0L);
  }

  /**
   * Test if we can get block locations after killing the primary avatar
   * and failing over to the standby avatar.
   */
  @Test
  public void testFailOver() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary");
    cluster.killPrimary();
    LOG.info("failing over");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testFailOverWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary");
    cluster.killPrimary(0);
    LOG.info("failing over");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  /**
   * Test if we can get block locations after killing the standby avatar.
   */
  @Test
  public void testKillStandby() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testKillStandbyWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  /**
   * Test if we can kill and resurrect the standby avatar and then do
   * a failover.
   */
  @Test
  public void testResurrectStandbyFailOver() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby();
    LOG.info("restarting standby");
    cluster.restartStandby();

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary");
    cluster.killPrimary();
    LOG.info("failing over");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testResurrectStandbyFailOverWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby(0);
    LOG.info("restarting standby");
    cluster.restartStandby(0);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary");
    cluster.killPrimary(0);
    LOG.info("failing over");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }


  /**
   * Test if we can get block locations after killing primary avatar,
   * failing over to standby avatar (making it the new primary),
   * restarting a new standby avatar, killing the new primary avatar and
   * failing over to the restarted standby.
   */
  @Test
  public void testDoubleFailOver() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary 1");
    cluster.killPrimary();
    LOG.info("failing over 1");
    cluster.failOver();
    LOG.info("restarting standby");
    cluster.restartStandby();

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary 2");
    cluster.killPrimary();
    LOG.info("failing over 2");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testDoubleFailOverWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary 1");
    cluster.killPrimary(0);
    LOG.info("failing over 1");
    cluster.failOver(0);
    LOG.info("restarting standby");
    cluster.restartStandby(0);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary 2");
    cluster.killPrimary(0);
    LOG.info("failing over 2");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  @After
  public void shutDown() throws Exception {
    dafs.close();
    cluster.shutDown();
  }

  @AfterClass
  public static void shutDownStatic() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }


  static int blocksInFile(FileSystem fs, Path path, long len) 
    throws IOException {
    FileStatus f = fs.getFileStatus(path);
    return fs.getFileBlockLocations(f, 0L, len).length;
  }

  private int blocksInFile() throws IOException {
    return blocksInFile(dafs, path, FILE_LEN);
  }

}
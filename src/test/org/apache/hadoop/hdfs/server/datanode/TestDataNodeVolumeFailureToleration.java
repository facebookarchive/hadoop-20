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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the ability of a DN to tolerate volume failures.
 */
public class TestDataNodeVolumeFailureToleration {

  private static final Log LOG = LogFactory.getLog(TestDataNodeVolumeFailureToleration.class);
  {
    ((Log4JLogger)TestDataNodeVolumeFailureToleration.LOG).getLogger().setLevel(Level.ALL);
  }

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;
  private String dataDir;
  private int dirId;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong("dfs.block.size", 512);
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 1);
    /*
     * Lower the DN heartbeat, DF rate, and recheck interval to one second
     * so state about failures and datanode death propagates faster.
     */
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.df.interval", 1000);
    conf.setInt("heartbeat.recheck.interval", 1000);
    
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dataDir = cluster.getDataDirectory();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  /**
   * Test the dfs.datanode.failed.volumes.tolerated configuration
   * option, ie the DN tolerates a failed-to-use scenario during
   * its start-up.
   */
  @Test
  public void testValidVolumesAtStartup() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    // Make sure no DNs are running.
    cluster.shutdownDataNodes();
    
    File dataDir1 = new File(cluster.getBaseDataDir(), "data1");
    dataDir1.mkdirs();
    
    File dataDir2 = new File(cluster.getBaseDataDir(), "data2");
    dataDir2.mkdirs();
    
    try {
      // Bring up a datanode with two default data dirs, but with one bad one.
      assertEquals("Couldn't chmod local vol", 0,
          FileUtil.chmod(dataDir1.toString(), "000"));
      conf.setInt("dfs.datanode.failed.volumes.tolerated", 1);

      // Start DN with manually managed DN dir
      String dfsDir = dataDir2.getPath() + "," + dataDir1.getPath();
      conf.set("dfs.data.dir", dfsDir);
      cluster.startDataNodes(conf, 1, false, null, null);
      cluster.waitActive();
      
      assertTrue("The DN should have started up fine.",
          cluster.isDataNodeUp());
      DataNode dn = cluster.getDataNodes().get(0);
      String si = dn.getFSDataset().getStorageInfo();
      assertFalse("The DN shouldn't have a bad directory.",
          si.contains(dataDir1.getPath()));
      assertTrue("The DN should have started with this directory",
          si.contains(dataDir2.getPath()));
    } finally {
      FileUtil.chmod(dataDir1.toString(), "755");
    }
  }

  /**
   * Test the dfs.datanode.failed.volumes.tolerated configuration
   * option, ie the DN shuts itself down when the number of failures
   * experienced drops below the tolerated amount.
   */
  @Test
  public void testConfigureMinValidVolumes() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Bring up two additional datanodes that need both of their volumes
    // functioning in order to stay up
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 0);
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    // Fail a volume on the 2nd DN
    File dn2Vol1 = new File(new File(dataDir, "data"+(2*1+1)), "current");
    try {
      assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(false));

      // Should only get two replicas (the first DN and the 3rd)
      Path file1 = new Path("/test1");
      DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
      DFSTestUtil.waitReplication(fs, file1, (short)2);
    
      DFSTestUtil.waitNSecond(2);
      assertFalse("2nd DN should be dead", cluster.getDataNodes().get(1).isDatanodeUp());
    
      // If we restore the volume we should still only be able to get
      // two replicas since the DN is still considered dead.
      assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(true));
      Path file2 = new Path("/test2");
      DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
      DFSTestUtil.waitReplication(fs, file2, (short)2);

      DFSTestUtil.waitNSecond(2);
      assertFalse("2nd DN should be dead", cluster.getDataNodes().get(1).isDatanodeUp());
    }
    finally {
      dn2Vol1.setExecutable(true);
    }
  }

  /** 
   * Restart the datanodes with a new volume tolerated value.
   * @param volTolerated number of dfs data dir failures to tolerate
   * @param manageDfsDirs whether the mini cluster should manage data dirs
   * @throws IOException
   */
  private void restartDatanodes(int volTolerated, boolean manageDfsDirs)
      throws IOException {
    // Make sure no datanode is running
    cluster.shutdownDataNodes();
    conf.setInt("dfs.datanode.failed.volumes.tolerated", volTolerated);
    cluster.startDataNodes(conf, 1, manageDfsDirs, null, null);
    cluster.waitActive();
  }

  /**
   * Test for different combination of volume configs and volumes tolerated 
   * values.
   */
  @Test
  public void testVolumeAndTolerableConfiguration() throws Exception {
    dirId = 3;
    // Check if Block Pool Service exit for an invalid conf value.
    testVolumeConfig(-1, 0, false);
    // Ditto if the value is too big.
    testVolumeConfig(100, 0, false);

    // Test for one failed volume with 1 tolerable volume
    testVolumeConfig(1, 1, true);
    // Test all good volumes
    testVolumeConfig(0, 0, true);

    // Test for one failed volume
    testVolumeConfig(0, 1, false);
    // Test all failed volumes
    testVolumeConfig(0, 2, false);
  }

  /**
   * Tests for a given volumes to be tolerated and volumes failed.
   */
  private void testVolumeConfig(int volumesTolerated, int volumesFailed,
      boolean expectedServiceState)
      throws IOException, InterruptedException, TimeoutException {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    
    File[] dirs = {
      new File(dataDir, "data" + (dirId++)),
      new File(dataDir, "data" + (dirId++))};
    for (File dir: dirs) {
      dir.mkdirs();
    }
    String dfsDir = dirs[0].getPath() + "," + dirs[1].getPath();
    conf.set("dfs.data.dir", dfsDir);

    try {
      for (int i = 0; i < volumesFailed; i++) {
        prepareDirToFail(dirs[i]);
      }
      try {
        restartDatanodes(volumesTolerated, false);
      }
      catch (IOException e) {
        LOG.error(e.toString());
      }
      
      if (expectedServiceState) {
        assertTrue(cluster.isDataNodeUp() && cluster.getDataNodes().get(0).isDatanodeUp());
      } else {
        assertTrue(!cluster.isDataNodeUp() || !cluster.getDataNodes().get(0).isDatanodeUp());
      }
    } finally {
      for (File dir : dirs) {
        FileUtil.chmod(dir.toString(), "755");
      }
    }
  }

  /** 
   * Prepare directories for a failure, set dir permission to 000
   * @param dir
   * @throws IOException
   * @throws InterruptedException
   */
  private void prepareDirToFail(File dir) throws IOException,
      InterruptedException {
    dir.mkdirs();
    assertEquals("Couldn't chmod local vol", 0,
        FileUtil.chmod(dir.toString(), "000"));
  }
}

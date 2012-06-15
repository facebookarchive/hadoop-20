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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.apache.hadoop.hdfs.MiniAvatarCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarDataNodeMultipleRegistrations {
  private static final Log LOG = 
    LogFactory.getLog(TestAvatarDataNodeMultipleRegistrations.class);
  Configuration conf;
  boolean simulatedStorage = false;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    // populate repl queues on standby (in safe mode)
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0f);
    conf.setLong("fs.avatar.standbyfs.initinterval", 1000);
    conf.setLong("fs.avatar.standbyfs.checkinterval", 1000);
  }

  /**
   * start multiple NNs and single DN and verifies per BP registrations and
   * handshakes.
   * 
   * @throws IOException
   */
  @Test
  public void test2NNRegistration() 
      throws IOException, ConfigException, InterruptedException{
    MiniAvatarCluster cluster = new MiniAvatarCluster(conf, 1, true, null, null, 2, true); 
    try {
      NameNodeInfo nn1 = cluster.getNameNode(0);
      NameNodeInfo nn2 = cluster.getNameNode(1);
      AvatarNode nn1zero = nn1.avatars.get(0).avatar;
      AvatarNode nn1one = nn1.avatars.get(1).avatar;
      AvatarNode nn2zero = nn2.avatars.get(0).avatar;
      AvatarNode nn2one = nn2.avatars.get(1).avatar;
      assertNotNull("cannot create nn1 avatar 0", nn1zero);
      assertNotNull("cannot create nn1 avatar 1", nn1one);
      assertNotNull("cannot create nn2 avatar 0", nn2zero);
      assertNotNull("cannot create nn2 avatar 1", nn2one);
      
      int ns1zero = nn1zero.getNamespaceID();
      int ns1one = nn1one.getNamespaceID();
      assertEquals("namespace ids for namenode 1 should be the same",
          ns1zero, ns1one);
      int ns2zero = nn2zero.getNamespaceID();
      int ns2one = nn2one.getNamespaceID();
      assertEquals("namespace ids for namenode 2 should be the same",
          ns2zero, ns2one);
      int lv1zero = nn1zero.getFSImage().getLayoutVersion();
      int lv1one = nn1one.getFSImage().getLayoutVersion();
      int lv2zero = nn2zero.getFSImage().getLayoutVersion();
      int lv2one = nn2one.getFSImage().getLayoutVersion();
      assertNotSame("namespace ids should be different", ns1zero, ns2zero);
      LOG.info("nn1zero: lv=" + lv1zero + ";uri=" + nn1zero.getNameNodeAddress());
      LOG.info("nn1one: lv=" + lv1one + ";uri=" + nn1one.getNameNodeAddress());
      LOG.info("nn2zero: lv=" + lv2zero + ";uri=" + nn2zero.getNameNodeAddress());
      LOG.info("nn2one: lv=" + lv2one + ";uri=" + nn2one.getNameNodeAddress());

      // check number of volumes in fsdataset
      AvatarDataNode dn = cluster.getDataNodes().get(0);
      
      for (NamespaceService nsos : dn.getAllNamespaceServices()) {
        LOG.info("reg: nsid =" + nsos.getNamespaceId() + "; name="
            + nsos.getNsRegistration().name + "; sid="
            + nsos.getNsRegistration().storageID + "; nna="
            + nsos.getNNSocketAddress());
      }

      NamespaceService nsos1 = dn.getAllNamespaceServices()[0];
      NamespaceService nsos2 = dn.getAllNamespaceServices()[1];

      // The order of bpos is not guaranteed, so fix the order
      if (nsos1.getNNSocketAddress().equals(nn2zero.getNameNodeDNAddress())) {
        NamespaceService tmp = nsos1;
        nsos1 = nsos2;
        nsos2 = tmp;
      }

      assertEquals("wrong nn address", nsos1.getNNSocketAddress(),
          nn1zero.getNameNodeDNAddress());
      assertEquals("wrong nn address", nsos2.getNNSocketAddress(),
          nn2zero.getNameNodeDNAddress());
      assertEquals("wrong nsid", nsos1.getNamespaceId(), ns1zero);
      assertEquals("wrong nsid", nsos2.getNamespaceId(), ns2zero);
    } finally {
      cluster.shutDown();
    }
  }

  /**
   * starts single nn and single dn and verifies registration and handshake
   * 
   * @throws IOException
   */
  @Test
  public void testFedSingleNN() 
      throws IOException, ConfigException, InterruptedException {
    MiniAvatarCluster cluster = new MiniAvatarCluster(conf, 1, true, null, null, 1, true); 
    try {
      NameNodeInfo nn1 = cluster.getNameNode(0);
      AvatarNode nn1zero = nn1.avatars.get(0).avatar;
      AvatarNode nn1one = nn1.avatars.get(1).avatar;
      assertNotNull("cannot create nn1 zero", nn1zero);
      assertNotNull("cannot create nn1 one", nn1one);

      int nsid1zero = nn1zero.getNamespaceID();
      int nsid1one = nn1one.getNamespaceID();
      assertEquals("namespace ids for namenode 1 should be the same",
          nsid1zero, nsid1one);
      int lv1zero = nn1zero.getFSImage().getLayoutVersion();
      int lv1one = nn1one.getFSImage().getLayoutVersion();
      LOG.info("nn1: lv=" + lv1zero + ";nsid=" + nsid1zero + ";uri="
          + nn1zero.getNameNodeAddress());
      LOG.info("nn1: lv=" + lv1one + ";nsid=" + nsid1one + ";uri="
          + nn1one.getNameNodeAddress());

      // check number of vlumes in fsdataset
      AvatarDataNode dn = cluster.getDataNodes().get(0);
      for (NamespaceService nsos : dn.getAllNamespaceServices()) {
        LOG.info("reg: nsid =" + nsos.getNamespaceId() + "; name="
            + nsos.getNsRegistration().name + "; sid="
            + nsos.getNsRegistration().storageID + "; nna="
            + nsos.getNNSocketAddress());
      }
      // try block report
      NamespaceService nsos1 = dn.getAllNamespaceServices()[0];
      nsos1.scheduleBlockReport(0);

      assertEquals("wrong nn address", nsos1.getNNSocketAddress(),
          nn1zero.getNameNodeDNAddress());
      assertEquals("wrong nsid", nsos1.getNamespaceId(), nsid1zero);
      cluster.shutDown();
      
      // Ensure all the BPOfferService threads are shutdown
      assertEquals(0, dn.getAllNamespaceServices().length);
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutDown();
      }
    }
  }
  
  @Test
  public void testMiniAvatarClusterWithMultipleNN()
      throws IOException, ConfigException, InterruptedException {
    Configuration conf = new Configuration();
    // start Federated cluster and add a node.
    MiniAvatarCluster cluster = new MiniAvatarCluster(conf, 1, true, null, null, 2, true); 
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(1)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    
    // add a node
    cluster.addNameNode(conf);
    Assert.assertEquals("(1)Should be 3 namenodes", 3, cluster.getNumNameNodes());
    cluster.shutDown();
        
    // 2. start with Federation flag set
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 1, true, null, null, 1, true);
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    
    // add a node
    cluster.addNameNode(conf);   
    Assert.assertEquals("(2)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    cluster.shutDown();

    // 3. start non-federated
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 1, true, null, null);
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    
    // add a node
    try {
      cluster.addNameNode(conf);
      Assert.fail("shouldn't be able to add another NN to non federated cluster");
    } catch (IOException e) {
      // correct 
      Assert.assertTrue(e.getMessage().startsWith("cannot add namenode"));
      Assert.assertEquals("(3)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    } finally {
      cluster.shutDown();
    }
  }
  
  /**
   * Test that file data becomes available before file is closed.
   */
  @Test
  public void testFileCreation() 
      throws IOException, ConfigException, InterruptedException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniAvatarCluster cluster = new MiniAvatarCluster(conf, 1, true, null, null, 3, true);
    try {

      for (int i = 0; i < cluster.getNumNameNodes(); i++) {
        FileSystem fs = cluster.getFileSystem(i);
        //
        // check that / exists
        //
        Path path = new Path("/");
        System.out.println("Path : \"" + path.toString() + "\"");
        System.out.println(fs.getFileStatus(path).isDir()); 
        assertTrue("/ should be a directory", 
                   fs.getFileStatus(path).isDir() == true);

        //
        // Create a directory inside /, then try to overwrite it
        //
        Path dir1 = new Path("/test_dir");
        fs.mkdirs(dir1);
        System.out.println("createFile: Creating " + dir1.getName() + 
          " for overwrite of existing directory.");
        try {
          fs.create(dir1, true); // Create path, overwrite=true
          fs.close();
          assertTrue("Did not prevent directory from being overwritten.", false);
        } catch (IOException ie) {
          if (!ie.getMessage().contains("already exists as a directory."))
            throw ie;
        }
      
        // create a new file in home directory. Do not close it.
        //
        Path file1 = new Path("filestatus.dat");
        FSDataOutputStream stm = TestFileCreation.createFile(fs, file1, 1);

        // verify that file exists in FS namespace
        assertTrue(file1 + " should be a file", 
                    fs.getFileStatus(file1).isDir() == false);
        System.out.println("Path : \"" + file1 + "\"");

        // write to file
        TestFileCreation.writeFile(stm);

        // Make sure a client can read it before it is closed.
        checkFile(fs, file1, 1);

        // verify that file size has changed
        long len = fs.getFileStatus(file1).getLen();
        assertTrue(file1 + " should be of size " + (numBlocks * blockSize) +
                   " but found to be of size " + len, 
                    len == numBlocks * blockSize);

        stm.close();

        // verify that file size has changed to the full size
        len = fs.getFileStatus(file1).getLen();
        assertTrue(file1 + " should be of size " + fileSize +
                   " but found to be of size " + len, 
                    len == fileSize);
     
        // Check storage usage 
        // can't check capacities for real storage since the OS file system may be changing under us.
        if (simulatedStorage) {
          AvatarDataNode dn = cluster.getDataNodes().get(0);
          int namespaceId = cluster.getNameNode(i).avatars.get(0).avatar.getNamespaceID();
          assertEquals(fileSize, dn.getFSDataset().getNSUsed(namespaceId));
          //Because all namespaces share the same simulated dataset
          assertEquals(SimulatedFSDataset.DEFAULT_CAPACITY-fileSize*(i+1), 
              dn.getFSDataset().getRemaining());
        }
      }
    } finally {
      cluster.shutDown();
    }
  }
  
  /**
   * Test that file data becomes available before file is closed.
   */
  @Test
  public void testFileCreationSimulated() 
      throws IOException, ConfigException, InterruptedException {
    simulatedStorage = true;
    testFileCreation();
    simulatedStorage = false;
  }
  
  //
  // verify that the data written to the full blocks are sane
  // 
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
          fileSys.getFileStatus(name), 0, fileSize);
      if (locations.length < numBlocks) {
        done = false;
        continue;
      }
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].getHosts().length < repl) {
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    final byte[] expected;
    if (simulatedStorage) {
      expected = new byte[numBlocks * blockSize];
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      expected = AppendTestUtil.randomBytes(seed, numBlocks*blockSize);
    }
    // do a sanity check. Read the file
    byte[] actual = new byte[numBlocks * blockSize];
    stm.readFully(0, actual);
    stm.close();
    checkData(actual, 0, expected, "Read 1");
  }
  
  static private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }
  
  @AfterClass
  public static void shutDownClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
}

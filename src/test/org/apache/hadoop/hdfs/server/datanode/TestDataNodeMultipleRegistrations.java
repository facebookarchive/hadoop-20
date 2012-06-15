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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeMultipleRegistrations {
  private static final Log LOG = 
    LogFactory.getLog(TestDataNodeMultipleRegistrations.class);
  Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  /**
   * start multiple NNs and single DN and verifies per BP registrations and
   * handshakes.
   * 
   * @throws IOException
   */
  @Test
  public void test2NNRegistration() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 1, true, null, 2);
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);

      int lv1 = nn1.getFSImage().getLayoutVersion();
      int lv2 = nn2.getFSImage().getLayoutVersion();
      int ns1 = nn1.getNamespaceID();
      int ns2 = nn2.getNamespaceID();
      assertNotSame("namespace ids should be different", ns1, ns2);
      LOG.info("nn1: lv=" + lv1 + ";uri=" + nn1.getNameNodeAddress());
      LOG.info("nn2: lv=" + lv2 + ";uri=" + nn2.getNameNodeAddress());

      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      
      for (NamespaceService nsos : dn.getAllNamespaceServices()) {
        LOG.info("reg: nsid =" + nsos.getNamespaceId() + "; name="
            + nsos.getNsRegistration().name + "; sid="
            + nsos.getNsRegistration().storageID + "; nna="
            + nsos.getNNSocketAddress());
      }

      NamespaceService nsos1 = dn.getAllNamespaceServices()[0];
      NamespaceService nsos2 = dn.getAllNamespaceServices()[1];

      // The order of bpos is not guaranteed, so fix the order
      if (nsos1.getNNSocketAddress().equals(nn2.getNameNodeDNAddress())) {
        NamespaceService tmp = nsos1;
        nsos1 = nsos2;
        nsos2 = tmp;
      }

      assertEquals("wrong nn address", nsos1.getNNSocketAddress(),
          nn1.getNameNodeDNAddress());
      assertEquals("wrong nn address", nsos2.getNNSocketAddress(),
          nn2.getNameNodeDNAddress());
      assertEquals("wrong nsid", nsos1.getNamespaceId(), ns1);
      assertEquals("wrong nsid", nsos2.getNamespaceId(), ns2);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * starts single nn and single dn and verifies registration and handshake
   * 
   * @throws IOException
   */
  @Test
  public void testFedSingleNN() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 1, true, null, 1);
    try {
      NameNode nn1 = cluster.getNameNode(0);
      assertNotNull("cannot create nn1", nn1);

      int nsid1 = nn1.getNamespaceID();
      int lv1 = nn1.getFSImage().getLayoutVersion();
      LOG.info("nn1: lv=" + lv1 + ";nsid=" + nsid1 + ";uri="
          + nn1.getNameNodeAddress());

      // check number of vlumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
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
          nn1.getNameNodeDNAddress());
      assertEquals("wrong nsid", nsos1.getNamespaceId(), nsid1);
      cluster.shutdown();
      
      // Ensure all the BPOfferService threads are shutdown
      assertEquals(0, dn.getAllNamespaceServices().length);
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testMiniDFSClusterWithMultipleNN() throws IOException {
    Configuration conf = new Configuration();
    // start Federated cluster and add a node.
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 1, true, null, 2);
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(1)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    
    // add a node
    
    cluster.addNameNode(conf, MiniDFSCluster.getFreePort());
    Assert.assertEquals("(1)Should be 3 namenodes", 3, cluster.getNumNameNodes());
    cluster.shutdown();
        
    // 2. start with Federation flag set
    conf = new Configuration();
    cluster = new MiniDFSCluster(0, conf, 1, true, null, 1);
    
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    
    // add a node
    cluster.addNameNode(conf, MiniDFSCluster.getFreePort());   
    Assert.assertEquals("(2)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    cluster.shutdown();
  }
  
  @Test
  public void testAddingNewNameNodeToNonFederatedCluster() throws IOException {
    // 3. start non-federate
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    
    // add a node
    try {
      cluster.addNameNode(conf, MiniDFSCluster.getFreePort());
      Assert.fail("shouldn't be able to add another NN to non federated cluster");
    } catch (IOException e) {
      // correct 
      Assert.assertTrue(e.getMessage().startsWith("cannot add namenode"));
      Assert.assertEquals("(3)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    } finally {
      cluster.shutdown();
    }
  }
}

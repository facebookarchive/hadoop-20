package org.apache.hadoop.hdfs.server.datanode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

public class TestRefreshNamenodes extends TestCase {
  static final Log LOG = LogFactory.getLog(TestRefreshNamenodes.class);
  private int[] namePorts = {20001, 20002, 20003, 20004};
  
  private void compareAddress(MiniDFSCluster cluster, DataNode dn, int[] nns) {
    NamespaceService[] nsoss = dn.getAllNamespaceServices();
    List<InetSocketAddress> expected = new ArrayList<InetSocketAddress>();
    for (int nn: nns) {
      expected.add(cluster.getNameNode(nn).getNameNodeDNAddress());
    }
    List<InetSocketAddress> nsAddrs = new ArrayList<InetSocketAddress>();
    for (NamespaceService nsos : nsoss) {
      nsAddrs.add(nsos.getNNSocketAddress());
    }
    assertEquals(expected.size(), nsAddrs.size());
    assertTrue(nsAddrs.containsAll(expected));
  }
  
  private void waitDataNodeInitialized(DataNode dn) {
    if (dn == null) {
      return ;
    }
    while (!dn.isInitialized()) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
    }
  }
  
  private void setupAddress(Configuration conf, int[] nns) {
    String nameserviceIdList = "";
    for (int i : nns) {
      String nameserviceId = MiniDFSCluster.NAMESERVICE_ID_PREFIX + i;
      // Create comma separated list of nameserviceIds
      if (nameserviceIdList.length() > 0) {
        nameserviceIdList += ",";
      }
      nameserviceIdList += nameserviceId;
    }
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIdList);
  } 
  
  @Test
  public void testRefreshNamenodes() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(namePorts[0], conf, 1, true, null, 1);
      DataNode dn = cluster.getDataNodes().get(0);
      assertEquals(dn.getAllNamespaceServices().length, 1);
      
      cluster.addNameNode(conf, namePorts[1]);
      assertEquals(dn.getAllNamespaceServices().length, 2);
      
      cluster.addNameNode(conf, namePorts[2]);
      assertEquals(dn.getAllNamespaceServices().length, 3);
      
      cluster.addNameNode(conf, namePorts[3]);
      assertEquals(dn.getAllNamespaceServices().length, 4);
      int[] nns = null;
      nns = new int[]{0, 1, 2, 3};
      compareAddress(cluster, dn, nns);
      nns = new int[]{0, 1};
      Configuration conf1 = new Configuration(conf);
      setupAddress(conf1, new int[]{0, 1});
      dn.refreshNamenodes(conf1);
      waitDataNodeInitialized(dn);
      compareAddress(cluster, dn, nns);
      
      nns = new int[]{0,2,3};
      Configuration conf2 = new Configuration(conf);
      setupAddress(conf2, new int[]{0,2,3});
      dn.refreshNamenodes(conf2);
      waitDataNodeInitialized(dn);
      compareAddress(cluster, dn, nns);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}

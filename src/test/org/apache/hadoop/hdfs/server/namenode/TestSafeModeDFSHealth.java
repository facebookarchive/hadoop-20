package org.apache.hadoop.hdfs.server.namenode;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSafeModeDFSHealth {

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    // Configure dn address so that client server is not started until we leave
    // safemode.
    conf.set("dfs.namenode.dn-address", "localhost:0");
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testDFSHealthInSafeMode() throws Exception {
    DFSTestUtil util = new DFSTestUtil("/testDFSHealthInSafeMode", 10, 1, 1024);
    util.createFiles(cluster.getFileSystem(), "/");
    cluster.shutdown();

    cluster = new MiniDFSCluster(conf, 0, false, null);

    // Namenode should now be in safemode.
    assertTrue(cluster.getNameNode().isInSafeMode());

    URL url = new URL("http://" + conf.get("dfs.http.address")
        + "/dfshealth.jsp");
    String result = DFSTestUtil.urlGet(url);
    assertNotNull(result);
    url = new URL("http://" + conf.get("dfs.http.address")
        + "/dfsnodelist.jsp");
    result = DFSTestUtil.urlGet(url);
    assertNotNull(result);
  }
}

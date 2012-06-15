package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.ipc.RPC;

import junit.framework.TestCase;

public class TestNameNodePorts extends TestCase {
  private static final String ERROR_MSG_PREFIX =
    "java.io.IOException: Unknown protocol to name node";
  public void testSinglePortStartup() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    NameNode nn = cluster.getNameNode();
    InetSocketAddress dnAddress = nn.getNameNodeDNAddress();
    InetSocketAddress clientAddress = nn.getNameNodeAddress();

    assertEquals(clientAddress, dnAddress);

    DatanodeProtocol dnProtocol = (DatanodeProtocol) RPC.waitForProxy(
        DatanodeProtocol.class, DatanodeProtocol.versionID, dnAddress, conf);
    // perform a dummy call
    dnProtocol.getProtocolVersion(DatanodeProtocol.class.getName(),
        DatanodeProtocol.versionID);
    ClientProtocol client = (ClientProtocol) RPC.waitForProxy(
        ClientProtocol.class, ClientProtocol.versionID, dnAddress, conf);
    client.getProtocolVersion(ClientProtocol.class.getName(),
        ClientProtocol.versionID);

    cluster.shutdown();
  }

  public void testBothPortsStartup() throws IOException {
    Configuration conf = new Configuration();
    NameNode.setDNProtocolAddress(conf, "localhost:0");

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    NameNode nn = cluster.getNameNode();
    InetSocketAddress dnAddress = nn.getNameNodeDNAddress();
    InetSocketAddress clientAddress = nn.getNameNodeAddress();

    assertNotSame(clientAddress, dnAddress);

    DatanodeProtocol dnProtocol = (DatanodeProtocol) RPC.waitForProxy(
        DatanodeProtocol.class, DatanodeProtocol.versionID, dnAddress, conf);
    // perform a dummy call
    dnProtocol.getProtocolVersion(DatanodeProtocol.class.getName(),
        DatanodeProtocol.versionID);

    try {
      dnProtocol = (DatanodeProtocol) RPC.waitForProxy(DatanodeProtocol.class,
          DatanodeProtocol.versionID, clientAddress, conf);
      // perform a dummy call
      dnProtocol.getProtocolVersion(DatanodeProtocol.class.getName(),
          DatanodeProtocol.versionID);
    } catch (IOException ex) {
      System.out.println("ERROR: " + ex.getMessage());
      assertTrue(ex.getMessage().startsWith(ERROR_MSG_PREFIX));
    }

    ClientProtocol client = (ClientProtocol) RPC.waitForProxy(
        ClientProtocol.class, ClientProtocol.versionID, clientAddress, conf);
    client.getProtocolVersion(ClientProtocol.class.getName(),
        ClientProtocol.versionID);
    try {
      client = (ClientProtocol) RPC.waitForProxy(ClientProtocol.class,
          ClientProtocol.versionID, dnAddress, conf);
      client.getProtocolVersion(ClientProtocol.class.getName(),
          ClientProtocol.versionID);
    } catch (IOException ex) {
      assertTrue(ex.getMessage().startsWith(ERROR_MSG_PREFIX));
    }
    cluster.shutdown();
  }
}

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that if two nodes in the pipeline go down, the client is still able to
 * perform the recovery using one primary node. This unit test reproduces a
 * scenario where the deadline for recovery was not set correctly (was too
 * short) and hence the recovery failed even from a primary datanode that was
 * working correctly.
 */
public class TestDFSClientTimeouts {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private static final int CONNECT_TIMEOUT = 2000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    InjectionHandler.set(new TimeoutInjectionHandler());
    conf = new Configuration();
    conf.setInt("ipc.client.connect.max.retries", 2);
    conf.setInt("ipc.client.connect.timeout", CONNECT_TIMEOUT);
    conf.setInt("dfs.socket.timeout", 1500);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.client.block.recovery.retries", 1);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  private static class TimeoutInjectionHandler extends InjectionHandler {
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEventCore.RPC_CLIENT_CONNECTION_FAILURE) {
        IOException e = (IOException) args[0];
        if (e instanceof ConnectException) {
          // Delay by 3 * (connect timeout) to simulate socket timeout
          // exception.
          try {
            Thread.sleep(3 * CONNECT_TIMEOUT);
          } catch (Exception ex) {
          }
        }
      }
    }
  }

  @Test
  public void runTest() throws Exception {
    String fileName = "/test";
    DFSOutputStream out = (DFSOutputStream) fs.create(new Path(fileName))
        .getWrappedStream();
    byte[] buf = new byte[BLOCK_SIZE / 2];
    out.write(buf);
    out.sync();
    DatanodeInfo[] pipeline = out.getPipeline();
    DatanodeInfo primary = Collections.min(Arrays.asList(pipeline));
    ArrayList<DataNode> dns = cluster.getDataNodes();
    for (int i = 0; i < dns.size(); i++) {
      if (dns.get(i).getPort() != primary.getPort()) {
        cluster.shutdownDataNode(i, false);
      }
    }
    out.write(buf);
    out.sync();
  }
}

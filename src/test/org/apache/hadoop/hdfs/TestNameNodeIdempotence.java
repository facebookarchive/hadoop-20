package org.apache.hadoop.hdfs;

import junit.framework.TestCase;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageAdapter;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;

import static org.apache.hadoop.hdfs.AppendTestUtil.loseLeases;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Level;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/* Test whether NameNode.close() and NmaeNode.addBlock() are idempotent
 */
public class TestNameNodeIdempotence extends TestCase {
  {
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) NameNode.LOG).getLogger().setLevel(Level.ALL);
  }

  MiniDFSCluster cluster;

  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", 512L);
    cluster = new MiniDFSCluster(conf, 1, true, null);
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
  }

  /**
   * Test closeFile() name-node RPC is idempotent
   */
  public void testIdepotentCloseCalls() throws IOException {
    ClientProtocol nn = cluster.getNameNode();
    FileSystem fs = cluster.getFileSystem();
    DFSClient dfsclient = ((DistributedFileSystem) fs).dfs;
    DFSClient mockDfs = spy(dfsclient);

    ClientProtocol mockNameNode = spy(nn);
    mockDfs.namenode = mockNameNode;

    String src = "/testNameNodeFingerprintSent1.txt";
    // Path f = new Path(src);

    FSDataOutputStream a_out = new FSDataOutputStream(mockDfs.create(src, true)); // fs.create(f);
    a_out.writeBytes("something");

    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        try {
          invocation.callRealMethod();
          return (Void) invocation.callRealMethod();
        } catch (Throwable e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          return null;
        }
      }
    }).when(mockDfs).closeFile(anyString(), anyLong(), (Block) anyObject());
    a_out.close();

    verify(mockNameNode, times(2)).complete(anyString(), anyString(),
        anyLong(), (Block) anyObject());

    boolean hasFail = false;
    try {
      nn.complete(src, "CC", 9, new Block(666));
    } catch (IOException e) {
      TestCase
          .assertTrue(e
              .getMessage()
              .contains(
                  "Try close a closed file: last block from client side doesn't match name-node. client:"));
      hasFail = true;
    }
    TestCase.assertTrue(hasFail);

    hasFail = false;
    try {
      nn.complete(src, "CC", 9, null);
    } catch (IOException e) {
      TestCase
          .assertTrue(
              e.getMessage(),
              e.getMessage()
                  .contains(
                      "No lease on /testNameNodeFingerprintSent1.txt File is not open for writing."));
      hasFail = true;
    }
    TestCase.assertTrue(hasFail);
  }

  /**
   * Test addBlock() name-node RPC is idempotent
   */
  public void testIdepotentCallsAddBlock() throws IOException {
    ClientProtocol nn = cluster.getNameNode();
    FileSystem fs = cluster.getFileSystem();
    DFSClient dfsclient = ((DistributedFileSystem) fs).dfs;

    String src = "/testNameNodeFingerprintSent1.txt";
    // Path f = new Path(src);

    DFSOutputStream dos = (DFSOutputStream) dfsclient.create(src, true,
        (short) 1, 512L);

    FSDataOutputStream a_out = new FSDataOutputStream(dos); // fs.create(f);

    for (int i = 0; i < 512; i++) {
      a_out.writeBytes("bc");
    }
    a_out.flush();

    LocatedBlocks lb = nn.getBlockLocations(src, 256, 257);
    LocatedBlock lb1 = nn.addBlockAndFetchMetaInfo(src, dfsclient.clientName,
        null, null, 512L, lb.getLocatedBlocks().get(lb.locatedBlockCount() - 1)
            .getBlock());
    LocatedBlock lb2 = nn.addBlockAndFetchMetaInfo(src, dfsclient.clientName,
        null, null, 512L, lb.getLocatedBlocks().get(lb.locatedBlockCount() - 1)
            .getBlock());
    TestCase.assertTrue("blocks: " + lb1.getBlock() + " and " + lb2.getBlock(),
        lb1.getBlock().equals(lb2.getBlock()));
  }

}

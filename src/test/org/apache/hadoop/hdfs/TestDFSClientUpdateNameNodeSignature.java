package org.apache.hadoop.hdfs;

import junit.framework.TestCase;


import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/* Test the name node's sending methods fingerprint with MetaInfo
 * and DFSClient's updating name-node proxy after that.
 */
public class TestDFSClientUpdateNameNodeSignature extends TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.LOG).getLogger().setLevel(Level.ALL);
  }
 
  MiniDFSCluster cluster;
  @Override
  protected void setUp() throws Exception{
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 1, true, null);
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  /**
   * This function tests the method signature fingerprint passed back from
   * name-node with MetaInfo is correct.
   */
  @SuppressWarnings("unchecked")
  public void testNameNodeFingerprintSent() throws IOException {
    InetSocketAddress addr = cluster.getNameNode().getNameNodeDNAddress();
    DFSClient client = new DFSClient(addr, cluster.getNameNode().getConf());

    client.namenode.create("/testNameNodeFingerprintSent.txt", FsPermission
        .getDefault(), client.getClientName(), true, (short)1, 65535L);

    
    Class<? extends VersionedProtocol> inter;
    try {
      inter = (Class<? extends VersionedProtocol>)Class.forName(ClientProtocol.class.getName());
    } catch (Exception e) {
      throw new IOException(e);
    }
    long serverVersion = ClientProtocol.versionID;
    int serverFpFromNn = ProtocolSignature.getFingerprint(ProtocolSignature.getProtocolSignature(
        0, serverVersion, inter).getMethods());
    
    LocatedBlockWithMetaInfo loc = client.namenode.addBlockAndFetchMetaInfo("/testNameNodeFingerprintSent.txt",
        client.getClientName(), null, 0L);
    
    int serverFp = loc.getMethodFingerPrint();
    TestCase.assertEquals(serverFpFromNn, serverFp);    

    FileSystem fs = cluster.getFileSystem();
    Path f = new Path("/testNameNodeFingerprintSent1.txt");
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("something");
    a_out.close();
    
    LocatedBlocksWithMetaInfo locs = client.namenode.openAndFetchMetaInfo("/testNameNodeFingerprintSent.txt",
        0L, 0L);
    TestCase.assertEquals(locs.getMethodFingerPrint(), serverFp);    
  }

  /**
   * Test when name-node's finger-print changes, client re-fetch the
   * name-node proxy.
   */
  public void testClientUpdateMethodList() throws IOException {
    InetSocketAddress addr = cluster.getNameNode().getNameNodeDNAddress();
    DFSClient client = new DFSClient(addr, cluster.getNameNode().getConf());
    ClientProtocol oldNamenode = client.namenode;
    
    // Client's name-node proxy should keep the same if the same namenode
    // sends the same fingerprint
    //
    OutputStream os = client.create("/testClientUpdateMethodList.txt", true);
    os.write(66);
    os.close();
    TestCase.assertSame(oldNamenode, client.namenode);    
    int oldFingerprint = cluster.getNameNode().getClientProtocolMethodsFingerprint();
    TestCase.assertEquals(oldFingerprint, client.namenodeProtocolProxy
        .getMethodsFingerprint());
    
    // Namenode's fingerprint will be different to client. Client is suppsoed
    // to get a new proxy.
    //
    cluster.getNameNode().setClientProtocolMethodsFingerprint(666);
    os = client.create("/testClientUpdateMethodList1.txt", true);
    os.write(88);
    os.close();
    TestCase.assertNotSame(oldNamenode, client.namenode);
    // Since we didn't change method list of name-node, the fingerprint
    // got from the new proxy should be the same as the previous one.
    TestCase.assertEquals(oldFingerprint, client.namenodeProtocolProxy
        .getMethodsFingerprint());
    
    // Client's name-node proxy should keep the same if the same namenode
    // sends the same fingerprint
    //
    ClientProtocol namenode1 = client.namenode;
    cluster.getNameNode().setClientProtocolMethodsFingerprint(oldFingerprint);
    DFSInputStream dis = client.open("/testClientUpdateMethodList.txt");
    int val = dis.read();
    TestCase.assertEquals(66, val);
    dis.close();
    TestCase.assertSame(namenode1, client.namenode);

    // Namenode's fingerprint will be different to client. Client is suppsoed
    // to get a new proxy.
    //
    cluster.getNameNode().setClientProtocolMethodsFingerprint(888);
    dis = client.open("/testClientUpdateMethodList1.txt");
    val = dis.read();
    TestCase.assertEquals(88, val);
    dis.close();
    // Since we didn't change method list of name-node, the fingerprint
    // got from the new proxy should be the same as the previous one.
    TestCase.assertNotSame(namenode1, client.namenode);
  }
  
  /**
   * Test when file not exist, client open should get FileNotFoundException
   */
  public void testClientOpenFileNotExist() throws IOException {
    InetSocketAddress addr = cluster.getNameNode().getNameNodeDNAddress();
    DFSClient client = new DFSClient(addr, cluster.getNameNode().getConf());
    try {
      client.open("/testFileNotFound.txt");
      fail("Expected exception not thrown");
    } catch (IOException e) {
      TestCase.assertTrue(e instanceof FileNotFoundException);
    }
  }
}

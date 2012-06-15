package org.apache.hadoop.hdfs.server.datanode;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RPC;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCopyBlockAPI {
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testCopyBlockAPI() throws Exception {
    // Generate source file and get its locations.
    String filename = "/testCopyBlockAPI";
    DFSTestUtil.createFile(fs, new Path(filename), 1023 * 10, (short) 3,
        (long) 0);
    FileStatus srcFileStatus = fs.getFileStatus(new Path(filename));
    LocatedBlocksWithMetaInfo lbkSrcMetaInfo = cluster.getNameNode()
        .openAndFetchMetaInfo(filename, 0, Long.MAX_VALUE);
    int srcNamespaceId = lbkSrcMetaInfo.getNamespaceID();
    LocatedBlock lbkSrc = lbkSrcMetaInfo.getLocatedBlocks().get(0);
    DatanodeInfo[] srcLocs = lbkSrc.getLocations();

    // Create destination file and add a single block.
    String newFile = "/testCopyBlockAPI_new";
    String clientName = newFile;
    fs.create(new Path(filename + "new"));
    cluster.getNameNode().create(newFile, srcFileStatus.getPermission(),
        clientName, true, true, srcFileStatus.getReplication(),
        srcFileStatus.getBlockSize());
    LocatedBlockWithMetaInfo lbkDstMetaInfo =
      cluster.getNameNode().addBlockAndFetchMetaInfo(newFile, clientName, null, srcLocs);
    int dstNamespaceId = lbkDstMetaInfo.getNamespaceID();
    LocatedBlock lbkDst = lbkDstMetaInfo;

    // Verify locations of src and destination block.
    DatanodeInfo[] dstLocs = lbkDst.getLocations();
    Arrays.sort(srcLocs);
    Arrays.sort(dstLocs);
    assertEquals(srcLocs.length, dstLocs.length);
    for (int i = 0; i < srcLocs.length; i++) {
      assertEquals(srcLocs[i], dstLocs[i]);
    }

    // Create datanode rpc connections.
    ClientDatanodeProtocol cdp2 = DFSClient.createClientDatanodeProtocolProxy(
        srcLocs[2], conf, 5 * 60 * 1000);

    Block srcBlock = new Block(lbkSrc.getBlock());
    Block dstBlock = new Block(lbkDst.getBlock());
    System.out.println("Copying src : " + srcBlock + " dst : " + dstBlock);

    // Find datanode object.
    DataNode datanode = null;
    for (DataNode dn : cluster.getDataNodes()) {
      DatanodeRegistration registration = dn.getDNRegistrationForNS(srcNamespaceId);
      if (registration.equals(srcLocs[0])) {
        datanode = dn;
        break;
      }
    }
    
    assertNotNull(datanode);

    // Submit a block transfer to location 2.
    ExecutorService pool = Executors.newSingleThreadExecutor();
    pool.submit(datanode.new DataTransfer(new DatanodeInfo[] { srcLocs[2] }, srcNamespaceId,
          srcBlock, dstNamespaceId, dstBlock, datanode));

    try {
      Thread.sleep(5000);
      // Submit another transfer to same location, should receive
      // BlockAlreadyExistsException.
      cdp2.copyBlock(srcNamespaceId, srcBlock, dstNamespaceId, dstBlock, srcLocs[2], false);
    } catch (RemoteException re) {
      // pass.
      return;
    } finally {
      // Shutdown RPC connections.
      RPC.stopProxy(cdp2);
    }
    fail("Second RPC did not throw Exception");
  }
}

package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestDatanodeDeath2 extends TestCase {
  private interface Callback {
    void execute();
  }

  private static final Logger LOG = Logger.getLogger(TestDatanodeDeath2.class);

  private static final String FILE1 = "/file1";
  private static final String FILE2 = "/file2";
  private static final String FILE3 = "/file3";
  private static final int BLOCK_SIZE = 8192;

  private MiniDFSCluster cluster;
  private FileSystem fileSystem;
  private Callback datanodeDeathCallback;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    Configuration conf = new Configuration();

    conf.setInt("heartbeat.recheck.interval", 1);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.replication", 3);

    cluster = new MiniDFSCluster(conf, 3, true, null);
    fileSystem = cluster.getFileSystem();
    datanodeDeathCallback = new Callback() {
      @Override
      public void execute() {
        // kill a datanode
        cluster.stopDataNode(0);
      }
    };
  }

  public void testDatanodeRemovedFromCreatePipeline() throws Exception {
    runTestDatanodeRemovedFromPipeline(false, datanodeDeathCallback);
  }

  public void testDatanodeRemovedFromAppendPipeline() throws Exception {
    runTestDatanodeRemovedFromPipeline(true, datanodeDeathCallback);
  }

  // condition is that an INodeFileUnderConstruction changes the targets
  // due to abandoning a block and requesting another.  Make sure that if
  // a Datanode dies after this, that it doesn't still think it needs to remove
  // itself from INodes that no longer point to it
  public void testBlockAbandoned() throws Exception {
    Callback newPipeline = new Callback() {
      @Override
      public void execute() {
          try {
            FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
            LocatedBlocks blocks =
              namesystem.getBlockLocations(FILE1, 0, 2* BLOCK_SIZE);
            List<LocatedBlock> blockList = blocks.getLocatedBlocks();
            String holder = ((DistributedFileSystem) fileSystem).getClient().clientName;

            // abandonBlock clears the targets of the INodeFileUnderConstruction
            namesystem.abandonBlock(
              blockList.get(blockList.size() - 1).getBlock(),
              FILE1,
              holder
            );

            // take down the datanode
            DataNode dataNode = cluster.getDataNodes().get(0);

            // get a new block for the same file which we exclude the node from
            Node excludedNode = cluster
              .getNameNode()
              .getNamesystem()
              .getDatanode(dataNode.getDNRegistrationForNS(
                  cluster.getNameNode().getNamespaceID()));
            namesystem.getAdditionalBlock(
              FILE1, holder, Arrays.<Node>asList(excludedNode)
            );

            dataNode.shutdown();
          }
          catch (IOException e) {
            fail("exception: " + StringUtils.stringifyException(e));
        }
      }
    };

    runTestDatanodeRemovedFromPipeline(false, newPipeline);
  }

  private void runTestDatanodeRemovedFromPipeline(
    boolean append, Callback datanodeDeath
  ) throws Exception {
    writeAndSyncFile(FILE1, append);
    writeAndSyncFile(FILE2, append);
    writeAndSyncFile(FILE3, append);

    cluster.waitForDNHeartbeat(0, 5000);
    
    DatanodeRegistration dnReg = 
        cluster.getDataNodes().get(0).getDNRegistrationForNS(
        cluster.getNameNode().getNamespaceID());

    // customizable callback that takes down the datanode
    datanodeDeath.execute();

    // this calls the same code-path as when a Datanode is marked dead
    // by lack of heartbeat for 10m
    cluster.getNameNode()
      .getNamesystem()
      .removeDatanode(dnReg);
    // now check that the killed datanode is not a valid target
    LocatedBlocks blockLocations =
      cluster.getNameNode().getBlockLocations(FILE1, 0, 1);

    assertTrue(
      "file not under construction",
      blockLocations.isUnderConstruction()
    );
    assertEquals(
      "expected number of block locations",
      2,
      blockLocations.get(0).getLocations().length
    );
  }

  private DFSOutputStream writeAndSyncFile(
    String file, boolean append
  ) throws IOException {
    DistributedFileSystem distributedFileSystem = (DistributedFileSystem) fileSystem;
    DFSOutputStream out =
      (DFSOutputStream) distributedFileSystem
      .getClient()
      .create(file, true);

    if (append) {
      out.write(DFSTestUtil.generateSequentialBytes(0, 1024));
      out.close();
      out =
        (DFSOutputStream) distributedFileSystem
          .getClient()
          .append(file, 0, null); // bufferSize ingored, progessable can be null
    }

    out.write(DFSTestUtil.generateSequentialBytes(0, 1024));
    out.sync();
    // simulate that client dies : we don't want the client to do lease recovery
    out.abortForTests();

    return out;
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();

    super.tearDown();
  }
}

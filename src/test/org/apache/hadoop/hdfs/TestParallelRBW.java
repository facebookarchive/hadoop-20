package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * This tests datanode upgrades and rollbacks and verifies that no data is lost
 * in these operations.
 */
public class TestParallelRBW {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static long oldCTime = 0L;
  private static final int lastBlockLen = 30;
  private static final int BLOCK_SIZE = 512;
  // Needed to run unit tests sequentially.
  private final static Lock lock = new ReentrantLock();

  @Before
  public void setUp() throws Exception {
    lock.lock();
    Field layoutVersion = FSConstants.class.getField("LAYOUT_VERSION");
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    // Conf options to speed up unit test execution.
    conf.setInt("dfs.client.block.recovery.retries", 1);
    conf.setInt("ipc.client.connect.max.retries", 1);
    conf.setInt("dfs.socket.timeout", 500);
    conf.setInt("dfs.datanode.blockscanner.threads", 3);
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @After
  public void tearDown() throws Exception {
    cluster.finalizeCluster(conf);
    cluster.shutdown();
    NameNode.format(conf);
    cluster.formatDataNodeDirs();
    lock.unlock();
  }

  // Create a file with RBWs
  private void createFile(FileSystem fs, FSDataOutputStream out,
      String fileName, int fileLen) throws IOException {
    Random random = new Random(fileName.hashCode());
    byte buffer[] = new byte[fileLen];
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();
    ((DFSOutputStream) out.getWrappedStream()).abortForTests();
  }

  private void verifyFile(FileSystem fs, String fileName, int fileLen)
      throws IOException {
    FSDataInputStream in = fs.open(new Path(fileName));
    byte expected[] = new byte[fileLen];
    byte actual[] = new byte[fileLen];
    Random random = new Random(fileName.hashCode());
    random.nextBytes(expected);
    in.readFully(actual);
    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testUpgradeRBW() throws Exception {
    String fileName = "/testUpgradeRBW1";
    int fileLen = 6 * BLOCK_SIZE + lastBlockLen;
    FileSystem fs = cluster.getFileSystem();
    FSDataOutputStream out = fs.create(new Path(fileName));
    createFile(fs, out, fileName, fileLen);
    // After the fsync, the last block len is set to 1.
    assertEquals(fileLen - (lastBlockLen - 1),
        fs.getFileStatus(new Path(fileName)).getLen());
    NamespaceInfo nsInfo = cluster.getNameNode().versionRequest();
    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(1,
          dn.data.getBlocksBeingWrittenReport(nsInfo.getNamespaceID()).length);
    }

    // Restart namenode and datanodes and perform an upgrade.
    cluster.shutdown();
    cluster = new MiniDFSCluster(0, conf, 3, false, true, StartupOption.UPGRADE, null);
    fs = cluster.getFileSystem();
    nsInfo = cluster.getNameNode().versionRequest();

    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(nsInfo.getCTime(), dn.getCTime(nsInfo.getNamespaceID()));
    }

    // After the fsync, the last block len is set to 1.
    assertEquals(fileLen - (lastBlockLen - 1),
        fs.getFileStatus(new Path(fileName)).getLen());
    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(1,
          dn.data.getBlocksBeingWrittenReport(nsInfo.getNamespaceID()).length);
    }

    // Verify sanity of files.
    verifyFile(fs, fileName, fileLen);

    // Now lets do a rollback and verify we still have the data.
    cluster.shutdown();
    cluster = new MiniDFSCluster(0, conf, 3, false, true, StartupOption.ROLLBACK, null);
    fs = cluster.getFileSystem();
    nsInfo = cluster.getNameNode().versionRequest();

    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(oldCTime, dn.getCTime(nsInfo.getNamespaceID()));
    }

    // After the fsync, the last block len is set to 1.
    assertEquals(fileLen - (lastBlockLen - 1),
        fs.getFileStatus(new Path(fileName)).getLen());
    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(1,
          dn.data.getBlocksBeingWrittenReport(nsInfo.getNamespaceID()).length);
    }
    // Verify sanity of files.
    verifyFile(fs, fileName, fileLen);
  }
}

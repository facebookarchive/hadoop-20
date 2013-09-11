package org.apache.hadoop.hdfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestFileStatusExtended {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private static final int MAX_BLOCKS = 10;
  private static final int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static final Random random = new Random();
  private static final Log LOG = LogFactory
      .getLog(TestFileStatusExtended.class);

  @Before
  public void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDownAfterClass() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  @Test
  public void testRandomFiles() throws Exception {
    String topDir = "testRandomFiles";
    DFSTestUtil util = new DFSTestUtil(topDir, 100, 10, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    NameNode nn = cluster.getNameNode();
    List<FileStatusExtended> stats = nn.getRandomFilesSample(0.4);
    assertNotNull(stats);
    assertTrue(stats.size() >= 10);
    for (FileStatusExtended stat : stats) {
      fs.exists(stat.getPath());
      assertEquals("", stat.getHolder());

      // Verify blocks
      Block[] statBlocks = stat.getBlocks();
      List<LocatedBlock> blocks = nn.getBlockLocations(
          stat.getPath().toString(), 0, Long.MAX_VALUE).getLocatedBlocks();
      assertEquals(blocks.size(), statBlocks.length);
      for (int i = 0; i < statBlocks.length; i++) {
        assertEquals(blocks.get(i).getBlock(), statBlocks[i]);
      }
      FileStatus st = nn.getFileInfo(stat.getPath().toString());
      assertEquals(st, (FileStatus) stat);
    }
  }

  private List<FileStatusExtended> dumpRandomFileInfo(String fileName,
      double percentage, NameNode nn) throws IOException {
    List<FileStatusExtended> stats = nn.getRandomFilesSample(percentage);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(fileName));
    try {
      // Write the number of entries.
      out.writeInt(stats.size());
      for (FileStatusExtended stat : stats) {
        stat.write(out);
      }
    } finally {
      out.close();
    }
    return stats;
  }

  @Test
  public void testDump() throws Exception {
    String topDir = "testDump";
    DFSTestUtil util = new DFSTestUtil(topDir, 50, 10, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    File fileName = new File(cluster.getBaseDataDir(), "dumpMeta");
    NameNode nn = cluster.getNameNode();
    List<FileStatusExtended> stats = dumpRandomFileInfo(
        fileName.getAbsolutePath(), 0.4, nn);
    DataInputStream in = new DataInputStream(new FileInputStream(
        fileName.getAbsolutePath()));
    int nFiles = in.readInt();
    assertEquals(stats.size(), nFiles);
    for (int i = 0; i < nFiles; i++) {
      FileStatusExtended stat = new FileStatusExtended();
      stat.readFields(in);
      assertEquals(stats.get(i), stat);
    }
  }

  @Test
  public void testPercentBasedSample() throws Exception {
    String topDir = "testPercentBasedSample";
    DFSTestUtil util = new DFSTestUtil(topDir, 200, 10, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    NameNode nn = cluster.getNameNode();
    List<FileStatusExtended> stats = nn.getRandomFilesSample(0.1);
    assertTrue(stats.size() >= 5);
  }

  @Test
  public void testPercentBasedSampleFull() throws Exception {
    String topDir = "testPercentBasedSample";
    DFSTestUtil util = new DFSTestUtil(topDir, 15, 10, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    NameNode nn = cluster.getNameNode();
    List<FileStatusExtended> stats = nn.getRandomFilesSample(1.0);
    assertTrue(15 >= stats.size());
  }

  @Test
  public void testInvalidPercentBasedSample1() throws Exception {
    try {
      cluster.getNameNode().getRandomFilesSample(0.0);
      fail("Did not throw : " + IllegalArgumentException.class);
    } catch (IllegalArgumentException e) {
      LOG.info("Expected exception : ", e);
    }
  }

  @Test
  public void testInvalidPercentBasedSample2() throws Exception {
    try {
      cluster.getNameNode().getRandomFilesSample(1.1);
      fail("Did not throw : " + IllegalArgumentException.class);
    } catch (IllegalArgumentException e) {
      LOG.info("Expected exception : ", e);
    }
  }

  @Test
  public void testInvalidPercentBasedSample3() throws Exception {
    try {
      cluster.getNameNode().getRandomFilesSample(-0.5);
      fail("Did not throw : " + IllegalArgumentException.class);
    } catch (IllegalArgumentException e) {
      LOG.info("Expected exception : ", e);
    }
  }

  @Test
  public void testFileUnderConstruction() throws Exception {
    String fileName = "/testFileUnderConstruction";
    FSDataOutputStream out = fs.create(new Path(fileName));
    byte [] buffer = new byte[BLOCK_SIZE * 5 + 256];
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();

    NameNode nn = cluster.getNameNode();
    List<FileStatusExtended> stats = nn.getRandomFilesSample(1);
    assertEquals(1, stats.size());
    assertEquals(((DistributedFileSystem) fs).getClient().clientName, stats
        .get(0).getHolder());
  }
}

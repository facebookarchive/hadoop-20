package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIoprio {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static int ioprioClass;
  private static int ioprioData;
  private static final int BLOCK_SIZE = 1024;
  private static final Log LOG = LogFactory.getLog(TestIoprio.class);

  @Before
  public void setUp() {
    ioprioClass = ioprioData = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = (DistributedFileSystem) cluster.getFileSystem();
    InjectionHandler.set(new IoprioHandler());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  private static class IoprioHandler extends InjectionHandler {
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.DATANODE_READ_BLOCK || event == InjectionEvent.DATANODE_WRITE_BLOCK) {
        try {
          int ioprio_value = NativeIO.ioprioGetIfPossible();
          if (ioprio_value != -1) {
            ioprioClass = ioprio_value >> 13;
            ioprioData = ioprio_value & ((1 << 13) - 1);
          }
        } catch (IOException ie) {
          LOG.warn("ioprio_get() failed", ie);
        }
      }
    }
  }

  public void createFile(Path p, int pri) throws Exception {
    WriteOptions options = new WriteOptions();
    options.setIoprio(NativeIO.IOPRIO_CLASS_BE, pri);
    OutputStream out = fs.create(p, null, true, 4096, (short) 3, BLOCK_SIZE,
        512, null, null, options);
    byte[] buffer = new byte[BLOCK_SIZE];
    out.write(buffer);
    out.close();
  }

  @Test
  public void testWrite() throws Exception {
    String fileName = "/test";
    Path p = new Path(fileName);
    for (int pri = 0; pri < 8; pri++) {
      createFile(p, pri);
      if (NativeIO.isAvailable()) {
        assertTrue(NativeIO.isIoprioPossible());
        assertEquals(NativeIO.IOPRIO_CLASS_BE, ioprioClass);
        assertEquals(pri, ioprioData);
      }
    }
  }

  @Test
  public void testRead() throws Exception {
    String fileName = "/test";
    Path p = new Path(fileName);
    for (int pri = 0; pri < 8; pri++) {
      createFile(p, pri);

      ioprioClass = ioprioData = 0;
      ReadOptions options = new ReadOptions();
      options.setIoprio(NativeIO.IOPRIO_CLASS_BE, pri);
      FSDataInputStream in = fs.open(p, 4096, options);

      byte[] buffer = new byte[BLOCK_SIZE];
      in.readFully(buffer);
      if (NativeIO.isAvailable()) {
        assertTrue(NativeIO.isIoprioPossible());
        assertEquals(NativeIO.IOPRIO_CLASS_BE, ioprioClass);
        assertEquals(pri, ioprioData);
      }
    }
  }

  @Test
  public void testPRead() throws Exception {
    String fileName = "/test";
    Path p = new Path(fileName);
    for (int pri = 0; pri < 8; pri++) {
      createFile(p, pri);

      ioprioClass = ioprioData = 0;
      ReadOptions options = new ReadOptions();
      options.setIoprio(NativeIO.IOPRIO_CLASS_BE, pri);
      FSDataInputStream in = fs.open(p, 4096, options);

      byte[] buffer = new byte[BLOCK_SIZE * 2];
      in.read(BLOCK_SIZE / 2, buffer, 0, BLOCK_SIZE / 2);

      if (NativeIO.isAvailable()) {
        assertTrue(NativeIO.isIoprioPossible());
        assertEquals(NativeIO.IOPRIO_CLASS_BE, ioprioClass);
        assertEquals(pri, ioprioData);
      }
    }
  }

  @Test
  public void testQuorumPReadWithOptions() throws Exception {
    Configuration newConf = new Configuration(conf);
    newConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    newConf.setInt(HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE, 10);
    runPreadTest(conf);
  }

  @Test
  public void testPReadWithOptions() throws Exception {
    runPreadTest(conf);
  }

  public void runPreadTest(Configuration conf) throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem(conf);
    String fileName = "/test";
    Path p = new Path(fileName);
    for (int pri = 0; pri < 8; pri++) {
      createFile(p, pri);

      ioprioClass = ioprioData = 0;
      DFSDataInputStream in = (DFSDataInputStream) fs.open(p);

      byte[] buffer = new byte[BLOCK_SIZE * 2];
      ReadOptions options = new ReadOptions();
      options.setIoprio(NativeIO.IOPRIO_CLASS_BE, pri);
      in.read(BLOCK_SIZE / 2, buffer, 0, BLOCK_SIZE / 2, options);

      if (NativeIO.isAvailable()) {
        assertTrue(NativeIO.isIoprioPossible());
        assertEquals(NativeIO.IOPRIO_CLASS_BE, ioprioClass);
        assertEquals(pri, ioprioData);
      }
    }
  }
}

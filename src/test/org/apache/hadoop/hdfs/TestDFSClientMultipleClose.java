package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDFSClientMultipleClose {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DFSClient dfs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
    dfs = ((DistributedFileSystem) cluster.getFileSystem()).getClient();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testMultipleFileCloseDFS() throws Exception {
    String testFile = "/testMultipleFileCloseDFS";
    OutputStream stream = dfs.create(testFile, true);
    // This would ensure a close on the stream would fail.
    cluster.getNameNode().stop();
    try {
      stream.close();
    } catch (IOException e) {
      try {
        stream.close();
      } catch (IOException e1) {
        System.out.println("Test passed.");
        return;
      }
      fail("Second close did not throw an exception");
    }
    fail("Close did not throw an exception");
  }
}

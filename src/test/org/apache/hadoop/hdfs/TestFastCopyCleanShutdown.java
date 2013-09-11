package org.apache.hadoop.hdfs;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Ensures that the FastCopy tool does not leave behind any dangling threads.
 */
public class TestFastCopyCleanShutdown {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 20;
  private static long MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static Set<Thread> threadsBefore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    threadsBefore = new HashSet<Thread>(Thread.getAllStackTraces()
        .keySet());
    conf = new Configuration();
    conf.setInt("ipc.client.connect.max.retries", 3);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fs.close();
    cluster.shutdown();
    DFSTestThreadUtil.checkRemainingThreads(threadsBefore);
  }
  
  @Test
  public void testCleanShutdown() throws Exception {
    String filename = "/testCleanShutdown";
    DFSTestUtil.createFile(fs, new Path(filename), MAX_FILE_SIZE, (short) 3,
        System.currentTimeMillis());

    FastCopy fastCopy = new FastCopy(conf);
    try {
      fastCopy.copy(filename, filename + "dst", (DistributedFileSystem) fs,
          (DistributedFileSystem) fs);
    } finally {
      fastCopy.shutdown();
    }
  }
}

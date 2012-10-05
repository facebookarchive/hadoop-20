package org.apache.hadoop.hdfs;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;
import static org.junit.Assert.*;

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

  // A list of threads that might hang around (maybe due to a JVM bug).
  private static final String[] excludedThreads = { "SunPKCS11" };

  private static boolean isExcludedThread(Thread th) {
    for (String badThread : excludedThreads) {
      if (th.getName().contains(badThread)) {
        return true;
      }
    }
    return false;
  }

  private static void checkRemainingThreads(Set<Thread> old) throws Exception {
    Thread.sleep(15000);

    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    threads.removeAll(old);
    if (threads.size() != 0) {
      System.out.println("Following threads are not clean up:");
      Iterator<Thread> it = threads.iterator();
      while (it.hasNext()) {
        Thread th = it.next();
        if (isExcludedThread(th)) {
          it.remove();
          continue;
        }
        System.out.println("Thread: " + th.getName());
      }
    }
    assertTrue("This is not a clean shutdown", threads.size() == 0);
  }

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
    checkRemainingThreads(threadsBefore);
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

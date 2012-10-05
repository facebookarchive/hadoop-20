package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarFastCopy {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static Log LOG = LogFactory.getLog(TestAvatarFastCopy.class);
  private static boolean pass = true;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    conf.setInt("fs.avatar.failover.checkperiod", 200);
    conf.setInt("dfs.blockreport.intervalMsec", 500);
    conf.setBoolean("fs.ha.retrywrites", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testFastCopy() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testFastCopy", 1, 1, 1024 * 5);
    util.createFiles(fs, "/testFastCopy");
    FastCopy fcp = new FastCopy(conf);
    String[] files = util.getFileNames("/testFastCopy");
    assertEquals(1, files.length);
    fcp.copy(files[0], "/dst1", (DistributedFileSystem) fs,
        (DistributedFileSystem) fs);
    assertTrue(FastCopySetupUtil.compareFiles(files[0], fs, "/dst1", fs));

    cluster.failOver();

    fcp.copy(files[0], "/dst2", (DistributedFileSystem) fs,
        (DistributedFileSystem) fs);
    assertTrue(FastCopySetupUtil.compareFiles(files[0], fs, "/dst2", fs));
  }

  private static class FastCopyThread extends Thread {

    private final String src;
    private boolean running;

    public FastCopyThread(String src) {
      this.src = src;
      running = true;
    }

    public void run() {
      try {
        Random r = new Random();
        FastCopy fcp = new FastCopy(conf);
        while (running) {
          int suffix = r.nextInt();
          String dstFile = "/dst" + suffix;
          fcp.copy(src, dstFile, (DistributedFileSystem) fs,
              (DistributedFileSystem) fs);
          pass = FastCopySetupUtil.compareFiles(src, fs, dstFile, fs);
        }
      } catch (Exception e) {
        pass = false;
        LOG.warn("Exception in FastCopy : ", e);
      }
    }
  }

  @Test
  public void testFastCopyUnderFailovers() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testFastCopy", 1, 1, 1024 * 5);
    util.createFiles(fs, "/testFastCopy");
    String[] files = util.getFileNames("/testFastCopy");
    assertEquals(1, files.length);
    FastCopyThread t = new FastCopyThread(files[0]);
    t.start();

    for (int i = 0; i < 3; i++) {
      cluster.failOver();
      cluster.restartStandby();
      Thread.sleep(20000);
    }
    t.running = false;
    t.join();
    assertTrue(pass);
  }

}

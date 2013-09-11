package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient.GetStat;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient.GetAddr;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class FailoverLoadTestUtil {

  protected static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 50;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  protected static volatile boolean pass = true;
  private static Random random = new Random();
  protected static Log LOG = LogFactory.getLog(FailoverLoadTestUtil.class);
  protected static int get_stats = 0;
  protected static int get_addr = 0;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setInt("fs.avatar.failover.checkperiod", 200);
    conf.setInt("dfs.client.block.write.locateFollowingBlock.retries", 20);
    conf.setBoolean("fs.ha.zookeeper.cache", true);
    conf.setLong("dfs.client.rpc.retry.sleep", 200);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    deleteCache();
    createData();
  }

  private static void deleteCache() throws Exception {
    CachingAvatarZooKeeperClient zk = ((DistributedAvatarFileSystem) cluster
        .getFileSystem()).failoverHandler.zk;

    GetStat stat = zk.new GetStat(cluster.getFileSystem().getUri());
    stat.getDataFile(zk.getCacheDir()).delete();
    stat.getLockFile(zk.getCacheDir()).delete();

    GetAddr addr = zk.new GetAddr(cluster.getFileSystem().getUri(), null, false);
    addr.getDataFile(zk.getCacheDir()).delete();
    addr.getLockFile(zk.getCacheDir()).delete();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    InjectionHandler.clear();
    deleteCache();
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  protected static void createData() throws Exception {
    String topDir = "" + random.nextInt();
    DFSTestUtil util = new DFSTestUtil(topDir, 10, 3, MAX_FILE_SIZE);
    util.createFiles(cluster.getFileSystem(), topDir);
  }

  protected static class TestHandler extends InjectionHandler {
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.CACHINGAVATARZK_GET_PRIMARY_ADDRESS) {
        get_addr++;
      } else if (event == InjectionEvent.AVATARZK_GET_REGISTRATION_TIME) {
        get_stats++;
      }
    }
  }

  protected static class LoadThread extends Thread {

    private volatile boolean running = true;
    private final FileSystem fs;

    public LoadThread() throws Exception {
      this(cluster.getFileSystem());
    }

    public LoadThread(FileSystem fs) {
      this.fs = fs;
    }

    public void cancel() {
      running = false;
    }

    public void run() {
      try {
        while (running) {
          String topDir = "" + random.nextInt();
          DFSTestUtil util = new DFSTestUtil(topDir, 1, 1, MAX_FILE_SIZE);
          util.createFiles(fs, topDir);
          pass = util.checkFiles(fs, topDir);
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        LOG.warn("Create failed : ", e);
        pass = false;
      }
    }
  }

}

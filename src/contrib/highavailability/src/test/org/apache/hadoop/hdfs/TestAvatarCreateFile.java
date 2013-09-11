package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.EOFException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarCreateFile {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static boolean pass = true;
  private static Log LOG = LogFactory.getLog(TestAvatarCreateFile.class);

  public void setUp(String name) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setInt("fs.avatar.failover.checkperiod", 1000);
    conf.setLong("dfs.client.rpc.retry.sleep", 1000);
    conf.setBoolean("fs.ha.retrywrites", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    pass = true;
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private static class FailoverThread extends Thread {
    public void run() {
      try {
        // Wait for a few creates to fail.
        Thread.sleep(20000);
        cluster.failOver(true);
      } catch (Exception e) {
        LOG.error("Exception in failover :", e);
        pass = false;
      }
    }
  }

  private class TestHandler extends InjectionHandler {

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.FAILOVERCLIENTPROTOCOL_AFTER_CREATE_FILE) {
        InjectionHandler.clear();
        throw new EOFException("FAIL create connection!");
      }
    }
  }

  @Test
  public void testCreateFile() throws Exception {
    setUp("testCreateFile");
    InjectionHandler.set(new TestHandler());
    cluster.clearZooKeeperNode(0);
    ClientProtocol namenode = ((DistributedAvatarFileSystem) fs).getClient()
        .getNameNodeRPC();
    Thread t = new FailoverThread();
    t.start();
    FsPermission perm = new FsPermission((short) 0264);
    namenode.create("/test", perm, ((DistributedAvatarFileSystem) fs)
        .getClient().getClientName(), true, true, (short) 3, (long) 1024);
    LOG.info("Done with create");
    t.join();
    assertTrue(fs.exists(new Path("/test")));
    assertTrue(pass);
  }

  @Test
  public void testCreateFileWithoutOverwrite() throws Exception {
    setUp("testCreateFileWithoutOverwrite");
    InjectionHandler.set(new TestHandler());
    cluster.clearZooKeeperNode(0);
    ClientProtocol namenode = ((DistributedAvatarFileSystem) fs).getClient()
        .getNameNodeRPC();
    Thread t = new FailoverThread();
    t.start();
    FsPermission perm = new FsPermission((short) 0264);
    namenode.create("/test1", perm, ((DistributedAvatarFileSystem) fs)
        .getClient().getClientName(), false, true, (short) 3, (long) 1024);
    LOG.info("Done with create");
    t.join();
    assertTrue(fs.exists(new Path("/test1")));
    assertTrue(pass);
  }
}

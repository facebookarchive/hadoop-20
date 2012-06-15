package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestPersistTxId {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random;

  public void setUp(boolean simulateEditLogCrash) throws IOException {
    conf = new Configuration();
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    conf.setBoolean("dfs.simulate.editlog.crash", simulateEditLogCrash);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
    random = new Random();
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  private long getCurrentTxId() {
    return cluster.getNameNode().getFSImage().getEditLog().getCurrentTxId();
  }

  public void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  @Test
  public void testTxIdMismatchHard() throws IOException { 
    setUp(false);
    // Create some edits and verify.
    createEdits(20);
    assertEquals(20, getCurrentTxId());
    cluster.getNameNode().getFSImage().getEditLog().setStartTransactionId(50);

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(60, getCurrentTxId());
    // Restart namenode and verify that it fails due to gap in txids.
    try {
      cluster.restartNameNode(0);
    } catch (IOException e) {
      System.out.println("Expected exception : " + e);
      return;
    }
    fail("Did not throw IOException");
  }

  @Test
  public void testTxIdMismatchSoft() throws IOException { 
    setUp(false);
    // Create some edits and verify.
    createEdits(20);
    assertEquals(20, getCurrentTxId());
    cluster.getNameNode().getFSImage().getEditLog().setStartTransactionId(50);

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(60, getCurrentTxId());
    // Restart namenode and verify that it does not fail.
    cluster.restartNameNode(0,
        new String[] {StartupOption.IGNORETXIDMISMATCH.getName()});
    fs = cluster.getFileSystem();
    assertEquals(60, getCurrentTxId());
    createEdits(20);
    assertEquals(80, getCurrentTxId());
  }

  @Test
  public void testPersistTxId() throws IOException {
    setUp(false);
    // Create some edits and verify.
    createEdits(20);
    assertEquals(20, getCurrentTxId());

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(30, getCurrentTxId());
    // Restart namenode and verify.
    cluster.restartNameNode(0);
    fs = cluster.getFileSystem();
    assertEquals(30, getCurrentTxId());

    // Add some more edits and verify.
    createEdits(20);
    assertEquals(50, getCurrentTxId());

    // Now save namespace and verify edits.
    cluster.getNameNode().saveNamespace(true, false);
    assertEquals(50, getCurrentTxId());
    createEdits(20);
    assertEquals(70, getCurrentTxId());
  }

  @Test
  public void testRestartWithCheckpoint() throws IOException {
    setUp(false);
    createEdits(20);
    assertEquals(20, getCurrentTxId());
    cluster.getNameNode().saveNamespace(true, false);
    createEdits(20);
    assertEquals(40, getCurrentTxId());
    cluster.restartNameNode(0);
    createEdits(20);
    assertEquals(60, getCurrentTxId());
  }

  @Test
  public void testCheckpointBySecondary() throws IOException {
    setUp(false);
    SecondaryNameNode sn = new SecondaryNameNode(conf);
    try {
      createEdits(20);
      assertEquals(20, getCurrentTxId());

      sn.doCheckpoint();

      assertEquals(20, getCurrentTxId());
      createEdits(20);
      assertEquals(40, getCurrentTxId());
    } finally {
      sn.shutdown();
    }
  }

  @Test
  public void testCheckpointBySecondaryAcrossRestart() throws IOException {
    setUp(false);
    SecondaryNameNode sn = new SecondaryNameNode(conf);
    try {
      createEdits(20);
      assertEquals(20, getCurrentTxId());

      sn.doCheckpoint();

      assertEquals(20, getCurrentTxId());
      createEdits(20);
      assertEquals(40, getCurrentTxId());

      cluster.restartNameNode(0);

      assertEquals(40, getCurrentTxId());
      createEdits(20);
      assertEquals(60, getCurrentTxId());
    } finally {
      sn.shutdown();
    }
  }

  @Test
  public void testMultipleRestarts() throws IOException {
    setUp(false);
    int restarts = random.nextInt(10);
    restarts = 2;
    int totalEdits = 0;
    for (int i = 0; i < restarts; i++) {
      int edits = getRandomEvenInt(50);
      totalEdits += edits;
      createEdits(edits);
      System.out.println("Restarting namenode");
      cluster.restartNameNode(0);
      System.out.println("Restart done");
      assertEquals(totalEdits, getCurrentTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits, getCurrentTxId());
  }

  @Test
  public void testMultipleRestartsWithCheckPoint() throws IOException {
    setUp(false);
    int restarts = random.nextInt(10);
    int totalEdits = 0;
    for (int i = 0; i < restarts; i++) {
      int edits = getRandomEvenInt(50);
      totalEdits += edits;
      createEdits(edits);
      if (random.nextBoolean()) {
        cluster.getNameNode().saveNamespace(true, false);
      }
      cluster.restartNameNode(0);
      assertEquals(totalEdits, getCurrentTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits, getCurrentTxId());
  }

  @Test
  public void testMultipleNameNodeCrashWithCheckpoint() throws Exception {
    setUp(true);
    int restarts = random.nextInt(10);
    int totalEdits = 0;
    for (int i = 0; i < restarts; i++) {
      int edits = getRandomEvenInt(50);
      totalEdits += edits;
      createEdits(edits);
      if (random.nextBoolean()) {
        cluster.getNameNode().saveNamespace(true, false);
      }
      cluster.getNameNode().getFSImage().getEditLog().logSync();
      cluster.getNameNode().getFSImage().unlockAll();
      cluster.restartNameNode(0);
      assertEquals(totalEdits, getCurrentTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits, getCurrentTxId());
  }

  // Gets a random even int.
  private int getRandomEvenInt(int limit) {
    int n = random.nextInt(limit);
    if (n % 2 == 0) {
      return n;
    } else {
      return n + 1;
    }
  }
}

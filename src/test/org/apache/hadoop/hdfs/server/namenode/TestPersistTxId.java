package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.PushbackInputStream;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestPersistTxId {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random;
  private static Log LOG = LogFactory.getLog(TestPersistTxId.class);

  public void setUp(boolean simulateEditLogCrash) throws IOException {
    conf = new Configuration();
    MiniDFSCluster.clearBaseDirectory(conf);
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    TestPersistTxIdInjectionHandler h = new TestPersistTxIdInjectionHandler();
    h.simulateEditLogCrash = simulateEditLogCrash;
    InjectionHandler.set(h);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
    random = new Random();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null)
      fs.close();
    if (cluster != null)
      cluster.shutdown();
    InjectionHandler.clear();
  }

  private long getLastWrittenTxId() {
    return cluster.getNameNode().getFSImage().getEditLog().getLastWrittenTxId();
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
    assertEquals(20, getLastWrittenTxId());
    cluster.getNameNode().getFSImage().getEditLog().setLastWrittenTxId(50);

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(60, getLastWrittenTxId());
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
    assertEquals(20, getLastWrittenTxId());
    cluster.getNameNode().getFSImage().getEditLog().setLastWrittenTxId(50);

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(60, getLastWrittenTxId());
    
    // we will answer "Continue" at txid mismatch
    PushbackInputStream stream = new PushbackInputStream(System.in,
        100);
    System.setIn(stream);
    // PushbackInputStream processes in reverse order.
    byte input[] = "c".getBytes();
    stream.unread(input);

    // Restart namenode and verify that it does not fail.
    cluster.restartNameNode(0,
        new String[] {StartupOption.IGNORETXIDMISMATCH.getName()});
    fs = cluster.getFileSystem();
    // restarting generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
    assertEquals(60 + 2, getLastWrittenTxId());
    createEdits(20);
    assertEquals(80 + 2, getLastWrittenTxId());
  }

  @Test
  public void testPersistTxId() throws IOException {
    setUp(false);
    // Create some edits and verify.
    createEdits(20);
    assertEquals(20, getLastWrittenTxId());

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(30, getLastWrittenTxId());
    // Restart namenode and verify.
    cluster.restartNameNode(0);
    fs = cluster.getFileSystem();
    
    // restarting generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
    assertEquals(30 + 2, getLastWrittenTxId());

    // Add some more edits and verify.
    createEdits(20);
    assertEquals(50 + 2, getLastWrittenTxId());

    // Now save namespace and verify edits.
    // savenamespace generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
    cluster.getNameNode().saveNamespace(true, false);
    assertEquals(50 + 4, getLastWrittenTxId());
    createEdits(20);
    assertEquals(70 + 4, getLastWrittenTxId());
  }

  @Test
  public void testRestartWithCheckpoint() throws IOException {
    setUp(false);
    createEdits(20);
    assertEquals(20, getLastWrittenTxId());
    cluster.getNameNode().saveNamespace(true, false);
    // SN generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
    createEdits(20);
    assertEquals(40 + 2, getLastWrittenTxId());
    cluster.restartNameNode(0);
    createEdits(20);
    // restarting generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
    assertEquals(60 + 4, getLastWrittenTxId());
  }

  @Test
  public void testCheckpointBySecondary() throws IOException {
    setUp(false);
    SecondaryNameNode sn = new SecondaryNameNode(conf);
    try {
      createEdits(20);
      assertEquals(20, getLastWrittenTxId());

      sn.doCheckpoint();

      // checkpoint generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
      assertEquals(20 + 2, getLastWrittenTxId());
      createEdits(20);
      assertEquals(40 + 2, getLastWrittenTxId());
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
      assertEquals(20, getLastWrittenTxId());

      sn.doCheckpoint();
      // checkpoint generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
      assertEquals(20 + 2, getLastWrittenTxId());
      createEdits(20);
      assertEquals(40 + 2, getLastWrittenTxId());

      // restart generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
      cluster.restartNameNode(0);

      assertEquals(40 + 4, getLastWrittenTxId());
      createEdits(20);
      assertEquals(60 + 4, getLastWrittenTxId());
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
      // restart generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
      assertEquals(totalEdits + 2*(i+1), getLastWrittenTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits + 2*restarts, getLastWrittenTxId());
  }

  @Test
  public void testMultipleRestartsWithCheckPoint() throws IOException {
    setUp(false);
    int sn = 0;
    int restarts = random.nextInt(10);
    int totalEdits = 0;
    for (int i = 0; i < restarts; i++) {
      int edits = getRandomEvenInt(50);
      totalEdits += edits;
      createEdits(edits);
      if (random.nextBoolean()) {
        sn++;
        cluster.getNameNode().saveNamespace(true, false);
      }
      cluster.restartNameNode(0);
      // restart generates OP_START_LOG_SEGMENT, OP_END_LOG_SEGMENT
      assertEquals(totalEdits + 2*(i+1) + (2*sn), getLastWrittenTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits + 2*restarts + (2*sn), getLastWrittenTxId());
  }

  @Test
  public void testMultipleNameNodeCrashWithCheckpoint() throws Exception {
    setUp(true);
    int sn = 0;
    int restarts = random.nextInt(10);
    int totalEdits = 0;
    for (int i = 0; i < restarts; i++) {
      int edits = getRandomEvenInt(50);
      totalEdits += edits;
      createEdits(edits);
      if (random.nextBoolean()) {
        sn++;
        cluster.getNameNode().saveNamespace(true, false);
      }
      cluster.getNameNode().getFSImage().getEditLog().logSync();
      cluster.getNameNode().getFSImage().storage.unlockAll();
      cluster.restartNameNode(0);
      // restart/sn generates OP_END_LOG_SEGMENT, as the shutdown crashes
      assertEquals(totalEdits + (i+1) + (sn*2), getLastWrittenTxId());
    }
    System.out.println("Number of restarts : " + restarts);
    assertEquals(totalEdits + restarts + (sn*2), getLastWrittenTxId());
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
  
  class TestPersistTxIdInjectionHandler extends InjectionHandler {
    boolean simulateEditLogCrash = false;

    @Override
    public boolean _trueCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.FSNAMESYSTEM_CLOSE_DIRECTORY
          && simulateEditLogCrash) {
        LOG.warn("Simulating edit log crash, not closing edit log cleanly as"
            + "part of shutdown");
        return false;
      }
      return true;
    }
  }
}

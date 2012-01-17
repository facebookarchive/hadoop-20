package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPersistTxId {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static Random random;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
    random = new Random();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fs.close();
    cluster.shutdown();
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
  public void testPersistTxId() throws IOException {
    // Create some edits and verify.
    createEdits(20);
    assertEquals(20, getLastWrittenTxId());

    // Closing each file would generate 10 edits.
    fs.close();
    assertEquals(30, getLastWrittenTxId());
    // Restart cluster and verify.
    cluster.restartNameNode(0);
    fs = cluster.getFileSystem();
    assertEquals(30, getLastWrittenTxId());

    // Add some more edits and verify.
    createEdits(20);
    assertEquals(50, getLastWrittenTxId());

    // Now save namespace and verify edits.
    cluster.getNameNode().saveNamespace(true, false);
    assertEquals(50, getLastWrittenTxId());
    createEdits(20);
    assertEquals(70, getLastWrittenTxId());
  }
}

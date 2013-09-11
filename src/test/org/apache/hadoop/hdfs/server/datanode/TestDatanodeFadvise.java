package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile.NativeOperation;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile.PosixFadviseRunnable;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestDatanodeFadvise {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static final int BLOCK_SIZE = 1024 * 1024 * 2;
  private static final int BLOCKS = 2;
  private static AtomicInteger nFadvise = new AtomicInteger();
  private static volatile boolean pass;

  public static void setUp(int dn, Configuration c) throws Exception {
    conf = c;
    conf.setBoolean("dfs.datanode.fadvise.secondary.replicas", true);
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniDFSCluster(conf, dn, true, null);
    pass = true;
    nFadvise.set(0);
  }

  private static class FadviseHandler extends InjectionHandler {
    private final int fadvise;

    public FadviseHandler(int fadvise) {
      this.fadvise = fadvise;
    }
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEventCore.NATIVEIO_POSIX_FADVISE) {
        nFadvise.incrementAndGet();
        int advise = (Integer) args[0];
        if (advise != fadvise) {
          pass = false;
        }
      } else if (event == InjectionEvent.DATANODE_SKIP_NATIVE_OPERATION) {
        NativeOperation op = (NativeOperation)args[1];
        if (op.getClass().equals(PosixFadviseRunnable.class)) {
          nFadvise.incrementAndGet();
          int advise = (Integer) args[0];
          if (advise != fadvise) {
            pass = false;
          }
        }
      }
    }
  }

  @Test
  public void testWriteWithRemoteFadvise() throws Exception {
    InjectionHandler.set(new FadviseHandler(NativeIO.POSIX_FADV_DONTNEED));
    setUp(3, new Configuration());
    DFSTestUtil util = new DFSTestUtil("/testWrite", 1, 3, BLOCK_SIZE * BLOCKS);
    util.createFiles(cluster.getFileSystem(), "/ABC");
    util.checkFiles(cluster.getFileSystem(), "/ABC");

    String[] fileNames = util.getFileNames("/ABC");
    assertEquals(1, fileNames.length);

    long start = System.currentTimeMillis();
    boolean replicated = false;
    long totalBlocks = -1;
    while (!replicated && (System.currentTimeMillis() - start < 30000)) {
      LocatedBlocks blocks = cluster.getNameNode().getBlockLocations(
          fileNames[0], 0, Long.MAX_VALUE);
      replicated = true;
      totalBlocks = blocks.getLocatedBlocks().size();
      for (LocatedBlock block : blocks.getLocatedBlocks()) {
        replicated = (replicated && (block.getLocations().length == 3));
      }
      Thread.sleep(1000);
    }
    assertTrue(replicated);

    assertTrue(NativeIO.isfadvisePossible());
    waitFadvise(2 * totalBlocks);
  }

  private void waitFadvise(long expected) throws Exception {
    long start = System.currentTimeMillis();
    while(System.currentTimeMillis() - start < 30000 && nFadvise.get() != expected) {
      Thread.sleep(1000);
    }
    assertTrue(pass);
    if (NativeIO.isAvailable()) {
      assertEquals(expected , nFadvise.get());
    }
  }


  private void runFileFadvise(int advise, int expectedFadvise) throws Exception {
    nFadvise.set(0);
    InjectionHandler.set(new FadviseHandler(advise));
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    WriteOptions options = new WriteOptions();
    options.setFadvise(advise);
    FSDataOutputStream out = fs.create(new Path("/test"), null, true, 1024,
        (short) 3, BLOCK_SIZE, 512, null, null, options);
    Random r = new Random();
    byte buffer[] = new byte[BLOCK_SIZE];
    r.nextBytes(buffer);
    out.write(buffer);
    out.close();

    assertTrue(NativeIO.isfadvisePossible());
    waitFadvise(expectedFadvise);
  }

  @Test
  public void testFileFadvise() throws Exception {
    setUp(1, new Configuration());
    for (int advise = 1; advise < 6; advise++) {
      runFileFadvise(advise, 1);
    }
  }

  @Test
  public void testFileRangeFadvise() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("dfs.datanode.fadvise.filerange.size", BLOCK_SIZE / 20);
    setUp(1, conf);
    for (int advise = 1; advise < 6; advise++) {
      // 10 fadvise since we have 1MB buffer at the end of a block.
      // 1 extra fadvise while closing the block.
      runFileFadvise(advise, 11);
    }
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    InjectionHandler.clear();
  }

}

package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.Utils.Builder;

import junit.framework.TestCase;

public class TestDirectoryRaidParityMovement extends TestCase {
  
  final static String TEST_DIR = new File(
      System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CHECKSUM_STORE_DIR = new File(TEST_DIR,
      "ckm_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String STRIPE_STORE_DIR = new File(TEST_DIR,
      "stripe_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(
            "org.apache.hadoop.raid.TestDirectoryRaidParityMovement");
  final static Random rand = new Random();
  final static int NUM_DATANODES = 3;
  final long blockSize = 8192L;
  final long[] fileSizes =
      new long[]{blockSize + blockSize/2, // block 0, 1
      3*blockSize,                  // block 2, 3
      blockSize + blockSize/2 + 1}; // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{blockSize, 2*blockSize, blockSize/2};
  
  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  Path root = null;
  Set<String> allExpectedMissingFiles = null;

  protected void mySetup() 
      throws Exception {
    conf = new Configuration();
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() 
                          + "/logs");
    }
    
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    
    // the RaidNode does the raiding inline
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    // use local block fixer
    conf.set("raid.blockfix.classname", 
        "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    
    conf.set("raid.server.address", "localhost:0");
    conf.setInt("fs.trash.interval", 1440);
    
    Utils.loadAllTestCodecs(conf, 5, 1, 3, "/destraid", "/destraidrs", 
        "/destraiddir", "/destraiddirrs");
    
    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();
    
    FileSystem.setDefaultUri(conf, namenode);
  }
  
  private void stopCluster() {
    if (null != dfs) {
      dfs.shutdown();
    }
  }

  private DistributedRaidFileSystem getRaidFS() throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl", 
             "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl", 
             "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    return (DistributedRaidFileSystem)FileSystem.get(dfsUri, clientConf);
  }

  private void doRaid(Path srcPath, Codec codec) throws IOException {
    RaidNode.doRaid(conf, fileSys.getFileStatus(srcPath),
              new Path("/destraid"), codec, 
              new RaidNode.Statistics(), 
                RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1);
  }
  
  public void testRenameOneFile() throws Exception {
    try {
      long[] crcs = new long[3];
      int[] seeds = new int[3];
      short repl = 1;
      Path dirPath = new Path("/user/dikang/raidtest");
      
      mySetup();
      DistributedRaidFileSystem raidFs = getRaidFS();
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = raidFs.getFileStatus(dirPath); 
      Codec codec = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      
      Path destPath = new Path("/user/dikang/raidtest_new");
      
      assertTrue(raidFs.exists(dirPath));
      assertFalse(raidFs.exists(destPath));
      
      ParityFilePair parity = ParityFilePair.getParityFile(
          codec, stat, conf);
      Path srcParityPath = parity.getPath();
      assertTrue(raidFs.exists(srcParityPath));
      
      raidFs.mkdirs(destPath);
      // do the rename file
      assertTrue(raidFs.rename(files[0], new Path(destPath, "file0")));
      // verify the results.
      assertFalse(raidFs.exists(files[0]));
      assertTrue(raidFs.exists(new Path(destPath, "file0")));
      assertTrue(raidFs.exists(srcParityPath));
      
      // rename the left files
      assertTrue(raidFs.rename(files[1], new Path(destPath, "file1")));
      assertTrue(raidFs.rename(files[2], new Path(destPath, "file2")));
      assertFalse(raidFs.exists(srcParityPath));
      
      Path newParityPath = new Path(codec.parityDirectory, 
          "user/dikang/raidtest_new");
      assertTrue(raidFs.exists(newParityPath));
    } finally {
      stopCluster();
    }
  }
  
  public void testRename() throws Exception {
    try {
      
      long[] crcs = new long[3];
      int[] seeds = new int[3];
      short repl = 1;
      Path dirPath = new Path("/user/dikang/raidtest");
      
      mySetup();
      DistributedRaidFileSystem raidFs = getRaidFS();
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = raidFs.getFileStatus(dirPath); 
      Codec codec = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      
      Path destPath = new Path("/user/dikang/raidtest_new");
      
      assertTrue(raidFs.exists(dirPath));
      assertFalse(raidFs.exists(destPath));
      
      ParityFilePair parity = ParityFilePair.getParityFile(
          codec, stat, conf);
      Path srcParityPath = parity.getPath();
      assertTrue(raidFs.exists(srcParityPath));
      // do the rename file
      assertTrue(raidFs.rename(dirPath, destPath));
      // verify the results.
      assertFalse(raidFs.exists(dirPath));
      assertTrue(raidFs.exists(destPath));
      assertFalse(raidFs.exists(srcParityPath));
      FileStatus srcDest = raidFs.getFileStatus(destPath);
      parity = ParityFilePair.getParityFile(codec, srcDest, conf);
      assertTrue(raidFs.exists(parity.getPath()));
    } finally {
      stopCluster();
    }
  }
  
  public void testDealWithParity() throws Exception {
    try {
      long[] crcs = new long[3];
      int[] seeds = new int[3];
      short repl = 1;
      Path dirPath = new Path("/user/dikang/raidtest");
      
      mySetup();
      DistributedRaidFileSystem raidFs = getRaidFS();
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = raidFs.getFileStatus(dirPath); 
      Codec codec = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      ParityFilePair parity = ParityFilePair.getParityFile(
          codec, stat, conf);
      Path srcParityPath = parity.getPath();
      assertTrue(raidFs.exists(srcParityPath));
      Path destPath = new Path("/user/dikang/raidtest_new");
      
      // try to delete the parity
      try {
        raidFs.delete(srcParityPath);
        fail();
      } catch (IOException e) {
        LOG.warn("Expected exception: " + e.getMessage(), e);
        assertTrue(e.getMessage().startsWith(
            "You can not delete a parity file"));
      }
      
      // try to rename the parity
      try {
        raidFs.rename(srcParityPath, destPath);
        fail();
      } catch (IOException e) {
        LOG.warn("Expected exception: " + e.getMessage(), e);
        assertTrue(e.getMessage().startsWith(
            "You can not rename a parity file"));
      }
      
    } finally {
      stopCluster();
    }
  }
  
  public void testDeleteOneFile() throws Exception {
    try {
      long[] crcs = new long[3];
      int[] seeds = new int[3];
      short repl = 1;
      Path dirPath = new Path("/user/dikang/raidtest");
      
      mySetup();
      DistributedRaidFileSystem raidFs = getRaidFS();
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = raidFs.getFileStatus(dirPath); 
      Codec codec = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      
      ParityFilePair parity = ParityFilePair.getParityFile(
          codec, stat, conf);
      Path srcParityPath = parity.getPath();
      assertTrue(raidFs.exists(srcParityPath));
      
      // delete one file
      assertTrue(raidFs.delete(files[0]));
      // verify the results
      assertFalse(raidFs.exists(files[0]));
      // we still have the parity file
      assertTrue(raidFs.exists(srcParityPath));
      
      // delete the left files
      assertTrue(raidFs.delete(files[1]));
      assertTrue(raidFs.delete(files[2]));
      
      // we will not touch the parity file.
      assertTrue(raidFs.exists(srcParityPath));
    } finally {
      stopCluster();
    }
  }

  public void testDeleteAndUndelete() throws Exception {
    try {
      long[] crcs = new long[3];
      int[] seeds = new int[3];
      short repl = 1;
      Path dirPath = new Path("/user/dikang/raidtest");
      
      mySetup();
      DistributedRaidFileSystem raidFs = getRaidFS();
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = raidFs.getFileStatus(dirPath); 
      Codec codec = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      
      ParityFilePair parity = ParityFilePair.getParityFile(
          codec, stat, conf);
      Path srcParityPath = parity.getPath();
      assertTrue(raidFs.exists(srcParityPath));

      // do the delete file
      assertTrue(raidFs.delete(dirPath));

      // verify the results.
      assertFalse(raidFs.exists(dirPath));
      assertFalse(raidFs.exists(srcParityPath));

      // do the undelete using non-exist userName
      String nonExistedUser = UUID.randomUUID().toString();
      assertFalse(raidFs.undelete(dirPath, nonExistedUser));

      // verify the results
      assertFalse(raidFs.exists(dirPath));
      assertFalse(raidFs.exists(srcParityPath));

      // do the undelete file using current userName
      assertTrue(raidFs.undelete(dirPath, null));
      //verify the results.
      assertTrue(raidFs.exists(dirPath));
      assertTrue(raidFs.exists(srcParityPath));
    } finally {
      stopCluster();
    }
  }
  
  public void testDeleteDirRaidedFile() throws Exception {
    
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    short repl = 1;
    Path dirPath = new Path("/user/dikang/raidtest");
    
    mySetup();
    //disable trash
    conf.setInt("fs.trash.interval", 0);
    
    DistributedRaidFileSystem raidFs = getRaidFS();
    Path[] files = TestRaidDfs.createTestFiles(dirPath,
        fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
    FileStatus stat = raidFs.getFileStatus(dirPath); 
    Codec codec = Codec.getCodec("dir-rs");
    RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
        new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
      false, repl, repl);
    
    try {
      raidFs.delete(files[0]);
      fail();
    } catch (Exception ex) {
      LOG.warn("Excepted error: " + ex.getMessage(), ex);
    } finally {
      stopCluster();
    }
  }
}

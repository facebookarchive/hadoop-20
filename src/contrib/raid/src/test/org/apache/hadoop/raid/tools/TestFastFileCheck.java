package org.apache.hadoop.raid.tools;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.ParityFilePair;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.TestDirectoryRaidDfs;
import org.apache.hadoop.raid.Utils;

import junit.framework.TestCase;

public class TestFastFileCheck extends TestCase {
  
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
            "org.apache.hadoop.raid.tools.TestFastFileCheck");
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
    
    Utils.loadAllTestCodecs(conf, 10, 1, 4, "/raid", "/raidrs", 
        "/raiddir", "/raiddirrs");
    
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
  
  private void doRaid(Path srcPath, Codec codec) throws IOException {
    RaidNode.doRaid(conf, fileSys.getFileStatus(srcPath),
              new Path(codec.parityDirectory), codec, 
              new RaidNode.Statistics(), 
                RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1);
  }
  
  public void testVerifySourceFile() throws Exception {
    mySetup();
    
    try {
      Path srcPath = new Path("/user/dikang/raidtest/file0");
      int numBlocks = 8;
      TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
          1, numBlocks, 8192L);
      assertTrue(fileSys.exists(srcPath));
      Codec codec = Codec.getCodec("rs");
      FileStatus stat = fileSys.getFileStatus(srcPath);
      
      // verify good file
      assertTrue(FastFileCheck.checkFile(conf, (DistributedFileSystem)fileSys, 
          fileSys, srcPath, null, codec, 
          RaidUtils.NULL_PROGRESSABLE, true));
      
      // verify bad file
      LocatedBlocks fileLoc =
          RaidDFSUtil.getBlockLocations((DistributedFileSystem)fileSys, 
              srcPath.toUri().getPath(),
              0, stat.getLen());
      // corrupt file1
      Random rand = new Random();
      int idx = rand.nextInt(numBlocks);
      TestRaidDfs.corruptBlock(srcPath, 
          fileLoc.getLocatedBlocks().get(idx).getBlock(), 
          NUM_DATANODES, true, dfs);
      
      assertFalse(FastFileCheck.checkFile(conf, (DistributedFileSystem)fileSys, 
          fileSys, srcPath, null, codec, 
          RaidUtils.NULL_PROGRESSABLE, true));
    } finally {
      stopCluster();
    }
  }
  
  public void testVerifyFile() throws Exception {
    mySetup();
    
    try {
      Path srcPath = new Path("/user/dikang/raidtest/file0");
      TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
          1, 8, 8192L);
      
      assertTrue(fileSys.exists(srcPath));
      Codec codec = Codec.getCodec("rs");

      // generate the parity files.
      doRaid(srcPath, codec);
      FileStatus stat = fileSys.getFileStatus(srcPath);
      
      // verify the GOOD_FILE
      ParityFilePair pfPair = ParityFilePair.getParityFile(codec, stat, conf);
      assertNotNull(pfPair);
      assertTrue(FastFileCheck.checkFile(conf, (DistributedFileSystem)fileSys, 
          fileSys, srcPath, pfPair.getPath(), codec, 
          RaidUtils.NULL_PROGRESSABLE, false));
      
      // verify the BAD_FILE
      fileSys.delete(srcPath);
      TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
          1, 8, 8192L);
      fileSys.setTimes(pfPair.getPath(), 
          fileSys.getFileStatus(srcPath).getModificationTime(), -1);
      stat = fileSys.getFileStatus(srcPath);
      pfPair = ParityFilePair.getParityFile(codec, stat, conf);
      assertNotNull(pfPair);
      assertFalse(FastFileCheck.checkFile(conf, (DistributedFileSystem)fileSys, 
          fileSys, srcPath, pfPair.getPath(), codec, 
          RaidUtils.NULL_PROGRESSABLE, false));
    } finally {
      stopCluster();
    }
  }
}

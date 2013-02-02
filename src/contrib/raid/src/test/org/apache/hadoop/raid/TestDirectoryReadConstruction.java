package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;

import junit.framework.TestCase;

public class TestDirectoryReadConstruction extends TestCase {
  
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
      "org.apache.hadoop.raid.TestDirectoryReadConstruction");
  final static Random rand = new Random();
  final static int NUM_DATANODES = 3;
  final long blockSize = 8192L;
  final long[] fileSizes =
      new long[]{blockSize + blockSize/2, // block 0, 1
      3*blockSize,                  // block 2, 3
      blockSize + blockSize/2 + 1}; // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{blockSize, 2*blockSize, blockSize/2};
  final Integer[] rsCorruptFileIdx1 = new Integer[]{0, 1, 2, 3, 5, 6, 7};
  final int[] rsNumCorruptBlocksInFiles1 = new int[] {2, 2, 3};
  
  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfsCluster = null;
  FileSystem fileSys = null;
  Path root = null;
  Set<String> allExpectedMissingFiles = null;
  
  protected void mySetup(int stripeLength) throws IOException {
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
    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 
        1, 3, "/destraid", "/destraidrs", false, true);
    conf.setInt("fs.trash.interval", 1440);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set(RaidNode.RAID_CHECKSUM_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalChecksumStore");
    conf.setBoolean(RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY, true);
    conf.set(LocalChecksumStore.LOCAL_CHECK_STORE_DIR_KEY, CHECKSUM_STORE_DIR);
    conf.set(RaidNode.RAID_STRIPE_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalStripeStore");
    conf.set(LocalStripeStore.LOCAL_STRIPE_STORE_DIR_KEY, STRIPE_STORE_DIR);
    
    File stripeStoreDir = new File(STRIPE_STORE_DIR);
    FileUtil.fullyDelete(stripeStoreDir);
    stripeStoreDir.mkdirs();
    stripeStoreDir.deleteOnExit();

    File checksumStoreDir = new File(CHECKSUM_STORE_DIR);
    FileUtil.fullyDelete(checksumStoreDir);
    checksumStoreDir.mkdirs();
    checksumStoreDir.deleteOnExit();
  }
  
  private void myTearDown() throws IOException {
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }

  public void testReadWithDeletingFiles() throws IOException {
    implReadFromStripeInfo(0);
  }
  
  public void testReadWithAddingFiles() throws IOException {
    implReadFromStripeInfo(1);
  }
  
  public void testReadWithRenamingFiles() throws IOException {
    implReadFromStripeInfo(2);
  }
  
  public void implReadFromStripeInfo(int operatorId) 
      throws IOException {
    final String testName = "testReadFromStripeInfo";
    LOG.info("Test " + testName + " started.");
    int stripeLength = 3;
    mySetup(stripeLength);
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    
    Path dirPath =new Path("/user/dikang/raidtestrs");
    Path[] files = TestRaidDfs.createTestFiles(dirPath, fileSizes, 
        blockSizes, crcs, seeds, fileSys, (short)1);
    LOG.info("Test " + testName + "created test files");

    FileStatus stat = fileSys.getFileStatus(dirPath);
    Codec codec = Codec.getCodec("rs");
    Path destPath = new Path("/destraidrs");
    try {
      RaidNode.doRaid(conf, stat, destPath, codec, new RaidNode.Statistics(), 
          RaidUtils.NULL_PROGRESSABLE, false, 1, 1);
      
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      
      // corrupt files
      this.corruptFiles(dirPath, crcs, rsCorruptFileIdx1, dfs, files, 
          rsNumCorruptBlocksInFiles1);

      if (operatorId == 0) {
        // delete file
        dfs.delete(files[0], true);
        LOG.info("Delete file: " + files[0]);
      } else if (operatorId == 1) {
        // add file
        TestRaidDfs.createTestFile(dfs, new Path(dirPath, "file3"), (short)1, 
                          5, blockSize);
      } else {
        // rename file
        Path newFile = new Path(files[0].toUri().getPath() + "_rename");
        dfs.rename(files[0], newFile);
        files[0] = newFile;
      }

      FileStatus newDirStat = dfs.getFileStatus(dirPath);
      // assert the modification is changed.
      assertNotSame(stat.getModificationTime(), 
          newDirStat.getModificationTime());
      
      // verify the files
      for (int i = 1; i < fileSizes.length; i++) {
        assertTrue("file " + files[i] + " not fixed",
            TestRaidDfs.validateFile(getRaidFS(), files[i], fileSizes[i],
              crcs[i]));
      }
      
    } finally {
      myTearDown();
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
  
  private void corruptFiles(Path dirPath, long[] crcs, 
      Integer[] corruptBlockIdxs, DistributedFileSystem dfs,
      Path[] files, int[] numCorruptBlocksInFiles) throws IOException {
    int totalCorruptFiles = DFSUtil.getCorruptFiles(dfs).length;
    TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
        crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);
    
    String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
    for (int i = 0; i < numCorruptBlocksInFiles.length; i++) {
      if (numCorruptBlocksInFiles[i] > 0)
        totalCorruptFiles++;
    }
    assertEquals("files not corrupted", totalCorruptFiles,
        corruptFiles.length);
    for (int i = 0; i< fileSizes.length; i++) {
      assertEquals("wrong number of corrupt blocks for file " + 
          files[i], numCorruptBlocksInFiles[i],
          RaidDFSUtil.corruptBlocksInFile(dfs,
          files[i].toUri().getPath(), 0, fileSizes[i]).size());
    }
  }
}

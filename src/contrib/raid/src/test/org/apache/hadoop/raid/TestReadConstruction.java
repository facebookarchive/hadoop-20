package org.apache.hadoop.raid;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import junit.framework.TestCase;

public class TestReadConstruction extends TestCase {

  final static String TEST_DIR = new File(
      System.getProperty("test.build.data",
          "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestReadConstruction");
  final static Random rand = new Random();
  final static int NUM_DATANODES = 3;

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  Path root = null;
  Set<String> allExpectedMissingFiles = null;
  
  protected void mySetup() throws Exception {
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
    Utils.loadTestCodecs(conf, 10, 1, 4, "/destraid", "/destraidrs");
    conf.setInt("fs.trash.interval", 1440);

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
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
              new Path(codec.parityDirectory), codec, 
              new RaidNode.Statistics(), 
              RaidUtils.NULL_PROGRESSABLE,
              false, 1, 1);
  }

  
  private boolean doThePartialTest(Codec codec,
                                int blockNum,
                                int[] corruptBlockIdxs) throws Exception {
    long blockSize = 8192 * 1024L;
    int bufferSize = 4192 * 1024;

    Path srcPath = new Path("/user/dikang/raidtest/file" + 
                            UUID.randomUUID().toString());

    long crc = TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
        1, blockNum, blockSize);
    
    DistributedRaidFileSystem raidFs = getRaidFS();
    assertTrue(raidFs.exists(srcPath));
    
    // generate the parity files.
    doRaid(srcPath, codec);

    FileStatus file1Stat = fileSys.getFileStatus(srcPath);
    long length = file1Stat.getLen();
    LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations((DistributedFileSystem)fileSys, 
            srcPath.toUri().getPath(),
            0, length);
    // corrupt file1
    
    for (int idx: corruptBlockIdxs) {
      corruptBlock(file1Loc.get(idx).getBlock(), 
                                dfs);
    }
    RaidDFSUtil.reportCorruptBlocks((DistributedFileSystem)fileSys, srcPath,
                         corruptBlockIdxs, blockSize);
    
    // verify the partial read
    byte[] buffer = new byte[bufferSize];
    FSDataInputStream in = raidFs.open(srcPath);
    
    long numRead = 0;
    CRC32 newcrc = new CRC32();
    
    int num = 0;
    while (num >= 0) {
      num = in.read(numRead, buffer, 0, bufferSize);
      if (num < 0) {
        break;
      }
      numRead += num;
      newcrc.update(buffer, 0, num);
    }
    in.close();

    if (numRead != length) {
      LOG.info("Number of bytes read " + numRead +
               " does not match file size " + length);
      return false;
    }

    LOG.info(" Newcrc " + newcrc.getValue() + " old crc " + crc);
    if (newcrc.getValue() != crc) {
      LOG.info("CRC mismatch of file " + srcPath.toUri().getPath() + ": " + 
                newcrc.getValue() + " vs. " + crc);
      return false;
    }
    return true;
  }
  
  public void testReadCorruptPartialSuccessRS() throws Exception {
    LOG.info("testReadCorruptPartialSuccessRS started");
    try {
      mySetup();
      int blockNum = 10;
      int[][] corruptBlockIdxs = new int[][] {{0, 3, 6, 9}, {0, 1, 2, 3}};
      for (int i=0; i<corruptBlockIdxs.length; i++) {
        assertTrue(doThePartialTest(Codec.getCodec("rs"), 
            blockNum, corruptBlockIdxs[i]));
      }
    } finally {
      if (null != dfs) {
        dfs.shutdown();
      }
      LOG.info("testReadCorruptPartialSuccessRS completed");
    }
  }
  
  public void testReadCorruptPartialSuccessXOR() throws Exception {
    LOG.info("testReadCorruptPartialSuccessXOR started");
    try {
      mySetup();
      int blockNum = 10;
      for (int i=0; i<blockNum; i++) {
        int[] corruptBlockIdxs = new int[]{i};
        LOG.info("Corrupt Block " + i + 
            " in testReadCorruptPartialSuccessXOR.");
        assertTrue(doThePartialTest(Codec.getCodec("xor"), 
                  blockNum, corruptBlockIdxs));
      }
    } finally {
      if (null != dfs) {
        dfs.shutdown();
      }
      LOG.info("testReadCorruptPartialSuccessXOR completed");
    }
  }
  
  public void testReadCorruptPartialFailRS() throws Exception {
    LOG.info("testReadCorruptPartialFailRS started");
    try {
      mySetup();
      int blockNum = 10;
      int[] corruptBlockIdxs = new int[]{0, 4, 6, 8, 9};
      doThePartialTest(Codec.getCodec("rs"), 
                        blockNum, corruptBlockIdxs);
      fail();
    } catch(ChecksumException e) {
      LOG.warn("Expected exception: " + e.getMessage(), e);
    } catch(BlockMissingException e) {
      LOG.warn("Expected exception: " + e.getMessage(), e);
    } finally {
      if (null != dfs) {
        dfs.shutdown();
      }
      LOG.info("testReadCorruptPartialFailRS completed");
    }
  }
  
  public void testReadCorruptPartialFailXOR() throws Exception {
    LOG.info("testReadCorruptPartialFailXOR started");
    try {
      mySetup();
      int blockNum = 10;
      int[] corruptBlockIdxs = new int[]{0, 1};
      doThePartialTest(Codec.getCodec("xor"), 
                        blockNum, corruptBlockIdxs);
      fail();
    } catch(ChecksumException e) {
      LOG.warn("Expected exception: " + e.getMessage(), e);
    } catch(BlockMissingException e) {
      LOG.warn("Expected exception: " + e.getMessage(), e);
    } finally {
      if (null != dfs) {
        dfs.shutdown();
      }
      LOG.info("testReadCorruptPartialFailXOR completed");
    }
  }
  
  static void corruptBlock(Block block, MiniDFSCluster dfs) 
      throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= TestDatanodeBlockScanner.corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block: " + block, corrupted);
  }
}

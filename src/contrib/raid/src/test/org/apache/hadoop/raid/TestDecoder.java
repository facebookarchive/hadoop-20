package org.apache.hadoop.raid;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.TestBlockFixer;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import junit.framework.TestCase;

public class TestDecoder extends TestCase {
  
  final static Log LOG = LogFactory.getLog(TestDecoder.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  Random rand = new Random();
  
  private void mySetup(int stripeLength) throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:0");

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3, "/destraid",
        "/destraidrs", true, "org.apache.hadoop.raid.XORCode", 
        "org.apache.hadoop.raid.ReedSolomonCode",
        false);

    conf.setBoolean("dfs.permissions", false);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
  }
  
  private void doRaid(Path srcPath, Codec codec) throws IOException {
    RaidNode.doRaid(conf, fileSys.getFileStatus(srcPath),
              new Path(codec.parityDirectory), codec, 
              new RaidNode.Statistics(), 
              RaidUtils.NULL_PROGRESSABLE,
              false, 1, 1);
  }
  
  public void testFixBlock() throws Exception {
    int[] parallelisms = new int[] {1, 3, 4};
    try {
      mySetup(5);
      for (int parallelism : parallelisms) {
        verifyDecoder("xor", parallelism);
        verifyDecoder("rs", parallelism);
      }
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
  }
  
  public void verifyDecoder(String code, int parallelism) throws Exception {
    Codec codec = Codec.getCodec(code);
    conf.setInt("raid.encoder.parallelism", parallelism);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("RaidTest1", "/user/dikang/raidtest/file" + code + parallelism,
        1, 1, code);
    cb.persist();
    Path srcPath = new Path("/user/dikang/raidtest/file" + code + parallelism +
        "/file1");
    long blockSize = 8192 * 1024L;
    
    long crc = TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
        1, 7, blockSize);
    doRaid(srcPath, codec);
    FileStatus srcStat = fileSys.getFileStatus(srcPath);
    ParityFilePair pair = ParityFilePair.getParityFile(codec, srcStat, conf);
    
    FileStatus file1Stat = fileSys.getFileStatus(srcPath);
    long length = file1Stat.getLen();
    LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations((DistributedFileSystem)fileSys, 
            srcPath.toUri().getPath(),
            0, length);
     
    // corrupt file
      
    int[] corruptBlockIdxs = new int[] {5};
    long errorOffset = 5 * blockSize;
    for (int idx: corruptBlockIdxs) {
      TestBlockFixer.corruptBlock(file1Loc.get(idx).getBlock(), dfsCluster);
    }
      
    RaidDFSUtil.reportCorruptBlocks((DistributedFileSystem)fileSys, srcPath,
        corruptBlockIdxs, blockSize);
    
    Decoder decoder = new Decoder(conf, codec);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    decoder.codec.simulateBlockFix = true;
    CRC32 oldCRC = decoder.fixErasedBlock(fileSys, srcStat, fileSys, 
        pair.getPath(), true, blockSize, errorOffset, blockSize, false, 
        out, null, null, false);
    
    decoder.codec.simulateBlockFix = false;
    out = new ByteArrayOutputStream();
    decoder.fixErasedBlock(fileSys, srcStat, fileSys, 
        pair.getPath(), true, blockSize, errorOffset, blockSize, false, 
        out, null, null, false);
    
    // calculate the new crc
    CRC32 newCRC = new CRC32();
    byte[] constructedBytes = out.toByteArray();
    newCRC.update(constructedBytes);
    
    assertEquals(oldCRC.getValue(), newCRC.getValue());
  }

}

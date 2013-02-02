package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import junit.framework.TestCase;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.apache.hadoop.raid.TestRaidNodeSelectFiles.TestRaidNodeInjectionHandler;
import org.apache.hadoop.raid.tools.FastFileCheck;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

public class TestMultiTasksEncoding extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_FILES = 8;
  final short targetReplication = 1;
  final short metaReplication = 1;
  final long stripeLength      = 3;
  final short srcReplication = 2;
  final static Log LOG =
      LogFactory.getLog("org.apache.hadoop.raid.TestMultiTasksEncoding");
  final static Random rand = new Random();

  Configuration conf;
  String namenode = null;
  MiniDFSCluster cluster = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  DistributedFileSystem dfs = null;
  String jobTrackerName = null;

  /**
   * create mapreduce and dfs clusters
   */
  protected void createClusters(boolean startMR) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    conf.setLong("dfs.blockreport.intervalMsec", 8000L);
    conf.setLong("dfs.client.rpc.retry.sleep", 1000L);
    conf.setLong(JobMonitor.JOBMONITOR_INTERVAL_KEY, 20000L);
    conf.setLong(RaidNode.TRIGGER_MONITOR_SLEEP_TIME_KEY, 3000L);
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
    conf.set("mapred.raid.http.address", "localhost:0");
    conf.setInt("dfs.datanode.numblocks", 1000);
    // keep them for debuging
    // conf.setInt("raid.decoder.bufsize", 512);
    // conf.setInt("raid.encoder.bufsize", 512);

    // scan every policy every 5 seconds
    conf.setLong("raid.policy.rescan.interval", 5 * 1000L);
    conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");

    // use local block fixer
    conf.set("raid.blockfix.classname", 
        TestDirectoryParityRegenerator.FakeBlockIntegerityMonitor.class.getName());
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");

    conf.set("raid.server.address", "localhost:0");
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 2);
    // raid two stripes at a time
    conf.setLong(RaidNode.RAID_ENCODING_STRIPES_KEY, 2L);
    // set checksum store
    TestBlockFixer.setChecksumStoreConfig(conf);
    TestRaidNode.loadTestCodecs(conf, 3, 3, 1, 4);

    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    dfs = (DistributedFileSystem)fileSys;
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    namenode = fileSys.getUri().toString();    
    FileSystem.setDefaultUri(conf, namenode);
    if (startMR) {
      mr = new MiniMRCluster(taskTrackers, namenode, 3);
      jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      conf.set("mapred.job.tracker", jobTrackerName);
    }
  }
    
  /**
   * stop clusters created earlier
   */
  protected void stopClusters() throws Exception {
    if (mr != null) { mr.shutdown(); }
    if (cluster != null) { cluster.shutdown(); }
  }
  
  public void createTestFiles(Path raidDir,
      HashMap<Codec, Path> filePaths,
      HashMap<Codec, Long[]> fileCRCs, 
      HashMap<Codec, String> fileLists) throws Exception {
    long[] fileSizes = new long[NUM_FILES];
    int[] seeds = new int[NUM_FILES];
    long[] blockSizes = new long[NUM_FILES];
    long totalSize = 0L;
    long seed = rand.nextLong();
    LOG.info("Use Seed " + seed + " to generate test file");
    rand.setSeed(seed);
    for (int i = 0; i < NUM_FILES; i++) {
      blockSizes[i] = rand.nextInt(20) * 512 + 4096;
      if (i > 4) {
        // small file
        fileSizes[i] = rand.nextInt((int)blockSizes[i]) + 1; 
      } else {
        fileSizes[i] = (rand.nextInt((int)stripeLength) + stripeLength)
            * blockSizes[i] + rand.nextLong() % blockSizes[i];
      }
      totalSize += fileSizes[i];
    }
    for (Codec codec : Codec.getCodecs()) {
      Path srcDir = new Path(raidDir, codec.id);
      long crcs[];
      if (codec.isDirRaid) {
        crcs = new long[NUM_FILES];
        TestRaidDfs.createTestFiles(srcDir, fileSizes,
            blockSizes, crcs, seeds, fileSys, srcReplication);
      } else {
        crcs = new long[1];
        crcs[0] = TestRaidDfs.createTestFile(fileSys,
            new Path(srcDir, "0"), (int)srcReplication,
            totalSize, blockSizes[0], rand.nextInt());
      }
      fileCRCs.put(codec, ArrayUtils.toObject(crcs));
      FileStatus[] files = fileSys.listStatus(srcDir);
      assertEquals(files.length, crcs.length);
      Path filePath = (codec.isDirRaid)? srcDir: new Path(srcDir, "0");
      if (fileLists != null) {
        FSDataOutputStream out = fileSys.create(new Path(fileLists.get(codec)));
        out.write(filePath.toString().getBytes());
        out.write("\n".getBytes());
        out.close();
      }
      if (filePaths != null) {
        filePaths.put(codec, filePath);
      }
    }
  }
  
  public void verifyCorrectness(Path raidDir,
      HashMap<Codec, Long[]> fileCRCs, 
      HashMap<Codec, String> fileLists) throws Exception {
    ChecksumStore ckmStore = RaidNode.createChecksumStore(conf, false);
    StripeStore stripeStore = RaidNode.createStripeStore(conf, false, fileSys);
    for (Codec codec : Codec.getCodecs()) {
      Path srcDir = new Path(raidDir, codec.id);
      FileStatus[] srcFiles = fileSys.listStatus(srcDir);
      HashSet<Block> blks = new HashSet<Block>();
      for (FileStatus stat : srcFiles) {
        ParityFilePair pfPair = ParityFilePair.getParityFile(codec,
            stat, conf);
        assertNotNull(pfPair);
        assertTrue(FastFileCheck.checkFile(conf, dfs, 
            fileSys, stat.getPath(), pfPair.getPath(), codec, null, false));
        LocatedBlocks lbs = dfs.getLocatedBlocks(stat.getPath(), 0,
            stat.getLen());
        for (LocatedBlock lb : lbs.getLocatedBlocks()) {
          assertNotNull(ckmStore.getChecksum(lb.getBlock()));
          blks.add(lb.getBlock());
        }
        LocatedBlocks plbs = dfs.getLocatedBlocks(
            pfPair.getPath(), 0, pfPair.getFileStatus().getLen());
        for (LocatedBlock lb : plbs.getLocatedBlocks()) {
          assertNotNull(ckmStore.getChecksum(lb.getBlock()));
          blks.add(lb.getBlock());
        }
      }
      if (codec.isDirRaid) {
        HashMap<Block, StripeInfo> stripeMapping = new HashMap<Block, StripeInfo>();
        HashMap<StripeInfo, Integer> stripeMappingNum =
            new HashMap<StripeInfo, Integer>();
        for (Block blk : blks) {
          StripeInfo newSi = stripeStore.getStripe(codec, blk);
          assertNotNull(newSi);
          if (stripeMapping.containsKey(blk)) {
            StripeInfo si = stripeMapping.get(blk);
            assertEquals(si.parityBlocks, newSi.parityBlocks);
            assertEquals(si.srcBlocks, newSi.srcBlocks);
            Integer num = stripeMappingNum.get(si);
            stripeMappingNum.put(si, num + 1);
          } else {
            stripeMapping.put(blk, newSi);
            for (Block otherBlk : newSi.parityBlocks) {
              stripeMapping.put(otherBlk, newSi);
            }
            for (Block otherBlk : newSi.srcBlocks) {
              stripeMapping.put(otherBlk, newSi);
            }
            stripeMappingNum.put(newSi, 1);
          }
        }
        int smallStripe = 0;
        for (StripeInfo si : stripeMappingNum.keySet()) {
          LOG.info("Checking stripe : " + si);
          assertEquals((Integer)(si.parityBlocks.size() + si.srcBlocks.size()),
              stripeMappingNum.get(si));
          int expectedSize = codec.parityLength + codec.stripeLength;
          assertTrue(stripeMappingNum.get(si) <= expectedSize);
          if (stripeMappingNum.get(si) < expectedSize) {
            smallStripe++;
          }
        }
        assertTrue("Only one small stripe is allowed", smallStripe <= 1);
      }
    }
    for (Codec codec : Codec.getCodecs()) {
      Path srcDir = new Path(raidDir, codec.id);
      FileStatus[] srcFiles = fileSys.listStatus(srcDir);
      Long[] crcs = fileCRCs.get(codec);
      long corruptTimes = 0;
      HashSet<String> corruptSet = new HashSet<String>();
      
      while (corruptTimes < NUM_FILES) {
        // corrupt all the source blocks 
        for (int i = 0; i < srcFiles.length; i++) {
          long crc = crcs[i];
          FileStatus victim = srcFiles[i];
          LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
              dfs, victim.getPath().toUri().getPath(),
              0, victim.getLen());
          int numBlocks = (int)RaidNode.numBlocks(victim);
          assertEquals("No file should be corrupt", 0,
              DFSUtil.getCorruptFiles(dfs).length);
          ArrayList<Integer> allIdxs = new ArrayList<Integer>();
          for (int j = 0 ;j < numBlocks; j++) {
            allIdxs.add(j);
          }
          Collections.shuffle(allIdxs);
          int numCorruptBlocks = (codec.parityLength > codec.stripeLength)? 
              numBlocks: codec.parityLength;
          int[] corruptBlockIdxs = new int[numCorruptBlocks];
          StringBuilder sb = new StringBuilder();
          sb.append("Corrupt blocks ");
          for (int j = 0; j < numCorruptBlocks; j++) {
            corruptBlockIdxs[j] = allIdxs.get(j);
            sb.append(" " + corruptBlockIdxs[j]);
          }
          sb.append(" of " + numBlocks + " blocks in the file " +
              victim.getPath());
          if (corruptSet.contains(sb.toString())) {
            // avoid fixing the same block 
            continue;
          }
          corruptSet.add(sb.toString());
          if (numCorruptBlocks == numBlocks) {
            corruptTimes = NUM_FILES;
          } else {
            corruptTimes++;
          }
          LOG.info(sb);
          for (int j = 0; j < numCorruptBlocks; j++) {
            TestBlockFixer.corruptBlock(
                locs.get(corruptBlockIdxs[j]).getBlock(),
                cluster);
          }
          RaidDFSUtil.reportCorruptBlocks(fileSys, victim.getPath(),
              corruptBlockIdxs, victim.getBlockSize());
          assertEquals("file should be corrupt", 1,
              DFSUtil.getCorruptFiles(dfs).length);
          BlockReconstructor.CorruptBlockReconstructor fixer =
              new BlockReconstructor.CorruptBlockReconstructor(conf);
          assertTrue(fixer.reconstructFile(victim.getPath(), null));
          long startTime = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTime < 60000) {
            Thread.sleep(1000);
            if (DFSUtil.getCorruptFiles(dfs).length == 0) {
              break;
            }
          }
          assertEquals("file " + victim.getPath() + " should be fixed", 0,
              DFSUtil.getCorruptFiles(dfs).length);
          long checkCRC = RaidDFSUtil.getCRC(fileSys, victim.getPath());
          assertEquals("file " + victim.getPath() + " not fixed",
                       crc, checkCRC);
        }
      }
    }
  }

  public void testFileListPolicy() throws Exception {
    LOG.info("Test testFileListPolicy started.");
    createClusters(true);
    // don't allow rescan, make sure only one job is submitted.
    conf.setLong("raid.policy.rescan.interval", 60 * 1000L);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    HashMap<Codec, String> fileLists = new HashMap<Codec, String>();
    HashMap<Codec, Long[]> fileCRCs = new HashMap<Codec, Long[]>();
    for (Codec codec : Codec.getCodecs()) {
      String fileList = "/user/rvadali/" + codec.id;
      cb.addAbstractPolicy(codec.id, targetReplication, metaReplication, 
          codec.id);
      cb.addFileListPolicy("policy-" + codec.id, fileList,
          codec.id); 
      fileLists.put(codec, fileList);
    }
    cb.persist();
    RaidNode cnode = null;
    Path raidDir = new Path("/user/raidtest/");
    try {
      createTestFiles(raidDir, null, fileCRCs, fileLists);
      LOG.info("Test testFileListPolicy created test files");
      cnode = RaidNode.createRaidNode(conf);
      DistRaidNode dcnode = (DistRaidNode) cnode;
      for (Codec codec : Codec.getCodecs()) {
        Path baseFile = new Path(raidDir, codec.id);
        if (!codec.isDirRaid) {
          Path srcFile = new Path(baseFile, "0");
          TestRaidDfs.waitForFileRaided(LOG, fileSys, srcFile, 
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(baseFile)),
              targetReplication);
          TestRaidDfs.waitForReplicasReduction(fileSys, srcFile,
              targetReplication);
        } else {
          TestRaidDfs.waitForDirRaided(LOG, fileSys, baseFile,
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(raidDir)), 
                  targetReplication);
          TestRaidDfs.waitForReplicasReduction(fileSys, baseFile,
              targetReplication);
        } 
      }
      long startTime = System.currentTimeMillis();
      while (dcnode.jobMonitor.jobsMonitored() > dcnode.jobMonitor.jobsSucceeded()
          && System.currentTimeMillis()  - startTime < 120000) {
        LOG.info("Waiting for all jobs to finish");
        Thread.sleep(2000);
      }
      assertEquals("All jobs should succeed", dcnode.jobMonitor.jobsMonitored(),
          dcnode.jobMonitor.jobsSucceeded());
      verifyCorrectness(raidDir, fileCRCs, fileLists);
      LOG.info("Test testFileListPolicy successful.");
    } catch (Exception e) {
      LOG.info("testFileListPolicy Exception ", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testFileListPolicy completed.");
  }
}
package org.apache.hadoop.raid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;
import org.apache.hadoop.raid.TestPlacementMonitor.FakeBlockAndDatanodeResolver;
import org.apache.hadoop.raid.TestPlacementMonitor.FakeExecutorService;
import org.apache.hadoop.raid.Utils.Builder;
import org.apache.log4j.Level;

import junit.framework.TestCase;

public class TestDirectoryPlacementMonitor extends TestCase {
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestDirectoryPlacementMonitor");
  final String[] racks =
    {"/rack1", "/rack1", "/rack1", "/rack1", "/rack1", "/rack1"};
  final String[] hosts =
    {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com",
     "host4.rack1.com", "host5.rack1.com", "host6.rack1.com"};
  final int stripeSize = 3;
  final long blockSize = 1024L;
  final long[] fileSizes =
      new long[]{blockSize + blockSize/2, // block 0, 1
                 3*blockSize,                  // block 2, 3
                 blockSize + blockSize/2 + 1}; // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{blockSize, 2*blockSize, blockSize/2};
  
  private boolean checkHistogram(Configuration conf, FileSystem fileSys,
      FileStatus dirStat, FileStatus parityStat) throws IOException {
    //Remove the cached items
    HashMap<String, LocatedFileStatus> cache = PlacementMonitor.locatedFileStatusCache.get();
    cache.clear();
    PlacementMonitor.locatedFileStatusCache.remove();
    PlacementMonitor placementMonitor = new PlacementMonitor(conf);
    placementMonitor.start();
    BlockMover blockMover = placementMonitor.blockMover;
    TestPlacementMonitor tpm = new TestPlacementMonitor();
    FakeExecutorService fakeBlockMover = tpm.new 
        FakeExecutorService();
    FakeBlockAndDatanodeResolver resolver = tpm.new 
        FakeBlockAndDatanodeResolver();
    blockMover.executor = fakeBlockMover;
    try {
      // Start to check the placement 
      List<BlockInfo> srcLstBI = placementMonitor.getBlockInfos(fileSys,
          dirStat);
      List<BlockInfo> parityLstBI = new LinkedList<BlockInfo>();
      if (parityStat != null)
          parityLstBI = placementMonitor.getBlockInfos(fileSys, parityStat);
      assertEquals("Source file blocks don't match parity file blocks", 
          parityLstBI.size(), RaidNode.numStripes(srcLstBI.size(), stripeSize));
      for (int i = 0; i< parityLstBI.size(); i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = i * stripeSize; j < srcLstBI.size() && j < (i+1)*stripeSize; j++) {
          sb.append(srcLstBI.get(j).blockLocation.getOffset() + ":");
          for (String name: srcLstBI.get(j).blockLocation.getNames()) {
            sb.append(name);
            sb.append(" ");
          }
          sb.append(";");
        }
        for (String name: parityLstBI.get(i).blockLocation.getNames()) {
          sb.append(name);
          sb.append(" ");
        }
        LOG.info("Stripe " + i + " " + sb);
      }
      placementMonitor.checkBlockLocations(
          srcLstBI, parityLstBI, Codec.getCodec("dir-rs"), null,
          dirStat, resolver);
      Map<Integer, Long> hist =
          placementMonitor.blockHistograms.get("dir-rs");
      for (Integer key: hist.keySet()) {
        LOG.info(" Integer: " + key + " Long: " + hist.get(key));
      }
      return hist.size() > 1;
    } finally {
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }
  /**
   * This test creates a bunch of files under the directory 
   * and monitor the block mover moves the blocks
   * @throws Exception
   */
  public void testDirectoryPlacementMonitor() throws Exception {
    TestRaidPurge trp = new TestRaidPurge();
    LOG.info("Test testDirectoryPlacementMonitor  started.");
    long targetReplication = 1;
    long metaReplication   = 1;
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    final Path dir1 = new Path("/user/dhruba/dirraidrstest/1");
    final Path destPath = new Path("/dir-raidrs/user/dhruba/dirraidrstest");

    trp.createClusters(true, 6, racks, hosts);
    trp.conf.setLong(PurgeMonitor.PURGE_MONITOR_SLEEP_TIME_KEY, 10000L);
    trp.conf.setBoolean(PlacementMonitor.SIMULATE_KEY, false);
    trp.conf.setBoolean(BlockMover.RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY, true);
    TestDirectoryRaidDfs.setupStripeStore(trp.conf, trp.fileSys);
    
    FileSystem fileSys = trp.fileSys;
    RaidNode cnode = null;
   
    try {
      trp.mySetup(targetReplication, metaReplication);
      Utils.loadTestCodecs(trp.conf, new Builder[] {
        Utils.getXORBuilder(),
        Utils.getXORBuilder().dirRaid(
            true).setParityDir("/dir-raid").setCodeId("dir-xor"),
        Utils.getRSBuilder().dirRaid(true).setParityDir(
            "/dir-raidrs").setCodeId("dir-rs").setParityLength(
            1).setStripeLength(stripeSize)
      });
      fileSys.delete(dir1, true);
      fileSys.delete(destPath, true);
      TestRaidDfs.createTestFiles(dir1, fileSizes, blockSizes,
          crcs, seeds, fileSys, (short)1);
      FileStatus dirStat = fileSys.getFileStatus(dir1);
      LOG.info("Created test files");
      // create an instance of the RaidNode
      Configuration localConf = new Configuration(trp.conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath);
      LOG.info("all are raided.");
      long startTime = System.currentTimeMillis();
      boolean badPlacement = true;
      while (System.currentTimeMillis() - startTime < 120000 && badPlacement) {
        badPlacement = checkHistogram(trp.conf, fileSys, dirStat, 
            fileSys.getFileStatus(new Path(destPath, dir1.getName())));
        Thread.sleep(3000);
      }
      assertEquals("Files are still in bad placement", badPlacement, false);
      LOG.info("Test testDirectoryPlacementMonitor completed.");
    } catch (Exception e) {
      LOG.info("testDirectoryPlacementMonitor fails", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPurge delete directory  " + dir1);
      fileSys.delete(dir1, true);
      trp.stopClusters();
    }
  }
}

package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;

public class TestTempDirectoryCleanUp extends TestCase {

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestTempDirectoryCleanUp");
  final static Random rand = new Random();

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;
  
  /**
   * create mapreduce and dfs clusters
   */
  private void createClusters(boolean local) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    Utils.loadTestCodecs(conf, 3, 10, 1, 5, "/raid", "/raidrs", false, false);

    conf.setLong("raid.policy.rescan.interval", 5 * 1000L);

    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    if (local) {
      conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    } else {
      conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    }

    // use local block fixer
    conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");

    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");
    conf.setInt(Encoder.RETRY_COUNT_PARTIAL_ENCODING_KEY, 1);

    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;

    // Because BlockPlacementPolicyRaid only allows one replica in each rack,
    // spread 6 nodes into 6 racks to make sure chooseTarget function could pick
    // more than one node. 
    String[] racks = {"/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6"};
    dfs = new MiniDFSCluster(conf, 6, true, racks);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
  }
  
  private void createTestFiles() throws IOException {
    TestRaidNode.createTestFiles(fileSys, "/user/dhruba/raidtest/",
        "/raid/user/dhruba/raidtest", 5, 7);
    TestRaidNode.createTestFiles(fileSys, "/user/dhruba/raidtest2/",
        "/raid/user/dhruba/raidtest2", 5, 7);
    TestRaidNode.createTestFiles(fileSys, "/user/dhruba/raidtest3/",
        "/raidrs/user/dhruba/raidtest3", 1, 10);
    LOG.info("Created test files");
  }
  
  private Configuration initializeConfig() throws IOException {
    short targetReplication = 2;
    short metaReplication   = 2;
    short rstargetReplication = 1;
    short rsmetaReplication   = 1;
    
    // Initialize Raid Policy config
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest",
        targetReplication, metaReplication);
    cb.addAbstractPolicy("abstractPolicy",targetReplication,
        metaReplication, "xor");
    cb.addPolicy("policy2", "/user/dhruba/raidtest2", "abstractPolicy");
    cb.addPolicy("policy3", "/user/dhruba/raidtest3",
        rstargetReplication, rsmetaReplication, "rs");
    cb.persist();
    // Initialize Raidnode config
    Configuration localConf = new Configuration(conf);
    //Avoid block mover to move blocks
    localConf.setInt(PlacementMonitor.BLOCK_MOVE_QUEUE_LENGTH_KEY, 0);
    localConf.setInt(PlacementMonitor.NUM_MOVING_THREADS_KEY, 1);
    localConf.setLong(JobMonitor.JOBMONITOR_INTERVAL_KEY, 3000L);
    return localConf;
  }
  
  /**
   * stop clusters created earlier
   */
  private void stopClusters() throws Exception {
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }
  
  /**
   * Return if the temp directories exist
   */
  private boolean checkTempDirectories(DistRaid job)
    throws IOException {
    boolean doesExist = false;
    for (Codec codec: Codec.getCodecs()) {
      Path tmpdir = new Path(codec.tmpParityDirectory, job.getJobID());
      FileSystem fs = tmpdir.getFileSystem(job.getConf());
      if (fs.exists(tmpdir)) {
        LOG.info("Temp directory " + tmpdir + " exists");
        doesExist = true;
      } else {
        LOG.info("Temp directory " + tmpdir + " does not exist");
      }
    }
    return doesExist;
  }
  
  /**
   * Test if the temporary directory is cleanup when the job is complete
   */
  public void testTempDirCleanUpWhenJobComplete() throws Exception {
    LOG.info("Test testTempDirCleanUpWhenJobComplete started.");
    createClusters(false);
    RaidNode cnode = null;
    try {
      createTestFiles();
      Configuration localConf = initializeConfig();
      cnode = RaidNode.createRaidNode(null, localConf);

      long start = System.currentTimeMillis();
      final int MAX_WAITTIME = 300000;
      
      assertTrue("cnode is not DistRaidNode", cnode instanceof DistRaidNode);
      DistRaidNode dcnode = (DistRaidNode) cnode;
      Set<DistRaid> jobSet = new HashSet<DistRaid>();
      while (jobSet.size() < 3 && System.currentTimeMillis() - start < MAX_WAITTIME) {
        List<DistRaid> jobs = dcnode.jobMonitor.getRunningJobs();
        for (DistRaid job: jobs) {
          if (jobSet.contains(job)) {
            continue;
          }
          if (checkTempDirectories(job)) {
            LOG.info("Detect new job " + job.getJobID());
            jobSet.add(job);
          }
        }
        LOG.info("Waiting for 3 running jobs, now is " + jobSet.size() + " jobs");
        Thread.sleep(1000);
      }
      
      while (dcnode.jobMonitor.runningJobsCount() > 0 &&
          System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for zero running jobs: " +
            dcnode.jobMonitor.runningJobsCount());
        Thread.sleep(1000);
      }
      
      for (DistRaid job: jobSet) {
        assertEquals("Temp directory for job " + job.getJobID() + 
            " should be deleted.", false, checkTempDirectories(job));
      }

      LOG.info("Test testTempDirCleanUpWhenJobComplete successful.");
    } catch (Exception e) {
      LOG.info("testTempDirCleanUpWhenJobComplete Exception ", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testTempDirCleanUpWhenJobComplete completed.");
  }
  
  /**
   * Test if the temporary directory is cleanup when the job is killed
   */
  public void testTempDirCleanUpWhenJobIsKilled() throws Exception {
    LOG.info("Test testTempDirCleanUpWhenJobIsKilled started.");
    createClusters(false);
    RaidNode cnode = null;
    try {
      createTestFiles();
      Configuration localConf = initializeConfig();
      cnode = RaidNode.createRaidNode(null, localConf);

      long start = System.currentTimeMillis();
      final int MAX_WAITTIME = 300000;
      
      assertTrue("cnode is not DistRaidNode", cnode instanceof DistRaidNode);
      DistRaidNode dcnode = (DistRaidNode) cnode;

      Set<DistRaid> jobSet = new HashSet<DistRaid>();
      while (jobSet.size() < 3 && System.currentTimeMillis() - start < MAX_WAITTIME) {
        List<DistRaid> jobs = dcnode.jobMonitor.getRunningJobs();
        for (DistRaid job: jobs) {
          if (jobSet.contains(job)) {
            continue;
          }
          if (checkTempDirectories(job)) {
            jobSet.add(job);
            LOG.info("Kill job " + job.getJobID());
            job.killJob();
          }
        }
        LOG.info("Waiting for 3 running jobs, now is " + jobSet.size() + " jobs");
        Thread.sleep(1000);
      }

      while (dcnode.jobMonitor.runningJobsCount() > 0 &&
          System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for zero running jobs: " +
            dcnode.jobMonitor.runningJobsCount());
        Thread.sleep(1000);
      }
      for (DistRaid job: jobSet) {
        assertEquals("Temp directory for job " + job.getJobID() + 
            " should be deleted.", false, checkTempDirectories(job));
      }
      LOG.info("Test testTempDirCleanUpWhenJobIsKilled successful.");
    } catch (Exception e) {
      LOG.info("testTempDirCleanUpWhenJobIsKilled Exception ", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testTempDirCleanUpWhenJobIsKilled completed.");
  }
}

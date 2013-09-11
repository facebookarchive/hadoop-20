package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.RaidNode.TriggerMonitor;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.log4j.Level;

import junit.framework.TestCase;

public class TestRaidNodeSelectFiles extends TestCase {

  final static Log LOG = LogFactory.getLog(TestRaidNodeSelectFiles.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CHECKSUM_STORE_DIR = new File(TEST_DIR, "ckm_store."
      + System.currentTimeMillis()).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml")
      .getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;
  Random rand = new Random();
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
    ((Log4JLogger) RaidNode.LOG).getLogger().setLevel(Level.ALL);
  }

  final long blockSize = 8192L;
  final long[] fileSizes = new long[] { 2 * blockSize + blockSize / 2,
      7 * blockSize, blockSize + blockSize / 2 + 1 };
  final long[] blockSizes = new long[] { blockSize, 2 * blockSize,
      blockSize / 2 };

  private void mySetup(int stripeLength) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 500 second
    conf.setLong("raid.policy.rescan.interval", 500000);
    
    // start the raiding job quickly.
    conf.setLong(RaidNode.TRIGGER_MONITOR_SLEEP_TIME_KEY, 100);

    // use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");
    // Make sure initial repl is smaller than NUM_DATANODES
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 1);

    Utils.loadAllTestCodecs(conf, stripeLength, 1, 3, "/destraid",
        "/destraidrs", "/dir-destraid", "/dir-destraidrs");

    conf.setBoolean("dfs.permissions", false);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addFileListPolicy("RaidTest1", "/user/dikang/rs_files.txt", 1, 1, "rs");
    cb.persist();
  }

  class TestRaidNodeInjectionHandler extends InjectionHandler {
    Map<InjectionEventI, Integer> events = new HashMap<InjectionEventI, Integer>();

    public TestRaidNodeInjectionHandler() {
      events.put(InjectionEvent.RAID_ENCODING_SKIP_PATH, 0);
      events.put(InjectionEvent.RAID_ENCODING_SKIP_PATH_TOO_NEW_MOD, 0);
    }

    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.RAID_ENCODING_SKIP_PATH ||
          event == InjectionEvent.RAID_ENCODING_SKIP_PATH_TOO_NEW_MOD) {
        events.put(event, events.get(event) + 1);
      }
    }
  }
  
  public void testTooNewModPeriod() throws Exception {
    int stripeLength = 3;
    try {
      mySetup(stripeLength);
      
      long[] crcs1 = new long[3];
      int[] seeds1 = new int[3];
      Path rsDirPath = new Path("/user/dikang/raidtestrs");
      Path[] rsRaidFiles = TestRaidDfs.createTestFiles(rsDirPath, fileSizes,
          blockSizes, crcs1, seeds1, fileSys, (short) 1);
      
      // write the rs files to raid txt
      Path rsFilesToRaid = new Path("/user/dikang/rs_files.txt");
      FSDataOutputStream fsout = fileSys.create(rsFilesToRaid);
      for (Path file : rsRaidFiles) {
        fsout.writeBytes(file.toUri().getPath() + "\n");
      }
      fsout.close();
      
      // create the InjectionHandler
      TestRaidNodeInjectionHandler h = new TestRaidNodeInjectionHandler();
      InjectionHandler.set(h);
      
      // create the RaidNode
      Configuration raidConf = getRaidNodeConfig(conf, true);
      cnode = RaidNode.createRaidNode(null, raidConf);

      // wait for the trigger monitor to scan the raid candidates.
      TriggerMonitor monitor = cnode.getTriggerMonitor();
      long startTime = System.currentTimeMillis();
      while (monitor.getLastTriggerTime() == 0 && 
          System.currentTimeMillis() - startTime < 60000) {
        Thread.sleep(500);
        LOG.info("Wait for the first round of TriggerMonitor to finish.");
      }
      
      // assert the Parity files
      Codec rs = Codec.getCodec("rs");
      for (Path file : rsRaidFiles) {
        FileStatus stat = fileSys.getFileStatus(file);
        assertFalse(ParityFilePair.parityExists(stat, rs, conf));
      }
      
      // verify the injection
      assertEquals(rsRaidFiles.length, 
          h.events.get(InjectionEvent.RAID_ENCODING_SKIP_PATH_TOO_NEW_MOD).intValue());
      
    } finally {
      myTearDown();
    }
  }

  public void testRaidPriority() throws Exception {
    int stripeLength = 3;
    try {
      mySetup(stripeLength);

      long[] crcs = new long[3];
      int[] seeds = new int[3];
      Path dirPath = new Path("/user/dikang/raidtestdir");
      Path[] dirRaidFiles = TestRaidDfs.createTestFiles(dirPath, fileSizes,
          blockSizes, crcs, seeds, fileSys, (short) 1);

      long[] crcs1 = new long[3];
      int[] seeds1 = new int[3];
      Path rsDirPath = new Path("/user/dikang/raidtestrs");
      Path[] rsRaidFiles = TestRaidDfs.createTestFiles(rsDirPath, fileSizes,
          blockSizes, crcs1, seeds1, fileSys, (short) 1);

      // raid the directory
      Codec dirRS = Codec.getCodec("dir-rs");
      RaidNode.doRaid(conf, fileSys.getFileStatus(dirPath), new Path(
          dirRS.parityDirectory), Codec.getCodec("dir-rs"),
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, false, 1, 1);

      // verify the parity
      for (Path file : dirRaidFiles) {
        FileStatus stat = fileSys.getFileStatus(file);
        assertTrue(ParityFilePair.parityExists(stat, dirRS, conf));
      }

      // write the rs files to raid txt
      Path rsFilesToRaid = new Path("/user/dikang/rs_files.txt");
      FSDataOutputStream fsout = fileSys.create(rsFilesToRaid);
      for (Path file : dirRaidFiles) {
        fsout.writeBytes(file.toUri().getPath() + "\n");
      }
      for (Path file : rsRaidFiles) {
        fsout.writeBytes(file.toUri().getPath() + "\n");
      }
      fsout.close();
      
      // create the InjectionHandler
      TestRaidNodeInjectionHandler h = new TestRaidNodeInjectionHandler();
      InjectionHandler.set(h);
      
      // sleep the modTimePeriod
      Thread.sleep(2000);

      // create the RaidNode
      Configuration raidConf = getRaidNodeConfig(conf, true);
      cnode = RaidNode.createRaidNode(null, raidConf);

      for (Path file : rsRaidFiles) {
        TestRaidDfs.waitForFileRaided(LOG, fileSys, file, new Path(
            "/destraidrs" + file.toUri().getPath()));
      }

      Codec rs = Codec.getCodec("rs");
      // assert the Parity files
      for (Path file : dirRaidFiles) {
        FileStatus stat = fileSys.getFileStatus(file);
        assertFalse(ParityFilePair.parityExists(stat, rs, conf));
      }
      for (Path file : rsRaidFiles) {
        FileStatus stat = fileSys.getFileStatus(file);
        assertTrue(ParityFilePair.parityExists(stat, rs, conf));
      }
      
      // verify the injection
      assertEquals(rsRaidFiles.length, 
          h.events.get(InjectionEvent.RAID_ENCODING_SKIP_PATH).intValue());
    } finally {
      myTearDown();
    }
  }

  public Configuration getRaidNodeConfig(Configuration conf, boolean local) {
    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
          "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
          "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    return localConf;
  }

  private void myTearDown() throws Exception {
    if (cnode != null) {
      cnode.stop();
      cnode.join();
    }
    if (mr != null) {
      mr.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    InjectionHandler.clear();
  }
}

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.TestDirectoryRaidDfs;
import org.apache.hadoop.raid.Utils;
import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestDirectoryRaidBlockPlacement extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private FSNamesystem namesystem = null;
  private BlockPlacementPolicyRaid policy = null;
  private FileSystem fs = null;
  private FileSystem localFileSys = null;
  private DistributedFileSystem dfs = null;
  private static long CAPACITY = 10240000L;
  private Path excludeFile;
  private Random rand = new Random();

  String[] racks1 = {"/rack1", "/rack1", "/rack1"};
  String[] hosts1 = {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com"};
  String[] racks2 = {"/rack1", "/rack2", "/rack2", "/rack2"};
  String[] hosts2 = {"host1.rack1.com", "host2.rack2.com", "host3.rack2.com",
                     "host4.rack2.com"};
  
  final static Log LOG =
      LogFactory.getLog(TestDirectoryRaidBlockPlacement.class);
  
  private void cleanFile(Path p) throws IOException {
    File f = new File(p.toUri().getPath());
    f.getParentFile().mkdirs();
    if (f.exists()) {
      f.delete();
    }
    f.createNewFile();
  }

  private void writeConfigFile(Path name, ArrayList<String> nodes) 
      throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);  
    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  } 

  protected void setupCluster(boolean simulated, long minFileSize, String[] racks,
      String[] hosts) throws IOException {
    conf = new Configuration();
    localFileSys = FileSystem.getLocal(conf);
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", 1L);
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
    conf.setLong("hdfs.raid.min.filesize", minFileSize);
    Utils.loadTestCodecs(conf, 5, 5, 1, 3, "/raid", "/raidrs", false, true);
    conf.setInt("io.bytes.per.checksum", 1);
    excludeFile = new Path(TEST_DIR, "exclude" + System.currentTimeMillis());
    cleanFile(excludeFile);
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
    writeConfigFile(excludeFile, null);
    
    if (!simulated) {
      cluster = new MiniDFSCluster(conf, hosts.length, true, racks, hosts);
    } else {
      long[] capacities = new long[]{CAPACITY, CAPACITY, CAPACITY};
      cluster = new MiniDFSCluster(0, conf, hosts.length, true, true, null, 
        racks, capacities);
    }
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    Assert.assertTrue("BlockPlacementPolicy type is not correct.",
        namesystem.replicator instanceof BlockPlacementPolicyRaid);
    policy = (BlockPlacementPolicyRaid) namesystem.replicator;
    fs = cluster.getFileSystem();
    dfs = (DistributedFileSystem)fs;
    TestDirectoryRaidDfs.setupStripeStore(conf, fs);
  }

  protected void closeCluster() throws IOException {
    if (null != fs) {
      fs.close();
    }
    if (null != cluster) {
      cluster.shutdown();
    }
  }
  
  private int printLocatedBlocks(Path filePath) throws Exception {
    LocatedBlocks lbs = dfs.getLocatedBlocks(filePath, 0L, Integer.MAX_VALUE);
    StringBuilder sb = new StringBuilder();
    sb.append("Path " + filePath + ":");
    int maxRepl = 0;
    for (LocatedBlock lb: lbs.getLocatedBlocks()) {
      sb.append(lb.getBlock());
      sb.append(":");
      for (DatanodeInfo loc: lb.getLocations()) {
        sb.append(loc.getHostName());
        sb.append(" ");
      }
      if (lb.getLocations().length > maxRepl) {
        maxRepl = lb.getLocations().length;
      }
    }
    LOG.info(sb.toString());
    return maxRepl;
  }
  
  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn,
      Configuration conf) throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
  }
  
  private void waitState(ArrayList<DatanodeInfo> infos, AdminStates state) throws Exception {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 60000) {
      int i = 0;
      for (DatanodeInfo di: infos) {
        if (di.getAdminState() == state) { 
          i++;
        }
      }
      if (i == infos.size()) {
        return;
      }
      Thread.sleep(1000);
    }
  }
  
  /*
   * This test creates a directory with 3 files and its fake parity file.
   * We decommissioned all nodes in the rack2 to make sure all data are stored
   * in rack1 machine.
   * Then we bring rack2 machines to normal state and create a non-raided file 
   * which is too small to be raided in the directory with 4 replicas
   * (1 in rack1 and 3 in rack2).
   * Then we reduce the replication to 3 to trigger chooseReplicatToDelete.
   * We verify remaining replicas has 1 in rack1 and 2 in rack2.
   */
  @Test
  public void testChooseReplicasToDeleteForSmallFile() throws Exception {
    try {
      setupCluster(false, 512L, racks2, hosts2);
      // create test files
      int numFiles = 4;
      long blockSize = 1024L;
      String parentDir = "/dir/";
      DFSClient client = getDfsClient(cluster.getNameNode(), conf);
      DatanodeInfo[] infos = client.datanodeReport(DatanodeReportType.LIVE);
      ArrayList<String> rack2nodes = new ArrayList<String>();
      ArrayList<DatanodeInfo> rack2di = new ArrayList<DatanodeInfo>();
      for (DatanodeInfo di: infos) {
        if (di.getHostName().contains("rack2")) {
          rack2nodes.add(di.getName());
          rack2di.add(cluster.getNameNode().namesystem.getDatanode(di));
        }
      }
      LOG.info("Decommission rack2 nodes");
      writeConfigFile(excludeFile, rack2nodes);
      cluster.getNameNode().namesystem.refreshNodes(conf);
      waitState(rack2di, AdminStates.DECOMMISSIONED);
      for (int i = 0; i < numFiles; i++) {
        if (i == 2) {
          continue;
        }
        String file = parentDir + "file" + i;
        Path filePath = new Path(file);
        TestRaidDfs.createTestFile(fs, filePath, 1, 1, blockSize);
        printLocatedBlocks(filePath);
      }
      LOG.info("Created " + (numFiles - 1) + " files");
      // create fake parity file
      Codec code = Codec.getCodec("xor");
      long numStripes = RaidNode.numStripes(numFiles, code.stripeLength);
      Path parityPath = new Path(code.parityDirectory, "dir");
      TestRaidDfs.createTestFile(fs, parityPath, 1,
          (int)numStripes * code.parityLength, blockSize);
      LOG.info("Create parity file: " + parityPath);
      printLocatedBlocks(parityPath);
      
      LOG.info("Bring back rack2 nodes out of decommission");
      writeConfigFile(excludeFile, null);
      cluster.getNameNode().namesystem.refreshNodes(conf);
      waitState(rack2di, AdminStates.NORMAL);
      
      Path smallFilePath = new Path(parentDir + "file2");
      TestRaidDfs.createTestFile(fs, smallFilePath, 4, 1, 256L);
      assertEquals("all datanodes should have replicas", hosts2.length,
          printLocatedBlocks(smallFilePath));
      LOG.info("Created small file: " + smallFilePath);
      
      LOG.info("Reduce replication to 3");
      dfs.setReplication(smallFilePath, (short)3);
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 120000 &&
             printLocatedBlocks(smallFilePath) == 4) {
        Thread.sleep(1000);
      }
      LocatedBlocks lbs = dfs.getLocatedBlocks(smallFilePath, 0L,
          Integer.MAX_VALUE); 
      boolean hasRack1 = false;
      for (DatanodeInfo di: lbs.getLocatedBlocks().get(0).getLocations()) {
        if (di.getNetworkLocation().contains("rack1")) {
          hasRack1 = true;
          break;
        }
      }
      assertTrue("We should keep the nodes in rack1", hasRack1);
    } finally {
      closeCluster();
    }
  }
  
  /*
   * This test start datanodes with simulated mode and keep running
   * chooseReplicaToDelete multiple times to get the average processing time
   * and number of allocated objects
   */
  @Test
  public void testDirXORChooseReplicasToDeletePerformance() throws Exception {
    try {
      setupCluster(true, 1L, racks1, hosts1);
      // create test files
      int numFiles = 1000;
      long blockSize = 1024L;
      String parentDir = "/dir/";
      for (int i = 0; i < numFiles; i++) {
        String file = parentDir + "file" + i;
        TestRaidDfs.createTestFile(fs, new Path(file), 3, 1, blockSize);
      }
      LOG.info("Created " + numFiles + " files");
      Codec code = Codec.getCodec("xor");
      FSNamesystem fsNameSys = cluster.getNameNode().namesystem;
      for (DatanodeDescriptor dd: fsNameSys.datanodeMap.values()) {
        LOG.info(dd);
      }
      // create fake parity file
      long numStripes = RaidNode.numStripes(numFiles, code.stripeLength);
      TestRaidDfs.createTestFile(fs, new Path(code.parityDirectory, "dir"), 3,
          (int)numStripes * code.parityLength, blockSize);
      long startTime = System.currentTimeMillis();
      long total = 0L;
      fsNameSys.readLock();
      for (BlocksMap.BlockInfo bi : fsNameSys.blocksMap.getBlocks()) {
        fsNameSys.replicator.chooseReplicaToDelete(bi.getINode(),
            bi, (short)3, fsNameSys.datanodeMap.values(),
            new ArrayList<DatanodeDescriptor>());
        total++;
      }
      fsNameSys.readUnlock();
      LOG.info("Average chooseReplicaToDelete time: " +
          ((double)(System.currentTimeMillis() - startTime) / total));
    } finally {
      closeCluster();
    }
  }

  @Test
  public void testGetCompanionBLocks() throws IOException {

    try {
      setupCluster(false, 1L, racks1, hosts1);
      String[] files = new String[] {"/dir/file1", "/dir/file2", "/dir/file3"};
      Codec codec = Codec.getCodec("rs");
      for (String file : files) {
        TestRaidDfs.createTestFile(fs, new Path(file), 3, 2, 8192L);
      }
      FileStatus stat = fs.getFileStatus(new Path("/dir"));
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec, 
          new RaidNode.Statistics(), 
          RaidUtils.NULL_PROGRESSABLE, false, 1, 1);       
      
      Collection<LocatedBlock> companionBlocks;
      
      for (int i=0; i<2; i++) {
        for (int j=0; j<2; j++) {
          companionBlocks = getCompanionBlocks(
              namesystem, policy, getBlocks(namesystem, 
                  files[i]).get(j).getBlock());
          Assert.assertEquals(8, companionBlocks.size());
        }
      }
      
      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, 
              files[2]).get(0).getBlock());
      Assert.assertEquals(8, companionBlocks.size());
      
      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, 
              files[2]).get(1).getBlock());
      Assert.assertEquals(4, companionBlocks.size());
      
      String parityFile = "/raidrs/dir";
      
      for (int i=0; i<3; i++) {
        companionBlocks = getCompanionBlocks(
            namesystem, policy, getBlocks(namesystem, 
                parityFile).get(i).getBlock());
        Assert.assertEquals(8, companionBlocks.size());
      }
      
      for (int i=3; i<6; i++) {
        companionBlocks = getCompanionBlocks(
            namesystem, policy, getBlocks(namesystem, 
                parityFile).get(i).getBlock());
        Assert.assertEquals(4, companionBlocks.size());
      }
    } finally {
      closeCluster();
    }
  }
  
  private Collection<LocatedBlock> getCompanionBlocks(
      FSNamesystem namesystem, BlockPlacementPolicyRaid policy,
      Block block) throws IOException {
    INodeFile inode = namesystem.blocksMap.getINode(block);
    BlockPlacementPolicyRaid.FileInfo info = 
        policy.getFileInfo(inode, inode.getFullPathName());
    return policy.getCompanionBlocks(inode.getFullPathName(), info, block, inode);
  }
  
  private List<LocatedBlock> getBlocks(FSNamesystem namesystem, String file) 
      throws IOException {
    FileStatus stat = namesystem.getFileInfo(file);
    return namesystem.getBlockLocations(
               file, 0, stat.getLen()).getLocatedBlocks();
  }

}

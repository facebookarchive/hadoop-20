/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyFakeData;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker;
import org.apache.hadoop.raid.RaidNode;
import junit.framework.TestCase;


public class TestBlockCopier extends TestCase {
  
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestBlockCopier");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  
  final static int STRIPE_LENGTH = 4;
  final static int BLOCK_SIZE = 8192;
  
  Configuration conf;
  NameNode namenode = null;
  MiniDFSCluster dfs = null;
  String hftp = null;
  MiniMRCluster mr = null;
  DistributedFileSystem fileSys = null;
  RaidNode raidnode = null;
  String jobTrackerName = null;
  Path hostsFile;
  Path excludeFile;
  ArrayList<String> decommissionedNodes = new ArrayList<String>();
  Random rand = new Random();
  
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }
  
  // Setup -- manually run before each test
  public void setup(int numDataNodes, int timeBeforeHar) 
      throws IOException, ClassNotFoundException {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    conf.setBoolean("dfs.use.inline.checksum", false);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);
    conf.set("mapred.raid.http.address", "localhost:0");

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());

    Utils.loadTestCodecs(conf, STRIPE_LENGTH, 1, 3, "/raid", "/raidrs");

    conf.setBoolean("dfs.permissions", false);
    
    // Prevent the namenode from replicating files
    conf.setInt("dfs.replication.interval", Integer.MAX_VALUE);
    conf.setClass("dfs.block.replicator.classname",
        BlockPlacementPolicyFakeData.class, BlockPlacementPolicy.class);
    
    // Set the class which is used for the copying operation
    //conf.setClass("raid.blockcopier.class", c, BlockCopyHelper.class);

    // Set up the mini-cluster
    dfs = new MiniDFSCluster(conf, numDataNodes, true, null, false);
    dfs.waitActive();
    fileSys = (DistributedFileSystem) dfs.getFileSystem();
    namenode = dfs.getNameNode();
    String namenodeRoot = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenodeRoot);
    mr = new MiniMRCluster(4, namenodeRoot, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    //FileSystem.setDefaultUri(conf, namenodeRoot);
    conf.set("mapred.job.tracker", jobTrackerName);
    
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<policy name = \"RaidTest1\"> " +
                      "<srcPath prefix=\"/user/hadoop/raidtest\"/> " +
                      "<codecId>xor</codecId> " +
                      "<property> " +
                        "<name>targetReplication</name> " +
                        "<value>1</value> " + 
                        "<description>after RAIDing, decrease the replication factor of a file to this value." +
                        "</description> " + 
                      "</property> " +
                      "<property> " +
                        "<name>metaReplication</name> " +
                        "<value>1</value> " + 
                        "<description> replication factor of parity file" +
                        "</description> " + 
                      "</property> " +
                      "<property> " +
                        "<name>modTimePeriod</name> " +
                        "<value>2000</value> " + 
                        "<description> time (milliseconds) after a file is modified to make it " +
                                       "a candidate for RAIDing " +
                        "</description> " + 
                      "</property> ";
    if (timeBeforeHar >= 0) {
      str +=
                      "<property> " +
                        "<name>time_before_har</name> " +
                        "<value>" + timeBeforeHar + "</value> " +
                        "<description> amount of time waited before har'ing parity files" +
                        "</description> " + 
                      "</property> ";
    }

    str +=
                   "</policy>" +
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
    
    // Set up raid node
    if (raidnode == null) {
      Configuration localConf = new Configuration(conf);
      localConf.setInt("raid.blockfix.interval", 1000);
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
      localConf.setLong("raid.blockfix.filespertask", 2L);
      
      try {
        raidnode = RaidNode.createRaidNode(null, localConf);
      } catch (ClassNotFoundException ex) {
        ex.printStackTrace();
        throw ex;
      }
    }
    
    // Set up excludes file
    excludeFile = new Path(TEST_DIR, "exclude");
    
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 4);
    writeExcludesFileAndRefresh(null);
  }
  
  // Teardown -- manually run after each test
  public void teardown() {
    if (raidnode != null) { 
      raidnode.stop();
      raidnode.join();
    }
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }
  
  
  public void testDataSource() throws IOException, ClassNotFoundException {
    final int numDatanodes = 5;
    final int blocksPerFile = STRIPE_LENGTH;
    setup(numDatanodes, -1);
    
    // Make a many-block file with repl=2. This should guarantee that at least
    // some blocks are on both decommissioned nodes.
    Path filePath = new Path("/user/hadoop/testDataSource");
    String[] fileNames = {"file0", "file1", "file2", "file3", "file4"};
    long[][] crcs = new long[fileNames.length][];
    FileStatus[] files = new FileStatus[fileNames.length];
    
    createRandomFiles(filePath, fileNames, 2, blocksPerFile, crcs, files);
    Worker bc = 
      ((DistBlockIntegrityMonitor) raidnode.blockIntegrityMonitor).getDecommissioningMonitor();
    
    for (FileStatus file : files) {
      printFileLocations(file);
    }
    
    
    Set<String> downNodes = new HashSet<String>();
    for (int i = 0; i < numDatanodes; i++) {
      // Decommission a node and test the data source.
      String downNode = decommissionOneNode();
      downNodes.add(downNode);

      // Compute which files have decommissioning blocks and how many
      HashMap<String, Integer> decomFiles = new HashMap<String, Integer>();
      for (FileStatus file : files) {

        String path = file.getPath().toUri().getPath();
        int decommissioningBlocks = 0;
        BlockLocation[] locations = 
          fileSys.getFileBlockLocations(file, 0, file.getLen());

        for (BlockLocation loc : locations) {
          String[] names = loc.getNames();
          if (downNodes.contains(names[0]) && downNodes.contains(names[1])) {
            decommissioningBlocks++;
          }
        }
        if (decommissioningBlocks > 0) {
          decomFiles.put(path, decommissioningBlocks);
        }
      }
      
      // Verify results
      // FIXME: re-enable test when the underlying issue in fsck/namesystem is resolved
      //assertEquals(decomFiles.keySet(), bf.getDecommissioningFiles().keySet());  
    }
    
    // Un-decommission those nodes and test the data source again.
    writeExcludesFileAndRefresh(null);
    assertEquals(0, bc.getLostFiles(fileSys).size());
    
    // Done.
    teardown();
  }
  
  
  public void testNameNodeBehavior() 
      throws IOException, ClassNotFoundException, InterruptedException {
    
    setup(2, -1);
    final int fileLenBlocks = STRIPE_LENGTH;
    final int repl = 1;
    
    // Get set up with datanode references
    DatanodeInfo[] nodeInfos = namenode.getDatanodeReport(DatanodeReportType.ALL);
    DatanodeDescriptor[] nodes = new DatanodeDescriptor[nodeInfos.length];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = namenode.namesystem.getDatanode(nodeInfos[i]);
      LOG.info("nodes[" + i + "]=" + nodes[i].getName());
    }
    
    // Create file with one block on nodes[1] and the rest on nodes[0]
    Path raidPath = new Path("/raidrs");
    Path filePath = new Path("/user/hadoop/testNameNodeBehavior/file");
    long[] crc = createRandomFileDispersed(filePath, fileLenBlocks, 
                                         nodes[0], nodes[1]);
    
    FileStatus file = fileSys.getFileStatus(filePath);
    
    // Raid the file; parity blocks go on nodes[0]
    BlockPlacementPolicyFakeData.lastInstance.overridingDatanode = nodes[0];
    
    RaidNode.doRaid(conf, file, raidPath, Codec.getCodec("rs"),
        new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, 
        false, repl, repl);
    Thread.sleep(1000);
    printFileLocations(file);
    
    BlockPlacementPolicyFakeData.lastInstance.overridingDatanode = null;
    
    // Now decommission the second node
    ArrayList<String> decommissioned = new ArrayList<String>();
    decommissioned.add(nodes[1].getName());
    
    writeExcludesFileAndRefresh(decommissioned);
    
    // Wait for the BlockRegenerator to do its thing
    long now = System.currentTimeMillis();
    BlockIntegrityMonitor bf = raidnode.blockIntegrityMonitor;
    while ((bf.getNumFilesCopied() == 0) && (bf.getNumFileCopyFailures() == 0)
      && ((System.currentTimeMillis() - now) < 30000)) {
      LOG.info("Waiting for the BlockRegenerator to finish... ");
      Thread.sleep(1000);
    }
    
    // Validate result
    printFileLocations(file);
    assertEquals(0, bf.getNumFileCopyFailures());
    assertEquals(1, bf.getNumFilesCopied());
    
    // No corrupt block fixing should have happened
    assertEquals("corrupt block fixer unexpectedly performed fixing", 
        0, bf.getNumFilesFixed());
    assertEquals("corrupt block fixer unexpectedly attempted fixing", 
        0, bf.getNumFileFixFailures());
    
    validateFileCopy(fileSys, filePath, file.getLen(), crc, false);
    
    teardown();
  }
  
  private String decommissionOneNode() throws IOException {
    
    DFSClient client = ((DistributedFileSystem)fileSys).getClient();
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    int index = 0;
    boolean found = false;
    while (!found) {
      index = rand.nextInt(info.length);
      if (!info[index].isDecommissioned() && !info[index].isDecommissionInProgress()) {
        found = true;
      }
    }
    String nodename = info[index].getName();
    System.out.println("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    decommissionedNodes.add(nodename);
    writeExcludesFileAndRefresh(decommissionedNodes);
    return nodename;
  }
  
  private long[] createRandomFile(Path file, int repl, int numBlocks) 
      throws IOException {

    long[] crcs = new long[numBlocks]; 
    CRC32 crc = new CRC32();
    Random rand = new Random();
    FSDataOutputStream stm = fileSys.create(file, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, BLOCK_SIZE);
    // Write whole blocks.
    byte[] b = new byte[(int)BLOCK_SIZE];
    for (int i = 1; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      
      crc.update(b);
      crcs[i-1] = crc.getValue();
      crc.reset();
    }
    // Write partial block.
    b = new byte[(int)BLOCK_SIZE/2 - 1];
    rand.nextBytes(b);
    stm.write(b);
    crc.update(b);
    crcs[crcs.length-1] = crc.getValue();

    stm.close();
    return crcs;//crc.getValue();
  }
  
  private static void printHexBufTmp(byte[] buf) {
    System.out.print("0x");
    for (int j = 0; j < buf.length; j++) {
      System.out.print(String.format("%02X", buf[j]));
    }
    System.out.println();
  }
  
  private long[] createRandomFileDispersed(Path file, int numBlocks,
               DatanodeDescriptor primaryNode, DatanodeDescriptor altNode) 
      throws IOException, InterruptedException {
    
    BlockPlacementPolicyFakeData bp = BlockPlacementPolicyFakeData.lastInstance;
    DatanodeDescriptor tmp = bp.overridingDatanode;
    
    final int repl = 1;
    long[] crcs = new long[numBlocks];
    CRC32 crc = new CRC32();
    Random rand = new Random();
    FSDataOutputStream stm = fileSys.create(file, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, BLOCK_SIZE);
    
    // Create the first block on the alt node
    bp.overridingDatanode = altNode;

    // fill random data into file
    final byte[] b = new byte[(int)BLOCK_SIZE];
    LOG.info("Writing first block (alt. host)");
    rand.nextBytes(b);
    stm.write(b);
    crc.update(b);
    crcs[0] = crc.getValue();

    stm.flush();
    Thread.sleep(1000); // What a hack. Le sigh.
    
    // Now we want to write on the altNode
    bp.overridingDatanode = primaryNode;
    
    // Write the rest of the blocks on primaryNode
    for (int i = 1; i < numBlocks; i++) {
      LOG.info("Writing block number " + i + " (primary host)");
      
      rand.nextBytes(b);
      stm.write(b);
      crc.reset();
      crc.update(b);
      crcs[i] = crc.getValue();
    }
    stm.close();
    Thread.sleep(1000);
    
    // Reset this guy
    bp.overridingDatanode = tmp;
    
    return crcs;
  }

  private void createRandomFiles(Path folder, String[] fileNames, 
                                 int repl, int numBlocks, 
                                 /*out*/ long[][] crcOut, FileStatus[] fileOut)
      throws IOException {

    int i = 0;
    for (String name : fileNames) {
      Path fPath = new Path(folder, name);
      crcOut[i] = createRandomFile(fPath, repl, numBlocks);
      fileOut[i++] = fileSys.getFileStatus(fPath);
    }
  }

  private void writeExcludesFileAndRefresh(ArrayList<String> nodes) 
      throws IOException {

    FileSystem fs = FileSystem.getLocal(conf);
    LOG.info(fs);

    // delete if it already exists
    if (fs.exists(excludeFile)) {
      fs.delete(excludeFile, true);
    }

    FSDataOutputStream stm = fs.create(excludeFile);

    if (nodes != null) {
      for (String node : nodes) {
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
    
    namenode.namesystem.refreshNodes(conf);
  }

  
  public void testReconstruction () throws Exception {
    final int numBlocks = STRIPE_LENGTH + 1;
    final int repl = 1;
    setup(10, -1);
    
    DistBlockIntegrityMonitor br = new DistBlockRegeneratorFake(conf);
    Worker bc = br.getDecommissioningMonitor();
    
    // Generate file
    Path raidPath = new Path("/raidrs");
    
    Path filePath = new Path("/user/hadoop/testReconstruction/file");
    long[] crcs = createRandomFile(filePath, repl, numBlocks);
    FileStatus file = fileSys.getFileStatus(filePath);
    RaidNode.doRaid(conf, file, raidPath, Codec.getCodec("rs"),
        new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE, 
        false, repl, repl);
    
    // Do some testing
    printFileLocations(file);

    // We're gonna "decommission" the file
    TestBlockCopier.decommissioningFiles = 
      new String[] { filePath.toUri().toString() };

    // "Decommission" each of the file's blocks in turn
    List<LocatedBlock> fileBlocks = 
        dfs.getNameNode().getBlockLocations(filePath.toUri().toString(), 
                                            0L,
                                            file.getLen()).getLocatedBlocks();
    

    for (LocatedBlock b : fileBlocks) {
      TestBlockCopier.decommissioningBlocks = new LocatedBlock[] { b };
      
      bc.checkAndReconstructBlocks();
      
      long start = System.currentTimeMillis();
      while ((br.jobsRunning() > 0) 
          && ((System.currentTimeMillis() - start) < 30000)) {
        LOG.info("Waiting on block regen jobs to complete ("
            + br.jobsRunning() + " running).");
        Thread.sleep(1000);
        bc.checkJobs();
      }
    }
    
    // Verify that each block now has an extra replica.
    printFileLocations(file);
    
    fileBlocks =
      dfs.getNameNode().getBlockLocations(filePath.toUri().toString(), 
          0L,
          file.getLen()).getLocatedBlocks();
    for (LocatedBlock b : fileBlocks) {
      assertEquals("block was improperly replicated", 
          repl+1, b.getLocations().length);
    }
    bc.updateStatus();
    assertEquals("unexpected copy failures occurred", 
        0, br.getNumFileCopyFailures());
    assertEquals("unexpected number of file copy operations", 
        numBlocks, br.getNumFilesCopied());
    
    // No corrupt block fixing should have happened
    assertEquals("corrupt block fixer unexpectedly performed fixing", 
        0, br.getNumFilesFixed());
    assertEquals("corrupt block fixer unexpectedly attempted fixing", 
        0, br.getNumFileFixFailures());
    
    // Verify file contents
    validateFileCopy(fileSys, filePath, file.getLen(), crcs, true);
    
    // Done
    teardown();
  }

  
  public static LocatedBlock[] decommissioningBlocks;
  public static String[] decommissioningFiles;
  
  
  // Assumes current repl is 2
  static void validateFileCopy(FileSystem fs, Path path, long size, 
                               long[] blockCrcs, boolean twiceThrough) 
      throws IOException {
    
    final int timesThrough = (twiceThrough ? 2 : 1);
    final int numBlocks = (int) Math.ceil((double)size / BLOCK_SIZE);
    
    // Check all the blocks timesThrough times
    FSDataInputStream in = fs.open(path);
    CRC32 crc = new CRC32();
    
    for (int i = 0; i < timesThrough; i++) {
      for (int b = 0; b < numBlocks; b++) {
        int chunkSize = (int) Math.min(BLOCK_SIZE, (size-(b*BLOCK_SIZE)));
        byte[] buf = new byte[chunkSize];
        
        in.read(buf);
        crc.reset();
        crc.update(buf);
        
        assertEquals(("Block crc " + b + " did not match on iteration " + i), 
                     blockCrcs[b], crc.getValue());
      }
      assert in.getPos() == size : "Did not read to end of file";
      if (i < (timesThrough - 1)) {
        in.seekToNewSource(0);
      }
    }
  }
  
  private void printFileLocations(FileStatus file)
      throws IOException {

    System.out.println(file.getPath() + " block locations:");
    
    BlockLocation[] locations = fileSys.getFileBlockLocations(file, 0, 
        file.getLen());
    for (int idx = 0; idx < locations.length; idx++) {
      String[] loc = locations[idx].getNames();
      System.out.print("Block[" + idx + "] : ");
      for (int j = 0; j < loc.length; j++) {
        System.out.print(loc[j] + " ");
      }
      System.out.println();
    }
  }
  

  static class DistBlockRegeneratorFake extends DistBlockIntegrityMonitor {
    
    public DistBlockRegeneratorFake(Configuration conf) throws Exception {
      super(conf);
    }
    
    @Override
    public void configureJob(Job job, 
       Class<? extends BlockReconstructor> rClass) {
      
      super.configureJob(job, rClass);
      
      LocatedBlock[] lb = TestBlockCopier.decommissioningBlocks;
      
      String[] hashes = new String[lb.length];
      for (int i = 0; i < lb.length; i++) {
        hashes[i] = Integer.toString(lb[i].getBlock().hashCode());
      }
      
      ((JobConf)job.getConfiguration()).setClass(ReconstructionMapper.RECONSTRUCTOR_CLASS_TAG, 
                                                 ReconstructorFakeData.class, 
                                                 BlockReconstructor.class);
      ((JobConf)job.getConfiguration()).setStrings("hdfs.testblockcopier.blockhashes", hashes);
    }
    
    @Override
    protected Map<String, Integer> getLostFiles(
        Pattern pattern, String[] dfsckArgs) throws IOException {
      
      Map<String, Integer> map = new HashMap<String, Integer>();
      
      // Disable CorruptionMonitor 
      if (pattern.equals(DistBlockIntegrityMonitor.LIST_CORRUPT_FILE_PATTERN)) {
        return map;
      }
      
      for (String file : TestBlockCopier.decommissioningFiles) {
        map.put(file, 1);
      }
      return map;
    }
  }
  
  static class ReconstructorFakeData extends BlockReconstructor.DecommissioningBlockReconstructor {

    private final Map<Integer, Boolean> decomBlockHashes;
    
    public ReconstructorFakeData(Configuration conf) throws IOException {
      super(conf);
      
      // Deserialize block hashes
      String[] hashes = 
        conf.getStrings("hdfs.testblockcopier.blockhashes", new String[0]);
      
      decomBlockHashes = new HashMap<Integer, Boolean>();
      for (String hash : hashes) {
        decomBlockHashes.put(Integer.parseInt(hash), true);
      }
    }
    
    @Override
    List<LocatedBlockWithMetaInfo> lostBlocksInFile(DistributedFileSystem fs,
                                           String uriPath,
                                           FileStatus stat)
        throws IOException {
      
      List<LocatedBlockWithMetaInfo> blocks = 
        new ArrayList<LocatedBlockWithMetaInfo>();
      
      VersionedLocatedBlocks locatedBlocks;
      int namespaceId = 0;
      int methodsFingerprint = 0;
      if (DFSClient.isMetaInfoSuppoted(fs.getClient().namenodeProtocolProxy)) {
        LocatedBlocksWithMetaInfo lbksm = fs.getClient().namenode.
                  openAndFetchMetaInfo(uriPath, 0, stat.getLen());
        namespaceId = lbksm.getNamespaceID();
        locatedBlocks = lbksm;
        methodsFingerprint = lbksm.getMethodFingerPrint();
      } else {
        locatedBlocks = fs.getClient().namenode.open(uriPath, 0, stat.getLen());
      }
      final int dataTransferVersion = locatedBlocks.getDataProtocolVersion();

      List<LocatedBlock> lb = locatedBlocks.getLocatedBlocks();
      
      for (LocatedBlock b : lb) {
        if (decomBlockHashes.get(b.getBlock().hashCode()) != null) {
          blocks.add(new LocatedBlockWithMetaInfo(b.getBlock(),
              b.getLocations(), b.getStartOffset(),
              dataTransferVersion, namespaceId, methodsFingerprint));
        }
      }
      
      return blocks;
    }
    
  }

}

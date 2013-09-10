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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.raid.RaidCodecBuilder;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RaidTask;
import org.apache.hadoop.hdfs.server.protocol.RaidTaskCommand;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumWriter;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRaidFile {
  public static final Log LOG = LogFactory.getLog(TestRaidFile.class);
  private MiniDFSCluster cluster;
  private NameNode nn;
  private DistributedFileSystem dfs;
  private DistributedFileSystem userdfs;
  private static long blockSize = 512;
  private static int numDataBlocks = 6;
  private static int numRSParityBlocks = 3;
  private static Configuration conf;
  private static UnixUserGroupInformation USER1;
  private static int id = 0;
  private static Random rand = new Random();
  private static byte[] bytes = new byte[(int)blockSize];
  static {
    conf = new Configuration();
    conf.setLong("dfs.block.size", blockSize);
    conf.setBoolean("dfs.permissions", true);
  }
  
  class FakeBlockGeneratorInjectionHandler extends InjectionHandler {
    @Override
    public void _processEventIO(InjectionEventI event, Object... args) 
        throws IOException {
      if (event == InjectionEvent.DATANODE_PROCESS_RAID_TASK) {
        int namespaceId = nn.getNamespaceID();
        DataNode dn = (DataNode)args[0];
        RaidTaskCommand rtc = (RaidTaskCommand)args[1];
        RaidTask[] tasks = rtc.tasks;
        for (RaidTask rw: tasks) {
          // Generate all parity block locally instead of sending them remotely
          try {
            for (int idx = 0; idx < rw.toRaidIdxs.length; idx++) {
              Block blk = rw.stripeBlocks[rw.toRaidIdxs[idx]];
              blk.setNumBytes(blockSize);
              BlockDataFile.Writer dataOut = 
                  ((BlockInlineChecksumWriter)dn.getFSDataset().writeToBlock(
                      namespaceId, blk, blk, false, false, 1, 512)).getBlockDataFile()
                      .getWriter(0);
              dataOut.write(bytes);
              dataOut.close();
              dn.finalizeAndNotifyNamenode(namespaceId, blk); 
            }
          } catch (IOException ioe) {
            LOG.warn(ioe);
          }
        }
      }
    }
  }
  
  @Before
  public void startUpCluster() throws IOException {
    RaidCodecBuilder.loadDefaultFullBlocksCodecs(conf, numRSParityBlocks,
        numDataBlocks);
    cluster = new MiniDFSCluster(conf, 4, true, null);
    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);
    nn = cluster.getNameNode();
    assertNotNull("Failed to get NameNode", nn);
    Configuration newConf = new Configuration(conf);
    USER1 = new UnixUserGroupInformation("foo", new String[] {"bar" });
    UnixUserGroupInformation.saveToConf(newConf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, USER1);
    userdfs = (DistributedFileSystem)FileSystem.get(newConf); // login as ugi
    InjectionHandler h = new FakeBlockGeneratorInjectionHandler();
    InjectionHandler.set(h);
    rand.nextBytes(bytes);
  }

  @After
  public void shutDownCluster() throws IOException {
    if(dfs != null) {
      dfs.close();
    }
    if (userdfs != null) {
      userdfs.close();
    }
    if(cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
    InjectionHandler.clear();
  }
  
  public static void raidFile(DistributedFileSystem fs, Path source, 
      String codecId, short expectedSourceRepl, String exceptionMessage)
          throws Exception {
    try {
      fs.raidFile(source, codecId, expectedSourceRepl);
    } catch (Exception e) {
      if (exceptionMessage == null) {
        // This is not expected
        throw e;
      }
      assertTrue("Exception " + e.getMessage() + " doesn't match " + 
                 exceptionMessage, e.getMessage().contains(exceptionMessage));
    }
  }
  
  /**
   * Test we could XOR Raid files with different number of blocks:
   * 1. Two full stripes
   * 2. one and half stripes
   * 3. less than one stripe 
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testRaidXORFile() throws Exception {
    raidFile(12, (short)2, "xor");
    raidFile(9, (short)3, "xor");
    raidFile(3, (short)2, "xor");
  }
  
  /**
   * Test we could RS Raid files with different number of blocks
   * 1. Two full stripes
   * 2. one and half stripes
   * 3. less than one stripe  
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testRaidRSFile() throws Exception {
    raidFile(12, (short)1, "rs");
    raidFile(9, (short)2, "rs");
    raidFile(3, (short)3, "rs");
  }
  
  private static INodeFile getINodeFile(NameNode nn, Path source) {
    INode[] inodes = nn.getNamesystem().dir.getExistingPathINodes(
        source.toUri().getPath());
    return (INodeFile)inodes[inodes.length - 1];
  }
  
  private static FileStatus verifyRaidFiles(NameNode nn, 
      DistributedFileSystem fileSys, FileStatus statBefore, 
      LocatedBlocks lbsBefore, Path source, long fileLen, long crc,
      short expectedSourceRepl, String codecId, boolean checkParityBlocks)
          throws Exception {
    FileStatus statAfter  = fileSys.getFileStatus(source);
    LocatedBlocks lbsAfter = fileSys.getLocatedBlocks(source, 0, fileLen);
    // Verify file stat
    assertEquals(statBefore.getBlockSize(), statAfter.getBlockSize());
    assertEquals(statBefore.getLen(), statAfter.getLen());
    assertEquals(expectedSourceRepl, statAfter.getReplication());
    
    // Verify getLocatedBlocks
    assertEquals(lbsBefore.getLocatedBlocks().size(), 
        lbsAfter.getLocatedBlocks().size());
    for (int i = 0; i < lbsBefore.getLocatedBlocks().size(); i++) {
      assertEquals(lbsBefore.get(i).getBlock(), lbsAfter.get(i).getBlock());
    }
    
    // Verify file content
    assertTrue("File content matches", DFSTestUtil.validateFile(fileSys, 
        statBefore.getPath(), statBefore.getLen(), crc));
    return statAfter;
  }
  
  private void fillChecksums(Path source) {
    INodeFile file = getINodeFile(nn, source);
    BlockInfo[] bis = file.getBlocks();
    for (int i = 0; i < bis.length; i++) {
      bis[i].setChecksum(1);
    }
  }
  
  /**
   * 1. Create a file
   * 2. Fill fake checksums in it
   * 3. Call raidFile to convert it into Raid format and return false. Namenode
   *  will start schedule raiding
   * 4. verify we could read the file
   * 5. Datanodes will receive RaidTaskCommand from namenode and jumps into
   *  FakeBlockGeneratorInjectionHandler, this handler will create a fake parity
   *  block in the datanode and notifies namenode 
   * 6. keep calling raidFile until all parity blocks are generated, then raidFile
   *  will succeed to reduce replication and return true
   * 
   */
  private void raidFile(int numBlocks, short expectedSourceRepl, String codecId)
      throws Exception {
    LOG.info("RUNNING testMergeFile numBlocks=" + numBlocks + 
        " sourceRepl=" + expectedSourceRepl + " codecId=" + codecId);
    id++;
    long fileLen = blockSize * numBlocks;
    Path dir = new Path ("/user/facebook" + id);
    assertTrue(dfs.mkdirs(dir));
    Path source = new Path(dir, "1");
    long crc = DFSTestUtil.createFile(dfs, source, fileLen, (short)3, 1);
    
    LOG.info("Fill fake checksums to the file");
    fillChecksums(source);
    
    ContentSummary cBefore = dfs.getContentSummary(dir);
    FileStatus statBefore = dfs.getFileStatus(source);
    LocatedBlocks lbsBefore = dfs.getLocatedBlocks(source, 0, fileLen);
    
    // now raid the file 
    boolean result = dfs.raidFile(source, codecId, expectedSourceRepl);
    assertTrue("raidFile should return false", !result);
    
    ContentSummary cAfter = dfs.getContentSummary(dir);
    
    // verify directory stat
    assertEquals("File count doesn't change", cBefore.getFileCount(),
        cAfter.getFileCount());
   
    verifyRaidFiles(nn, dfs, statBefore, lbsBefore, source, fileLen, crc,
        statBefore.getReplication(), codecId, false);
    LocatedBlocks lbsAfter = dfs.getLocatedBlocks(source, blockSize, fileLen);
    assertEquals(numBlocks - 1, lbsAfter.getLocatedBlocks().size());
    for (int i = 0; i < numBlocks - 1; i++) {
      assertEquals(lbsBefore.get(i + 1).getBlock(), lbsAfter.get(i).getBlock());
    }
    
    String otherCodec = codecId.equals("xor") ? "rs" : "xor";
    raidFile(dfs, source, otherCodec, (short)2,
        "raidFile: couldn't raid a raided file");
    
    RaidCodec codec = RaidCodec.getCodec(codecId);
    long startTime = System.currentTimeMillis();
    result = false;
    while (System.currentTimeMillis() - startTime < 70000 && !result) {
      DFSTestUtil.waitNSecond(3);
      result = dfs.raidFile(source, codecId, expectedSourceRepl);
    }
    assertTrue("Finish raiding", result);
    verifyRaidFiles(nn, dfs, statBefore, lbsBefore, source, fileLen, crc,
        expectedSourceRepl, codecId, true);
    if (codec.minSourceReplication >= 2) {
      try {
        dfs.setReplication(source, (short)(codec.minSourceReplication-1));
        assertTrue("setReplication should fail", false);
      } catch (IOException ioe) {
        assertTrue("fail to setReplication", 
            ioe.getMessage().contains("Couldn't set replication smaller than "));
      }
    }
  }
  
  /**
   * Test raidFile fails with expected exception for different illegal cases
   * such as empty file, directory, hardlinked files, files without checksums,
   * files without permission...
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testRaidFileIllegalCases() throws Exception {
    LOG.info("Running testRaidFileIllegalCases");
    int numBlocks = 6;
    long fileLen = blockSize * numBlocks;
    
    Path dir = new Path ("/user/facebook");
    assertTrue(dfs.mkdirs(dir));
    Path source = new Path(dir, "1");
    Path dest = new Path(dir, "2");
    DFSTestUtil.createFile(dfs, source, fileLen, (short)3, 1);
    Path emptyFile = new Path("/empty");
    DFSTestUtil.createFile(dfs, emptyFile, 0L, (short)3, 1);
    
    raidFile(dfs, source, "nonexist", (short)2, 
        "raidFile: codec nonexist doesn't exist");
    raidFile(dfs, source, "xor", (short)1,
        "raidFile: expectedSourceRepl is smaller than ");
    
    dfs.setOwner(source, "foo", "bar");
    LOG.info("Disallow write on " + source);
    dfs.setPermission(source, new FsPermission((short)0577));
    raidFile(userdfs, source, "xor", (short)2, "Permission denied");
    
    LOG.info("Enable write on " + source);
    dfs.setPermission(source, new FsPermission((short)0777));
    
    LOG.info("Test different types of files");
    raidFile(dfs, new Path("/nonexist"), "rs", (short)1,
        "raidFile: source file doesn't exist");
    
    raidFile(dfs, dir, "rs", (short)1, "raidFile: source file is a directory");
    
    raidFile(dfs, emptyFile, "rs", (short)1, "raidFile: source file is empty");
    
    raidFile(dfs, source, "rs", (short)1, 
        "raidFile: not all source blocks have checksums");

    LOG.info("Hardlink the file to " + dest);
    dfs.hardLink(source, dest);
    raidFile(dfs, dest, "rs", (short)1, "raidFile: cannot raid a hardlinked file");
    raidFile(dfs, source, "rs", (short)1, "raidFile: cannot raid a hardlinked file");    
  }
}

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
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.raid.RaidCodecBuilder;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMergeFile {
  public static final Log LOG = LogFactory.getLog(TestMergeFile.class);

  private static final short REPL_FACTOR = 2;
  
  private MiniDFSCluster cluster;
  private NameNode nn;
  private DistributedFileSystem dfs;
  private DistributedFileSystem userdfs;
  private static long blockSize = 512;
  private static int numDataBlocks = 6;
  private static int numRSParityBlocks = 3;
  private static Configuration conf;
  private static Random rand = new Random();
  private static UnixUserGroupInformation USER1;
  private static int id = 0;
  static {
    conf = new Configuration();
    conf.setLong("dfs.block.size", blockSize);
    conf.setBoolean("dfs.permissions", true);
  }
  
  @Before
  public void startUpCluster() throws IOException {
    RaidCodecBuilder.loadDefaultFullBlocksCodecs(conf, numRSParityBlocks,
        numDataBlocks);
    cluster = new MiniDFSCluster(conf, REPL_FACTOR, true, null);
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
  }
  
  public void mergeFile(Path parity, Path source, String codecId, int[] checksums,
      String exceptionMessage) throws Exception {
    mergeFile(dfs, parity, source, codecId, checksums, exceptionMessage);
  }
  
  public void mergeFile(DistributedFileSystem fs, Path parity, Path source, 
      String codecId, int[] checksums, String exceptionMessage) throws Exception {
    try {
      fs.merge(parity, source, codecId, checksums);
    } catch (Exception e) {
      if (exceptionMessage == null) {
        // This is not expected
        throw e;
      }
      assertTrue("Exception " + e.getMessage() + " doesn't match " + 
                 exceptionMessage, e.getMessage().contains(exceptionMessage));
    }
  }
  
  @Test
  public void testMergeXORFile() throws Exception {
    mergeFile(12, 2, (short)2, "xor");
    mergeFile(9, 2, (short)1, "xor");
    mergeFile(3, 1, (short)2, "xor");
  }
  
  @Test
  public void testMergeRSFile() throws Exception {
    mergeFile(12, 6, (short)1, "rs");
    mergeFile(9, 6, (short)1, "rs");
    mergeFile(3, 3, (short)2, "rs");
  }
  
  /**
   * @return current file status of file
   */
  public static FileStatus verifyMergeFiles(DistributedFileSystem fileSys, FileStatus statBefore, 
      LocatedBlocks lbsBefore, Path source, long fileLen, long crc) throws Exception {
    FileStatus statAfter  = fileSys.getFileStatus(source);
    LocatedBlocks lbsAfter = fileSys.getLocatedBlocks(source, 0, fileLen);
    // Verify file stat
    assertEquals(statBefore.getBlockSize(), statAfter.getBlockSize());
    assertEquals(statBefore.getLen(), statAfter.getLen());
    assertEquals(statBefore.getReplication(), statAfter.getReplication());
    
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
  
  public void mergeFile(int numBlocks, int parityBlocks, short sourceRepl,
      String codecId) throws Exception {
    LOG.info("RUNNING testMergeFile numBlocks=" + numBlocks + 
        " parityBlocks=" + parityBlocks + " sourceRepl=" + sourceRepl +
        " codecId=" + codecId);
    id++;
    long fileLen = blockSize * numBlocks;
    long parityLen = blockSize * parityBlocks;
    Path dir = new Path ("/user/facebook" + id);
    assertTrue(dfs.mkdirs(dir));
    Path source = new Path(dir, "1");
    Path dest = new Path(dir, "2");
    long crc = DFSTestUtil.createFile(dfs, source, fileLen, sourceRepl, 1);
    
    Path parityDir = new Path("/raid/user/facebook" + id);
    assertTrue(dfs.mkdirs(parityDir));
    RaidCodec codec = RaidCodec.getCodec(codecId);
    Path parity = new Path(parityDir, "1");
    DFSTestUtil.createFile(dfs, parity, parityLen,
        codec.parityReplication, 1);
    int[] checksums = new int[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      checksums[i] = rand.nextInt();
    }
    
    ContentSummary cBefore = dfs.getContentSummary(dir);
    ContentSummary cParityBefore = dfs.getContentSummary(parityDir);
    FileStatus statBefore = dfs.getFileStatus(source);
    LocatedBlocks lbsBefore = dfs.getLocatedBlocks(source, 0, fileLen);
    dfs.setTimes(parity, statBefore.getModificationTime(), 0);
    
    // now merge
    dfs.merge(parity, source, codecId, checksums);
    
    ContentSummary cAfter = dfs.getContentSummary(dir);
    ContentSummary cParityAfter = dfs.getContentSummary(parityDir);
    
    // verify directory stat
    assertEquals("File count doesn't change", cBefore.getFileCount(),
        cAfter.getFileCount());
    assertEquals("Space consumed is increased", 
        cBefore.getSpaceConsumed() + parityLen * codec.parityReplication,
        cAfter.getSpaceConsumed());
    assertEquals("Parity file is removed", cParityBefore.getFileCount() - 1,
        cParityAfter.getFileCount());
    assertEquals("Space consumed is 0", 0, cParityAfter.getSpaceConsumed());
   
    // Verify parity is removed
    assertTrue(!dfs.exists(parity));
    
    verifyMergeFiles(dfs, statBefore, lbsBefore, source, fileLen, crc);
   
    LocatedBlocks lbsAfter = dfs.getLocatedBlocks(source, blockSize, fileLen);
    assertEquals(numBlocks - 1, lbsAfter.getLocatedBlocks().size());
    for (int i = 0; i < numBlocks - 1; i++) {
      assertEquals(lbsBefore.get(i + 1).getBlock(), lbsAfter.get(i).getBlock());
    }
    assertTrue("Should not be able to hardlink a raided file", 
        !dfs.hardLink(source, dest));
  }
  
  @Test
  public void testMergeIllegalCases() throws Exception {
    LOG.info("Running testMergeIllegalCases");
    int numBlocks = 6;
    long fileLen = blockSize * numBlocks;
    
    Path dir = new Path ("/user/facebook");
    assertTrue(dfs.mkdirs(dir));
    Path source = new Path(dir, "1");
    Path dest = new Path(dir, "2");
    DFSTestUtil.createFile(dfs, source, fileLen, REPL_FACTOR, 1);
    FileStatus stat = dfs.getFileStatus(source);
    Path raidDir = new Path("/raid/user/facebook");
    assertTrue(dfs.mkdirs(raidDir));
    Path badParity = new Path(raidDir, "1");
    DFSTestUtil.createFile(dfs, badParity, blockSize * 2,
        (short)1, 1);
    int[] checksums = new int[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      checksums[i] = rand.nextInt();
    }
    Path emptyFile = new Path("/empty");
    DFSTestUtil.createFile(dfs, emptyFile, 0L, REPL_FACTOR, 1);
    
    mergeFile(badParity, source, "xor", null, 
        "merge: checksum array is empty or null");
    mergeFile(badParity, source, "nonexist", checksums,
        "merge: codec nonexist doesn't exist");
    
    dfs.setOwner(source, "foo", "bar");
    dfs.setOwner(badParity, "foo", "bar");
    dfs.setOwner(raidDir, "foo", "bar");
    
    LOG.info("Disallow write on " + source);
    dfs.setPermission(source, new FsPermission((short)0577));
    mergeFile(userdfs, badParity, source, "rs", checksums, "Permission denied");
    
    LOG.info("Enable write on " + source + " and disable read on " + badParity);
    dfs.setPermission(source, new FsPermission((short)0777));
    dfs.setPermission(badParity, new FsPermission((short)0377));
    mergeFile(userdfs, badParity, source, "rs", checksums, "Permission denied");
    
    LOG.info("Enable read on " + badParity + " and disable write on " + raidDir);
    dfs.setPermission(badParity, new FsPermission((short)0777));
    dfs.setPermission(raidDir, new FsPermission((short)0577));
    mergeFile(userdfs, badParity, source, "rs", checksums, "Permission denied");
    dfs.setPermission(raidDir, new FsPermission((short)0777));
    
    LOG.info("Test different types of files");
    mergeFile(new Path("/nonexist"), source, "rs", checksums,
        "merge: source file or parity file doesn't exist");
    mergeFile(badParity, new Path("/nonexist"), "rs", checksums,
        "merge: source file or parity file doesn't exist");
    
    mergeFile(raidDir, source, "rs", checksums,
        "merge: source file or parity file is a directory");
    mergeFile(badParity, dir, "rs", checksums,
        "merge: source file or parity file is a directory");
    
    LOG.info("Set modification time of parity to be a different number");
    dfs.setTimes(badParity, stat.getModificationTime() + 1, 0);
    mergeFile(badParity, source, "rs", checksums,
        "merge: source file and parity file doesn't have the same modification time");
    dfs.setTimes(badParity, stat.getModificationTime(), 0);
    dfs.setTimes(emptyFile, stat.getModificationTime(), 0);
    mergeFile(emptyFile, source, "rs", checksums, 
        "merge: parity file's replication doesn't match codec's parity replication");
    dfs.setReplication(emptyFile, (short)1);
    mergeFile(emptyFile, source, "rs", checksums, "merge: /empty is empty");
    mergeFile(badParity, emptyFile, "rs", checksums, "merge: /empty is empty");
    mergeFile(badParity, source, "rs", new int[5], "merge: checksum length ");
    mergeFile(badParity, source, "rs", checksums, "merge: expect parity blocks ");
    
    LOG.info("Hardlink the file to " + dest);
    dfs.hardLink(source, dest);
    mergeFile(emptyFile, source, "rs", checksums, 
        "merge: source file or parity file is hardlinked");
    mergeFile(dest, emptyFile, "rs", checksums, 
        "merge: source file or parity file is hardlinked");
  }
}

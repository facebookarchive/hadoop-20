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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.raid.RaidCodecBuilder;
import org.junit.Test;

public class TestINodeFile {
  public static final Log LOG = LogFactory.getLog(TestINodeFile.class);
  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private String userName = "Test";
  private short replication = 2;
  private long preferredBlockSize;
  private int numDataBlocks = 10;
  private int numParityBlocks = 4;
  private int numStripeblocks = numDataBlocks + numParityBlocks;
  private short parityReplication = 1;
  private Random rand = new Random();

  /** Convenient method to create {@link INodeFile} */
  INodeFile createINodeFile(short replication, long preferredBlockSize) {
    return new INodeFile(INodeId.GRANDFATHER_INODE_ID, new PermissionStatus(userName, null,
        FsPermission.getDefault()), null, replication,
        0L, 0L, preferredBlockSize, null);
  }
  
  INodeFile createINodeRaidFile(short replication, RaidCodec codec, 
      long blockSize, BlockInfo[] blocks) {
    return new INodeFile(INodeId.GRANDFATHER_INODE_ID, 
        new PermissionStatus(userName, null,
        FsPermission.getDefault()), blocks, replication,
        1L, 2L, preferredBlockSize, codec);
  }
  
  @Test
  public void testEmptyINodeRaidStorage() throws IOException {
    INodeFile emptyFile = createINodeRaidFile(replication,
        RaidCodecBuilder.getRSCodec("rs", 4, 10, RaidCodec.FULL_BLOCK, 
            parityReplication, parityReplication), preferredBlockSize, null);
    BlockInfo fakeBlockInfo = new BlockInfo(new Block(0, 0, 0), 1);
    assertEquals(2L, emptyFile.accessTime);
    assertEquals(1L, emptyFile.modificationTime);
    assertEquals(replication, emptyFile.getReplication());
    assertEquals(StorageType.RAID_STORAGE, emptyFile.getStorageType());
    assertEquals(null, emptyFile.getLastBlock());
    assertFalse(emptyFile.isLastBlock(fakeBlockInfo));
    LOG.info("Test getBlockIndex");
    try {
      emptyFile.getBlockIndex(fakeBlockInfo, "");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().startsWith("blocks is null for file "));     
    }
    
    LOG.info("Test computeContentSummary");
    long[] summary = new long[]{0L, 0L, 0L, 0L};
    emptyFile.computeContentSummary(summary);
    assertEquals(0, summary[0]);
    assertEquals(1, summary[1]);
    assertEquals(0, summary[3]);
    
    LOG.info("Test collectSubtreeBlocksAndClear");
    ArrayList<BlockInfo> removedBlocks = new ArrayList<BlockInfo>();
    ArrayList<INode> removedINodes = new ArrayList<INode>();
    assertEquals(1, emptyFile.collectSubtreeBlocksAndClear(removedBlocks,
        Integer.MAX_VALUE, removedINodes));
    assertEquals(null, emptyFile.getStorage());
    assertEquals(0, removedBlocks.size());
    assertEquals(1, removedINodes.size());
    assertEquals(emptyFile, removedINodes.get(0));
  }
  
  @Test
  public void testINodeRaidStorage() throws IOException {
    int preferredBlockSize = 64*1024*1024;
    
    // Test Random file
    for (int i = 0; i < 10; i++) {
      int numBlocks = rand.nextInt(10)*numStripeblocks
          + numParityBlocks + rand.nextInt(numDataBlocks) + 1;
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      INodeFile inf = createINodeRaidFile(replication, 
          RaidCodecBuilder.getRSCodec("rs", 4, 10, RaidCodec.FULL_BLOCK,
              parityReplication, parityReplication), preferredBlockSize, blocks);
      long fileSize = 0L;
      long diskSpace = 0L;
      for (int j = 0; j < numBlocks; j++) {
        Block blk = new Block(rand.nextLong(), 
            rand.nextInt(preferredBlockSize), rand.nextInt());
        if (j % numStripeblocks < numParityBlocks) {
          blocks[j] = new BlockInfo(blk, parityReplication);
          diskSpace += blk.getNumBytes() * parityReplication;
        } else {
          blocks[j] = new BlockInfo(blk, replication);
          fileSize += blk.getNumBytes();
          diskSpace += blk.getNumBytes() * replication;
        }
        blocks[j].setINode(inf);
      }
      assertEquals(2L, inf.accessTime);
      assertEquals(1L, inf.modificationTime);
      assertEquals(replication, inf.getReplication());
      assertEquals(StorageType.RAID_STORAGE, inf.getStorageType());
      assertEquals(blocks[numBlocks - 1], inf.getLastBlock());
      assertTrue(inf.isLastBlock(blocks[numBlocks - 1]));
      assertFalse(inf.isLastBlock(blocks[0]));
      
      LOG.info("Test getBlockIndex");
      int blkIdx = rand.nextInt(numBlocks);
      assertEquals(blkIdx, inf.getBlockIndex(blocks[blkIdx], ""));
      
      LOG.info("Test computeContentSummary");
      long[] summary = new long[]{0L, 0L, 0L, 0L};
      inf.computeContentSummary(summary);
      assertEquals(fileSize, summary[0]);
      assertEquals(1, summary[1]);
      assertEquals(diskSpace, summary[3]);
      
      LOG.info("Test collectSubtreeBlocksAndClear");
      
      ArrayList<BlockInfo> removedBlocks = new ArrayList<BlockInfo>();
      ArrayList<INode> removedINodes = new ArrayList<INode>();
      assertEquals(1, inf.collectSubtreeBlocksAndClear(removedBlocks,
          Integer.MAX_VALUE, removedINodes));
      assertEquals(null, inf.getStorage());
      assertEquals(numBlocks, removedBlocks.size());
      for (int j = 0; j < numBlocks; j++) {
        assertEquals(blocks[j], removedBlocks.get(j));
      }
      assertEquals(1, removedINodes.size());
      assertEquals(inf, removedINodes.get(0));
    }
  }
      
  
  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testReplicationBelowLowerBound ()
              throws IllegalArgumentException {
    replication = -1;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", preferredBlockSize,
           inf.getPreferredBlockSize());
  }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
                 inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = -1;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
  }

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE+1;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
  }
  
  /**
   * Verify root always has ROOT_INODE_ID and new formated fsimage has last
   * allocated inode id ROOT_INODE_ID. Validate correct lastInodeId is persisted.
   * @throws IOException
   */
  @Test
  public void testInodeId() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().getNamesystem();
      FSDirectory fsDir = fsn.dir;
      DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();
      long lastId = fsDir.getLastInodeId();

      // Ensure root has the correct inode ID
      // Last inode ID should be root inode ID and inode map size should be 1
      int inodeCount = 1;
      long expectedLastInodeId = INodeId.ROOT_INODE_ID;
      assertEquals(fsDir.rootDir.getId(), INodeId.ROOT_INODE_ID);
      assertEquals(expectedLastInodeId, lastId);
      assertEquals(inodeCount, fsDir.getInodeMapSize());

      // Create a directory
      Path path = new Path("/testone");
      assertTrue(fs.mkdirs(path));
      assertEquals(++expectedLastInodeId, fsDir.getLastInodeId());
      assertEquals(++inodeCount, fsDir.getInodeMapSize());

      /**
       * Create a file
       * Before this, we have
       * /
       *  /testone
       */
      DFSTestUtil.createFile(fs, new Path("/testone/file"), 1024, (short) 1, 0);
      assertEquals(++inodeCount, fsDir.getInodeMapSize());
      assertEquals(++expectedLastInodeId, fsDir.getLastInodeId());
      
      /**
       * Create an empty directory
       * Before this, we have
       * /
       *  /testone
       *    /file
       */
      Path delPath = new Path("/testone/edir");
      assertTrue(fs.mkdirs(delPath));
      assertEquals(++inodeCount, fsDir.getInodeMapSize());
      assertEquals(++expectedLastInodeId, fsDir.getLastInodeId());
      
      /**
       * Remove empty directory, make sure we remove it correctly
       * Before this, we have
       * /
       *  /testone
       *    /emptydir
       *    /file
       */
      fs.delete(delPath, true, true);
      assertEquals(--inodeCount, fsDir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      
      /**
       * Rename a directory, should not change last inode id
       * Before this, we have
       * /
       *  /testone
       *    /file
       */
      Path renamedPath = new Path("/testtwo");
      assertTrue(fs.rename(path, renamedPath));
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      
      /**
       * Delete testtwo and ensure inode map size decreases
       * Before this, we have
       * /
       *  /testtwo
       *    /file
       */
      assertTrue(fs.delete(renamedPath, true));
      inodeCount -= 2;
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());

      // Make sure editlog is loaded correctly 
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      fsDir = fsn.dir;
      assertEquals(INodeId.ROOT_INODE_ID, fsDir.rootDir.getId());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());

      /**
       * Create /testone/fileone /testtwo/filetwo
       * Before this, we have
       * /
       */
      String file1 = "/testone/fileone";
      String file2 = "/testone/filetwo";
      DFSTestUtil.createFile(fs, new Path(file1), 512, (short) 1, 0);
      DFSTestUtil.createFile(fs, new Path(file2), 512, (short) 1, 0);
      inodeCount += 3; // testone, fileone and filetwo are created
      expectedLastInodeId += 3;
      assertEquals(inodeCount, fsDir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      
      /**
       * Concat the /testone/fileone /testone/filetwo into /testone/filetwo
       * Before this, we have
       * /
       *  /testone
       *    /fileone
       *    /filetwo
       */
      fs.concat(new Path(file2), new Path[] {new Path(file1)}, false);
      inodeCount--; // file1 and file2 are concatenated to file2
      assertEquals(inodeCount, fsDir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      
      /**
       * Delete test1
       * Before this, we have
       * /
       *  /testone
       *    /filetwo
       */
      assertTrue(fs.delete(new Path("/testone"), true));
      inodeCount -= 2; // testone and filetwo is deleted
      assertEquals(inodeCount, fsDir.getInodeMapSize());
      GSet<INode, INode> beforeRestart = fsDir.getInodeMap();

      // Make sure edit log is loaded correctly 
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      fsDir = fsn.dir;
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      DFSTestUtil.assertInodemapEquals(beforeRestart, fsDir.getInodeMap());

      /**
       * Create two inodes testtwo and testtwo/filetwo
       * Before this, we have
       * /
       */
      DFSTestUtil.createFile(fs, new Path("/testtwo/filetwo"), 1024, (short) 1, 0);
      expectedLastInodeId += 2;
      inodeCount += 2;
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      assertEquals(inodeCount, fsDir.getInodeMapSize());

      /**
       * create /testfour, and /testfour/file.
       * /testfour/file is a file under construction
       * Before this, we have
       * /
       *  /testtwo
       *    /filetwo
       */
      FSDataOutputStream outStream = fs.create(new Path("/testfour/file"));
      assertTrue(outStream != null);
      expectedLastInodeId += 2;
      inodeCount += 2;
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      long expectedFile4InodeId = fsDir.getINode("/testfour/file").getId();
      assertEquals(inodeCount, fsDir.getInodeMapSize());
      beforeRestart = fsDir.getInodeMap();

      // Apply editlogs to fsimage, ensure inodeUnderConstruction is handled
      fsn.enterSafeMode();
      fsn.saveNamespace(false, true);
      fsn.leaveSafeMode(false);

      outStream.close();

      // The lastInodeId in fsimage should remain the same after reboot
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      fsDir = fsn.dir;
      
      /**
       * Before this, we have
       * /
       *  /testtwo
       *    /filetwo
       *  /testfour
       *    /file
       */
      assertEquals(INodeId.ROOT_INODE_ID, fsDir.getINode("/").getId());
      assertEquals(expectedLastInodeId, fsDir.getLastInodeId());
      // Make sure the id still the same
      assertEquals(expectedFile4InodeId, fsDir.getINode("/testfour/file").getId());
      DFSTestUtil.assertInodemapEquals(beforeRestart, fsDir.getInodeMap());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /** Helper function while debugging the test */
  private void printINodeMap(GSet<INode, INode> inodeMap) {
    Iterator<INode> itr = inodeMap.iterator();
    while(itr.hasNext()) {
      INode inode = itr.next();
      System.out.println("Id is " + inode.getId() + " " + inode);
    }
  }
  
  /**
   * Test whether the inode in inodeMap has been replaced after regular inode
   * replacement
   */
  @Test
  public void testINodeReplacement() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      final FSNamesystem fsn = cluster.getNameNode().getNamesystem();
      final FileSystem hdfs = cluster.getFileSystem();
      final FSDirectory fsdir = fsn.dir;
      int expectedInodeMapSize = 1;
      
      final Path dir = new Path("/dir");
      hdfs.mkdirs(dir);
      INode dirNode = fsdir.getINode(dir.toString());
      INode dirNodeFromId = fsdir.getINode(dirNode.getId());
      assertSame(dirNode, dirNodeFromId);
      assertEquals(++expectedInodeMapSize, fsdir.getInodeMapSize());
      
      // set quota to dir, which leads to node replacement
      assertTrue(dirNode instanceof INodeDirectory);
      fsdir.setQuota("/dir", Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
      INode newDirNode = fsdir.getINode(dir.toString());
      assertTrue(newDirNode instanceof INodeDirectoryWithQuota);
      // the inode in inodeMap should also be replaced
      dirNodeFromId = fsdir.getINode(newDirNode.getId());
      assertSame(newDirNode, dirNodeFromId);
      assertEquals(expectedInodeMapSize, fsdir.getInodeMapSize());
  
      fsdir.setQuota("/dir", -1, -1);
      dirNode = fsdir.getINode(dir.toString());
      assertTrue(dirNode instanceof INodeDirectory);
      // the inode in inodeMap should also be replaced
      dirNodeFromId = fsdir.getINode(dirNode.getId());
      assertSame(dirNode, dirNodeFromId);
      assertEquals(expectedInodeMapSize, fsdir.getInodeMapSize());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

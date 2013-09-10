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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link DirectoryScanner} handling of differences
 * between blocks on the disk and block in memory.
 */
public class TestDirectoryScanner {
  private static final Log LOG = LogFactory.getLog(TestDirectoryScanner.class);
  private static final Configuration CONF = new Configuration();
  private static final int DEFAULT_GEN_STAMP = 9999;

  private MiniDFSCluster cluster;
  private Integer nsid;
  private FSDataset fds = null;
  private DirectoryScanner scanner = null;
  private Random rand = new Random();
  private Random r = new Random();
  
  /**
   * The mock is done to do synchronous file deletion instead of async one
   */
  static class FSDatasetAsyncDiscServiceMock extends
      FSDatasetAsyncDiskService {

    FSDatasetAsyncDiscServiceMock(File[] volumes, Configuration conf) {
      super(volumes, conf);
    }

    @Override
    void deleteAsyncFile(FSVolume volume, File file) {
      DataNode.LOG.info("Scheduling file " + file.toString() + " for deletion");
      new FileDeleteTask(volume, file).run();
    }
  }

  /**
   * This class is made to simplify test reading; 
   * instead of writing 
   * <code>scanAndAssert(1, 2, 3, 4, 5, 6)</code> - a method with a 
   * 6 argumetns so it's hard to remember their order,
   * it uses the builder pattern to make things more clear
   * <code>
   *  checker()
   *    .setTotalBlocks(1)
   *    .setDiffSize(2)
   *    .setMissingMetaFile(3)
   *    .setMissingBlockFile(4)
   *    .setMissingMemoryBlocks(5)
   *    .setMismatchBlocks(6)
   *    .scanAndAssert();
   * </code>
   */
  static class ScanChecker {
    long totalBlocks;
    int diffSize;
    long missingMetaFile;
    long missingBlockFile;
    long missingMemoryBlocks;
    long mismatchBlocks;
    
    DirectoryScanner scanner;
    int nsid;

    ScanChecker(DirectoryScanner scanner, int nsid) {
      this.scanner = scanner;
      this.nsid = nsid;
    }

    public ScanChecker setTotalBlocks(long totalBlocks) {
      this.totalBlocks = totalBlocks;
      return this;
    }

    public ScanChecker setDiffSize(int diffSize) {
      this.diffSize = diffSize;
      return this;
    }

    public ScanChecker setMissingMetaFile(long missingMetaFile) {
      this.missingMetaFile = missingMetaFile;
      return this;
    }

    public ScanChecker setMissingBlockFile(long missingBlockFile) {
      this.missingBlockFile = missingBlockFile;
      return this;
    }

    public ScanChecker setMissingMemoryBlocks(long missingMemoryBlocks) {
      this.missingMemoryBlocks = missingMemoryBlocks;
      return this;
    }

    public ScanChecker setMismatchBlocks(long mismatchBlocks) {
      this.mismatchBlocks = mismatchBlocks;
      return this;
    }
    
    public ScanChecker setZeroDiff() {
      return this.setDiffSize(0)
        .setMissingMetaFile(0)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(0);
    }

    /** 
     * Runs scanner and asserts its results with the predefined values
     */
    public void scanAndAssert() {
      assertTrue(scanner.getRunStatus());
      scanner.run();

      assertTrue(scanner.diffsPerNamespace.containsKey(nsid));
      LinkedList<DirectoryScanner.ScanDifference> diff = scanner.diffsPerNamespace.get(nsid);
      assertTrue(scanner.statsPerNamespace.containsKey(nsid));
      DirectoryScanner.Stats stats = scanner.statsPerNamespace.get(nsid);

      assertEquals(diffSize, diff.size());
      assertEquals(totalBlocks, stats.totalBlocks);
      assertEquals(missingMetaFile, stats.missingMetaFile);
      assertEquals(missingBlockFile, stats.missingBlockFile);
      assertEquals(missingMemoryBlocks, stats.missingMemoryBlocks);
      assertEquals(mismatchBlocks, stats.mismatchBlocks);
    }
  }

  public ScanChecker checker() {
    // closure to the current instance
    return new ScanChecker(scanner, nsid);
  }

  /** create a file with a length of <code>fileLen</code> */
  private void createFile(String fileName, long fileLen) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, fileLen, (short) 1, r.nextLong());
  }

  /** Truncate a block file */
  private long truncateBlockFile() throws IOException {
    synchronized (fds) {
      for (DatanodeBlockInfo b : TestDirectoryScannerDelta.getBlockInfos(fds, nsid)) {
        File f = b.getBlockDataFile().getFile();
        File mf = BlockWithChecksumFileWriter.getMetaFile(f, b.getBlock());
        // Truncate a block file that has a corresponding metadata file
        if (f.exists() && f.length() != 0 && mf.exists()) {
          FileOutputStream s = new FileOutputStream(f);
          FileChannel channel = s.getChannel();
          channel.truncate(0);
          LOG.info("Truncated block file " + f.getAbsolutePath());
          long blockId = b.getBlock().getBlockId();
          s.close();
          return blockId;
        }
      }
    }
    return 0;
  }

  /** Delete a block file */
  private long deleteBlockFile() {
    synchronized(fds) {
      for (DatanodeBlockInfo b : TestDirectoryScannerDelta.getBlockInfos(fds, nsid)) {
        File f = b.getBlockDataFile().getFile();
        File mf = BlockWithChecksumFileWriter.getMetaFile(f, b.getBlock());
        // Delete a block file that has corresponding metadata file
        if (f.exists() && mf.exists() && f.delete()) {
          LOG.info("Deleting block file " + f.getAbsolutePath());
          return b.getBlock().getBlockId();
        }
      }
    } // sync
    throw new IllegalStateException("Cannot complete a block file deletion");
  }

  /** Delete block meta file */
  private long deleteMetaFile() {
    synchronized(fds) {
      for (DatanodeBlockInfo b : TestDirectoryScannerDelta.getBlockInfos(fds, nsid)) {
        File file = BlockWithChecksumFileWriter.getMetaFile(b
            .getBlockDataFile().getFile(), b.getBlock());
        // Delete a metadata file
        if (file.exists() && file.delete()) {
          LOG.info("Deleting metadata file " + file.getAbsolutePath());
          return b.getBlock().getBlockId();
        }
      }
    } // sync
    throw new IllegalStateException("cannot complete a metafile deletion");
  }

  /** Get a random blockId that is not used already */
  private long getFreeBlockId() {
    long id = rand.nextLong();
    while (true) {
      id = rand.nextLong();
      if (fds.volumeMap.get(
          nsid, new Block(id, 0, GenerationStamp.WILDCARD_STAMP)) == null) {
        break;
      }
    }
    return id;
  }

  private String getBlockFile(long id) {
    return Block.BLOCK_FILE_PREFIX + id;
  }

  private String getMetaFile(long id) {
    return Block.BLOCK_FILE_PREFIX + id + "_" + DEFAULT_GEN_STAMP
        + Block.METADATA_EXTENSION;
  }

  /** Create a block file in a random volume*/
  private long createBlockFile() throws IOException {
    FSVolume[] volumes = fds.volumes.getVolumes();
    int index = rand.nextInt(volumes.length - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes[index].getNamespaceSlice(nsid).getCurrentDir();
    File file = new File(finalizedDir, getBlockFile(id));
    if (file.createNewFile()) {
      LOG.info("Created block file " + file.getName());
    }
    return id;
  }

  /** Create a metafile in a random volume*/
  private long createMetaFile() throws IOException {
    FSVolume[] volumes = fds.volumes.getVolumes();
    int index = rand.nextInt(volumes.length - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes[index].getNamespaceSlice(nsid).getCurrentDir();
    File file = new File(finalizedDir, getMetaFile(id));
    if (file.createNewFile()) {
      LOG.info("Created metafile " + file.getName());
    }
    return id;
  }

  /** Create block file and corresponding metafile in a rondom volume */
  private long createBlockMetaFile() throws IOException {
    FSVolume[] volumes = fds.volumes.getVolumes();
    int index = rand.nextInt(volumes.length - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes[index].getNamespaceSlice(nsid).getCurrentDir();
    File file = new File(finalizedDir, getBlockFile(id));
    if (file.createNewFile()) {
      LOG.info("Created block file " + file.getName());

      // Create files with same prefix as block file but extension names
      // such that during sorting, these files appear around meta file
      // to test how DirectoryScanner handles extraneous files
      String name1 = file.getAbsolutePath() + ".l";
      String name2 = file.getAbsolutePath() + ".n";
      file = new File(name1);
      if (file.createNewFile()) {
        LOG.info("Created extraneous file " + name1);
      }

      file = new File(name2);
      if (file.createNewFile()) {
        LOG.info("Created extraneous file " + name2);
      }

      file = new File(finalizedDir, getMetaFile(id));
      if (file.createNewFile()) {
        LOG.info("Created metafile " + file.getName());
      }
    }
    return id;
  }

  @Test
  public void testDirectoryScanner() throws Exception {
    // Run the test with and without parallel scanning
    for (int parallelism = 1; parallelism < 3; parallelism++) {
      runTest(parallelism);
    }
  }

  public void runTest(int parallelism) throws Exception {
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setInt("dfs.datanode.directoryscan.interval", 1000);
    CONF.setBoolean("dfs.use.inline.checksum", false);

    try {
      cluster = new MiniDFSCluster(CONF, 1, true, null);
      cluster.waitActive();
      nsid = cluster.getNameNode().getNamesystem().getNamespaceId();
      fds = (FSDataset) cluster.getDataNodes().get(0).getFSDataset();
      CONF.setInt(DirectoryScanner.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                  parallelism);
      // setting up mock that removes files immediately
      List<File> volumes = new ArrayList<File>();
      for (FSVolume vol : fds.volumes.getVolumes()) {
        volumes.add(vol.getDir());
      }
      fds.asyncDiskService = new FSDatasetAsyncDiscServiceMock(
          volumes.toArray(new File[volumes.size()]), CONF);
      DataNode dn = cluster.getDataNodes().get(0);
      
      scanner = dn.directoryScanner;

      // Add file with 100 blocks
      long totalBlocks = 100;
      createFile("/tmp/t1", CONF.getLong("dfs.block.size", 100) * totalBlocks);

      // Test1: No difference between in-memory and disk
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test2: block metafile is missing
      long blockId = deleteMetaFile();

      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(1)
        .setMissingMetaFile(1)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(1)
        .scanAndAssert();
      verifyGenStamp(blockId, Block.GRANDFATHER_GENERATION_STAMP);
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test3: block file is missing
      blockId = deleteBlockFile();
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(1)
        .setMissingMetaFile(0)
        .setMissingBlockFile(1)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(0)
        .scanAndAssert();
      totalBlocks--;
      verifyDeletion(blockId);
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test4: A block file exists for which there is no metafile and
      // a block in memory
      blockId = createBlockFile();
      totalBlocks++;
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(1)
        .setMissingMetaFile(1)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(1)
        .setMismatchBlocks(0)
        .scanAndAssert();
      verifyAddition(blockId, Block.GRANDFATHER_GENERATION_STAMP, 0);
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test5: A metafile exists for which there is no block file and
      // a block in memory
      blockId = createMetaFile();
      checker()
        .setTotalBlocks(totalBlocks+1)
        .setDiffSize(1)
        .setMissingMetaFile(0)
        .setMissingBlockFile(1)
        .setMissingMemoryBlocks(1)
        .setMismatchBlocks(0)
        .scanAndAssert();
      File metafile = new File(getMetaFile(blockId));
      assertTrue(!metafile.exists());
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test6: A block file and metafile exists for which there is no block in
      // memory
      blockId = createBlockMetaFile();
      totalBlocks++;
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(1)
        .setMissingMetaFile(0)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(1)
        .setMismatchBlocks(0)
        .scanAndAssert();
      verifyAddition(blockId, DEFAULT_GEN_STAMP, 0);
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test7: Delete bunch of metafiles
      for (int i = 0; i < 10; i++) {
        blockId = deleteMetaFile();
      }
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(10)
        .setMissingMetaFile(10)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(10)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test8: Delete bunch of block files
      for (int i = 0; i < 10; i++) {
        blockId = deleteBlockFile();
      }
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(10)
        .setMissingMetaFile(0)
        .setMissingBlockFile(10)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(0)
        .scanAndAssert();
      totalBlocks -= 10;
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test9: create a bunch of blocks files
      for (int i = 0; i < 10 ; i++) {
        blockId = createBlockFile();
      }
      totalBlocks += 10;
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(10)
        .setMissingMetaFile(10)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(10)
        .setMismatchBlocks(0)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test10: create a bunch of metafiles
      for (int i = 0; i < 10 ; i++) {
        blockId = createMetaFile();
      }
      checker()
        .setTotalBlocks(totalBlocks+10)
        .setDiffSize(10)
        .setMissingMetaFile(0)
        .setMissingBlockFile(10)
        .setMissingMemoryBlocks(10)
        .setMismatchBlocks(0)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test11: create a bunch block files and meta files
      for (int i = 0; i < 10 ; i++) {
        blockId = createBlockMetaFile();
      }
      totalBlocks += 10;
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(10)
        .setMissingMetaFile(0)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(10)
        .setMismatchBlocks(0)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test12: truncate block files to test block length mismatch
      for (int i = 0; i < 10 ; i++) {
        truncateBlockFile();
      }
      checker()
        .setTotalBlocks(totalBlocks)
        .setDiffSize(10)
        .setMissingMetaFile(0)
        .setMissingBlockFile(0)
        .setMissingMemoryBlocks(0)
        .setMismatchBlocks(10)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks)
        .setZeroDiff()
        .scanAndAssert();

      // Test13: all the conditions combined
      createMetaFile();
      createBlockFile();
      createBlockMetaFile();
      deleteMetaFile();
      deleteBlockFile();
      truncateBlockFile();
      checker()
        .setTotalBlocks(totalBlocks+3)
        .setDiffSize(6)
        .setMissingMetaFile(2)
        .setMissingBlockFile(2)
        .setMissingMemoryBlocks(3)
        .setMismatchBlocks(2)
        .scanAndAssert();
      checker()
        .setTotalBlocks(totalBlocks+1)
        .setZeroDiff()
        .scanAndAssert();
      totalBlocks = 1;

      // Test14: validate clean shutdown of DirectoryScanner
      scanner.shutdown();
      assertFalse(scanner.getRunStatus());

    } finally {
      scanner.shutdown();
      cluster.shutdown();
    }
  }

  private void verifyAddition(long blockId, long genStamp, long size) throws IOException{
    final DatanodeBlockInfo replicainfo;
    replicainfo = fds.volumeMap.get(nsid, new Block(blockId, 0, GenerationStamp.WILDCARD_STAMP));
    assertNotNull(replicainfo);

    // Added block has the same file as the one created by the test
    File file = new File(getBlockFile(blockId));
    assertEquals(file.getName(), fds.getBlockFile(nsid, new Block(blockId)).getName());

    // Generation stamp is same as that of created file
    LOG.info("------------------: " + genStamp + " : " +
        replicainfo.getBlock().getGenerationStamp());
    assertEquals(genStamp, replicainfo.getBlock().getGenerationStamp());

    // File size matches
    assertEquals(size, replicainfo.getBlock().getNumBytes());
  }

  private void verifyDeletion(long blockId) {
    // Ensure block does not exist in memory
    assertNull(fds.volumeMap.get(nsid, new Block(blockId, 0, GenerationStamp.WILDCARD_STAMP)));

  }
  
  private void verifyGenStamp(long blockId, long genStamp) {
    final DatanodeBlockInfo memBlock;
    memBlock = fds.volumeMap.get(nsid, new Block(blockId, 0, GenerationStamp.WILDCARD_STAMP));
    assertNotNull(memBlock);
    assertEquals(genStamp, memBlock.getBlock().getGenerationStamp());
  }
}
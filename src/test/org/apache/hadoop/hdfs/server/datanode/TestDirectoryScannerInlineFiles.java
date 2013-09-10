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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.TestDirectoryScanner.ScanChecker;
import org.apache.hadoop.util.DataChecksum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDirectoryScannerInlineFiles  {
  
  final static Log LOG = LogFactory.getLog(TestDirectoryScannerDelta.class);

  static MiniDFSCluster cluster;
  static int nsid;
  static DataNode dn;
  static DirectoryScanner scanner;
  static FSDatasetDelta delta;
  static FSDataset data;
  static FileSystem fs;
  static final Random rand = new Random();
  static int totalBlocks = 0;
  
  final int checksumType;
  
  
  /**
   * Testcase will be run per each DataChecksum type
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testNumbers() {
    Object[][] data = new Object[][] { {DataChecksum.CHECKSUM_CRC32}, {DataChecksum.CHECKSUM_CRC32C} };
    return Arrays.asList(data);
  }
  
  public TestDirectoryScannerInlineFiles(int checksumType) {
    this.checksumType = checksumType;
  }
  
  @BeforeClass
  public static void setUpCluster() {
    LOG.info("setting up!");
    Configuration CONF = new Configuration();
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setInt("dfs.datanode.directoryscan.interval", 1000);
    
    try{
      cluster = new MiniDFSCluster(CONF, 1, true, null);
      cluster.waitActive();

      dn = cluster.getDataNodes().get(0);
      nsid = dn.getAllNamespaces()[0];
      scanner = dn.directoryScanner;
      data = (FSDataset)dn.data;
      Field f = DirectoryScanner.class.getDeclaredField("delta");
      f.setAccessible(true);
      delta = (FSDatasetDelta)f.get(scanner);

      fs = cluster.getFileSystem();
      
      List<File> volumes = new ArrayList<File>();
      for(FSVolume vol : data.volumes.getVolumes()) {
        volumes.add(vol.getDir());
      }
      data.asyncDiskService = new TestDirectoryScanner.FSDatasetAsyncDiscServiceMock(
          volumes.toArray(new File[volumes.size()]), CONF);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("setup failed");
    }
  }
  
  private String getInlineBlockFileName(long blkid, int checksumType, int checksumSize) {
    String inlineFileName = "blk_" + blkid + "_1001_2_"+ checksumType + "_" + checksumSize;
    return inlineFileName;
  }
  
  /** Get a random blockId that is not used already */
  private long getFreeBlockId() {
    long id = rand.nextLong();
    while (true) {
      id = rand.nextLong();
      if (data.volumeMap.get(
          nsid, new Block(id, 0, GenerationStamp.WILDCARD_STAMP)) == null) {
        break;
      }
    }
    return id;
  }
  
  /** Create block file and corresponding metafile in a rondom volume */
  private long createInlineBlockFile(int checksumType) throws IOException {
    FSVolume[] volumes = data.volumes.getVolumes();
    int index = rand.nextInt(volumes.length - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes[index].getNamespaceSlice(nsid).getCurrentDir();
    int checksumSize = DataChecksum.getChecksumSizeByType(checksumType);
    String inlineFileName = getInlineBlockFileName(id, checksumType, checksumSize);
    File file = new File(finalizedDir, inlineFileName);
    assertTrue(file.createNewFile());
    PrintWriter pw = new PrintWriter(file);
    int desiredLength = (int)BlockInlineChecksumReader.getFileLengthFromBlockSize(1, 1, checksumSize);
    for(int i = 0; i < desiredLength; i++) {
      pw.write(Character.getNumericValue('0'));
    }
    pw.close();
    LOG.info("Created block file " + file.getName());
    return id;
  }
  
  private void truncateFile(File f) throws IOException {
    assertTrue(f.exists() && f.length() != 0);
    FileOutputStream s = new FileOutputStream(f);
    FileChannel channel = s.getChannel();
    channel.truncate(0);
    LOG.info("Truncated block file " + f.getAbsolutePath());
    s.close();
  }
  
  public ScanChecker checker() {
    return new ScanChecker(scanner, nsid);
  }
  
  private void removeFromMemory(long blkid) {
    data.volumeMap.remove(nsid, new Block(blkid));
    assertNull(delta.get(nsid, new Block(blkid)));      
  }
  
  private void removeFromDisk(long blkid) {
    DatanodeBlockInfo info = data.volumeMap.get(nsid, new Block(blkid));
    assertTrue(info.getBlockDataFile().getFile().delete());      
  }
  
  private void truncateOnDisk(long blkid) throws IOException {
    DatanodeBlockInfo info = data.volumeMap.get(nsid, new Block(blkid));
    truncateFile(info.getBlockDataFile().getFile());      
  }
  
  @Test
  public void testInlineBlocks() throws Exception {
    LinkedList<Long> newFormatBlocks = new LinkedList<Long>();
    // Test 1: blocks on disk and not in memory
    for(int i = 0; i < 100; i++) {
      newFormatBlocks.add(createInlineBlockFile(checksumType));        
    }
    totalBlocks += newFormatBlocks.size();
    checker()
      .setTotalBlocks(totalBlocks)
      .setDiffSize(newFormatBlocks.size())
      .setMissingMetaFile(0)
      .setMissingBlockFile(0)
      .setMissingMemoryBlocks(newFormatBlocks.size())
      .setMismatchBlocks(0)
      .scanAndAssert();
    checker()
      .setTotalBlocks(totalBlocks)
      .setZeroDiff()
      .scanAndAssert();

    // Test 2: removed some blocks from disk
    for(int i = 0; i < 5; i++) {
      removeFromDisk(newFormatBlocks.removeFirst());
      totalBlocks--;
    }
    checker()
      .setTotalBlocks(totalBlocks)
      .setDiffSize(5)
      .setMissingMetaFile(5)
      .setMissingBlockFile(5)
      .setMissingMemoryBlocks(0)
      .setMismatchBlocks(0)
      .scanAndAssert();
    checker()
      .setTotalBlocks(totalBlocks)
      .setZeroDiff()
      .scanAndAssert();
    
    // Test 3: removed some blocks from memory
    for(int i = 0; i < 5; i++) {
      removeFromMemory(newFormatBlocks.removeFirst());
    }
    checker()
      .setTotalBlocks(totalBlocks)
      .setDiffSize(5)
      .setMissingMetaFile(0)
      .setMissingBlockFile(0)
      .setMissingMemoryBlocks(5)
      .setMismatchBlocks(0)
      .scanAndAssert();
    checker()
      .setTotalBlocks(totalBlocks)
      .setZeroDiff()
      .scanAndAssert();
    
    // Test 3: truncate some blocks
    for(int i = 0; i < 5; i++) {
      truncateOnDisk(newFormatBlocks.removeFirst());
    }
    checker()
      .setTotalBlocks(totalBlocks)
      .setDiffSize(5)
      .setMissingMetaFile(0)
      .setMissingBlockFile(0)
      .setMissingMemoryBlocks(0)
      .setMismatchBlocks(5)
      .scanAndAssert();
    checker()
      .setTotalBlocks(totalBlocks)
      .setZeroDiff()
      .scanAndAssert();
    
    // Test 4: truncate some, remove from memory some, remove from disk some
    
    
    for(int i = 0; i < 5; i++) {
      removeFromDisk(newFormatBlocks.removeFirst());
      totalBlocks--;
    }
    for(int i = 0; i < 7; i++) {
      removeFromMemory(newFormatBlocks.removeFirst());
    }
    for(int i = 0; i < 11; i++) {
      truncateOnDisk(newFormatBlocks.removeFirst());
    }

    checker()
      .setTotalBlocks(totalBlocks)
      .setDiffSize(5+7+11)
      .setMissingMetaFile(5)
      .setMissingBlockFile(5)
      .setMissingMemoryBlocks(7)
      .setMismatchBlocks(11)
      .scanAndAssert();
    checker()
      .setTotalBlocks(totalBlocks)
      .setZeroDiff()
      .scanAndAssert();
    
  }
  
  @AfterClass
  public static void tearDownCluster() {
    LOG.info("Tearing down");
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  
}

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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;

import junit.framework.TestCase;

/** Test if a datanode can correctly upgrade itself */
public class TestDatanodeRestart extends TestCase {
  private FSDataset getFSDS(MiniDFSCluster cluster) {
    DataNode dn = cluster.getDataNodes().get(0);
    return (FSDataset) dn.data;
  }
  
  // test finalized replicas persist across DataNode restarts
  public void testFinalizedReplicas() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", 1024L);
    conf.setLong("dfs.block.crc.flush.interval", 100);
    conf.setInt("dfs.write.packet.size", 512);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      // test finalized replicas
      final String TopDir = "/test";
      DFSTestUtil util = new DFSTestUtil("TestCrcCorruption", 2, 3, 8 * 1024);
      util.createFiles(fs, TopDir, (short) 3);
      util.waitReplication(fs, TopDir, (short) 3);
      util.checkFiles(fs, TopDir);
      
      FSDataset data = getFSDS(cluster);
      // save all the block CRC info;
      NamespaceMap nm = data.volumeMap.getNamespaceMap(data.volumeMap.getNamespaceList()[0]);
      Map<Block, Integer> bim = new HashMap<Block, Integer>();
      for (int i = 0; i < nm.getNumBucket(); i++) {
        for (Entry<Block, DatanodeBlockInfo> entry : nm.getBucket(i).blockInfoMap
            .entrySet()) {
          bim.put(entry.getKey(), entry.getValue().getBlockCrc());
        }
      }
      // Wait another rounnd of block crc flushing happens
      long lastFlush = Long.MAX_VALUE;
      for (int i = 0; i < 100; i++) {
        if (data.blockCrcMapFlusher.lastFlushed > 0) {
          lastFlush = data.blockCrcMapFlusher.lastFlushed;
          break;
        } else if (i == 99) {
          TestCase.fail("block CRC file is not flushed.");
        } else {
          Thread.sleep(100);
        }
      }
      for (int i = 0; i < 100; i++) {
        if (data.blockCrcMapFlusher.lastFlushed > lastFlush) {
          break;
        } else if (i == 99) {
          TestCase.fail("block CRC file is not flushed.");
        } else {
          Thread.sleep(100);
        }
      }
      cluster.restartDataNodes();
      cluster.waitActive();
      // Verify that block CRC is recovered.
      data = getFSDS(cluster);
      nm = data.volumeMap.getNamespaceMap(data.volumeMap.getNamespaceList()[0]);
      for (int i = 0; i < nm.getNumBucket(); i++) {
        for (Entry<Block, DatanodeBlockInfo> entry : nm.getBucket(i).blockInfoMap
            .entrySet()) {
          TestCase.assertEquals(bim.get(entry.getKey()).intValue(), entry
              .getValue().getBlockCrc());
        }
      }
      
      util.checkFiles(fs, TopDir);
    } finally {
      cluster.shutdown();
    }
  }

  // test rbw replicas persist across DataNode restarts
  public void testRbwReplicas() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", 1024L);
    conf.setInt("dfs.write.packet.size", 512);
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    try {
      testRbwReplicas(cluster, false);
      testRbwReplicas(cluster, true);
    } finally {
      cluster.shutdown();
    }
  }

  private void testRbwReplicas(MiniDFSCluster cluster, boolean isCorrupt)
      throws IOException {
    FSDataOutputStream out = null;
    try {
      FileSystem fs = cluster.getFileSystem();
      NamespaceInfo nsInfo = cluster.getNameNode().versionRequest();
      final int fileLen = 515;
      // create some rbw replicas on disk
      byte[] writeBuf = new byte[fileLen];
      new Random().nextBytes(writeBuf);
      final Path src = new Path("/test.txt");
      out = fs.create(src);
      out.write(writeBuf);
      out.sync();
      DataNode dn = cluster.getDataNodes().get(0);
      // corrupt rbw replicas
      for (FSVolume volume : ((FSDataset) dn.data).volumes.getVolumes()) {
        File rbwDir = volume.getRbwDir(nsInfo.getNamespaceID());
        for (File file : rbwDir.listFiles()) {
          if (isCorrupt) {
            if (Block.isSeparateChecksumBlockFilename(file.getName())) {
              new RandomAccessFile(file, "rw").setLength(fileLen - 1); // corrupt
            } else if (Block.isInlineChecksumBlockFilename(file.getName())) {
              new RandomAccessFile(file, "rw").setLength(file.length() - 1); // corrupt
            }
          }
        }
      }
      cluster.restartDataNodes();
      cluster.waitActive();
      dn = cluster.getDataNodes().get(0);

      // check volumeMap: one rbw replica
      NamespaceMap volumeMap = ((FSDataset) (dn.data)).volumeMap
          .getNamespaceMap(nsInfo.getNamespaceID());
      assertEquals(1, volumeMap.size());
      Block replica = null;
      for (int i = 0; i < volumeMap.getNumBucket(); i++) {
        Set<Block> blockSet = volumeMap.getBucket(i).blockInfoMap.keySet();
        if (blockSet.isEmpty()) {
          continue;
        }
        Block r = blockSet.iterator().next();
        if (r != null) {
          replica = r;
          break;
        }
      }
      if (isCorrupt) {
        assertEquals((fileLen - 1), replica.getNumBytes());
      } else {
        assertEquals(fileLen, replica.getNumBytes());
      }
      dn.data.invalidate(nsInfo.getNamespaceID(), new Block[] { replica });
      fs.delete(src, false);
    } finally {
      IOUtils.closeStream(out);
    }
  }
}

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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

/**
 * This test verifies that block verification occurs on the datanode
 */
public class TestCorruptBlocks extends TestCase {
  
  private static boolean corruptReplicaGS(
      Block block, int datanodeIndex, MiniDFSCluster cluster)
      throws IOException {
    Block newBlock = new Block(block);
    newBlock.setGenerationStamp(block.getGenerationStamp() + 1);
    int bytesPerChecksum = cluster.conf.getInt(
        "io.bytes.per.checksum", FSConstants.DEFAULT_BYTES_PER_CHECKSUM);
    boolean corrupted = false;
    for (int i = datanodeIndex*2; i < datanodeIndex*2+2; i++) {
      File blockFile = new File(cluster.getBlockDirectory("data" + (i+1)),
          block+".meta");
      if (blockFile.exists()) {
        blockFile.renameTo(new File(
            cluster.getBlockDirectory("data" + (i+1)), newBlock+".meta"));
        corrupted = true;
        continue;
      }
      File blockFileInlineChecksum = new File(cluster.getBlockDirectory("data"
          + (i + 1)), BlockInlineChecksumWriter.getInlineChecksumFileName(
          block, FSConstants.CHECKSUM_TYPE, bytesPerChecksum));
      if (blockFileInlineChecksum.exists()) {
        blockFileInlineChecksum.renameTo(new File(
            cluster.getBlockDirectory("data" + (i + 1)),
            BlockInlineChecksumWriter.getInlineChecksumFileName(
                newBlock, FSConstants.CHECKSUM_TYPE, bytesPerChecksum)));
        corrupted = true;
        continue;
      }
    }
    return corrupted;
  }

  public void testMismatchedBlockGS() throws IOException {
    Configuration conf = new Configuration();
    final short REPLICATION_FACTOR = 1;
    MiniDFSCluster cluster = new MiniDFSCluster(
        conf, REPLICATION_FACTOR, true, null);
    try {
      cluster.waitActive();

      FileSystem fs = cluster.getFileSystem();
      Path file1 = new Path("/tmp/file1");

      // create a file
      DFSTestUtil.createFile(fs, file1, 10, REPLICATION_FACTOR, 0);

      // corrupt its generation stamp
      Block block = DFSTestUtil.getFirstBlock(fs, file1);
      corruptReplicaGS(block, 0, cluster);
      
      // stop and start the cluster
      cluster.shutdown();
      cluster = new MiniDFSCluster(
          conf, REPLICATION_FACTOR, false, null, false);
      cluster.waitActive();
      assertTrue(cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_GET));
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Verify that there is a missing block
      assertEquals(1, 
          cluster.getNameNode().getNamesystem().getMissingBlocksCount());
    } finally {
      cluster.shutdown();
    }
  }
}

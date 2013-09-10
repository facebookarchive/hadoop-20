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

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.util.DataChecksum;

public abstract class FSDatasetTestUtil {

  /**
   * Truncate the given block in place, such that the new truncated block
   * is still valid (ie checksums are updated to stay in sync with block file)
   */
  public static void truncateBlock(DataNode dn,
                                   Block block,
                                   long newLength,
                                   int namespaceId,
                                   boolean useInlineChecksum)
    throws IOException {
    FSDataset ds = (FSDataset) dn.data;

    ReplicaToRead rr = ds.getReplicaToRead(namespaceId, block);
    File blockFile = rr.getDataFileToRead();
    if (blockFile == null) {
      throw new IOException("Can't find block file for block " +
        block + " on DN " + dn);
    }
    if (useInlineChecksum) {
      new BlockInlineChecksumWriter(ds.getReplicaToRead(namespaceId, block)
          .getBlockDataFile(), DataChecksum.CHECKSUM_CRC32, dn.conf.getInt(
          "io.bytes.per.checksum", 512), HdfsConstants.DEFAULT_PACKETSIZE)
          .truncateBlock(newLength);
    } else {
      File metaFile = BlockWithChecksumFileWriter.findMetaFile(blockFile);
      new BlockWithChecksumFileWriter(ds.getReplicaToRead(namespaceId, block)
          .getBlockDataFile(), metaFile).truncateBlock(
        blockFile.length(), newLength);
    }
    ((DatanodeBlockInfo) (ds.getReplicaToRead(namespaceId, block)))
        .syncInMemorySize();
  }

  public static void truncateBlockFile(File blockFile, long newLength,
      boolean useInlineChecksum, int bytesPerChecksum)    throws IOException {
    if (useInlineChecksum) {
      new BlockInlineChecksumWriter(new BlockDataFile(blockFile, null),
          DataChecksum.CHECKSUM_CRC32, bytesPerChecksum,
          HdfsConstants.DEFAULT_PACKETSIZE).truncateBlock(newLength);
    } else {
      File metaFile = BlockWithChecksumFileWriter.findMetaFile(blockFile);
      new BlockWithChecksumFileWriter(new BlockDataFile(blockFile, null),
          metaFile).truncateBlock(blockFile.length(), newLength);
    }
  }
}

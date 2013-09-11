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

import org.apache.hadoop.util.CrcConcat;

/**
 * This class is used to progressively calculate block CRC using
 * chunk CRCs.
 */
public class BlockCrcUpdater{
  boolean blockCrcOk = true; // whether the information of blockCrc can be used
  int blockCrcOffset; // The offset the blockCrc up to.
  int blockCrc; // block CRC up to last full CRC

  int bytesPerChecksum;
  
  BlockCrcUpdater(int bytesPerChecksum, boolean enable) {
    this.bytesPerChecksum= bytesPerChecksum;
    blockCrcOk = enable;
  }
  
  void updateBlockCrc(long offset, int length, int crc) {
    if (!blockCrcOk) {
      return;
    }

    if (offset != blockCrcOffset) {
      DataNode.LOG.warn("File  CRC for last complete chunk is for offset "
          + blockCrcOffset + " but data are written for offset " + offset);
      blockCrcOk = false;
      return;
    }
    if (blockCrcOffset == 0) {
      blockCrc = crc;
    } else {
      blockCrc = CrcConcat.concatCrc(blockCrc, crc, length);
    }
    blockCrcOffset += length;
  }

  
  public boolean isCrcValid(long expectedLen) {
    if (expectedLen != blockCrcOffset) {
      return false;
    }
    return blockCrcOk;
  }

  public int getBlockCrc() {
    return blockCrc;
  }

  public int getBlockCrcOffset() {
    return blockCrcOffset;
  }
  
  public void disable() {
    blockCrcOk = false;
  }
  
}

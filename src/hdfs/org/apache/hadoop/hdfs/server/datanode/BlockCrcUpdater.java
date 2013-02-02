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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.CrcConcat;
import org.mortbay.log.Log;

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
  
  void updateBlockCrc(long offset, boolean partialAllowed, int length,
      int crc) {
    if (!blockCrcOk) {
      return;
    }
    if (!partialAllowed && length != bytesPerChecksum) {
      blockCrcOk = false;
      return;
    }

    if (offset != blockCrcOffset) {
      Log.warn("File  CRC for last complete chunk is for offset "
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

  
  public boolean isCrcValid() {
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

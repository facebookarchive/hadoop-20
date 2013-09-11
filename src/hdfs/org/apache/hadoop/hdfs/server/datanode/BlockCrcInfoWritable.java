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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolumeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * The data structure used to stream block CRC information of one block from and
 * to a local file.
 */
class BlockCrcInfoWritable implements Writable {
  static int LATEST_BLOCK_CRC_FILE_VERSION = 1;

  long blockId;
  long blockGenStamp;
  int blockCrc;

  BlockCrcInfoWritable() {
  }

  void set(long blockId, long blockGenStamp, int blockCrc) {
    this.blockId = blockId;
    this.blockGenStamp = blockGenStamp;
    this.blockCrc = blockCrc;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(blockGenStamp);
    out.writeInt(blockCrc);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.blockGenStamp = in.readLong();
    this.blockCrc = in.readInt();
  }
}

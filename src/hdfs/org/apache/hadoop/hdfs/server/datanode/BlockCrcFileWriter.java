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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;


/**
 * Class to write block CRC file to disk.
 * 
 * Format of the file:
 * 
 * +--------------------+
 * |   FORMAT_VERSION   |
 * +--------------------+
 * |  NUMBER OF BUCKETS |
 * +--------------------+   -------------
 * | numRecords bucket 0|
 * +--------------------+
 * | bucket 0 record 0  |
 * |                    |
 * +--------------------+      Bucket 0
 * | bucket 0 record 1  |
 * |                    |
 * +--------------------+
 * |     ......         |
 * |                    |
 * +--------------------+   -------------
 * | numRecords bucket 1|
 * +--------------------+
 * | bucket 1 record 0  |
 * |                    |      Bucket 1
 * |     ......         |
 * |                    |
 * +--------------------+   -------------
 * |     ......         |
 * |                    |    Other buckets
 * +--------------------+   -------------
 * 
 * Every record is encoded as BlockCrcInfoWriteable.
 * 
 * If a bucket has no record, 0 is filled in number of records field
 * without any data for actual records.
 * 
 */
class BlockCrcFileWriter {
  public static final Log LOG = LogFactory.getLog(BlockCrcFileWriter.class);
  final DataOutput out;
  private int version;
  private int numBuckets;
  private int currentBucket;
  private boolean hasException;

  BlockCrcFileWriter(DataOutput out, int version, int numBuckets) {
    this.out = out;
    this.version = version;
    this.numBuckets = numBuckets;
    this.currentBucket = 0;
    this.hasException = false;
  }
  
  void writeHeader() throws IOException {
    if (hasException) {
      throw new IOException("Has Exception");
    }
    hasException = true;
    out.writeInt(version);
    out.writeInt(numBuckets);
    hasException = false;
  }
  
  void writeBucket(Map<Block, DatanodeBlockInfo> mbd) throws IOException {
    if (hasException) {
      throw new IOException("Has Exception");
    }
    hasException = true;

    if (currentBucket >= numBuckets) {
      throw new IOException("more buckets to write than header indicates.");
    }

    out.writeInt(mbd.size());

    BlockCrcInfoWritable tmpBlockCrcInfo = new BlockCrcInfoWritable();

    for (Map.Entry<Block, DatanodeBlockInfo> bde : mbd.entrySet()) {
      Block block = bde.getKey();
      DatanodeBlockInfo binfo = bde.getValue();

      tmpBlockCrcInfo.set(block.getBlockId(), block.getGenerationStamp(),
          binfo.getBlockCrc());
      tmpBlockCrcInfo.write(out);
    }

    
    currentBucket++;
    hasException = false;
  }
  
  void checkFinish() throws IOException {
    if (hasException) {
      throw new IOException("Has Exception");
    }
    if (currentBucket != numBuckets) {
      throw new IOException("Number of buckets doesn't match header.");
    }
  }
  
}

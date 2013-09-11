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
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Class to read block CRC file from disk.
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
class BlockCrcFileReader {
  public static final Log LOG = LogFactory.getLog(BlockCrcFileReader.class);
  final private DataInput in;

  private int numBuckets;
  private int currentBucket;
  private int numRecordsInBucket;
  private int numRecordsReadInBucket;

  BlockCrcFileReader(DataInput in) {
    this.in = in;
  }
  
  int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Read header of the file
   * @throws IOException
   */
  void readHeader() throws IOException {
    int version = in.readInt();
    if (version != BlockCrcInfoWritable.LATEST_BLOCK_CRC_FILE_VERSION) {
      throw new IOException("Version " + version + " is not supported.");
    }
    numBuckets = in.readInt();
    currentBucket = -1;
    numRecordsReadInBucket = 0;
    numRecordsInBucket = 0;    
  }

  /**
   * Find the bucket ID for the next record. If current bucket hasn't yet been
   * finished, then the current bucket ID will be returned. Otherwise, it will
   * keep reading the input file until it finds the next non-empty bucket and
   * return this bucket's ID.
   * 
   * After the call, the position of the input stream will be just before the
   * next record.
   * 
   * @return bucket ID for next record. -1 if no more record left.
   * @throws IOException
   */
  int moveToNextRecordAndGetItsBucketId() throws IOException {
    while (numRecordsReadInBucket >= numRecordsInBucket) {
      if (currentBucket + 1>= numBuckets) {
        // We've finished all the records.
        return -1;
      } else {
        numRecordsInBucket = in.readInt();
        currentBucket++;
        numRecordsReadInBucket = 0;
      }
    }
    return currentBucket;
  }
  
  /** 
   * Get information for the next blockCRC record. NULL if not more left.
   * @return
   * @throws IOException
   */
  BlockCrcInfoWritable getNextRecord() throws IOException {
    // By calling getBucketIdForNextRecord(), we make sure the next field
    // to read is the next record (if there is any record left in the file)
    // Also, by checking the return value, we know whether we've finished
    // the file.
    if (moveToNextRecordAndGetItsBucketId() == -1) {
      return null;
    }

    BlockCrcInfoWritable crcInfo = new BlockCrcInfoWritable();
    crcInfo.readFields(in);
    numRecordsReadInBucket++;
    return crcInfo;
  }
}

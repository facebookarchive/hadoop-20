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

package org.apache.hadoop.raid;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;

public abstract class ChecksumStore {
  /**
   * Initialize the checksum store with config
   */
  abstract public void initialize(Configuration conf,
      boolean createStore) throws IOException;
  
  /**
   * Fetch the checksum for a lost block
   * lookup the checksum from the store with blockId and generationStamp
   * @param blk 
   * @return checksum
   */
  abstract public Long getChecksum(Block blk) throws IOException;
  
  /**
   * Save the checksum for a raided block into store and compare the old value
   * with new value, if different throw an exception
   * @param blk
   * @param newChecksum
   * @param oldChecksum
   * @throws IOException
   */
  public Long putIfAbsentChecksum(Block blk, Long newChecksum)
      throws IOException {
    Long oldChecksum = putIfAbsent(blk, newChecksum);
    if (oldChecksum!= null && !oldChecksum.equals(newChecksum)) {
      throw new IOException("Block " + blk.toString()
        + " has different checksums " + oldChecksum + "(old) and " +
          newChecksum+ "(new)");
    }
    return oldChecksum;
  }
  
  /**
   * Save the checksum for a block into store without comparing the values
   * @param blk
   * @param newChecksum 
   * @throws IOException
   */
  abstract public void putChecksum(Block blk, Long newChecksum)
      throws IOException;
  
  abstract public Long putIfAbsent(Block blk, Long newChecksum)
      throws IOException;
  
  abstract public int size() throws IOException;
  
  abstract public boolean isEmpty() throws IOException;
  
  abstract public boolean hasChecksum(Block blk) throws IOException;
  
  abstract public void clear() throws IOException; 
}

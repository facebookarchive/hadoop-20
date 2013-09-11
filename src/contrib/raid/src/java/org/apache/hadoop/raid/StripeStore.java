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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * StripeStore is only used by the directory-raid to store the block
 * information of stripes. In directory-raid, we could put blocks from 
 * different files into the same stripe. It's necessary to record the 
 * information somewhere in case directory is changed and we could not
 * retrieve the stripe information by listStatuing the directory any 
 * more
 * StripeStore should support following basic functions:
 * 1. Given a block, retrieve other blocks in the same stripe in order 
 * as well as the erasure code id 
 * 2. Store a stripe of blocks into the stripe store 
 */

public abstract class StripeStore {
  static public class StripeInfo {
    public Codec codec;
    public Block block;
    public List<Block> parityBlocks;
    public List<Block> srcBlocks;
    public StripeInfo(Codec newCodec, Block newBlock,
        List<Block> newParityBlocks, List<Block> newSrcBlocks) {
      this.codec = newCodec;
      this.block = newBlock;
      this.parityBlocks = newParityBlocks;
      this.srcBlocks = newSrcBlocks;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(codec.id);
      sb.append(" ");
      sb.append(block);
      sb.append(" parity blocks:");
      for (Block blockId : parityBlocks) {
        sb.append(" ");
        sb.append(blockId);
      }
      sb.append(" source blocks:");
      for (Block blockId : srcBlocks) {
        sb.append(" ");
        sb.append(blockId);
      }
      return sb.toString();
    }
    
    public int getBlockIdxInStripe(Block block) {
      int i = 0;
      for (Block parityBlock : parityBlocks) {
        if (parityBlock.equals(block)) {
          return i;
        }
        i++;
      }
      
      for (Block srcBlock : srcBlocks) {
        if (srcBlock.equals(block)) {
          return i;
        }
        i++;
      }
      return -1;
    }
  }
  
  /**
   * Initialize the config based on the given Filesystem
   */
  public Configuration initializeConf(String[] keys,
      Configuration conf, FileSystem fs) throws IOException {
    Configuration newConf = new Configuration(conf);
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    String suffix = fs.getUri().getAuthority();
    for (String key: keys) {
      String value = conf.get(key + "." + suffix);
      if (value != null) {
        newConf.set(key, value);
      }
    }
    return newConf;
  }
  
  /**
   * connect to the stripe store, if createStore is true, we create one if 
   * it doesn't exist.
   */
  abstract public void initialize(Configuration conf, boolean createStore,
      FileSystem fs) throws IOException;
  
  /**
   * Store a List of blocks within one stripe
   * The order of blocks in the list matters.
   */
  abstract public void putStripe(Codec codec, List<Block> parityBlks,
      List<Block> srcBlks) throws IOException;
  
  /**
   * Given a block, retrieve the information of its stripe
   * in order return null if nothing is found for the block and codec
   */
  abstract public StripeInfo getStripe(Codec codec, Block block)
      throws IOException;
  
  /**
   * Get the number of stripes in the store 
   */
  abstract public int numStripes() throws IOException; 
  
  /**
   * Clear the store
   */
  abstract public void clear() throws IOException;
}

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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.io.*;

/**
 * A wrap class that provides customized serialized form for block reports
 */
public class BlockReport implements Writable {
  static final WritableFactory FACTORY = new WritableFactory() {
    public Writable newInstance() { return new BlockReport(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(BlockReport.class, FACTORY);
  }

  protected long[] blocks;

  public BlockReport() {}

  public BlockReport(long[] blocks) {
    this.blocks = blocks;
  }

  public long[] getBlockReportInLongs() {return blocks;}
  
  public Block getBlock(int index){
    Block b = new Block();
    BlockListAsLongs.getBlockInfo(b, blocks, index);
    return b;
  }
  
  public void setBlock(Block b, int index){
    BlockListAsLongs.setBlockInfo(b, blocks, index);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(blocks.length);
    for(long block : blocks) {
      out.writeLong(block);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blocks = new long[in.readInt()];
    for (int i = 0; i<blocks.length; i++) {
      blocks[i] = in.readLong();
    }
  }
}

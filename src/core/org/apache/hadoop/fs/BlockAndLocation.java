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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/*
 * A BlockLocation lists a block and the block's hosts, offset and length
 * 
 */
public class BlockAndLocation extends BlockLocation {

  static {               // register a ctor
    WritableFactories.setFactory
      (BlockAndLocation.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockAndLocation(); }
       });
  }

  private long blockId; // the block's id
  private long blockGenStamp; // the block's generation stamp

  /**
   * Default Constructor
   */
  public BlockAndLocation() {
    super();
    blockId = 0L;
    blockGenStamp = 0L;
  }


  /**
   * Constructor with block id, gen stamp, host, name, network topology, offset,
   * length and corrupt flag
   */
  public BlockAndLocation(long blockId, long blockGenStamp,
                       String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length, boolean corrupt) {
    super(names, hosts, topologyPaths, offset, length, corrupt);
    this.blockId = blockId;
    this.blockGenStamp = blockGenStamp;
  }

  /**
   * Get this block's id
   * @return block id
   */
  public long getBlockId() {
    return this.blockId;
  }
  
  /**
   * Get this block's generation stamp
   * @return block gen stamp
   */
  public long getBlockGenerationStamp() {
    return this.blockGenStamp;
  }
  
  /**
   * Implement write of Writable
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(blockGenStamp);
    super.write(out);
  }
  
  /**
   * Implement readFields of Writable
   */
  public void readFields(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.blockGenStamp = in.readLong();
    super.readFields(in);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("blk_").append(blockId);
    result.append("_").append(blockGenStamp).append("@");
    result.append(super.toString());
    return result.toString();
  }
}

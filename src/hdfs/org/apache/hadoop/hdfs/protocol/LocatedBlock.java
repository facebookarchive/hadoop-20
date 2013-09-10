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
package org.apache.hadoop.hdfs.protocol;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/****************************************************
 * A LocatedBlock is a pair of Block, DatanodeInfo[]
 * objects.  It tells where to find a Block.
 * 
 ****************************************************/
@ThriftStruct
public class LocatedBlock implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlock.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlock(); }
       });
  }

  private Block b;
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfo[] locs;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;

  /**
   */
  public LocatedBlock() {
    this(new Block(), new DatanodeInfo[0], 0L, false);
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs, long startOffset) {
    this(b, locs, startOffset, false);
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs, long startOffset,
                      boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = new DatanodeInfo[0];
    } else {
      this.locs = locs;
    }
  }

  @ThriftConstructor
  public LocatedBlock(@ThriftField(1) Block block, @ThriftField(2) List<DatanodeInfo> datanodes,
      @ThriftField(3) long startOffset, @ThriftField(4) boolean corrupt) {
    this(block, datanodes.toArray(new DatanodeInfo[datanodes.size()]), startOffset, corrupt);
  }

  /**
   */
  @ThriftField(1)
  public Block getBlock() {
    return b;
  }

  /**
   */
  public DatanodeInfo[] getLocations() {
    return locs;
  }

  @ThriftField(2)
  public List<DatanodeInfo> getDatanodes() {
    return Arrays.asList(locs);
  }

  @ThriftField(3)
  public long getStartOffset() {
    return offset;
  }

  public void setBlockSize(long size) {
    b.setNumBytes(size);
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  @ThriftField(value = 4, name = "corrupt")
  public boolean isCorrupt() {
    return this.corrupt;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(corrupt);
    out.writeLong(offset);
    b.write(out);
    out.writeInt(locs.length);
    for (int i = 0; i < locs.length; i++) {
      locs[i].write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.corrupt = in.readBoolean();
    offset = in.readLong();
    this.b = new Block();
    b.readFields(in);
    int count = in.readInt();
    this.locs = new DatanodeInfo[count];
    for (int i = 0; i < locs.length; i++) {
      locs[i] = new DatanodeInfo();
      locs[i].readFields(in);
    }
  }

  @Override
  public String toString() {
    return "[offset " + offset + " size " + getBlockSize() + " end " +
        (offset + getBlockSize()) + "]";
  }

  public static void write(DataOutput out, LocatedBlock elem) throws IOException {
    LocatedBlock block = new LocatedBlock(elem.getBlock(), elem.getLocations(),
        elem.getStartOffset(), elem.isCorrupt());
    block.write(out);
  }

  public static LocatedBlock read(DataInput in) throws IOException {
    LocatedBlock block = new LocatedBlock();
    block.readFields(in);
    return block;
  }
}

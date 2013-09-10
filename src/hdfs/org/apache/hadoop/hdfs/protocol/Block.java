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

import java.io.*;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

/**************************************************
 * A Block is a Hadoop FS primitive, identified by a 
 * long.
 *
 **************************************************/
@ThriftStruct
public class Block implements Writable, Comparable<Block> {
  
  public static final String BLOCK_FILE_PREFIX = "blk_";
  public static final String METADATA_EXTENSION = ".meta";
  static {                                      // register a ctor
    WritableFactories.setFactory
      (Block.class,
       new WritableFactory() {
         public Writable newInstance() { return new Block(); }
       });
  }

  // generation stamp of blocks that pre-date the introduction of
  // a generation stamp.
  public static final long GRANDFATHER_GENERATION_STAMP = 0;

  /**
   * Determine whether the data file is for a block of non-inline checksum
   * The file name is of this format:
   * blk_<blk_id>
   * @param name
   * @return
   */
  public static boolean isSeparateChecksumBlockFilename(String name) {
    if (isBlockFileName(name) && name.indexOf('.') < 0
        && name.indexOf('_', BLOCK_FILE_PREFIX.length()) < 0) {
      return true;
    } 
    return false;
  }


  /**
   * Determine whether the file name is for a inline checksum block.
   * Inline checksum blocks are of this format:
   * blk_(blockId)_(generation_id)_(version)_(checksum_type)_(bytes_per_checksum)
   * @param fileName
   * @return
   */
  public static boolean isInlineChecksumBlockFilename(String fileName) {
    if (!isBlockFileName(fileName)) {
      return false;
    }
    int index_sep = fileName.indexOf('_', Block.BLOCK_FILE_PREFIX.length());
    return index_sep > 0 && fileName.indexOf('.', index_sep) < 0;
  }
  
  /**
   * Determine if the file name is a valid block/checksum file name.
   */
  private static boolean isBlockFileName(String fileName) {
    return fileName.startsWith(BLOCK_FILE_PREFIX);
  }
  
  public static long filename2id(String name) {
    int endIndex = - 1;
    if ((endIndex = name.indexOf('_', BLOCK_FILE_PREFIX.length())) < 0) {
      return Long.parseLong(name.substring(BLOCK_FILE_PREFIX.length()));
    } else {
      return Long.parseLong(name.substring(BLOCK_FILE_PREFIX.length(), endIndex));
    }
  }

  private long blockId;
  private long numBytes;
  private long generationStamp;

  public Block() {this(0, 0, 0);}

  @ThriftConstructor
  public Block(@ThriftField(1) long blockId, @ThriftField(2) long numBytes,
      @ThriftField(3) long generationStamp) {
    set(blockId, numBytes, generationStamp);
  }

  public Block(final long blkid) {this(blkid, 0, GenerationStamp.WILDCARD_STAMP);}

  public Block(Block blk) {this(blk.blockId, blk.numBytes, blk.generationStamp);}

  /**
   * Find the blockid from the given filename
   */
  public Block(File f, long len, long genstamp) {
    this(filename2id(f.getName()), len, genstamp);
  }

  public final void set(long blkid, long len, long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
  }
  /**
   */
  @ThriftField(1)
  public long getBlockId() {
    return blockId;
  }
  
  public void setBlockId(long bid) {
    blockId = bid;
  }

  /**
   */
  public String getBlockName() {
    return BLOCK_FILE_PREFIX + String.valueOf(blockId);
  }

  /**
   */
  @ThriftField(2)
  public long getNumBytes() {
    return numBytes;
  }
  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  @ThriftField(3)
  public long getGenerationStamp() {
    return generationStamp;
  }
  
  public void setGenerationStamp(long stamp) {
    generationStamp = stamp;
  }

  /**
   * Get the hint for delete an extra replica
   */
  public String getDelHints() {
    return null;
  }
  
  /**
   */
  public String toString() {
    return getBlockName() + "_" + getGenerationStamp();
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }

  public void readFields(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes + 
                            " Blockid " + blockId +
                            " GenStamp " + generationStamp);
    }
  }

  /////////////////////////////////////
  // Comparable
  /////////////////////////////////////
  static void validateGenerationStamp(long generationstamp) {
    if (generationstamp == GenerationStamp.WILDCARD_STAMP) {
      throw new IllegalStateException("generationStamp (=" + generationstamp
          + ") == GenerationStamp.WILDCARD_STAMP");
    }    
  }

  /** {@inheritDoc} */
  public int compareTo(Block b) {
    //Wildcard generationStamp is NOT ALLOWED here
    validateGenerationStamp(this.generationStamp);
    validateGenerationStamp(b.generationStamp);

    if (blockId < b.blockId) {
      return -1;
    } else if (blockId == b.blockId) {
      return GenerationStamp.compare(generationStamp, b.generationStamp);
    } else {
      return 1;
    }
  }

  /** {@inheritDoc} */
  public boolean equals(Object o) {
    if (!(o instanceof Block)) {
      return false;
    }
    final Block that = (Block)o;
    //Wildcard generationStamp is ALLOWED here
    return this.blockId == that.blockId
      && GenerationStamp.equalsWithWildcard(
          this.generationStamp, that.generationStamp);
  }

  /** {@inheritDoc} */
  public int hashCode() {
    //GenerationStamp is IRRELEVANT and should not be used here
    return 37 * 17 + (int) (blockId^(blockId>>>32));
  }

  public static void writeSafe(DataOutput out, Block elem) throws IOException {
    if (elem != null) {
      out.writeBoolean(true);
      Block block = new Block(elem);
      block.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  public static Block readSafe(DataInput in) throws IOException {
    if (in.readBoolean()) {
      Block b = new Block();
      b.readFields(in);
      return b;
    } else {
      return null;
    }
  }
}

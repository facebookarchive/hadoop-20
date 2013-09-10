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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Collection of blocks with their locations and the file length.
 */
@ThriftStruct
public class LocatedBlocks implements Writable {
  public static final Log LOG = LogFactory.getLog(LocatedBlocks.class);
  private long fileLength;
  private List<LocatedBlock> blocks; // array of blocks with prioritized locations
  private boolean underConstruction;

  private static final Comparator<LocatedBlock> BLOCK_START_OFFSET_COMPARATOR =
    new Comparator<LocatedBlock>() {
    @Override
    public int compare(LocatedBlock a, LocatedBlock b) {
      long aBeg = a.getStartOffset();
      long bBeg = b.getStartOffset();
      if (aBeg < bBeg)
        return -1;
      if (aBeg > bBeg)
        return 1;
      return 0;
    }
  };

  public LocatedBlocks() {
    fileLength = 0;
    blocks = null;
    underConstruction = false;
  }

  @ThriftConstructor
  public LocatedBlocks(@ThriftField(1) long fileLength,
      @ThriftField(2) List<LocatedBlock> locatedBlocks,
      @ThriftField(3) boolean isUnderConstuction) {
    this.fileLength = fileLength;
    blocks = locatedBlocks;
    underConstruction = isUnderConstuction;
  }

  /** Shallow copy constructor */
  public <V extends LocatedBlocks> LocatedBlocks(V other) {
    this(other.getFileLength(), other.getLocatedBlocks(), other.isUnderConstruction());
  }

  /**
   * Get located blocks.
   */
  @ThriftField(2)
  public List<LocatedBlock> getLocatedBlocks() {
    return blocks;
  }

  /**
   * Get located block.
   */
  public LocatedBlock get(int index) {
    return blocks.get(index);
  }

  /**
   * Get number of located blocks.
   */
  public int locatedBlockCount() {
    return blocks == null ? 0 : blocks.size();
  }

  /**
   *
   */
  @ThriftField(1)
  public long getFileLength() {
    return this.fileLength;
  }

  /**
   * Return true if the file was under construction when
   * this LocatedBlocks was constructed, false otherwise.
   */
  @ThriftField(3)
  public boolean isUnderConstruction() {
    return underConstruction;
  }

  /**
   * Sets the file length of the file.
   */
  public void setFileLength(long length) {
    this.fileLength = length;
  }

  /**
   * If file is under construction, set block size of the last block. It updates
   * file length in the same time.
   */
  public synchronized  void setLastBlockSize(long blockId, long blockSize) {
    assert blocks.size() > 0;
    LocatedBlock last = blocks.get(blocks.size() - 1);
    if (underConstruction && blockSize > last.getBlockSize()) {
      assert blockId == last.getBlock().getBlockId();
      this.setFileLength(this.getFileLength() + blockSize - last.getBlockSize());
      last.setBlockSize(blockSize);

      if (LOG.isDebugEnabled()) {
        LOG.debug("DFSClient setting last block " + last + " to length "
            + blockSize + " filesize is now " + getFileLength());
      }
    }
  }

  /**
   * Find block containing the specified offset, or insertion point (encoded as
   * -i-1) of the given offset among starting offsets of the blocks if there is
   * no exact match. A nonnegative return value means an exact match.
   *
   * @return the index of the block starting with the given offset if there is
   *         an exact match, or <b>-i-1</b>, where i is is the insertion point
   *         of the given offset (as defined in
   *         {@link Collections#binarySearch(List, Object, Comparator)}).
   */
  protected int binarySearchBlockStartOffsets(long offset) {
    LocatedBlock key = new LocatedBlock();
    key.setStartOffset(offset);
    return Collections.binarySearch(blocks, key,
        BLOCK_START_OFFSET_COMPARATOR);
  }

  public void insertRange(List<LocatedBlock> newBlocks) {
    // Is there anything to insert?
    if (newBlocks.isEmpty())
      return;

    // Find where to start inserting the new blocks.
    final int insertIdx = getInsertIndex(binarySearchBlockStartOffsets(
        newBlocks.get(0).getStartOffset()));

    final List<LocatedBlock> oldBlocks = blocks;  // for readability

    if (insertIdx >= oldBlocks.size()) {
      oldBlocks.addAll(newBlocks);
      return;
    }

    // Merged blocks from both old and new block arrays.
    List<LocatedBlock> mergedBlocks = new ArrayList<LocatedBlock>(
        newBlocks.size());

    int numOldBlocks = oldBlocks.size();
    int numNewBlocks = newBlocks.size();

    int oldIdx = insertIdx;
    int newIdx = 0;

    LocatedBlock oldBlk = oldBlocks.get(oldIdx);
    LocatedBlock newBlk = newBlocks.get(newIdx);

    long oldOff = oldBlk.getStartOffset();
    long newOff = newBlk.getStartOffset();

    // Merge newBlocks with a sub-list of oldBlocks.
    while (newIdx < numNewBlocks) {
      boolean advanceOld = false;
      boolean advanceNew = false;
      if (newOff <= oldOff) {
        // We always take the new block if two offsets are the same.
        mergedBlocks.add(newBlk);
        advanceOld = newOff == oldOff;
        advanceNew = true;
      } else {
        mergedBlocks.add(oldBlk);
        advanceOld = true;
      }

      if (advanceOld) {
        ++oldIdx;
        if (oldIdx < numOldBlocks) {
          oldBlk = oldBlocks.get(oldIdx);
          oldOff = oldBlk.getStartOffset();
        } else {
          oldBlk = null;
          oldOff = Long.MAX_VALUE;
        }
      }

      if (advanceNew) {
        ++newIdx;
        if (newIdx < numNewBlocks) {
          newBlk = newBlocks.get(newIdx);
          newOff = newBlk.getStartOffset();
        }
        // otherwise, we will break out of the loop
      }
    }

    // We have finished our merge of the sublist of the old blocks array
    // starting from insertIdx (inclusive) to the current value of oldIdx
    // (exclusive) with newBlocks. Now replace that sublist with the new merged
    // sublist.

    List<LocatedBlock> subList = oldBlocks.subList(insertIdx, oldIdx);
    subList.clear();
    subList.addAll(mergedBlocks);
  }

  public static int getInsertIndex(int binSearchResult) {
    return binSearchResult >= 0 ? binSearchResult : -(binSearchResult+1);
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlocks.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlocks(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(this.fileLength);
    out.writeBoolean(underConstruction);
    // write located blocks
    int nrBlocks = locatedBlockCount();
    out.writeInt(nrBlocks);
    if (nrBlocks == 0) {
      return;
    }
    for (LocatedBlock blk : this.blocks) {
      blk.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.fileLength = in.readLong();
    underConstruction = in.readBoolean();
    // read located blocks
    int nrBlocks = in.readInt();
    this.blocks = new ArrayList<LocatedBlock>(nrBlocks);
    for (int idx = 0; idx < nrBlocks; idx++) {
      LocatedBlock blk = new LocatedBlock();
      blk.readFields(in);
      this.blocks.add(blk);
    }
  }

  @Override
  public String toString() {
    return getLocatedBlocks().toString();
  }

}

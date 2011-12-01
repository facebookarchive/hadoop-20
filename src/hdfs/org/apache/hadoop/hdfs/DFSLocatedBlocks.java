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
package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 * Wrapper for LocatedBlocks with lock protection for concurrent updates.
 */
public class DFSLocatedBlocks extends LocatedBlocks {

  private volatile long fileLength = 0; // an optimization to get fileLength without locks
  private ReentrantReadWriteLock lock;

  public DFSLocatedBlocks(LocatedBlocks lbs) {
    super(lbs.getFileLength(), lbs.getLocatedBlocks(), lbs.isUnderConstruction());
    this.fileLength = lbs.getFileLength();
    lock = new ReentrantReadWriteLock(true); // fair
  }
  
  public DFSLocatedBlocks(long flength, List<LocatedBlock> blks, boolean isUnderConstuction) {
    super(flength, blks, isUnderConstuction);
    this.fileLength = flength;
  }
  
  /**
   * Get located blocks.
   */
  public List<LocatedBlock> getLocatedBlocks() {
    readLock();
    try {
      return super.getLocatedBlocks();
    } finally {
      readUnlock();
    }
  }

  /**
   * @return a copy of the block location array. Used in testing.
   */
  List<LocatedBlock> getLocatedBlocksCopy() {
    readLock();
    try {
      return new ArrayList<LocatedBlock>(super.getLocatedBlocks());
    } finally {
      readUnlock();
    }
  }

  /**
   * Get located block.
   */
  public LocatedBlock get(int index) {
    readLock();
    try {
      return super.get(index);
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get number of located blocks.
   */
  public int locatedBlockCount() {
    readLock();
    try {
      return super.locatedBlockCount();
    } finally {
      readUnlock();
    }
  }

  /**
   * 
   */
  public long getFileLength() {
    return this.fileLength;
  }

  /**
   * Return true if file was under construction when 
   * this LocatedBlocks was constructed, false otherwise.
   */
  public boolean isUnderConstruction() {
    readLock();
    try {
      return super.isUnderConstruction();
    } finally {
      readUnlock();
    }
  }

  /**
   * Sets the file length of the file.
   */
  public void setFileLength(long length) {
    writeLock();
    try {
      super.setFileLength(length);
      this.fileLength = length;
    } finally {
      writeUnlock();
    }
  }

  public void insertRange(List<LocatedBlock> newBlocks) {
    if (newBlocks.isEmpty())
      return;

    writeLock();
    try {
      super.insertRange(newBlocks);
    } finally {
      writeUnlock();
    }
  }

  public LocatedBlock getBlockContainingOffset(long offset) {
    readLock();
    try {
      int blockIdx = super.binarySearchBlockStartOffsets(offset);
      List<LocatedBlock> locatedBlocks = super.getLocatedBlocks();
      if (blockIdx >= 0)
        return locatedBlocks.get(blockIdx);  // exact match

      blockIdx = LocatedBlocks.getInsertIndex(blockIdx);
      // Here, blockIdx is the "insertion point" of the queried offset in
      // the array (the index of the first element greater than the offset),
      // which by definition means that
      //
      // locatedBlocks.get(blockIdx - 1).getStartOffset() < offset &&
      // offset < locatedBlocks.get(blockIdx).getStartOffset().
      //
      // In particular, if blockIdx == 0, then
      // offset < locatedBlocks.get(0).getStartOffset().

      if (blockIdx == 0)
        return null;  // The offset is not found in known blocks.

      LocatedBlock blk = locatedBlocks.get(blockIdx - 1);
      long blkStartOffset = blk.getStartOffset();
      if (offset < blkStartOffset) {
        // By definition of insertion point, 
        // locatedBlocks.get(blockIdx - 1).getStartOffset() < offset.
        throw new AssertionError("Invalid insertion point: " +
            blockIdx + " for offset " + offset + " (located blocks: " +
            locatedBlocks + ")");
      }

      long blkLen = blk.getBlockSize();
      if (offset < blkStartOffset + blkLen)
        return blk;

      // Block not found in the location cache, the caller should ask the
      // namenode instead.
      return null;  

    } finally {
      readUnlock();
    }
  }
  private void readLock() {
    lock.readLock().lock();
  }
  private void readUnlock() {
    lock.readLock().unlock();
  }
  private void writeLock() {
    lock.writeLock().lock();
  }
  private void writeUnlock() {
    lock.writeLock().unlock();
  }

  @Override
  public String toString() {
    return getLocatedBlocks().toString();
  }

}

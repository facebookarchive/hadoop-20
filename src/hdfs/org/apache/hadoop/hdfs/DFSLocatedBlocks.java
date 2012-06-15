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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
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
   * Return ture if file was under construction when 
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
  
  /**
   * Find block containing specified offset.
   * 
   * @return block if found, or null otherwise.
   */
  public int findBlock(long offset) {
    readLock();
    try {
      return super.findBlock(offset);
    } finally {
      readUnlock();
    }
  }
  
  public void insertRange(long offset, List<LocatedBlock> newBlocks) {
    writeLock();
    try {
      // recheck if block is already in the cache. This has to be done
      // within the write lock, otherwise the insertion point could
      // have changed.
      int blockIdx = super.findBlock(offset);
      if (blockIdx < 0) {
        blockIdx = super.getInsertIndex(blockIdx);
        super.insertRange(blockIdx, newBlocks);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns the block at exactly the given offset, if that block's location is
   * cached.
   *
   * @param offset the start offset of the block
   * @return the located block starting at the given offset or null if not
   *         cached
   */
  public LocatedBlock getBlockAt(long offset) {
    readLock();
    try {
      int blockIdx = super.findBlock(offset);
      if (blockIdx < 0)
        return null;
      return super.getLocatedBlocks().get(blockIdx);
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
}

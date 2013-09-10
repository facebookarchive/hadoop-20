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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 * Wrapper for LocatedBlocks with lock protection for concurrent updates.
 */
public class DFSLocatedBlocks extends LocatedBlocks {

  static final int DEFAULT_BLOCK_EXPIRE_TIME = -1;
  static final double DEFAULT_BLOCK_EXPIRE_RANDOM_FACTOR = 0.1d;

  private final int blkLocInfoExpireTimeout;
  private volatile long fileLength = 0; // an optimization to get fileLength without locks
  private ReentrantReadWriteLock lock;
  private Map<LocatedBlock, Long> blkLocInfoExpireMap;
  private long timeBlkLocInfoExpire;
  
  /**
   * Deprecate block location information that has lived for too long
   */
  public void blockLocationInfoExpiresIfNeeded() {
    if (blkLocInfoExpireTimeout < 0) {
      return;
    }
    long timeNow = System.currentTimeMillis();
    if (timeBlkLocInfoExpire < timeNow) {
      this.writeLock();
      try {
        long newTimeBlockExpire = Long.MAX_VALUE;
        List<LocatedBlock> listToRemove = new ArrayList<LocatedBlock>();
        for (LocatedBlock lb : blkLocInfoExpireMap.keySet()) {
          long expireTime = blkLocInfoExpireMap.get(lb);
          if (expireTime < timeNow) {
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Expire cached block location for " + lb);
            }
            listToRemove.add(lb);
          } else if (expireTime < newTimeBlockExpire) {
            newTimeBlockExpire = expireTime;
          } else {
          }
        }
        super.getLocatedBlocks().removeAll(listToRemove);
        for (LocatedBlock lb : listToRemove) {
          blkLocInfoExpireMap.remove(lb);
        }
        this.timeBlkLocInfoExpire = newTimeBlockExpire;
      } finally {
        this.writeUnlock();
      }
    }
  }
  
  /**
   * Initial the blockExpireMap, which keeps track of the expiration time of all
   * block location info.
   * 
   * @param expireTime
   *          time for the located block to expire
   */
  private void initBlkLocInfoExpireMap(long expireTime) {
    if (blkLocInfoExpireTimeout < 0) {
      return;
    }

    this.blkLocInfoExpireMap = new HashMap<LocatedBlock, Long>(this
        .getLocatedBlocks().size());
    for (LocatedBlock lb : this.getLocatedBlocks()) {
      blkLocInfoExpireMap.put(lb, expireTime);
    }
    timeBlkLocInfoExpire = expireTime;
  }

  public DFSLocatedBlocks(LocatedBlocks lbs, int blockExpireTimeout) {
    super(lbs.getFileLength(), lbs.getLocatedBlocks(), lbs.isUnderConstruction());
    this.blkLocInfoExpireTimeout = blockExpireTimeout;
    this.fileLength = lbs.getFileLength();
    lock = new ReentrantReadWriteLock(true); // fair
    initBlkLocInfoExpireMap(System.currentTimeMillis() + blockExpireTimeout);
  }
  
  public DFSLocatedBlocks(long flength, List<LocatedBlock> blks, boolean isUnderConstuction) {
    super(flength, blks, isUnderConstuction);
    this.blkLocInfoExpireTimeout = DEFAULT_BLOCK_EXPIRE_TIME;
    this.fileLength = flength;
    initBlkLocInfoExpireMap(System.currentTimeMillis() + blkLocInfoExpireTimeout);
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
  
  @Override
  public void setLastBlockSize(long blockId, long blockSize) {
    writeLock();
    try {
      super.setLastBlockSize(blockId, blockSize);
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
      Map<LocatedBlock, Long> newBlockExpireMap = new HashMap<LocatedBlock, Long>(
          this.getLocatedBlocks().size());
      long expireTime = System.currentTimeMillis() + blkLocInfoExpireTimeout;
      long newTimeBlockExpire = expireTime;
      for (LocatedBlock lb : this.getLocatedBlocks()) {
        if (blkLocInfoExpireTimeout >= 0) {
          if (blkLocInfoExpireMap.containsKey(lb)) {
            long expireTimeForBlock = blkLocInfoExpireMap.get(lb);
            newBlockExpireMap.put(lb, expireTimeForBlock);
            if (expireTimeForBlock < newTimeBlockExpire) {
              newTimeBlockExpire = expireTimeForBlock;
            }
          } else {
            newBlockExpireMap.put(lb, expireTime);
          }
        }
      }
      this.blkLocInfoExpireMap = newBlockExpireMap;
      this.timeBlkLocInfoExpire = newTimeBlockExpire;
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
      if (offset < blkStartOffset + blkLen
          || (offset == blkStartOffset + blkLen && isUnderConstruction() &&
          blockIdx == locatedBlocks.size())) {
        return blk;
      }

      // Block not found in the location cache, the caller should ask the
      // namenode instead.
      return null;  

    } finally {
      readUnlock();
    }
  }

  /**
   * Determine whether the input block is the block under-construction
   * for the file. If the current file is not under-construction, always
   * false is returned.
   * 
   * The result is the best guess based on unknown
   * information. The bottom line is, when the position equals to file length,
   * the block selected will be return true. This has to be guaranteed to make
   * sure the available size updating logic will always be triggered when
   * reading to the end of a under-construction file.
   * 
   * @param block
   * @return
   */
  public boolean isUnderConstructionBlock(Block block) {
    if (!isUnderConstruction()) {
      return false;
    }
    LocatedBlock lastBlock = this.get(this.locatedBlockCount() - 1);

    // There are potential inconsistency when counting the size of the
    // last block, but fileLength is not likely to be under-estimated
    // the size, unless the last block size is 0. 
    if ((this.fileLength <= lastBlock.getStartOffset()
        + lastBlock.getBlockSize())
        && lastBlock.getBlock().equals(block)) {
      return true;
    }
    return false;
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

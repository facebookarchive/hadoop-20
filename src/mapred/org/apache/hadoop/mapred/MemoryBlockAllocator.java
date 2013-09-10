/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class MemoryBlockAllocator {
  private static final Log LOG =
      LogFactory.getLog(MemoryBlockAllocator.class);
  
  enum  PreferedMeoryBlockSize{
    SIZE_32K(32 * 1024),
    SIZE_64K(64 * 1024), 
    SIZE_128K(128 * 1024), 
    SIZE_256K(256 * 1024), 
    SIZE_512K(512 * 1024), 
    SIZE_1M(1 * 1024 * 1024), 
    SIZE_2M(2 * 1024 * 1024),
    SIZE_4M(4 * 1024 * 1024);

    protected static PreferedMeoryBlockSize[] PreferedMemBlockSorted;

    static {
      PreferedMemBlockSorted = PreferedMeoryBlockSize.values();
      Arrays.sort(PreferedMemBlockSorted,
          new Comparator<PreferedMeoryBlockSize>() {
            @Override
            public int compare(PreferedMeoryBlockSize t0,
                PreferedMeoryBlockSize t1) {
              return t1.size - t0.size;
            }
          });
    }

    private final int size;

    PreferedMeoryBlockSize(int size) {
      this.size = size;
    }

    /**
     * Find a preferred memory block size
     * 
     * @param softBufferSize
     *          total memory that can be used before doing a spill
     * @param reducerNum
     *          number of reducers in this job
     * @return
     */
    public static PreferedMeoryBlockSize findPreferedSize(
        int softBufferSize, int reducerNum) {
      // each reducer got at least 2 memory block. This is to help reduce
      // memory block borrowing.
      int averageMem = (int) (softBufferSize / (reducerNum * 2));
      for (PreferedMeoryBlockSize predefined : PreferedMeoryBlockSize.PreferedMemBlockSorted) {
        if(averageMem > predefined.getSize()) {
          return predefined;
        }
      }
      // return the smallest unit
      return PreferedMemBlockSorted[PreferedMemBlockSorted.length - 1];
    }

    public int getSize() {
      return size;
    }

  }
  
  class MemoryBlockInstancePool {
    private List<MemoryBlock> freeNormalMemoryBlocks;
    private List<MemoryBlock> waitForMerge;
    private List<ChildMemoryBlock> orphanMemoryBlocks;
    
    public MemoryBlockInstancePool() {
      this.freeNormalMemoryBlocks = new LinkedList<MemoryBlock>();
      this.waitForMerge = new LinkedList<MemoryBlock>();
      this.orphanMemoryBlocks = new LinkedList<ChildMemoryBlock>();
    }

    void addMemoryBlock(MemoryBlock memoryBlock) {
      memoryBlock.reset();
      if (memoryBlock instanceof ChildMemoryBlock) {
        ChildMemoryBlock childMB = (ChildMemoryBlock) memoryBlock;
        Iterator<MemoryBlock> toMergeIter =
            this.waitForMerge.iterator();
        boolean foundParent = false;
        while (toMergeIter.hasNext()) {
          MemoryBlock toMergeMemBlock = toMergeIter.next();
          if (childMB.getParentMemoryBlock().equals(toMergeMemBlock)) {
            toMergeMemBlock.returnChild(childMB);
            foundParent = true;
            if (toMergeMemBlock.childNum == 0) {
              toMergeIter.remove();
              freeNormalMemoryBlocks.add(toMergeMemBlock);
            }
            break;
          }
        }

        if (!foundParent) {
          this.orphanMemoryBlocks.add(childMB);
        }
      } else {
        if (memoryBlock.childNum != 0) {
          // go through orphanMemoryBlocks
          Iterator<ChildMemoryBlock> orphanMemBlockIter =
              orphanMemoryBlocks.iterator();
          while (orphanMemBlockIter.hasNext()) {
            ChildMemoryBlock orphanMemBlock =
                orphanMemBlockIter.next();
            if (orphanMemBlock.getParentMemoryBlock().equals(
                memoryBlock)) {
              orphanMemBlockIter.remove();
              memoryBlock.returnChild(orphanMemBlock);
            }
          }
        }

        if (memoryBlock.childNum == 0) {
          freeNormalMemoryBlocks.add(memoryBlock);
        } else {
          waitForMerge.add(memoryBlock);
        }
      }
    }
    
    
    MemoryBlock allocateMemoryBlock() {
      if (freeNormalMemoryBlocks.size() > 0) {
        return freeNormalMemoryBlocks.remove(0);
      }
      return null;
    }

    public void clear() {
      freeNormalMemoryBlocks.clear();
      orphanMemoryBlocks.clear();
      waitForMerge.clear();
    }
  }

  private final int bufferSize;
  private final int softBufferSize;
  private final PreferedMeoryBlockSize blockSize;
  private final int minBorrowableSize;
  private final BlockMapOutputCollector collector;
  
  int maxMemoryBlockLength;
  //first, assume each record is 128 bytes.
  int avgRecordLen = 128;
  long totalCollectedRecNum = 0;

  private int consumedBufferMem = 0;
  private int unassignedStartOffset;
  private int allocatedSize;
  
  private int allocatedRecordMem;

  private List<MemoryBlockHolder> memoryBlockHolders;
  private MemoryBlockInstancePool memBlockStore;
  
  // need to clean up memory block store after spill, it
  // is because there are maybe big blocks there.
  private boolean needToCleanMemBlockStore = false;

  public MemoryBlockAllocator(int bufferSize, int softBufferSize,
      int mapperNum, int reducerNum,
      BlockMapOutputCollector blockMapOutputBuffer) {
    this.bufferSize = bufferSize;
    this.softBufferSize = softBufferSize;
    unassignedStartOffset = 0;
    allocatedSize = 0;
    blockSize =
        PreferedMeoryBlockSize.findPreferedSize(softBufferSize,
            reducerNum);
    memoryBlockHolders = new ArrayList<MemoryBlockHolder>();
    minBorrowableSize = findPreferedBorrowSize(bufferSize-softBufferSize, reducerNum);
    memBlockStore = new MemoryBlockInstancePool();
    collector = blockMapOutputBuffer;
  }

  private int findPreferedBorrowSize(int softBufferSize,
      int reducerNum) {
    int borrowUnit = (int) (softBufferSize / reducerNum);
    if (borrowUnit > (blockSize.getSize() / 4)) {
      borrowUnit = blockSize.getSize() / 4;
    } else if (borrowUnit > (blockSize.getSize() / 2)) {
      borrowUnit = blockSize.getSize() / 2;
    }
    return borrowUnit;
  }
  
  public void registerMemoryBlockHolder(MemoryBlockHolder holder) {
    if (memoryBlockHolders.contains(holder)) {
      return;
    }
    memoryBlockHolders.add(holder);
  }

  protected void reset() {
    unassignedStartOffset = 0;
    allocatedSize = 0;
    consumedBufferMem = 0;
    memBlockStore.clear();
  }
  
  private void sortAndSpill() throws IOException {
    collector.sortAndSpill();
    if(needToCleanMemBlockStore) {
      reset();
      needToCleanMemBlockStore = false;
    }
  }

  public MemoryBlock allocateMemoryBlock(int minSize)
      throws IOException {
    boolean requireSortAndSpill = this.shouldSpill();
    if(requireSortAndSpill) {
      sortAndSpill();
    }

    int left = left();
    int toAllocateSize = blockSize.getSize();
    boolean bigRecord = false;
    if (minSize > toAllocateSize) {
      if(left < minSize) {
        return null;        
      } else {
        toAllocateSize = minSize;
        bigRecord = true;
      }
    }

    MemoryBlock ret;
    boolean onlyFromStore =
        ((allocatedRecordMem + allocatedSize) > softBufferSize)
            || (left < minBorrowableSize);
    if (onlyFromStore) {
      if(bigRecord) {
        // for big record, can not allocate memory block from in-mem store
        return null;
      }
      // if only from store is true, that means we can not allocate new memory
      // block from buffer anymore.
      ret = memBlockStore.allocateMemoryBlock();
      if (ret == null) {
        sortAndSpill();
        ret = memBlockStore.allocateMemoryBlock();
      }
      return ret;
    }
    
    //if left > borrowUnitSize, we can still use it.
    if (left < toAllocateSize && left > minBorrowableSize
        && minSize < minBorrowableSize) {
      toAllocateSize = left;
    }

    // if reach here, minSize must be less than or equal to toAllocateSize
    if (left < toAllocateSize) {
      ret = memBlockStore.allocateMemoryBlock();
      if (ret == null) {
        ret = borrowMemoryBlock();
      }
    } else {
      int eleNum = (int) (toAllocateSize / this.avgRecordLen);
      if(eleNum < 10) {
        eleNum = 10;
      }
      ret = new MemoryBlock(unassignedStartOffset, toAllocateSize,
              this, eleNum);
      unassignedStartOffset += ret.getSize();
      this.allocatedSize += ret.getSize();
      if(bigRecord) {
        this.needToCleanMemBlockStore = true;
      }
    }

    return ret;
  }
  
  public void incAllocatedRecordMem(int size) {
    this.allocatedRecordMem +=size;
  }
  
  public void decAllocatedRecordMem(int size) {
    this.allocatedRecordMem -=size;
  }
  
  private MemoryBlock borrowMemoryBlock() {
    if (memoryBlockHolders == null || memoryBlockHolders.size() == 0) {
      return null;
    }
    Collections.sort(memoryBlockHolders,
        new Comparator<MemoryBlockHolder>() {
          @Override
          public int compare(MemoryBlockHolder o1,
              MemoryBlockHolder o2) {
            MemoryBlock memBlk1 = o1.getCurrentOpenMemoryBlock();
            MemoryBlock memBlk2 = o2.getCurrentOpenMemoryBlock();
            int left1 = memBlk1 == null ? 0 : memBlk1.left();
            int left2 = memBlk2 == null ? 0 : memBlk2.left();
            return left2 - left1;
          }
        });
    MemoryBlock blk =
        memoryBlockHolders.get(0).getCurrentOpenMemoryBlock();
    int startPos = blk.shrinkFromEnd(minBorrowableSize);
    if(startPos < 0) {
      return null;
    }
    MemoryBlock ret =
        new ChildMemoryBlock(startPos, minBorrowableSize, this,
            minBorrowableSize / avgRecordLen, blk);
    return ret;
  }

  public int left() {
    return bufferSize - allocatedSize;
  }

  public int suggestNewSize(int oldSize) {
    if (oldSize <= 0) {
      throw new IllegalArgumentException("old size is negative.");
    } else if (oldSize < 10) {
      oldSize = 10;
    }
    int newSize = (int) (oldSize * 1.25);
    if (oldSize < maxMemoryBlockLength
        && newSize > maxMemoryBlockLength) {
      return maxMemoryBlockLength;
    }
    return newSize;
  }

  public void finishMemoryBlock(MemoryBlock memoryBlock) {
    int currentPtr = memoryBlock.getValid();
    if (currentPtr > maxMemoryBlockLength) {
      maxMemoryBlockLength = currentPtr;
    }
    consumedBufferMem += memoryBlock.getSize();

    long newCollectedRecordsNum = totalCollectedRecNum + currentPtr;
    int oldAvgRecordLen = avgRecordLen;
    avgRecordLen =
        (int) (((avgRecordLen * totalCollectedRecNum) + memoryBlock.getUsed())
            / newCollectedRecordsNum);
    if (avgRecordLen == 0) {
      String errAvgRecordLen = String.format("oldAvgRecordLen:%d, totalCollectedRecNum:%d, memroryBlock.used:%d", 
          oldAvgRecordLen, totalCollectedRecNum, memoryBlock.getUsed());
      LOG.info(errAvgRecordLen);
    }
    totalCollectedRecNum = newCollectedRecordsNum;
  }
  
  public void freeMemoryBlock(MemoryBlock memoryBlock) {
    memBlockStore.addMemoryBlock(memoryBlock);
    consumedBufferMem -= memoryBlock.getSize();
  }

  public boolean shouldSpill() {
    return consumedBufferMem > softBufferSize;
  }

  public long getEstimatedSize() {
    return this.consumedBufferMem;
  }
}

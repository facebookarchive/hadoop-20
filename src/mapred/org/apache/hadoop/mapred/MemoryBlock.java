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

import org.apache.hadoop.io.BufferTooSmallException;
import org.apache.hadoop.io.BytesWritable;

/**
 * This should be kept in the CPU cache. So assume the random access inside
 * this one MemoryBlock is very cheap.
 */
class MemoryBlock {
  final int startPos;
  private int size;
  final MemoryBlockAllocator memAllocator;
  
  //number of borrows happened for this memory block;
  int childNum = 0;

  int used;
  // a list of global offsets
  int[] offsets;
  int[] keyLenArray;
  int[] valueLenArray;
  int currentPtr;

  /**
   * when use the iterator, make sure there must be at least 1 record in this
   * memory block
   */
  class KeyValuePairIterator {

    int currentReadPos = 0;

    public boolean hasNext() {
      if (currentReadPos < currentPtr - 1) {
        return true;
      }
      return false;
    }
    
    public void next() {
      currentReadPos ++;
    }

    public int getCurrentOffset() {
      return offsets[currentReadPos];
    }

    public int getCurrentKeyLen() {
      return keyLenArray[currentReadPos];
    }

    public int getCurrentValueLen() {
      return valueLenArray[currentReadPos];
    }
    
    public MemoryBlock getMemoryBlock() {
      return MemoryBlock.this;
    }

    public int getCurrentReadPos() {
      return currentReadPos;
    }

  }

  
  public KeyValuePairIterator iterator() {
    return new KeyValuePairIterator();
  }

  public MemoryBlock(int startOffset, int allocateSize,
      MemoryBlockAllocator memoryBlockAllocator, int elemNum) {
    startPos = startOffset;
    size = allocateSize;
    if(startPos < 0 || size < 0) {
      throw new IllegalArgumentException(
          "startPos or size is negative, startPos = " + startPos
              + ", size is " + size);
    }
    used = 0;
    memAllocator = memoryBlockAllocator;
    offsets = new int[elemNum];
    keyLenArray = new int[elemNum];
    valueLenArray = new int[elemNum];
    currentPtr = 0;
    memAllocator.incAllocatedRecordMem(elemNum * 4 * 3);
  }

  public void addOffset(int internalOffset, int keyLen, int valLen) {
    offsets[currentPtr] = startPos + internalOffset;
    keyLenArray[currentPtr] = keyLen;
    valueLenArray[currentPtr] = valLen;
    currentPtr++;
    if (currentPtr >= offsets.length) {
      enlargeCapacity();
    }
  }

  private void enlargeCapacity() {
    int newSize = memAllocator.suggestNewSize(offsets.length);
    memAllocator.decAllocatedRecordMem(offsets.length * 4 * 3);
    int[] newOffsetArray = new int[newSize];
    System.arraycopy(offsets, 0, newOffsetArray, 0, offsets.length);
    offsets = newOffsetArray;
    int[] newKeyLenArray = new int[newSize];
    System.arraycopy(keyLenArray, 0, newKeyLenArray, 0, keyLenArray.length);
    keyLenArray = newKeyLenArray;
    int[] newValLenArray = new int[newSize];
    System.arraycopy(valueLenArray, 0, newValLenArray, 0, valueLenArray.length);
    valueLenArray = newValLenArray;
    memAllocator.incAllocatedRecordMem(newSize * 4 * 3);
  }
  
  public void collectKV(byte[] kvbuffer, BytesWritable key,
      BytesWritable value) throws BufferTooSmallException {
    int oldUsed = used;
    int keyLen = key.copyTo(kvbuffer, startPos + used);
    used += keyLen;
    int valLen = value.copyTo(kvbuffer, startPos + used);
    used += valLen;
    addOffset(oldUsed, keyLen, valLen);
  }

	public void finish() {
	  memAllocator.finishMemoryBlock(this);
	}

  public int getStartPos() {
    return startPos;
  }

  public int getSize() {
    return size;
  }

  public int shrinkFromEnd(int shrinkSize) {
    if (left() < shrinkSize) {
      return -1;
    }
    size -= shrinkSize;
    childNum++;
    return startPos + size;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public int[] getKeyLenArray() {
    return keyLenArray;
  }

  public int[] getValueLenArray() {
    return valueLenArray;
  }
  
  public int getValid() {
    return currentPtr;
  }

  public int left() {
    return this.size - this.used;
  }
  
  public int getUsed() {
    return used;
  }

  public void reset() {
    this.used = 0;
    this.currentPtr = 0;
  }

  public void returnChild(ChildMemoryBlock orphanMemBlock) {
    childNum--;
    size += orphanMemBlock.getSize();
  }

}
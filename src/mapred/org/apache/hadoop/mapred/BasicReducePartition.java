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
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.LexicographicalComparerHolder;
import org.apache.hadoop.util.QuickSort;

public abstract class BasicReducePartition<K extends BytesWritable, V extends BytesWritable>
    implements MemoryBlockHolder {

  class OffsetSortable implements IndexedSortable {

    private int[] offsets;
    private int[] keyLenArray;
    private int[] valLenArray;
    private byte[] kvbuffer;

    public OffsetSortable(int[] offsets, int[] keyLenArray, int[] valLenArray,
        byte[] kvbuffer) {
      this.offsets = offsets;
      this.keyLenArray = keyLenArray;
      this.valLenArray = valLenArray;
      this.kvbuffer = kvbuffer;
    }

    public OffsetSortable(MemoryBlock memBlock, byte[] kvbuffer) {
      this.offsets = memBlock.getOffsets();
      this.keyLenArray = memBlock.getKeyLenArray();
      this.valLenArray = memBlock.getValueLenArray();
      this.kvbuffer = kvbuffer;
    }

    @Override
    public int compare(int i, int j) {
      return LexicographicalComparerHolder.compareBytes(kvbuffer, offsets[i],
          keyLenArray[i], offsets[j], keyLenArray[j]);
    }

    @Override
    public void swap(int i, int j) {
      swapElement(offsets, i, j);
      swapElement(keyLenArray, i, j);
      swapElement(valLenArray, i, j);
    }

    private void swapElement(int[] array, int i, int j) {
      int tmp = array[i];
      array[i] = array[j];
      array[j] = tmp;
    }
  }

  protected MemoryBlockAllocator memoryBlockAllocator;
  protected byte[] kvbuffer;
  protected final TaskReporter reporter;
  protected BlockMapOutputCollector<K, V> collector;

  protected int partition;

  protected int collectedBytesSize;
  protected int collectedRecordsNum;
  
  protected MemoryBlock currentBlock;
  protected List<MemoryBlock> memoryBlocks;


  public BasicReducePartition(int reduceNum,
      MemoryBlockAllocator memoryBlockAllocator, byte[] kvBuffer,
      BlockMapOutputCollector<K, V> collector, TaskReporter reporter)
      throws IOException {
    this.partition = reduceNum;
    this.collectedBytesSize = 0;
    this.collectedRecordsNum = 0;
    this.memoryBlockAllocator = memoryBlockAllocator;
    this.kvbuffer = kvBuffer;
    this.collector = collector;
    this.reporter = reporter;
    this.memoryBlockAllocator.registerMemoryBlockHolder(this);
    initMemoryBlocks();
  }
  
  protected void initMemoryBlocks() {
    memoryBlocks = new ArrayList<MemoryBlock>();
  }

  protected void sortMemBlock(MemoryBlock memBlock) {
    if (memBlock.currentPtr <= 0) {
      return;
    }
    // quick sort the offsets
    OffsetSortable sortableObj = new OffsetSortable(memBlock, kvbuffer);
    QuickSort quickSort = new QuickSort();
    quickSort.sort(sortableObj, 0, memBlock.currentPtr);
  }

  protected void sortIndividualMemoryBlock(List<MemoryBlock> memBlks) {
    if (memBlks == null) {
      return;
    }
    for (MemoryBlock memBlk : memBlks) {
      if (memBlk != null) {
        sortMemBlock(memBlk);
      }
    }
  }

  public int getCollectedRecordsNum() {
    return collectedRecordsNum;
  }

  public int getCollectedBytesSize() {
    return collectedBytesSize;
  }

  abstract void groupOrSort();

  public abstract KeyValueSpillIterator getKeyValueSpillIterator();

  public abstract IndexRecord spill(JobConf job, FSDataOutputStream out,
      Class<K> keyClass, Class<V> valClass, CompressionCodec codec,
      Counter spillCounter) throws IOException;
  
  public abstract int collect(K key, V value) throws IOException;

  @Override
  public MemoryBlock getCurrentOpenMemoryBlock() {
    return currentBlock;
  }

  @Override
  public List<MemoryBlock> getClosedMemoryBlocks() {
    return memoryBlocks;
  }
}

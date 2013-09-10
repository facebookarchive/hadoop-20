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
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.MemoryBlock.KeyValuePairIterator;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.LexicographicalComparerHolder;
import org.apache.hadoop.util.PriorityQueue;

class ReducePartition<K extends BytesWritable, V extends BytesWritable> extends
    BasicReducePartition<K, V> {

  class KeyValueSortedArray extends
      PriorityQueue<KeyValuePairIterator> implements
      KeyValueSpillIterator {
    int totalRecordsNum = 0;
    MemoryBlockIndex memoryBlockIndex;
    boolean first = true;

    public KeyValueSortedArray( List<MemoryBlock> sortedMemBlks,
         int collectedRecordsNum) {
      totalRecordsNum = collectedRecordsNum;
      initHeap(sortedMemBlks);
      memoryBlockIndex = new MemoryBlockIndex();
    }

    public void reset( List<MemoryBlock> sortedMemBlks,
         int collectedRecordsNum) {
      totalRecordsNum = collectedRecordsNum;
      initHeap(sortedMemBlks);
      memoryBlockIndex = new MemoryBlockIndex();
    }

    private void initHeap(List<MemoryBlock> sortedMemBlks) {
      initialize(sortedMemBlks.size());
      clear();
      for (MemoryBlock memBlk : sortedMemBlks) {
        put(memBlk.iterator());
      }
      first = true;
    }

    public MemoryBlockIndex next() {
      if (totalRecordsNum == 0 || size() == 0) {
        return null;
      }

      if (!first) {
        KeyValuePairIterator top = top();
        if (top.hasNext()) {
          top.next();
          adjustTop();
        } else {
          MemoryBlock freeMemBlk = pop().getMemoryBlock();
          putMemoryBlockFree(freeMemBlk);
          if (size() == 0) {
            return null;
          }
        }
      } else {
        first = false;
      }

      KeyValuePairIterator top = top();
      MemoryBlock memBlock = top.getMemoryBlock();
      memoryBlockIndex.setMemoryBlockIndex(memBlock, top
          .getCurrentReadPos());
      return memoryBlockIndex;
    }

    @Override
    protected boolean lessThan(final Object a, final Object b) {
      KeyValuePairIterator kv1 = (KeyValuePairIterator) a;
      KeyValuePairIterator kv2 = (KeyValuePairIterator) b;
      int cmpRet =
          LexicographicalComparerHolder.compareBytes(kvbuffer, kv1
              .getCurrentOffset(), kv1.getCurrentKeyLen(), kv2
              .getCurrentOffset(), kv2.getCurrentKeyLen());
      return cmpRet < 0;
    }

    @Override
    public int getRecordNumber() {
      return totalRecordsNum;
    }
  }

  protected KeyValueSortedArray keyValueSortArray;

  public ReducePartition(int reduceNum,
      MemoryBlockAllocator memoryBlockAllocator, byte[] kvBuffer,
      BlockMapOutputCollector<K, V> collector, TaskReporter reporter)
      throws IOException {
    super(reduceNum, memoryBlockAllocator, kvBuffer, collector, reporter);
  }

  public void putMemoryBlockFree(MemoryBlock memoryBlock) {
    memoryBlockAllocator.freeMemoryBlock(memoryBlock);
  }

  public int collect(K key, V value) throws IOException {
    int keyLen = key.getLength();
    int valLen = value.getLength();

    int recordSize = keyLen + valLen;
    
    if (currentBlock == null || recordSize > currentBlock.left()) {
      MemoryBlock newBlock =
          memoryBlockAllocator.allocateMemoryBlock(recordSize);
      if (newBlock == null) {
        collector.spillSingleRecord(key, value, partition);
        return recordSize;
      }
      if(currentBlock != null) {
        closeCurrentMemoryBlock();
      }
      currentBlock = newBlock;
    }
    currentBlock.collectKV(kvbuffer, key, value);
    collectedRecordsNum = collectedRecordsNum + 1;
    collectedBytesSize = collectedBytesSize + recordSize;
    return recordSize;
  }

  public KeyValueSpillIterator getKeyValueSpillIterator() {
    return keyValueSortArray;
  }

  public IndexRecord spill(JobConf job, FSDataOutputStream out,
      Class<K> keyClass, Class<V> valClass, CompressionCodec codec,
      Counter spillCounter) throws IOException {
    IFile.Writer<K, V> writer = null;
    IndexRecord rec = new IndexRecord();
    long segmentStart = out.getPos();
    try {
      writer = new Writer<K, V>(job, out, keyClass, valClass, codec,
          spillCounter);
      // spill directly
      KeyValueSpillIterator kvSortedArray = this.getKeyValueSpillIterator();
      MemoryBlockIndex memBlkIdx = kvSortedArray.next();
      while (memBlkIdx != null) {
        int pos = memBlkIdx.getIndex();
        MemoryBlock memBlk = memBlkIdx.getMemoryBlock();
        writer.append(kvbuffer, memBlk.offsets[pos], memBlk.keyLenArray[pos],
            memBlk.valueLenArray[pos]);
        memBlkIdx = kvSortedArray.next();
      }
    } finally {
      // close the writer
      if (null != writer)
        writer.close();
    }
    rec.startOffset = segmentStart;
    rec.rawLength = writer.getRawLength();
    rec.partLength = writer.getCompressedLength();
    writer = null;
    return rec;
  }

  public void groupOrSort() {
    reporter.progress();
    List<MemoryBlock> memBlks = snapShot();
    for (int i = 0; i < memBlks.size(); i++) {
      MemoryBlock memBlk = memBlks.get(i);
      sortMemBlock(memBlk);
    }
    // now do a merge sort on the list of memory blocks
    if (keyValueSortArray == null) {
      keyValueSortArray = new KeyValueSortedArray(memBlks,
          getCollectedRecordsNum());
    } else {
      keyValueSortArray.reset(memBlks, getCollectedRecordsNum());
    }
    this.collectedRecordsNum = 0;
    this.collectedBytesSize = 0;
  }

  private List<MemoryBlock> snapShot() {
    closeCurrentMemoryBlock();
    List<MemoryBlock> ret = memoryBlocks;
    initMemoryBlocks();
    return ret;
  }

  private void closeCurrentMemoryBlock() {
    if (currentBlock != null) {
      if (currentBlock.getValid() <= 0) {
        putMemoryBlockFree(currentBlock);
        return;
      }
      currentBlock.finish();
      memoryBlocks.add(currentBlock);
    }
    currentBlock = null;
  }
  
  public class ReducePartitionIFileReader extends IFile.Reader<K, V> {

    private KeyValueSpillIterator keyValueIterator;
    private DataOutputBuffer dataOutputBuffer;

    public ReducePartitionIFileReader() throws IOException {
      super(null, null, getCollectedBytesSize(), null, null);
      keyValueIterator = getKeyValueSpillIterator();
      dataOutputBuffer = new DataOutputBuffer();
    }

    @Override
    public long getPosition() throws IOException {
      // InMemoryReader does not initialize streams like Reader, so in.getPos()
      // would not work. Instead, return the number of uncompressed bytes read,
      // which will be correct since in-memory data is not compressed.
      return bytesRead;
    }

    @Override
    public long getLength() {
      return getCollectedBytesSize() + 8
          * keyValueIterator.getRecordNumber();
    }

    public boolean next(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      MemoryBlockIndex memBlkIdx = keyValueIterator.next();
      if (memBlkIdx != null) {
        int pos = memBlkIdx.getIndex();
        MemoryBlock memBlk = memBlkIdx.getMemoryBlock();
        int offset = memBlk.offsets[pos];
        int keyLen = memBlk.keyLenArray[pos];
        int valLen = memBlk.valueLenArray[pos];
        dataOutputBuffer.reset();
        dataOutputBuffer.writeInt(keyLen);
        dataOutputBuffer.write(kvbuffer, offset, keyLen);
        dataOutputBuffer.writeInt(valLen);
        dataOutputBuffer.write(kvbuffer, offset + keyLen, valLen);
        key.reset(dataOutputBuffer.getData(), 0, keyLen
            + WritableUtils.INT_LENGTH_BYTES);
        value.reset(dataOutputBuffer.getData(), keyLen
            + WritableUtils.INT_LENGTH_BYTES, valLen
            + WritableUtils.INT_LENGTH_BYTES);
        return true;
      }
      return false;
    }

    public void close() {
    }

  }

  public ReducePartitionIFileReader getIReader() throws IOException {
    return new ReducePartitionIFileReader();
  }

}

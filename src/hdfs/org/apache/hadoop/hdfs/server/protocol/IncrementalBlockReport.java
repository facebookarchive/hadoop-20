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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.util.LightWeightBitSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IncrementalBlockReport extends BlockReport implements Writable {

  private long[] delHintsMap;
  private String[] delHints;

  private transient int currentBlock = 0;
  private transient int currentHint = 0;

  public IncrementalBlockReport() {
    currentBlock = 0;
    currentHint = 0;
  }

  public IncrementalBlockReport(Block[] blocks) {

    currentBlock = 0;
    currentHint = 0;

    if (blocks == null || blocks.length == 0) {
      this.delHintsMap = LightWeightBitSet.getBitSet(0);
      this.delHints = new String[0];
      this.blocks = new long[0];
      return;
    }
    this.delHintsMap = LightWeightBitSet.getBitSet(blocks.length);

    ArrayList<String> hints = new ArrayList<String>(0);
 
    for (int i = 0; i < blocks.length; i++) {
      Block b = blocks[i];
      if (b instanceof ReceivedBlockInfo) {
        ReceivedBlockInfo rbi = (ReceivedBlockInfo) b;
        hints.add(rbi.getDelHints());
        LightWeightBitSet.set(delHintsMap, i);
      }
    }
    this.delHints = hints.toArray(new String[hints.size()]);
    this.blocks = BlockListAsLongs.convertToArrayLongs(blocks);
  }

  // should be used to re-read the report
  public void resetIterator() {
    currentBlock = 0;
    currentHint = 0;
  }

  public boolean hasNext() {
    return currentBlock < blocks.length / BlockListAsLongs.LONGS_PER_BLOCK;
  }
  
  public int getLength() {
    return blocks.length / BlockListAsLongs.LONGS_PER_BLOCK;
  }

  public String getNext(Block b) {
    String hint = null;
    BlockListAsLongs.getBlockInfo(b, blocks, currentBlock);
    if (LightWeightBitSet.get(delHintsMap, currentBlock)) {
      hint = delHints[currentHint];
      currentHint++;
    }
    currentBlock++;
    return hint;
  }

  public long[] getHintsMap() {
    return delHintsMap;
  }

  public String[] getHints() {
    return delHints;
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(delHintsMap.length);
    for (long mapEntry : delHintsMap) {
      out.writeLong(mapEntry);
    }
    out.writeInt(delHints.length);
    for (String delHint : delHints) {
      Text.writeStringOpt(out, delHint);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    delHintsMap = new long[in.readInt()];
    for (int i = 0; i < delHintsMap.length; i++) {
      delHintsMap[i] = in.readLong();
    }
    delHints = new String[in.readInt()];
    for (int i = 0; i < delHints.length; i++) {
      delHints[i] = Text.readStringOpt(in);
    }
    currentBlock = 0;
    currentHint = 0;
  }
}

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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.raid.RaidCodec;


/**
 * A RaidTask includes:
 * 1. codec:            the codec used for this stripe.
 * 2. stripeBlocks:     all block information in the stripe.
 * 3. locations:        the locations for *good* blocks and the locations to send the constructed blocks.
 * 4. toRaidIdxs:       the Indexes of the blocks in this stripe to be constructed.
 *
 *
 * RaidTask format:
 *    CodecId
 *    # of blocks in the stripe
 *    for each block:
 *      block
 *      # of replicas
 *      replica machine ip to read from or sent to.
 *    # of blocks to construct
 *    for each block:
 *      index of this block in the stripe.
 */
public class RaidTask implements Writable {
  
  public RaidCodec codec;
  public Block[] stripeBlocks;
  public ArrayList<ArrayList<DatanodeInfo>> locations;
  public int[] toRaidIdxs;
  
  public RaidTask() {}
  
  public RaidTask(RaidCodec codec, Block[] stripeBlocks,
      ArrayList<ArrayList<DatanodeInfo>> locations,
      int[] toRaidIdxs) {
    if (codec == null || stripeBlocks == null || 
        locations == null || toRaidIdxs == null) {
      throw new IllegalArgumentException("Null Arguments");
    }
    
    if (stripeBlocks.length != locations.size()) {
      throw new IllegalArgumentException("All blocks should have locations");
    }
    
    this.codec = codec;
    this.stripeBlocks = stripeBlocks;
    this.locations = locations;
    this.toRaidIdxs = toRaidIdxs;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {
    WritableFactories.setFactory(RaidTask.class, 
        new WritableFactory() {
      @Override
      public Writable newInstance() {
        return new RaidTask();
      }});
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    
    if (this == obj) {
      return true;
    }
    
    RaidTask that = (RaidTask) obj;
    if (codec == null || that.codec == null) {
      return false;
    }
    
    if (stripeBlocks == null || that.stripeBlocks == null) {
      return false;
    }
    
    if (stripeBlocks.length == 0 || that.stripeBlocks.length == 0) {
      return false;
    }
    
    if (stripeBlocks.length != that.stripeBlocks.length) {
      return false;
    }
    
    if (!codec.equals(that.codec)) {
      return false;
    }
    
    return stripeBlocks[0].equals(that.stripeBlocks[0]);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeStringOpt(out, codec.id);
    out.writeInt(stripeBlocks.length);
    // stripe info
    for (int j = 0; j < stripeBlocks.length; j++) {
      stripeBlocks[j].write(out);
      ArrayList<DatanodeInfo> datanodes = locations.get(j);
      out.writeInt(datanodes.size());
      for (DatanodeInfo dn : datanodes) {
        dn.write(out);
      }
    }
    
    // blocks to construct
    out.writeInt(toRaidIdxs.length);
    for (int idx : toRaidIdxs) {
      out.writeInt(idx);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String codecId = Text.readStringOpt(in);
    codec = RaidCodec.getCodec(codecId);
    int numStripeBlocks = in.readInt();
    
    stripeBlocks = new Block[numStripeBlocks];
    locations = new ArrayList<ArrayList<DatanodeInfo>>(numStripeBlocks);
    // read the stripe info
    for (int j = 0; j < numStripeBlocks; j++) {
      stripeBlocks[j] = new Block();
      stripeBlocks[j].readFields(in);
      
      int numDatanodes = in.readInt();
      ArrayList<DatanodeInfo> dns = new ArrayList<DatanodeInfo>(numDatanodes);
      for (int k = 0; k < numDatanodes; k++) {
        DatanodeInfo dn = new DatanodeInfo();
        dn.readFields(in);
        dns.add(dn);
      }
      locations.add(dns);
    }
    
    // read the block indexes to be constructed
    int numRaidIdxs = in.readInt();
    toRaidIdxs = new int[numRaidIdxs];
    for (int j = 0; j < numRaidIdxs; j++) {
      toRaidIdxs[j] = in.readInt();
    }
  }
}

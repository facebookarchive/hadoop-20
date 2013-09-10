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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

// DO NOT remove final without consulting {@link WrapperWritable#create(V object)}
@ThriftStruct
public final class LocatedBlockWithOldGS extends LocatedBlockWithMetaInfo {
  //The last generation stamp of the block
  //This is for the append operation, because we will bump up the 
  //generation stamp.
  private long oldGenerationStamp = GenerationStamp.WILDCARD_STAMP;

  @ThriftField(8)
  public long getOldGenerationStamp() {
    return oldGenerationStamp;
  }
  
  public LocatedBlockWithOldGS() {
  }

  public LocatedBlockWithOldGS(Block b, DatanodeInfo[] locs, long startOffset,
      int dataProtocolVersion, int namespaceid, int methodFingerPrint,
      long oldGenerationStamp) {
    super(b, locs, startOffset, dataProtocolVersion, namespaceid, 
        methodFingerPrint);
    this.oldGenerationStamp = oldGenerationStamp;
  }

  @ThriftConstructor
  public LocatedBlockWithOldGS(@ThriftField(1) Block block,
      @ThriftField(2) List<DatanodeInfo> datanodes, @ThriftField(3) long startOffset,
      @ThriftField(4) boolean corrupt, @ThriftField(5) int dataProtocolVersion,
      @ThriftField(6) int namespaceId, @ThriftField(7) int methodFingerPrint,
      @ThriftField(8) long oldGenerationStamp) {
    super(block, datanodes, startOffset, corrupt, dataProtocolVersion, namespaceId,
        methodFingerPrint);
    this.oldGenerationStamp = oldGenerationStamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(oldGenerationStamp);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    oldGenerationStamp = in.readLong();
  }
}

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Collection of blocks with their locations, the file length, and
 * namenode meta info like data transfer version number and namespace id
 */
// DO NOT remove final without consulting {@link WrapperWritable#create(V object)}
@ThriftStruct
public final class LocatedBlocksWithMetaInfo extends VersionedLocatedBlocks {
  private int namespaceid = -1; // the namespace id that the file belongs to\0
  private int methodFingerPrint;

  LocatedBlocksWithMetaInfo() {
  }

  @ThriftConstructor
  public LocatedBlocksWithMetaInfo(@ThriftField(1) long fileLength,
      @ThriftField(2) List<LocatedBlock> locatedBlocks, @ThriftField(3) boolean isUnderConstuction,
      @ThriftField(4) int dataProtocolVersion, @ThriftField(5) int namespaceId,
      @ThriftField(6) int methodFingerPrint) {
    super(fileLength, locatedBlocks, isUnderConstuction, dataProtocolVersion);
    this.namespaceid = namespaceId;
    this.methodFingerPrint = methodFingerPrint;
  }

  /**
   * Get namespace id
   */
  @ThriftField(5)
  public int getNamespaceID() {
    return this.namespaceid;
  }

  @ThriftField(6)
  public int getMethodFingerPrint() {
    return methodFingerPrint;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlocksWithMetaInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlocksWithMetaInfo(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(this.namespaceid);
    out.writeInt(this.methodFingerPrint);
    super.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.namespaceid = in.readInt();
    this.methodFingerPrint = in.readInt();
    super.readFields(in);
  }
}

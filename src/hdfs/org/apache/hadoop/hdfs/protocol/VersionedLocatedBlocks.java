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
 * data transfer version number.
 */
@ThriftStruct
public class VersionedLocatedBlocks extends LocatedBlocks {
  private int dataProtocolVersion = -1;  // data transfer version number

  VersionedLocatedBlocks() {
  }
  
  @ThriftConstructor
  public VersionedLocatedBlocks(@ThriftField(1) long fileLength,
      @ThriftField(2) List<LocatedBlock> locatedBlocks, @ThriftField(3) boolean isUnderConstuction,
      @ThriftField(4) int dataProtocolVersion) {
    super(fileLength, locatedBlocks, isUnderConstuction);
    this.dataProtocolVersion = dataProtocolVersion;
  }

  /**
   * Get data transfer version number
   */
  @ThriftField(4)
  public int getDataProtocolVersion() {
    return this.dataProtocolVersion;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (VersionedLocatedBlocks.class,
       new WritableFactory() {
         public Writable newInstance() { return new VersionedLocatedBlocks(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(this.dataProtocolVersion);
    super.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.dataProtocolVersion = in.readInt();
    super.readFields(in);
  }
}

/* Licensed to the Apache Software Foundation (ASF) under one
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
import java.util.Arrays;
import java.util.List;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * This class defines a partial listing of a directory to support
 * iterative directory listing.
 */
// DO NOT remove final without consulting {@link WrapperWritable#create(V object)}
@ThriftStruct
public final class LocatedDirectoryListing extends DirectoryListing {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedDirectoryListing.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedDirectoryListing(); }
       });
  }

  private LocatedBlocks[] blockLocations;

  /**
   * default constructor
   */
  public LocatedDirectoryListing() {
  }
  
  /**
   * constructor
   * @param partialListing a partial listing of a directory
   * @param remainingEntries number of entries that are left to be listed
   */
  public LocatedDirectoryListing(HdfsFileStatus[] partialListing, 
      LocatedBlocks[] blockLocations,
      int remainingEntries) {
    super(partialListing, remainingEntries);
    if (blockLocations == null) {
      throw new IllegalArgumentException("block locations should not be null");
    }

    if(blockLocations.length != partialListing.length) {
      throw new IllegalArgumentException(
          "location list and status list do not have the same length");
    }
    
    this.blockLocations = blockLocations;
  }

  @ThriftConstructor
  public LocatedDirectoryListing(@ThriftField(1) List<HdfsFileStatus> fileStatusList,
      @ThriftField(2) List<LocatedBlocks> locatedBlocks, @ThriftField(3) int remainingEntries) {
    this(fileStatusList.toArray(new HdfsFileStatus[fileStatusList.size()]), locatedBlocks.toArray(
        new LocatedBlocks[locatedBlocks.size()]), remainingEntries);
  }

  @ThriftField(1)
  public List<HdfsFileStatus> getFileStatusList() {
    return Arrays.asList(getPartialListing());
  }

  @ThriftField(2)
  public List<LocatedBlocks> getLocatedBlocks() {
    return Arrays.asList(getBlockLocations());
  }

  @ThriftField(3)
  public int getRemainingEntries() {
    return super.getRemainingEntries();
  }

  /**
   * Get the list of block locations
   * @return the lsit of block locations
   */
  public LocatedBlocks[] getBlockLocations() {
    return blockLocations;
  }
  
  // Writable interface
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int numEntries = getPartialListing().length;
    blockLocations = new LocatedBlocks[numEntries];
    for (int i=0; i<numEntries; i++) {
      blockLocations[i] = new LocatedBlocks();
      blockLocations[i].readFields(in);
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    for (LocatedBlocks loc : blockLocations) {
      loc.write(out);
    }
  }
}

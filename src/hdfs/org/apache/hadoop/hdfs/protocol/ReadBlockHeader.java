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

import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.InjectionHandler;

/**
 * The header for the OP_READ_BLOCK datanode operation.
 */
public class ReadBlockHeader extends DataTransferHeader implements Writable {

  private int namespaceId;
  private long blockId;
  private long genStamp;
  private long startOffset;
  private long len;
  private String clientName;
  private boolean reuseConnection = false;
  private boolean shouldProfile = false;
  private ReadOptions options = new ReadOptions();

  public ReadBlockHeader(final VersionAndOpcode versionAndOp) {
    super(versionAndOp);
  }

  public ReadBlockHeader(final int dataTransferVersion,
      final int namespaceId, final long blockId, final long genStamp,
      final long startOffset, final long len, final String clientName,
      final boolean reuseConnection, final boolean shouldProfile) {
    super(dataTransferVersion, DataTransferProtocol.OP_READ_BLOCK);
    set(namespaceId, blockId, genStamp, startOffset, len, clientName,
        reuseConnection, shouldProfile);
  }

  public void set(int namespaceId, long blockId, long genStamp,
      long startOffset, long len, String clientName, boolean reuseConnection,
      boolean shouldProfile) {
    this.namespaceId = namespaceId;
    this.blockId = blockId;
    this.genStamp = genStamp;
    this.startOffset = startOffset;
    this.len = len;
    this.clientName = clientName;
    this.reuseConnection = reuseConnection;
    this.shouldProfile = shouldProfile;
  }

  public void setReadOptions(ReadOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("options cannot be null");
    }
    this.options = options;
  }

  public ReadOptions getReadOptions() {
    return options;
  }

  public int getNamespaceId() {
    return namespaceId;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getGenStamp() {
    return genStamp;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getLen() {
    return len;
  }

  public String getClientName() {
    return clientName;
  }
  
  public boolean getReuseConnection() {
    return reuseConnection;
  }
  
  public boolean getShouldProfile() {
    return shouldProfile;
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////
  public void write(DataOutput out) throws IOException {
    InjectionHandler.processEvent(InjectionEvent.READ_BLOCK_HEAD_BEFORE_WRITE);
    
    if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
      out.writeInt(namespaceId);
    }
    out.writeLong(blockId);
    out.writeLong(genStamp);
    out.writeLong(startOffset);
    out.writeLong(len);
    Text.writeString(out, clientName);
    if (getDataTransferVersion() >= DataTransferProtocol.READ_REUSE_CONNECTION_VERSION) {
      out.writeBoolean(reuseConnection);
    }
    
    if (getDataTransferVersion() >= DataTransferProtocol.READ_PROFILING_VERSION) {
      out.writeBoolean(shouldProfile);
    }
    if (getDataTransferVersion() >= DataTransferProtocol.BITSETOPTIONS_READ_VERSION) {
      options.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    namespaceId = in.readInt();
    blockId = in.readLong();
    genStamp = in.readLong();
    startOffset = in.readLong();
    len = in.readLong();
    clientName = Text.readString(in);
    if (getDataTransferVersion() >= DataTransferProtocol.READ_REUSE_CONNECTION_VERSION) {
      reuseConnection = in.readBoolean();
    }
    
    if (getDataTransferVersion() >= DataTransferProtocol.READ_PROFILING_VERSION) {
      shouldProfile = in.readBoolean();
    }
    if (getDataTransferVersion() >= DataTransferProtocol.BITSETOPTIONS_READ_VERSION) {
      options.readFields(in);
    }
  }
}

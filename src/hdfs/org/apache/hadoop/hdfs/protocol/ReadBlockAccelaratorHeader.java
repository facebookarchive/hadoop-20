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

import org.apache.hadoop.io.Text;

/**
 * The header for the OP_READ_BLOCK_ACCELARATOR datanode operation.
 */
public class ReadBlockAccelaratorHeader extends DataTransferHeader {

  private int namespaceId;
  private long blockId;
  private long genStamp;
  private long startOffset;
  private long len;
  private String clientName;
  
  public ReadBlockAccelaratorHeader(final VersionAndOpcode versionAndOp) {
    super(versionAndOp);
  }
  
  public ReadBlockAccelaratorHeader(final int dataTransferVersion,
      final int namespaceId, final long blockId, final long genStamp,
      final long startOffset, final long len, final String clientName) {
    super(dataTransferVersion, DataTransferProtocol.OP_READ_BLOCK_ACCELERATOR);
    set(namespaceId, blockId, genStamp, startOffset, len, clientName);
  }
  
  public void set(final int namespaceId, final long blockId, final long genStamp,
      final long startOffset, final long len, final String clientName) {
    this.namespaceId = namespaceId;
    this.blockId = blockId;
    this.genStamp = genStamp;
    this.startOffset = startOffset;
    this.len = len;
    this.clientName = clientName;
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
  
  public void write(DataOutput out) throws IOException {
    if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
      out.writeInt(namespaceId);
    }
    out.writeLong(blockId);
    out.writeLong(genStamp);
    out.writeLong(startOffset);
    out.writeLong(len);
    Text.writeString(out, clientName);
  }

  public void readFields(DataInput in) throws IOException {
    namespaceId = in.readInt();
    blockId = in.readLong();
    genStamp = in.readLong();
    startOffset = in.readLong();
    len = in.readLong();
    clientName = Text.readString(in);
  }
  
}

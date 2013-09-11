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
import org.apache.hadoop.io.Writable;

public class AppendBlockHeader extends DataTransferHeader implements Writable {
  
  private int namespaceId;
  private long blockId;
  private long numBytes;
  private long genStamp;
  private int pipelineDepth;
  
  final private WritePipelineInfo writePipelineInfo = new WritePipelineInfo();

  
  public AppendBlockHeader(final VersionAndOpcode versionAndOp) {
    super(versionAndOp);
  }
  
  public AppendBlockHeader(final int dataTransferVersion,
      final int namespaceId, final long blockId, final long numBytes,
      final long genStamp,
      final int pipelineDepth,
      final boolean hasSrcDataNode, final DatanodeInfo srcDataNode,
      final int numTargets, final DatanodeInfo[] nodes, final String clientName) {
    super(dataTransferVersion, DataTransferProtocol.OP_APPEND_BLOCK);
    set(namespaceId, blockId, numBytes, genStamp, pipelineDepth, 
        hasSrcDataNode, srcDataNode, numTargets, nodes, clientName);
  }

  public void set(int namespaceId, long blockId, long numBytes, long genStamp,
      final int pipelineDepth, boolean hasSrcDataNode,
      DatanodeInfo srcDataNode, int numTargets, DatanodeInfo[] nodes,
      String clientName) {
    this.namespaceId = namespaceId;
    this.blockId = blockId;
    this.numBytes = numBytes;
    this.genStamp = genStamp;
    this.pipelineDepth = pipelineDepth;
    writePipelineInfo.set(hasSrcDataNode, srcDataNode, numTargets, nodes, clientName);
  }
  
  public WritePipelineInfo getWritePipelineInfo() {
    return writePipelineInfo;
  }

  public int getNamespaceId() {
    return namespaceId;
  }

  public long getBlockId() {
    return blockId;
  }
  
  public long getNumBytes() {
    return numBytes;
  }

  public long getGenStamp() {
    return genStamp;
  }

  public int getPipelineDepth() {
    return pipelineDepth;
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////
  public void write(DataOutput out) throws IOException {
    if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
      out.writeInt(namespaceId);
    }
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(genStamp);
    out.writeInt(pipelineDepth);
    getWritePipelineInfo().write(getDataTransferVersion(), out);
  }

  public void readFields(DataInput in) throws IOException {
    namespaceId = in.readInt();
    blockId = in.readLong();
    numBytes = in.readLong();
    genStamp = in.readLong();
    pipelineDepth = in.readInt(); // num of datanodes in entire pipeline
    getWritePipelineInfo().readFields(getDataTransferVersion(), in);
  }
}

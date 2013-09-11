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

import java.io.*;

import org.apache.hadoop.io.*;


/**
 * The header for the OP_WRITE_BLOCK datanode operation.
 */
public class WriteBlockHeader extends DataTransferHeader implements Writable {

  private int namespaceId;
  private long blockId;
  private long genStamp;
  private int pipelineDepth;
  private boolean recoveryFlag;

  final private WritePipelineInfo writePipelineInfo = new WritePipelineInfo();

  public WriteBlockHeader(final VersionAndOpcode versionAndOp) {
    super(versionAndOp);
  }

  public WriteBlockHeader(final int dataTransferVersion,
      final int namespaceId, final long blockId, final long genStamp,
      final int pipelineDepth, final boolean recoveryFlag,
      final boolean hasSrcDataNode, final DatanodeInfo srcDataNode,
      final int numTargets, final DatanodeInfo[] nodes,
      final String clientName) {
    super(dataTransferVersion, DataTransferProtocol.OP_WRITE_BLOCK);
    set(namespaceId, blockId, genStamp, pipelineDepth, recoveryFlag,
        hasSrcDataNode, srcDataNode, numTargets, nodes, clientName);
  }

  public void set(int namespaceId, long blockId, long genStamp,
      final int pipelineDepth, boolean recoveryFlag, boolean hasSrcDataNode,
      DatanodeInfo srcDataNode, int numTargets, DatanodeInfo[] nodes,
      String clientName) {
    this.namespaceId = namespaceId;
    this.blockId = blockId;
    this.genStamp = genStamp;
    this.pipelineDepth = pipelineDepth;
    this.recoveryFlag = recoveryFlag;

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

  public long getGenStamp() {
    return genStamp;
  }

  public int getPipelineDepth() {
    return pipelineDepth;
  }

  public boolean isRecoveryFlag() {
    return recoveryFlag;
  }


  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////
  public void write(DataOutput out) throws IOException {
    if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
      out.writeInt(namespaceId);
    }
    out.writeLong(blockId);
    out.writeLong(genStamp);
    out.writeInt(pipelineDepth);
    out.writeBoolean(recoveryFlag); // recovery flag
    getWritePipelineInfo().write(getDataTransferVersion(), out);
  }

  public void readFields(DataInput in) throws IOException {
    namespaceId = in.readInt();
    blockId = in.readLong();
    genStamp = in.readLong();
    pipelineDepth = in.readInt(); // num of datanodes in entire pipeline
    recoveryFlag = in.readBoolean(); // is this part of recovery?
    getWritePipelineInfo().readFields(getDataTransferVersion(), in);
  }
}

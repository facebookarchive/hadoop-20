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
import org.apache.hadoop.util.DataChecksum;

/**
 * The header for the OP_WRITE_BLOCK datanode operation.
 */
public class WriteBlockHeader extends DataTransferHeader implements Writable {

  private int namespaceId;
  private long blockId;
  private long genStamp;
  private int pipelineDepth;
  private boolean recoveryFlag;
  private boolean hasSrcDataNode;
  private DatanodeInfo srcDataNode = null;
  private int numTargets;
  private DatanodeInfo[] nodes;
  private String clientName;

  public WriteBlockHeader(final VersionAndOpcode versionAndOp) {
    super(versionAndOp);
  }

  public WriteBlockHeader(final int dataTransferVersion,
      final int namespaceId, final long blockId, final long genStamp,
      final int pipelineDepth, final boolean recoveryFlag,
      final boolean hasSrcDataNode, final DatanodeInfo srcDataNode,
      final int numTargets, final DatanodeInfo[] nodes, final String clientName) {
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
    this.hasSrcDataNode = hasSrcDataNode;
    this.srcDataNode = srcDataNode;
    this.numTargets = numTargets;
    this.nodes = nodes;
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

  public int getPipelineDepth() {
    return pipelineDepth;
  }

  public boolean isRecoveryFlag() {
    return recoveryFlag;
  }

  public boolean isHasSrcDataNode() {
    return hasSrcDataNode;
  }

  public DatanodeInfo getSrcDataNode() {
    return srcDataNode;
  }

  public int getNumTargets() {
    return numTargets;
  }

  public DatanodeInfo[] getNodes() {
    return nodes;
  }

  public String getClientName() {
    return clientName;
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
    Text.writeString(out, clientName);
    out.writeBoolean(hasSrcDataNode); // Not sending src node information
    if (hasSrcDataNode) { // pass src node information
      srcDataNode.write(out);
    }
    out.writeInt(numTargets);
    for (int i = 1; i <= numTargets; i++) {
      nodes[i].write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    namespaceId = in.readInt();
    blockId = in.readLong();
    genStamp = in.readLong();
    pipelineDepth = in.readInt(); // num of datanodes in entire pipeline
    recoveryFlag = in.readBoolean(); // is this part of recovery?
    clientName = Text.readString(in); // working on behalf of this client
    hasSrcDataNode = in.readBoolean(); // is src node info present
    if (hasSrcDataNode) {
      srcDataNode = new DatanodeInfo();
      srcDataNode.readFields(in);
    }
    numTargets = in.readInt();
    if (numTargets < 0) {
      throw new IOException("Mislabelled incoming datastream.");
    }
    nodes = new DatanodeInfo[numTargets];
    for (int i = 0; i < nodes.length; i++) {
      DatanodeInfo tmp = new DatanodeInfo();
      tmp.readFields(in);
      nodes[i] = tmp;
    }
  }
}

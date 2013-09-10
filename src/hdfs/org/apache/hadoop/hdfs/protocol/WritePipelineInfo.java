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
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * Class to send and receive write pipeline information, used by write block and
 * append block
 */
public class WritePipelineInfo {

  private boolean hasSrcDataNode;
  private DatanodeInfo srcDataNode = null;
  private int numTargets;
  private DatanodeInfo[] nodes;
  private String clientName;
  private WriteOptions options = new WriteOptions();

  public void set(boolean hasSrcDataNode, DatanodeInfo srcDataNode,
      int numTargets, DatanodeInfo[] nodes, String clientName) {
    this.hasSrcDataNode = hasSrcDataNode;
    this.srcDataNode = srcDataNode;
    this.numTargets = numTargets;
    this.nodes = nodes;
    this.clientName = clientName;
  }

  public boolean hasSrcDataNode() {
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

  public void setWriteOptions(WriteOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("options cannot be null");
    }
    this.options = options;
  }

  public WriteOptions getWriteOptions() {
    return options;
  }

  
  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////
  public void write(final int dataTransferVersion, DataOutput out)
      throws IOException {
    Text.writeString(out, clientName);
    out.writeBoolean(hasSrcDataNode); // Not sending src node information
    if (hasSrcDataNode) { // pass src node information
      srcDataNode.write(out);
    }
    out.writeInt(numTargets);
    for (int i = 1; i <= numTargets; i++) {
      nodes[i].write(out);
    }
    if (dataTransferVersion >= DataTransferProtocol.BITSETOPTIONS_VERSION) {
      options.write(out);
    }
  }

  public void readFields(final int dataTransferVersion, DataInput in)
      throws IOException {
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
    if (dataTransferVersion >= DataTransferProtocol.BITSETOPTIONS_VERSION) {
      options.readFields(in);
    }
  }
}

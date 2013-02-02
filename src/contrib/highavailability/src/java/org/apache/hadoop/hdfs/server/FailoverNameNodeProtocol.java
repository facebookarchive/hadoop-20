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

package org.apache.hadoop.hdfs.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.FailoverClientHandler;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;

/**
 * This class implements the NamenodeProtocol such that it can work correctly
 * across AvatarNode failovers. Currently we support this protocol only for
 * the use case of the Balancer. Rest of the API's are left for implementation
 * in future when we actually need them.
 */
public class FailoverNameNodeProtocol implements NamenodeProtocol {

  private NamenodeProtocol namenode;
  private final FailoverClientHandler failoverHandler;
  private final Log LOG = LogFactory.getLog(FailoverNameNodeProtocol.class);

  public FailoverNameNodeProtocol(NamenodeProtocol namenode,
      FailoverClientHandler failoverHandler) {
    this.namenode = namenode;
    this.failoverHandler = failoverHandler;
  }

  public NamenodeProtocol getNameNode() {
    return this.namenode;
  }

  public void setNameNode(NamenodeProtocol namenode) {
    this.namenode = namenode;
  }

  @Override
  public long getProtocolVersion(final String protocol, final long clientVersion)
      throws VersionIncompatible, IOException {
    return (failoverHandler.new ImmutableFSCaller<Long>() {

      @Override
      public Long call() throws IOException {
        return namenode.getProtocolVersion(protocol, clientVersion);
      }

    }).callFS();
  }

  @Override
  public ProtocolSignature getProtocolSignature(final String protocol,
      final long clientVersion, final int clientMethodsHash) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<ProtocolSignature>() {

      @Override
      public ProtocolSignature call() throws IOException {
        return namenode.getProtocolSignature(protocol, clientVersion,
            clientMethodsHash);
      }

    }).callFS();
  }

  @Override
  public BlocksWithLocations getBlocks(final DatanodeInfo datanode,
      final long size) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<BlocksWithLocations>() {
      @Override
      public BlocksWithLocations call() throws IOException {
        return namenode.getBlocks(datanode, size);
      }
    }).callFS();
  }

  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    throw new IOException("Operation not supported");
  }

  @Override
  public void rollFsImage(CheckpointSignature newImageSignature)
      throws IOException {
    throw new IOException("Operation not supported");
  }

  @Override
  public long[] getBlockLengths(final long[] blockIds) {
    throw new RuntimeException("Operation not supported");
  }

  @Override
  public CheckpointSignature getCheckpointSignature() {
    throw new RuntimeException("Operation not supported");
  }

  @Override
  public LocatedBlocksWithMetaInfo updateDatanodeInfo(
      LocatedBlocks locatedBlocks) throws IOException {
    throw new IOException("Operation not supported");
  }

  @Override
  public long getTransactionID() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long l) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int register() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
}

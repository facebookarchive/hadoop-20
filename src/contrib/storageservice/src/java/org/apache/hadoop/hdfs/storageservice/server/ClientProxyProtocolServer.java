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
package org.apache.hadoop.hdfs.storageservice.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProxyProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.hdfs.storageservice.StorageServiceConfigKeys;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.Closeable;
import java.io.IOException;

/** This class maintains RPC server for Hadoop's clients and serves as entry point for requests. */
final class ClientProxyProtocolServer implements ClientProxyProtocol, Closeable {
  private final ClientProxyService proxyService;
  private final Server server;

  public ClientProxyProtocolServer(ClientProxyCommons commons,
      ClientProxyService proxyService) throws IOException {
    this.proxyService = proxyService;
    Configuration conf = getServerConfig(commons.conf);
    server = RPC.getServer(this, "0.0.0.0", conf.getInt(StorageServiceConfigKeys.PROXY_RPC_PORT_KEY,
        StorageServiceConfigKeys.PROXY_RPC_PORT_DEFAULT), 10, false, conf, false);
    server.start();
  }

  private static Configuration getServerConfig(Configuration conf) {
    Configuration confCopy = new Configuration(conf);
    confCopy.setBoolean("ipc.server.tcpnodelay", true);
    confCopy.setBoolean("ipc.direct.handling", true);
    return confCopy;
  }

  @Override
  public void close() throws IOException {
    server.stop(true);
  }

  public int getPort() {
    return server.getListenerAddress().getPort();
  }

  ////////////////////////////////////////
  // VersionedProtocol
  ////////////////////////////////////////

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return ClientProxyProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
  }

  ////////////////////////////////////////
  //  ClientProxyProtocol
  ////////////////////////////////////////

  @Override
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public OpenResponse open(OpenRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void create(CreateRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public AppendResponse append(AppendRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void recoverLease(RecoverLeaseRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public boolean setReplication(SetReplicationRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void setPermission(SetPermissionRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public void setOwner(SetOwnerRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public void abandonBlock(AbandonBlockRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public void abandonFile(AbandonFileRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public AddBlockResponse addBlock(AddBlockRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public boolean complete(CompleteRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void reportBadBlocks(ReportBadBlocksRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public boolean hardLink(HardLinkRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public boolean rename(RenameRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void concat(ConcatRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public boolean delete(DeleteRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public boolean mkdirs(MkdirsRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void renewLease(RenewLeaseRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public StatsResponse getStats(GetStatsRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public void fsync(FSyncRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public void setTimes(SetTimesRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public void updatePipeline(UpdatePipelineRequest req) throws IOException {
    proxyService.handleRequest(req);
  }

  @Override
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public boolean raidFile(RaidFileRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }

  @Override
  public PingResponse ping(PingRequest req) throws IOException {
    return proxyService.handleRequest(req);
  }
}

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

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.hdfs.protocol.TClientProxyProtocol;
import org.apache.hadoop.hdfs.storageservice.StorageServiceConfigKeys;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.Arrays;

/** Server for Thrift clients */
final class TClientProxyProtocolServer implements TClientProxyProtocol {
  private static final Log LOG = LogFactory.getLog(TClientProxyProtocolServer.class);
  private final ClientProxyService proxyService;
  private final ThriftServer server;

  public TClientProxyProtocolServer(ClientProxyCommons commons, ClientProxyService proxyService) {
    this.proxyService = proxyService;
    ThriftCodecManager codecManager = new ThriftCodecManager();
    ThriftEventHandler eventHandler = new ThriftEventHandler();
    ThriftServiceProcessor processor = new ThriftServiceProcessor(codecManager, Arrays.asList(
        eventHandler), this);
    server = new ThriftServer(processor, getServerConfig(commons.conf)).start();
  }

  @Override
  public void close() {
    IOUtils.cleanup(LOG, server);
  }

  private RemoteException translateException(IOException e) {
    // TODO: exceptions translation
    return new RemoteException(e);
  }

  private <T> T unifiedHandler(Request<T> request) throws RemoteException {
    try {
      return proxyService.handleRequest(request);
    } catch (IOException e) {
      throw translateException(e);
    }
  }

  /** Translates Hadoop's Configuration into Thrift's server config */
  private static ThriftServerConfig getServerConfig(Configuration conf) {
    ThriftServerConfig serverConfig = new ThriftServerConfig();
    serverConfig.setPort(conf.getInt(StorageServiceConfigKeys.PROXY_THRIFT_PORT_KEY,
        StorageServiceConfigKeys.PROXY_THRIFT_PORT_DEFAULT));
    return serverConfig;
  }

  public int getPort() {
    return server.getPort();
  }

  ////////////////////////////////////////
  //  TClientProxyProtocol
  ////////////////////////////////////////

  @Override
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public OpenResponse open(OpenRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void create(CreateRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public AppendResponse append(AppendRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void recoverLease(RecoverLeaseRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public boolean setReplication(SetReplicationRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void setPermission(SetPermissionRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public void setOwner(SetOwnerRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public void abandonBlock(AbandonBlockRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public void abandonFile(AbandonFileRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public AddBlockResponse addBlock(AddBlockRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public boolean complete(CompleteRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void reportBadBlocks(ReportBadBlocksRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public boolean hardLink(HardLinkRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public boolean rename(RenameRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void concat(ConcatRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public boolean delete(DeleteRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public boolean mkdirs(MkdirsRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void renewLease(RenewLeaseRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public StatsResponse getStats(GetStatsRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public void fsync(FSyncRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public void setTimes(SetTimesRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public void updatePipeline(UpdatePipelineRequest req) throws RemoteException {
    unifiedHandler(req);
  }

  @Override
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public boolean raidFile(RaidFileRequest req) throws RemoteException {
    return unifiedHandler(req);
  }

  @Override
  public PingResponse ping(PingRequest req) throws RemoteException {
    return unifiedHandler(req);
  }
}

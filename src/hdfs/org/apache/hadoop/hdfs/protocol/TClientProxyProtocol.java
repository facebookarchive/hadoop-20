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

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.ipc.RemoteException;

import java.io.Closeable;

@ThriftService
public interface TClientProxyProtocol extends Closeable {

  ///////////////////////////////////////
  // File contents operations
  ///////////////////////////////////////

  /** {@link ClientProtocol#getBlockLocations(String, long, long)} */
  @ThriftMethod
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#openAndFetchMetaInfo(String, long, long)} */
  @ThriftMethod
  public OpenResponse open(OpenRequest req) throws RemoteException;

  /** {@link ClientProtocol#create(String, FsPermission, String, boolean, boolean, short, long)} */
  @ThriftMethod
  public void create(CreateRequest req) throws RemoteException;

  /** {@link ClientProtocol#appendAndFetchOldGS(String, String)} */
  @ThriftMethod
  public AppendResponse append(AppendRequest req) throws RemoteException;

  /** {@link ClientProtocol#recoverLease(String, String)} */
  @ThriftMethod
  public void recoverLease(RecoverLeaseRequest req) throws RemoteException;

  /** {@link ClientProtocol#closeRecoverLease(String, String, boolean)} */
  @ThriftMethod
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws RemoteException;

  /** {@link ClientProtocol#setReplication(String, short)} */
  @ThriftMethod
  public boolean setReplication(SetReplicationRequest req) throws RemoteException;

  /** {@link ClientProtocol#setPermission(String, FsPermission)} */
  @ThriftMethod
  public void setPermission(SetPermissionRequest req) throws RemoteException;

  /** {@link ClientProtocol#setOwner(String, String, String)} */
  @ThriftMethod
  public void setOwner(SetOwnerRequest req) throws RemoteException;

  /** {@link ClientProtocol#abandonBlock(Block, String, String)} */
  @ThriftMethod
  public void abandonBlock(AbandonBlockRequest req) throws RemoteException;

  /** {@link ClientProtocol#abandonFile(String, String)} */
  @ThriftMethod
  public void abandonFile(AbandonFileRequest req) throws RemoteException;

  /**
   * {@link ClientProtocol#addBlockAndFetchMetaInfo(String, String, DatanodeInfo[], DatanodeInfo[],
   * long, Block)}
   */
  @ThriftMethod
  public AddBlockResponse addBlock(AddBlockRequest req) throws RemoteException;

  /** {@link ClientProtocol#complete(String, String, long, Block)} */
  @ThriftMethod
  public boolean complete(CompleteRequest req) throws RemoteException;

  /** {@link ClientProtocol#reportBadBlocks(LocatedBlock[])} */
  @ThriftMethod
  public void reportBadBlocks(ReportBadBlocksRequest req) throws RemoteException;

  ///////////////////////////////////////
  // Namespace management
  ///////////////////////////////////////

  /** {@link ClientProtocol#hardLink(String, String)} */
  @ThriftMethod
  public boolean hardLink(HardLinkRequest req) throws RemoteException;

  /** {@link ClientProtocol#getHardLinkedFiles(String)} */
  @ThriftMethod
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#rename(String, String)} */
  @ThriftMethod
  public boolean rename(RenameRequest req) throws RemoteException;

  /** {@link ClientProtocol#concat(String, String[], boolean)} */
  @ThriftMethod
  public void concat(ConcatRequest req) throws RemoteException;

  /** {@link ClientProtocol#delete(String, boolean)} */
  @ThriftMethod
  public boolean delete(DeleteRequest req) throws RemoteException;

  /** {@link ClientProtocol#mkdirs(String, FsPermission)} */
  @ThriftMethod
  public boolean mkdirs(MkdirsRequest req) throws RemoteException;

  /** {@link ClientProtocol#iterativeGetOpenFiles(String, int, String)} */
  @ThriftMethod
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws RemoteException;

  /** {@link ClientProtocol#getPartialListing(String, byte[])} */
  @ThriftMethod
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#getLocatedPartialListing(String, byte[])} */
  @ThriftMethod
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws RemoteException;

  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /** {@link ClientProtocol#renewLease(String)} */
  @ThriftMethod
  public void renewLease(RenewLeaseRequest req) throws RemoteException;

  /** {@link ClientProtocol#getStats()} */
  @ThriftMethod
  public StatsResponse getStats(GetStatsRequest req) throws RemoteException;

  /** {@link ClientProtocol#getPreferredBlockSize(String)} */
  @ThriftMethod
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws RemoteException;

  /** {@link ClientProtocol#listCorruptFileBlocks(String, String)} */
  @ThriftMethod
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#getHdfsFileInfo(String)} */
  @ThriftMethod
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws RemoteException;

  /** {@link ClientProtocol#getContentSummary(String)} */
  @ThriftMethod
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#fsync(String, String)} */
  @ThriftMethod
  public void fsync(FSyncRequest req) throws RemoteException;

  /** {@link ClientProtocol#setTimes(String, long, long)} */
  @ThriftMethod
  public void setTimes(SetTimesRequest req) throws RemoteException;

  /** {@link ClientProtocol#updatePipeline(String, Block, Block, DatanodeID[])} */
  @ThriftMethod
  public void updatePipeline(UpdatePipelineRequest req) throws RemoteException;

  /** {@link ClientProtocol#getDataTransferProtocolVersion()} */
  @ThriftMethod
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      RemoteException;

  /** {@link ClientProtocol#getBlockInfo(long)} */
  @ThriftMethod
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws RemoteException;

  /** {@link ClientProtocol#raidFile(String, String, short)} */
  @ThriftMethod
  public boolean raidFile(RaidFileRequest req) throws RemoteException;

  /** Latency measurement call */
  @ThriftMethod
  public PingResponse ping(PingRequest req) throws RemoteException;
}

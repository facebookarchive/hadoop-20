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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface ClientProxyProtocol extends VersionedProtocol {
  public static final long versionID = 1L;

  ///////////////////////////////////////
  // File contents operations
  ///////////////////////////////////////

  /** {@link ClientProtocol#getBlockLocations(String, long, long)} */
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws IOException;

  /** {@link ClientProtocol#openAndFetchMetaInfo(String, long, long)} */
  public OpenResponse open(OpenRequest req) throws IOException;

  /** {@link ClientProtocol#create(String, FsPermission, String, boolean, boolean, short, long)} */
  public void create(CreateRequest req) throws IOException;

  /** {@link ClientProtocol#appendAndFetchOldGS(String, String)} */
  public AppendResponse append(AppendRequest req) throws IOException;

  /** {@link ClientProtocol#recoverLease(String, String)} */
  public void recoverLease(RecoverLeaseRequest req) throws IOException;

  /** {@link ClientProtocol#closeRecoverLease(String, String, boolean)} */
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws IOException;

  /** {@link ClientProtocol#setReplication(String, short)} */
  public boolean setReplication(SetReplicationRequest req) throws IOException;

  /** {@link ClientProtocol#setPermission(String, FsPermission)} */
  public void setPermission(SetPermissionRequest req) throws IOException;

  /** {@link ClientProtocol#setOwner(String, String, String)} */
  public void setOwner(SetOwnerRequest req) throws IOException;

  /** {@link ClientProtocol#abandonBlock(Block, String, String)} */
  public void abandonBlock(AbandonBlockRequest req) throws IOException;

  /** {@link ClientProtocol#abandonFile(String, String)} */
  public void abandonFile(AbandonFileRequest req) throws IOException;

  /**
   * {@link ClientProtocol#addBlockAndFetchMetaInfo(String, String, DatanodeInfo[], DatanodeInfo[],
   * long, Block)}
   */
  public AddBlockResponse addBlock(AddBlockRequest req) throws IOException;

  /** {@link ClientProtocol#complete(String, String, long, Block)} */
  public boolean complete(CompleteRequest req) throws IOException;

  /** {@link ClientProtocol#reportBadBlocks(LocatedBlock[])} */
  public void reportBadBlocks(ReportBadBlocksRequest req) throws IOException;

  ///////////////////////////////////////
  // Namespace management
  ///////////////////////////////////////

  /** {@link ClientProtocol#hardLink(String, String)} */
  public boolean hardLink(HardLinkRequest req) throws IOException;

  /** {@link ClientProtocol#getHardLinkedFiles(String)} */
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      IOException;

  /** {@link ClientProtocol#rename(String, String)} */
  public boolean rename(RenameRequest req) throws IOException;

  /** {@link ClientProtocol#concat(String, String[], boolean)} */
  public void concat(ConcatRequest req) throws IOException;

  /** {@link ClientProtocol#delete(String, boolean)} */
  public boolean delete(DeleteRequest req) throws IOException;

  /** {@link ClientProtocol#mkdirs(String, FsPermission)} */
  public boolean mkdirs(MkdirsRequest req) throws IOException;

  /** {@link ClientProtocol#iterativeGetOpenFiles(String, int, String)} */
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws IOException;

  /** {@link ClientProtocol#getPartialListing(String, byte[])} */
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws IOException;

  /** {@link ClientProtocol#getLocatedPartialListing(String, byte[])} */
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws IOException;

  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /** {@link ClientProtocol#renewLease(String)} */
  public void renewLease(RenewLeaseRequest req) throws IOException;

  /** {@link ClientProtocol#getStats()} */
  public StatsResponse getStats(GetStatsRequest req) throws IOException;

  /** {@link ClientProtocol#getPreferredBlockSize(String)} */
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws IOException;

  /** {@link ClientProtocol#listCorruptFileBlocks(String, String)} */
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      IOException;

  /** {@link ClientProtocol#getHdfsFileInfo(String)} */
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws IOException;

  /** {@link ClientProtocol#getContentSummary(String)} */
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws IOException;

  /** {@link ClientProtocol#fsync(String, String)} */
  public void fsync(FSyncRequest req) throws IOException;

  /** {@link ClientProtocol#setTimes(String, long, long)} */
  public void setTimes(SetTimesRequest req) throws IOException;

  /** {@link ClientProtocol#updatePipeline(String, Block, Block, DatanodeID[])} */
  public void updatePipeline(UpdatePipelineRequest req) throws IOException;

  /** {@link ClientProtocol#getDataTransferProtocolVersion()} */
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      IOException;

  /** {@link ClientProtocol#getBlockInfo(long)} */
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws IOException;

  /** {@link ClientProtocol#raidFile(String, String, short)} */
  public boolean raidFile(RaidFileRequest req) throws IOException;

  /** Latency measurement call */
  public PingResponse ping(PingRequest req) throws IOException;
}

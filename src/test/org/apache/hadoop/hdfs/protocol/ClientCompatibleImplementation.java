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

import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.namenode.NameNode.safeAsList;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.safeToArray;

/**
 * Provides default implementation of {@link ClientProxyProtocol} calls for an object which
 * implements {@link ClientProtocol}.
 */
public abstract class ClientCompatibleImplementation
    implements ClientProxyProtocol, ClientProtocol {
  @Override
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws IOException {
    return new BlockLocationsResponse(getBlockLocations(req.getSrc(), req.getOffset(),
        req.getLength()));
  }

  @Override
  public OpenResponse open(OpenRequest req) throws IOException {
    return new OpenResponse(openAndFetchMetaInfo(req.getSrc(), req.getOffset(), req.getLength()));
  }

  @Override
  public void create(CreateRequest req) throws IOException {
    create(req.getSrc(), req.getMasked(), req.getClientName(), req.isOverwrite(),
        req.isCreateParent(), req.getReplication(), req.getBlockSize());
  }

  @Override
  public AppendResponse append(AppendRequest req) throws IOException {
    return new AppendResponse(appendAndFetchOldGS(req.getSrc(), req.getClientName()));
  }

  @Override
  public void recoverLease(RecoverLeaseRequest req) throws IOException {
    recoverLease(req.getSrc(), req.getClientName());
  }

  @Override
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws IOException {
    return closeRecoverLease(req.getSrc(), req.getClientName(), req.isDiscardLastBlock());
  }

  @Override
  public boolean setReplication(SetReplicationRequest req) throws IOException {
    return setReplication(req.getSrc(), req.getReplication());
  }

  @Override
  public void setPermission(SetPermissionRequest req) throws IOException {
    setPermission(req.getSrc(), req.getPermission());
  }

  @Override
  public void setOwner(SetOwnerRequest req) throws IOException {
    setOwner(req.getSrc(), req.getUsername(), req.getGroupname());
  }

  @Override
  public void abandonBlock(AbandonBlockRequest req) throws IOException {
    abandonBlock(req.getBlock(), req.getSrc(), req.getClientName());
  }

  @Override
  public void abandonFile(AbandonFileRequest req) throws IOException {
    abandonFile(req.getSrc(), req.getClientName());
  }

  @Override
  public AddBlockResponse addBlock(AddBlockRequest req) throws IOException {
    return new AddBlockResponse(addBlockAndFetchMetaInfo(req.getSrc(), req.getClientName(),
        safeToArray(req.getExcludedNodes(), new DatanodeInfo[0]), safeToArray(req.getFavoredNodes(),
        new DatanodeInfo[0]), req.getStartPos(), req.getLastBlock()));
  }

  @Override
  public boolean complete(CompleteRequest req) throws IOException {
    return complete(req.getSrc(), req.getClientName(), req.getFileLen(), req.getLastBlock());
  }

  @Override
  public void reportBadBlocks(ReportBadBlocksRequest req) throws IOException {
    reportBadBlocks(safeToArray(req.getBlocks(), new LocatedBlock[0]));
  }

  @Override
  public boolean hardLink(HardLinkRequest req) throws IOException {
    return hardLink(req.getSrc(), req.getDst());
  }

  @Override
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      IOException {
    return new HardLinkedFilesResponse(safeAsList(getHardLinkedFiles(req.getSrc())));
  }

  @Override
  public boolean rename(RenameRequest req) throws IOException {
    return rename(req.getSrc(), req.getDst());
  }

  @Override
  public void concat(ConcatRequest req) throws IOException {
    concat(req.getTrg(), safeToArray(req.getSrcs(), new String[0]), req.isRestricted());
  }

  @Override
  public boolean delete(DeleteRequest req) throws IOException {
    return delete(req.getSrc(), req.isRecursive());
  }

  @Override
  public boolean mkdirs(MkdirsRequest req) throws IOException {
    return mkdirs(req.getSrc(), req.getMasked());
  }

  @Override
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws IOException {
    return new IterativeGetOpenFilesResponse(safeAsList(iterativeGetOpenFiles(req.getSrc(),
        req.getMillis(), req.getStart())));
  }

  @Override
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws IOException {
    return new PartialListingResponse(getPartialListing(req.getSrc(), req.getStartAfter()));
  }

  @Override
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws IOException {
    return new LocatedPartialListingResponse(getLocatedPartialListing(req.getSrc(),
        req.getStartAfter()));
  }

  @Override
  public void renewLease(RenewLeaseRequest req) throws IOException {
    renewLease(req.getClientName());
  }

  @Override
  public StatsResponse getStats(GetStatsRequest req) throws IOException {
    return new StatsResponse(getStats());
  }

  @Override
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws IOException {
    return getPreferredBlockSize(req.getSrc());
  }

  @Override
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      IOException {
    return new CorruptFileBlocksResponse(listCorruptFileBlocks(req.getSrc(), req.getCookie()));
  }

  @Override
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws IOException {
    return new FileInfoResponse(getHdfsFileInfo(req.getSrc()));
  }

  @Override
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws IOException {
    return new ContentSummaryResponse(getContentSummary(req.getSrc()));
  }

  @Override
  public void fsync(FSyncRequest req) throws IOException {
    fsync(req.getSrc(), req.getClientName());
  }

  @Override
  public void setTimes(SetTimesRequest req) throws IOException {
    setTimes(req.getSrc(), req.getMtime(), req.getAtime());
  }

  @Override
  public void updatePipeline(UpdatePipelineRequest req) throws IOException {
    updatePipeline(req.getClientName(), req.getOldBlock(), req.getNewBlock(), safeToArray(
        req.getNewNodes(), new DatanodeID[0]));
  }

  @Override
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      IOException {
    return getDataTransferProtocolVersion();
  }

  @Override
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws IOException {
    return new BlockInfoResponse(getBlockInfo(req.getBlockId()));
  }

  @Override
  public boolean raidFile(RaidFileRequest req) throws IOException {
    return raidFile(req.getSrc(), req.getCodecId(), req.getExpectedSourceReplication());
  }
}

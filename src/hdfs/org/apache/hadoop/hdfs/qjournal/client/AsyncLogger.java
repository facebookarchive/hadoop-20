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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface for a remote log which is only communicated with asynchronously.
 * This is essentially a wrapper around {@link QJournalProtocol} with the key
 * differences being:
 * 
 * <ul>
 * <li>All methods return {@link ListenableFuture}s instead of synchronous
 * objects.</li>
 * <li>The {@link RequestInfo} objects are created by the underlying
 * implementation.</li>
 * </ul>
 */
public interface AsyncLogger {
  
  interface Factory {
    AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr);
  }

  /**
   * Send a batch of edits to the logger.
   * @param segmentTxId the first txid in the current segment
   * @param firstTxnId the first txid of the edits.
   * @param numTxns the number of transactions in the batch
   * @param data the actual data to be sent
   */
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data);

  /**
   * Begin writing a new log segment.
   * 
   * @param txid the first txid to be written to the new log
   */
  public ListenableFuture<Void> startLogSegment(long txid);

  /**
   * Finalize a log segment.
   * 
   * @param startTxId the first txid that was written to the segment
   * @param endTxId the last txid that was written to the segment
   */
  public ListenableFuture<Void> finalizeLogSegment(
      long startTxId, long endTxId);

  /**
   * Allow the remote node to purge edit logs earlier than this.
   * @param minTxIdToKeep the min txid which must be retained
   */
  public ListenableFuture<Void> purgeLogsOlderThan(long minTxIdToKeep);

  /**
   * Transition the image directory.
   */
  public ListenableFuture<Void> transitionImage(NamespaceInfo nsInfo,
      Transition transition, StartupOption option);

  /**
   * Transition the journal directory.
   */
  public ListenableFuture<Void> transitionJournal(NamespaceInfo nsInfo,
      Transition transition, StartupOption option);

  /**
   * @return whether or not the remote node has any valid journal data.
   */
  public ListenableFuture<Boolean> isJournalFormatted();
  
  /**
   * @return the state of the last epoch on the target node.
   */
  public ListenableFuture<GetJournalStateResponseProto> getJournalState();

  /**
   * Begin a new epoch on the target node.
   */
  public ListenableFuture<NewEpochResponseProto> newEpoch(long epoch);
  
  /**
   * Fetch the list of edit logs available on the remote node.
   */
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId);

  /**
   * Prepare recovery. The writer sends an RPC to each JN asking to prepare
   * recovery for the given segment. Each JN responds with information about the
   * current state of the segment from its local disk, including its length and
   * its finalization status (finalized or in-progress). If this state
   * corresponds to a previously-accepted recovery proposal, then the JN also
   * includes the epoch number of the writer which proposed it.
   */
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      long segmentTxId);

  /**
   * Accept a recovery proposal. Based on the responses to the {\tt
   * PrepareRecovery} call, the recovering writer designates one of the segments
   * (and its respective logger) as the master for recovery. This segment is
   * picked such that it must contain all previously committed transactions, and
   * such that, if any previous recovery process succeeded, the same decision is
   * reached.
   */
  public ListenableFuture<Void> acceptRecovery(SegmentStateProto log,
      String fromUrl);

  /**
   * Set the epoch number used for all future calls.
   */
  public void setEpoch(long e);

  /**
   * Let the logger know the highest committed txid across all loggers in the
   * set. This txid may be higher than the last committed txid for <em>this</em>
   * logger. See HDFS-3863 for details.
   */
  public void setCommittedTxId(long txid, boolean force);

  /**
   * Build an HTTP URL to fetch the log segment with the given startTxId.
   */
  public URL buildURLToFetchLogs(long segmentTxId, long position);
  
  /**
   * Build an HTTP URL to fetch the image with given txid.
   */
  public URL buildURLToFetchImage(long txid);
  
  /**
   * Tear down any resources, connections, etc. The proxy may not be used
   * after this point, and any in-flight RPCs may throw an exception.
   */
  public void close();

  /**
   * Append an HTML-formatted report for this logger's status to the provided
   * StringBuilder. This is displayed on the NN web UI.
   */
  public void appendHtmlReport(StringBuilder sb);
  
  /**
   * Get HTML version of toString()
   * @return
   */
  public String toHTMLString();
  
  /**
   * Analyze journal storage
   */
  public ListenableFuture<GetStorageStateProto> analyzeJournalStorage();
  
  /**
   * Roll the image.
   */
  public ListenableFuture<Void> saveDigestAndRenameCheckpointImage(long txid, MD5Hash digest);

  /**
   * Fetch the list of edit logs available on the remote node.
   */
  ListenableFuture<RemoteImageManifest> getImageManifest(long fromTxnId);
  
  /**
   * Analyze image storage, return storage info and state.
   */
  public ListenableFuture<GetStorageStateProto> analyzeImageStorage();
  
  /**
   * @return whether or not the remote node has any valid image data.
   */
  public ListenableFuture<Boolean> isImageFormatted();
}

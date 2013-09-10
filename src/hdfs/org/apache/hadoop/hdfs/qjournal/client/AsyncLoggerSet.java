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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.jasper.compiler.JspUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Wrapper around a set of Loggers, taking care of fanning out
 * calls to the underlying loggers and constructing corresponding
 * {@link QuorumCall} instances.
 */
class AsyncLoggerSet {
  static final Log LOG = LogFactory.getLog(AsyncLoggerSet.class);

  private final List<AsyncLogger> loggers;
  
  private static final long INVALID_EPOCH = -1;
  private long myEpoch = INVALID_EPOCH;
  
  public AsyncLoggerSet(List<AsyncLogger> loggers) {
    this.loggers = ImmutableList.copyOf(loggers);
  }
  
  /**
   * Set the epoch number used for all future calls.
   */
  void setEpoch(long e) {
    Preconditions.checkState(!isEpochEstablished(),
        "Epoch already established: epoch=%s", myEpoch);
    myEpoch = e;
    for (AsyncLogger l : loggers) {
      l.setEpoch(e);
    }
  }

  /**
   * Set the highest successfully committed txid seen by the writer.
   * This should be called after a successful write to a quorum, and is used
   * for extra sanity checks against the protocol. See HDFS-3863.
   */
  public void setCommittedTxId(long txid, boolean force) {
    for (AsyncLogger logger : loggers) {
      logger.setCommittedTxId(txid, force);
    }
  }

  /**
   * @return true if an epoch has been established.
   */
  boolean isEpochEstablished() {
    return myEpoch != INVALID_EPOCH;
  }
  
  /**
   * @return the epoch number for this writer. This may only be called after
   * a successful call to {@link #createNewUniqueEpoch(NamespaceInfo)}.
   */
  long getEpoch() {
    Preconditions.checkState(isEpochEstablished(),
        "No epoch created yet");
    return myEpoch;
  }

  /**
   * Close all of the underlying loggers.
   */
  void close() {
    for (AsyncLogger logger : loggers) {
      logger.close();
    }
  }
  
  void purgeLogsOlderThan(long minTxIdToKeep) {
    for (AsyncLogger logger : loggers) {
      logger.purgeLogsOlderThan(minTxIdToKeep);
    }
  }

  /**
   * Wait for a quorum of loggers to respond to the given call. If a quorum
   * can't be achieved, throws a QuorumException.
   * @param q the quorum call
   * @param timeoutMs the number of millis to wait
   * @param operationName textual description of the operation, for logging
   * @return a map of successful results
   * @throws QuorumException if a quorum doesn't respond with success
   * @throws IOException if the thread is interrupted or times out
   */
  <V> Map<AsyncLogger, V> waitForWriteQuorum(QuorumCall<AsyncLogger, V> q,
      int timeoutMs, String operationName) throws IOException {
    int majority = getMajoritySize();
    int numLoggers = loggers.size();
    checkMajoritySize(majority, numLoggers);
    return waitForQuorumInternal(q, loggers.size(), majority, numLoggers
        - majority + 1, majority, timeoutMs, operationName);
  }
  
  /**
   * Wait for a all loggers to respond to the given call. 
   * 
   * This is useful for operations like obtaining manifests, etc.
   * Fail if there is a majority of exceptions, or majority of successes cannot 
   * be achieved. Even if majority of successes is achieved, the call
   * waits for all responses.
   * 
   * @param q the quorum call
   * @param timeoutMs the number of millis to wait
   * @param operationName textual description of the operation, for logging
   * @return a map of successful results
   * @throws QuorumException if a quorum doesn't respond with success
   * @throws IOException if the thread is interrupted or times out
   */
  <V> Map<AsyncLogger, V> waitForReadQuorumWithAllResponses(
      QuorumCall<AsyncLogger, V> q, int timeoutMs, String operationName)
      throws IOException {
    int majority = getMajoritySize();
    int numLoggers = loggers.size();
    checkMajoritySize(majority, numLoggers);
    // we do not stop waiting even if we get majority
    return waitForQuorumInternal(q, loggers.size(), -1, majority, numLoggers
        - majority + 1, timeoutMs, operationName);
  }
  
  private void checkMajoritySize(int majority, int numLoggers)
      throws IOException {
    if (majority > numLoggers) {
      throw new IOException("Waiting for majority " + majority + " of "
          + numLoggers + " loggers, which is impossible.");
    }
  }
  
  private <V> Map<AsyncLogger, V> waitForQuorumInternal(
      QuorumCall<AsyncLogger, V> q, int minResponses, int minSuccesses,
      int maxExceptions, int majority, int timeoutMs, String operationName)
      throws IOException {
    try {
      q.waitFor(
          minResponses, // either all respond 
          minSuccesses, // or we get a majority successes
          maxExceptions, // or we get a majority failures,
          timeoutMs, operationName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting " + timeoutMs + "ms for a " +
          "quorum of nodes to respond.");
    } catch (TimeoutException e) {
      if (q.countSuccesses() < majority) {
        // for waitForReadQuorumWithAllResponses, we do not want to fail here
        throw new IOException("Timed out waiting " + timeoutMs
            + "ms for a quorum of nodes to respond. countResponses: "
            + q.countResponses() + " countSuccess: " + q.countSuccesses()
            + " countException: " + q.countExceptions());
      }
    }
    
    // for calls where we want all responses, we need to re-check here if
    // the number of successes is at least the majority size, since
    // minSuccesses=-1
    if (q.countSuccesses() < majority) {
      q.throwQuorumException("Got too many exceptions to achieve quorum size " +
          getMajorityString());
    }    
    return q.getResults();
  }
  
  /**
   * @return the number of nodes which are required to obtain a quorum.
   */
  int getMajoritySize() {
    return loggers.size() / 2 + 1;
  }
  
  /**
   * @return a textual description of the majority size (eg "2/3" or "3/5")
   */
  String getMajorityString() {
    return getMajoritySize() + "/" + loggers.size();
  }

  /**
   * @return the number of loggers behind this set
   */
  int size() {
    return loggers.size();
  }
  
  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(loggers) + "]";
  }
  
  public String toHTMLString() {
    String html = "[";
    for (AsyncLogger logger : loggers) {
      html += logger.toHTMLString() + ",";
    }
    html = html.substring(0, html.length() - 1) + "]";
    return html;
  }

  /**
   * Append an HTML-formatted status readout on the current
   * state of the underlying loggers.
   * @param sb the StringBuilder to append to
   */
  void appendHtmlReport(StringBuilder sb) {
    sb.append("<table class=\"storage\">");
    sb.append("<thead><tr><td>JN</td><td>Status</td></tr></thead>\n");
    for (AsyncLogger l : loggers) {
      sb.append("<tr>");
      sb.append("<td>" + JspUtil.escapeXml(l.toString()) + "</td>");
      sb.append("<td>");
      l.appendHtmlReport(sb);
      sb.append("</td></tr>\n");
    }
    sb.append("</table>");
  }

  /**
   * @return the (mutable) list of loggers, for use in tests to
   * set up spies
   */
  @VisibleForTesting
  List<AsyncLogger> getLoggersForTests() {
    return loggers;
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // The rest of this file is simply boilerplate wrappers which fan-out the
  // various IPC calls to the underlying AsyncLoggers and wrap the result
  // in a QuorumCall.
  ///////////////////////////////////////////////////////////////////////////
  
  public QuorumCall<AsyncLogger, GetJournalStateResponseProto> getJournalState() {
    Map<AsyncLogger, ListenableFuture<GetJournalStateResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.getJournalState());
    }
    return QuorumCall.create(calls);    
  }
  
  public QuorumCall<AsyncLogger, Boolean> isJournalFormatted() {
    Map<AsyncLogger, ListenableFuture<Boolean>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.isJournalFormatted());
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger,NewEpochResponseProto> newEpoch(
      NamespaceInfo nsInfo,
      long epoch) {
    Map<AsyncLogger, ListenableFuture<NewEpochResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.newEpoch(epoch));
    }
    return QuorumCall.create(calls);    
  }

  public QuorumCall<AsyncLogger, Void> startLogSegment(
      long txid) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.startLogSegment(txid));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> finalizeLogSegment(long firstTxId,
      long lastTxId) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.finalizeLogSegment(firstTxId, lastTxId));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> sendEdits(
      long segmentTxId, long firstTxnId, int numTxns, byte[] data) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = 
        logger.sendEdits(segmentTxId, firstTxnId, numTxns, data);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, RemoteEditLogManifest>
      getEditLogManifest(long fromTxnId) {
    Map<AsyncLogger,
        ListenableFuture<RemoteEditLogManifest>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<RemoteEditLogManifest> future =
          logger.getEditLogManifest(fromTxnId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger, PrepareRecoveryResponseProto>
      prepareRecovery(long segmentTxId) {
    Map<AsyncLogger,
      ListenableFuture<PrepareRecoveryResponseProto>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<PrepareRecoveryResponseProto> future =
          logger.prepareRecovery(segmentTxId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger,Void>
      acceptRecovery(SegmentStateProto log, String fromURL) {
    Map<AsyncLogger, ListenableFuture<Void>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.acceptRecovery(log, fromURL);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger, GetStorageStateProto> analyzeJournalStorage() {
    Map<AsyncLogger, ListenableFuture<GetStorageStateProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<GetStorageStateProto> future =
          logger.analyzeJournalStorage();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  
  QuorumCall<AsyncLogger, Void> transitionImage(NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = logger.transitionImage(nsInfo,
          transition, startOpt);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger, Void> transitionJournal(NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = logger.transitionJournal(nsInfo,
          transition,
          startOpt);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, RemoteImageManifest> getImageManifest(
      long fromTxnId) {
    Map<AsyncLogger, ListenableFuture<RemoteImageManifest>> calls = Maps
        .newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<RemoteImageManifest> future = logger
          .getImageManifest(fromTxnId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  
  URLImageInputStream getImageInputStream(long txid, int httpTimeout) {
    URLImageInputStream stream = null;
    for (AsyncLogger logger : loggers) {
      try {
        stream = new URLImageInputStream(logger, txid, httpTimeout);
        break;
      } catch (IOException e) {
        LOG.warn("Could not obtain image stream for logger: " + logger
            + " for txid : " + txid, e);
      }
    }
    return stream;
  }
  
  public QuorumCall<AsyncLogger, Void> saveDigestAndRenameCheckpointImage(
      long txid, MD5Hash digest) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.saveDigestAndRenameCheckpointImage(txid, digest));
    }
    return QuorumCall.create(calls);
  }
  
  QuorumCall<AsyncLogger, GetStorageStateProto> analyzeImageStorage() {
    Map<AsyncLogger, ListenableFuture<GetStorageStateProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<GetStorageStateProto> future =
          logger.analyzeImageStorage();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Boolean> isImageFormatted() {
    Map<AsyncLogger, ListenableFuture<Boolean>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.isImageFormatted());
    }
    return QuorumCall.create(calls);
  }
}

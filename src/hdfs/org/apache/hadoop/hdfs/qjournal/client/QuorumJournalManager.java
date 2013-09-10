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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.ImageInputStream;
import org.apache.hadoop.hdfs.server.namenode.ImageManager;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.namenode.RemoteStorageState;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImage;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A JournalManager that writes to a set of remote JournalNodes,
 * requiring a quorum of nodes to ack each write.
 */
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager, ImageManager {
  public static final Log LOG = LogFactory.getLog(QuorumJournalManager.class);

  // Timeouts for which the QJM will wait for each of the following actions.
  private final int startSegmentTimeoutMs;
  private final int prepareRecoveryTimeoutMs;
  private final int acceptRecoveryTimeoutMs;
  private final int finalizeSegmentTimeoutMs;
  private final int selectInputStreamsTimeoutMs;
  private final int getImageManifestTimeoutMs;
  private final int getJournalStateTimeoutMs;
  private final int newEpochTimeoutMs;
  private final int writeTxnsTimeoutMs;
  private final int httpConnectReadTimeoutMs;
  
  private final int imageUploadBufferSize;
  private final int imageUploadMaxBufferedChunks;

  // Since these don't occur during normal operation, we can
  // use rather lengthy timeouts 
  private final int dirTransitionTimeoutMs;     // 10 mins by default
  private final int hasDataTimeoutMs;    // 10 mins by default
  
  public static final Object QJM_URI_SCHEME = "qjm";
  
  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private boolean isActiveWriter;
  
  private final String journalId;
  private final AsyncLoggerSet loggers;
  
  private final NameNodeMetrics metrics;
  
  private final boolean hasImageStorage;
  private volatile boolean imageDisabled = false;
  
  private final List<String> httpAddresses;
  
  public QuorumJournalManager(Configuration conf, URI uri,
      NamespaceInfo nsInfo, NameNodeMetrics metrics, boolean hasImageStroage)
      throws IOException {
    this(conf, uri, nsInfo, IPCLoggerChannel.FACTORY, metrics, hasImageStroage);
  }

  public QuorumJournalManager(Configuration conf, URI uri, NamespaceInfo nsInfo,
      AsyncLogger.Factory loggerFactory, NameNodeMetrics metrics,
      boolean hasImageStorage) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.loggers = new AsyncLoggerSet(createLoggers(loggerFactory));
    this.metrics = metrics;
    this.journalId = parseJournalId(uri);

    // Configure timeouts.
    this.startSegmentTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT);
    this.prepareRecoveryTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT);
    this.acceptRecoveryTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT);
    this.finalizeSegmentTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT);
    this.selectInputStreamsTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT);
    this.getImageManifestTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_GET_IMAGE_MANIFEST_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_GET_IMAGE_MANIFEST_TIMEOUT_DEFAULT);
    this.getJournalStateTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT);
    this.newEpochTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT);
    this.writeTxnsTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT);
    this.httpConnectReadTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_DEFAULT);
    this.dirTransitionTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_FORMAT_TIMEOUT_KEY, 
        JournalConfigKeys.DFS_QJOURNAL_FORMAT_TIMEOUT_DEFAULT);
    this.hasDataTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_HAS_DATA_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_HAS_DATA_TIMEOUT_DEFAULT);
    
    this.hasImageStorage = hasImageStorage;
    if (hasImageStorage) {
      LOG.info("QJM Journal: " + uri + " will store image.");
    }
    this.imageUploadBufferSize = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_IMAGE_BUFFER_SIZE_KEY,
        JournalConfigKeys.DFS_QJOURNAL_IMAGE_BUFFER_SIZE_DEFAULT);
    this.imageUploadMaxBufferedChunks = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_IMAGE_MAX_BUFFERED_CHUNKS_KEY,
        JournalConfigKeys.DFS_QJOURNAL_IMAGE_MAX_BUFFERED_CHUNKS_DEFAULT);
    this.httpAddresses = getHttpAddresses();
  }
  
  protected List<AsyncLogger> createLoggers(
      AsyncLogger.Factory factory) throws IOException {
    return createLoggers(conf, uri, nsInfo, factory);
  }

  static String parseJournalId(URI uri) {
    String path = uri.getPath();
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "Bad URI '%s': must identify journal in path component",
        uri);
    String journalId = path.substring(1);
    checkJournalId(journalId);
    return journalId;
  }
  
  public static void checkJournalId(String jid) {
    if (jid == null || jid.isEmpty() || jid.startsWith(".")) {
      throw new IllegalArgumentException("bad journal id: " + jid);
    }
  }

  
  /**
   * Fence any previous writers, and obtain a unique epoch number
   * for write-access to the journal nodes.
   *
   * @return the new, unique epoch number
   */
  Map<AsyncLogger, NewEpochResponseProto> createNewUniqueEpoch()
      throws IOException {
    Preconditions.checkState(!loggers.isEpochEstablished(),
        "epoch already created");
    
    Map<AsyncLogger, GetJournalStateResponseProto> lastPromises =
      loggers.waitForWriteQuorum(loggers.getJournalState(),
          getJournalStateTimeoutMs, "getJournalState()");
    
    long maxPromised = Long.MIN_VALUE;
    for (GetJournalStateResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    
    long myEpoch = maxPromised + 1;
    Map<AsyncLogger, NewEpochResponseProto> resps =
        loggers.waitForWriteQuorum(loggers.newEpoch(nsInfo, myEpoch),
            newEpochTimeoutMs, "newEpoch(" + myEpoch + ")");
        
    loggers.setEpoch(myEpoch);
    return resps;
  }
  
  public void updateNamespaceInfo(StorageInfo si) {  
    // update nsInfo
    nsInfo.layoutVersion = si.layoutVersion;
    nsInfo.namespaceID = si.namespaceID;
    nsInfo.cTime = si.cTime;
  }
  
  private void invokeDirTransition(QuorumCall<AsyncLogger, Void> call,
      String methodName) throws IOException {
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, dirTransitionTimeoutMs,
          methodName);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for" + methodName
          + "() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for " + methodName
          + "() response");
    }

    if (call.countExceptions() > 0) {
      call.throwQuorumException("Could not upgrade one or more JournalNodes");
    }
  }

  @Override
  public void transitionJournal(StorageInfo si, Transition transition,
      StartupOption startOpt) throws IOException {
    if (Transition.UPGRADE == transition
        || Transition.COMPLETE_UPGRADE == transition
        || Transition.ROLLBACK == transition
        || Transition.FORMAT == transition) {
      updateNamespaceInfo(si);
    }
    invokeDirTransition(loggers.transitionJournal(nsInfo, transition, startOpt),
        transition.toString() + " journal");
  }

  @Override
  public boolean hasSomeJournalData() throws IOException {
    return hasSomeDataInternal(false);
  }
  
  @Override
  public boolean hasSomeImageData() throws IOException {
    return hasSomeDataInternal(true);
  }
  
  /**
   * Checks if any data is available in the underlying storage.
   * Returns true if any of the nodes has some data.
   */
  private boolean hasSomeDataInternal(boolean image) throws IOException {
    QuorumCall<AsyncLogger, Boolean> call = image ? loggers.isImageFormatted() :
        loggers.isJournalFormatted();

    try {
      call.waitFor(loggers.size(), 0, 0, hasDataTimeoutMs, "hasSomeData");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while determining if JNs have data");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for response from loggers");
    }
    
    if (call.countExceptions() > 0) {
      call.throwQuorumException(
          "Unable to check if JNs are ready for formatting");
    }
    
    // If any of the loggers returned with a non-empty manifest, then
    // we should prompt for format.
    for (Boolean hasData : call.getResults().values()) {
      if (hasData) {
        return true;
      }
    }

    // Otherwise, none were formatted, we can safely format.
    return false;
  }

  /**
   * Run recovery/synchronization for a specific segment.
   * Postconditions:
   * <ul>
   * <li>This segment will be finalized on a majority
   * of nodes.</li>
   * <li>All nodes which contain the finalized segment will
   * agree on the length.</li>
   * </ul>
   * 
   * @param segmentTxId the starting txid of the segment
   * @throws IOException
   */
  private void recoverUnclosedSegment(long segmentTxId) throws IOException {
    Preconditions.checkArgument(segmentTxId > -1);
    LOG.info("Beginning recovery of unclosed segment starting at txid " +
        segmentTxId);
    
    // Step 1. Prepare recovery
    QuorumCall<AsyncLogger,PrepareRecoveryResponseProto> prepare =
        loggers.prepareRecovery(segmentTxId);
    Map<AsyncLogger, PrepareRecoveryResponseProto> prepareResponses=
        loggers.waitForWriteQuorum(prepare, prepareRecoveryTimeoutMs,
            "prepareRecovery(" + segmentTxId + ")");
    LOG.info("Recovery prepare phase complete. Responses:\n" +
        QuorumCall.mapToString(prepareResponses));

    // Determine the logger who either:
    // a) Has already accepted a previous proposal that's higher than any
    //    other
    //
    //  OR, if no such logger exists:
    //
    // b) Has the longest log starting at this transaction ID
    
    // TODO: we should collect any "ties" and pass the URL for all of them
    // when syncing, so we can tolerate failure during recovery better.
    Entry<AsyncLogger, PrepareRecoveryResponseProto> bestEntry = Collections.max(
        prepareResponses.entrySet(), SegmentRecoveryComparator.INSTANCE); 
    AsyncLogger bestLogger = bestEntry.getKey();
    PrepareRecoveryResponseProto bestResponse = bestEntry.getValue();
    
    // Log the above decision, check invariants.
    if (bestResponse.hasAcceptedInEpoch()) {
      LOG.info("Using already-accepted recovery for segment " +
          "starting at txid " + segmentTxId + ": " +
          bestEntry);
    } else if (bestResponse.hasSegmentState()) {
      LOG.info("Using longest log: " + bestEntry);
    } else {
      // None of the responses to prepareRecovery() had a segment at the given
      // txid. This can happen for example in the following situation:
      // - 3 JNs: JN1, JN2, JN3
      // - writer starts segment 101 on JN1, then crashes before
      //   writing to JN2 and JN3
      // - during newEpoch(), we saw the segment on JN1 and decide to
      //   recover segment 101
      // - before prepare(), JN1 crashes, and we only talk to JN2 and JN3,
      //   neither of which has any entry for this log.
      // In this case, it is allowed to do nothing for recovery, since the
      // segment wasn't started on a quorum of nodes.

      // Sanity check: we should only get here if none of the responses had
      // a log. This should be a postcondition of the recovery comparator,
      // but a bug in the comparator might cause us to get here.
      for (PrepareRecoveryResponseProto resp : prepareResponses.values()) {
        assert !resp.hasSegmentState() :
          "One of the loggers had a response, but no best logger " +
          "was found.";
      }

      LOG.info("None of the responders had a log to recover: " +
          QuorumCall.mapToString(prepareResponses));
      return;
    }
    
    SegmentStateProto logToSync = bestResponse.getSegmentState();
    assert segmentTxId == logToSync.getStartTxId();
    
    // Sanity check: none of the loggers should be aware of a higher
    // txid than the txid we intend to truncate to
    for (Map.Entry<AsyncLogger, PrepareRecoveryResponseProto> e :
         prepareResponses.entrySet()) {
      AsyncLogger logger = e.getKey();
      PrepareRecoveryResponseProto resp = e.getValue();

      if (resp.hasLastCommittedTxId() &&
          resp.getLastCommittedTxId() > logToSync.getEndTxId()) {
        throw new AssertionError("Decided to synchronize log to " + logToSync +
            " but logger " + logger + " had seen txid " +
            resp.getLastCommittedTxId() + " committed");
      }
    }
    
    URL syncFromUrl = bestLogger.buildURLToFetchLogs(segmentTxId, 0);
    
    QuorumCall<AsyncLogger,Void> accept = loggers.acceptRecovery(logToSync, syncFromUrl.toString());
    loggers.waitForWriteQuorum(accept, acceptRecoveryTimeoutMs,
        "acceptRecovery(" + logToSync + ")");

    // If one of the loggers above missed the synchronization step above, but
    // we send a finalize() here, that's OK. It validates the log before
    // finalizing. Hence, even if it is not "in sync", it won't incorrectly
    // finalize.
    QuorumCall<AsyncLogger, Void> finalize =
        loggers.finalizeLogSegment(logToSync.getStartTxId(), logToSync.getEndTxId()); 
    loggers.waitForWriteQuorum(finalize, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)",
            logToSync.getStartTxId(),
            logToSync.getEndTxId()));
  }
  
  static List<AsyncLogger> createLoggers(Configuration conf,
      URI uri, NamespaceInfo nsInfo, AsyncLogger.Factory factory)
          throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = getLoggerAddresses(uri);
    String jid = parseJournalId(uri);
    for (InetSocketAddress addr : addrs) {
      ret.add(factory.createLogger(conf, nsInfo, jid, addr));
    }
    return ret;
  }
 
  private static List<InetSocketAddress> getLoggerAddresses(URI uri)
      throws IOException {
    String[] parts = parseAddresses(uri);    
    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : parts) {
      addrs.add(NetUtils.createSocketAddr(
          addr, JournalConfigKeys.DFS_JOURNALNODE_RPC_PORT_DEFAULT));
    }
    return addrs;
  }
  
  private static String[] parseAddresses(URI uri) {
    String authority = uri.getAuthority();
    
    Preconditions.checkArgument(authority != null && !authority.isEmpty(),
        "URI has no authority: " + uri);
    
    String[] parts = StringUtils.split(authority, ';');
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }

    if (parts.length % 2 == 0) {
      LOG.warn("Quorum journal URI '" + uri + "' has an even number " +
          "of Journal Nodes specified. This is not recommended!");
    }
    return parts;
  }
  
  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
    QuorumCall<AsyncLogger,Void> q = loggers.startLogSegment(txId);
    loggers.waitForWriteQuorum(q, startSegmentTimeoutMs,
        "startLogSegment(" + txId + ")");
    return new QuorumOutputStream(loggers, txId, FSEditLog.sizeFlushBuffer,
        writeTxnsTimeoutMs, metrics, journalId);
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    QuorumCall<AsyncLogger,Void> q = loggers.finalizeLogSegment(
        firstTxId, lastTxId);
    loggers.waitForWriteQuorum(q, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)", firstTxId, lastTxId));
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // This purges asynchronously -- there's no need to wait for a quorum
    // here, because it's always OK to fail.
    LOG.info("Purging remote journals older than txid " + minTxIdToKeep);
    loggers.purgeLogsOlderThan(minTxIdToKeep);
  }
  
  @Override
  public void setCommittedTxId(long txid, boolean force) {
    LOG.info("Set committed transaction ID " + txid + " force=" + force);
    loggers.setCommittedTxId(txid, force);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    Preconditions.checkState(!isActiveWriter, "already active writer");
    
    LOG.info("Starting recovery process for unclosed journal segments...");
    Map<AsyncLogger, NewEpochResponseProto> resps = createNewUniqueEpoch();
    LOG.info("Successfully started new epoch " + loggers.getEpoch());

    if (LOG.isDebugEnabled()) {
      LOG.debug("newEpoch(" + loggers.getEpoch() + ") responses:\n" +
        QuorumCall.mapToString(resps));
    }
    
    long mostRecentSegmentTxId = Long.MIN_VALUE;
    for (NewEpochResponseProto r : resps.values()) {
      if (r.hasLastSegmentTxId()) {
        mostRecentSegmentTxId = Math.max(mostRecentSegmentTxId,
            r.getLastSegmentTxId());
      }
    }
    
    // On a completely fresh system, none of the journals have any
    // segments, so there's nothing to recover.
    if (mostRecentSegmentTxId != Long.MIN_VALUE) {
      recoverUnclosedSegment(mostRecentSegmentTxId);
    }
    isActiveWriter = true;
  }

  @Override
  public void close() throws IOException {
    loggers.close();
  }
  
  /**
   * Select input streams.
   * inProgressOk should be true only for tailing, not for startup
   */
  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk, boolean validateInProgressSegments)
      throws IOException {

    QuorumCall<AsyncLogger, RemoteEditLogManifest> q =
        loggers.getEditLogManifest(fromTxnId);
    // we insist on getting all responses, even if they are to be exceptions
    // this will fail if we cannot get majority of successes
    Map<AsyncLogger, RemoteEditLogManifest> resps = loggers
        .waitForReadQuorumWithAllResponses(q, selectInputStreamsTimeoutMs,
            "selectInputStreams");
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("selectInputStream manifests:\n" +
          Joiner.on("\n").withKeyValueSeparator(": ").join(resps));
    }
    
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (Map.Entry<AsyncLogger, RemoteEditLogManifest> e : resps.entrySet()) {
      AsyncLogger logger = e.getKey();
      RemoteEditLogManifest manifest = e.getValue();
      
      for (RemoteEditLog remoteLog : manifest.getLogs()) {
        EditLogInputStream elis = new URLLogInputStream(logger,
            remoteLog.getStartTxId(), httpConnectReadTimeoutMs);
        if (elis.isInProgress() && !inProgressOk) {
          continue;
        }
        allStreams.add(elis);
      }
    }
    // we pass 0 as min redundance as we do not care about this here
    JournalSet.chainAndMakeRedundantStreams(
        streams, allStreams, fromTxnId, inProgressOk, 0);
  }
  
  @Override
  public String toString() {
    return "QJM to " + loggers;
  }
  
  @Override
  public String toHTMLString() {
    return "QJM to " + loggers.toHTMLString();
  }

  @VisibleForTesting
  AsyncLoggerSet getLoggerSetForTests() {
    return loggers;
  }
  
  @Override
  public RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    throw new IOException("Not supported");
  }
  
  /**
   * Translates byte[] journal id into String. This will be done only the first
   * time we are accessing a journal.
   */
  public static String journalIdBytesToString(byte[] jid) {
    char[] charArray = new char[jid.length];
    for (int i = 0; i < jid.length; i++) {
      charArray[i] = (char) jid[i];
    }
    return new String(charArray, 0, charArray.length);
  }
  
  /**
   * Translates String journal id into byte[].
   * We assume the id is ascii.
   */
  public static byte[] journalIdStringToBytes(String jid) {
    byte[] byteArray = new byte[jid.length()];
    for (int i = 0; i < jid.length(); i++) {
      byteArray[i] = (byte) jid.charAt(i);
    }
    return byteArray;
  }

  @Override
  public boolean hasImageStorage() {
    return hasImageStorage;
  }
  
  /**
   * Consistent with JournalNode.getJournalHttpAddress
   */
  private List<String> getHttpAddresses() {
    
    String[] hosts = JournalConfigHelper.getJournalHttpHosts(conf);
    String pref = "http://";
    
    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = pref + hosts[i];
    }    
    return Arrays.asList(hosts);
  }

  // Imagemanager methods.

  @Override
  public void transitionImage(StorageInfo si, Transition transition,
      StartupOption startOpt) throws IOException {
    if (Transition.UPGRADE == transition
        || Transition.COMPLETE_UPGRADE == transition
        || Transition.ROLLBACK == transition
        || Transition.FORMAT == transition) {
      updateNamespaceInfo(si);
    }
    invokeDirTransition(loggers.transitionImage(nsInfo, transition, startOpt),
        transition.toString() + " image");
  }
  
  /**
   * Creates output stream for image at txid to the underlying quorum of journal
   * nodes.
   */
  @Override
  public OutputStream getCheckpointOutputStream(long txid) throws IOException {
    return new HttpImageUploadStream(httpAddresses, journalId, nsInfo, txid,
        loggers.getEpoch(), imageUploadBufferSize, imageUploadMaxBufferedChunks);
  }
  

  /**
   * Roll image and save md5 digest to the underlying nodes. This is a quorum
   * roll, and we ensure that it can succeed only on the nodes that consumed
   * entirely the uploaded image.
   */
  @Override
  public boolean saveDigestAndRenameCheckpointImage(long txid, MD5Hash digest) {
    try {
      LOG.info("Saving md5: " + digest + " for txid: " + txid);
      QuorumCall<AsyncLogger, Void> q = loggers
          .saveDigestAndRenameCheckpointImage(txid, digest);
      loggers.waitForWriteQuorum(q, writeTxnsTimeoutMs,
          "saveDigestAndRenameCheckpointImage(" + txid + ")");
      return true;
    } catch (IOException e) {
      LOG.error("Exception when rolling the image:", e);
      return false;
    }
  }

  /**
   * Return true if the last image
   */
  @Override
  public boolean isImageDisabled() {
    return imageDisabled;
  }
  
  /**
   * Set image status.
   */
  @Override
  public void setImageDisabled(boolean isDisabled) {
    this.imageDisabled = isDisabled;
  }
  
  /**
   * Get manifest for the images stored in journal nodes. An image is considered
   * valid if it appears in majority of the nodes, with a valid md5 sum. The
   * returned images are sorted according to their transaction id.
   */
  @Override
  public RemoteImageManifest getImageManifest(long fromTxnId)
      throws IOException {
    QuorumCall<AsyncLogger, RemoteImageManifest> q = loggers
        .getImageManifest(fromTxnId);
    Map<AsyncLogger, RemoteImageManifest> resps = loggers
        .waitForReadQuorumWithAllResponses(q, getImageManifestTimeoutMs,
            "getImageManifest");
    return createImageManifest(resps.values());
  }

  /**
   * Concatenate manifests obtained from the underlying journalnodes. The final
   * manifest will contain only the images committed to the majority of the
   * nodes. Images with no md5 associated are ignored. Also, the md5 must match
   * between images from different journal nodes.
   */
  static RemoteImageManifest createImageManifest(
      Collection<RemoteImageManifest> resps) throws IOException {
    // found valid images (with md5 hash)
    Map<Long, RemoteImage> images = Maps.newHashMap();

    for (RemoteImageManifest rm : resps) {
      for (RemoteImage ri : rm.getImages()) {
        if (ri.getDigest() == null) {
          LOG.info("Skipping: " + ri + " as it does not have md5 digest");
          continue;
        }
        if (images.containsKey(ri.getTxId())) {
          // we already have seen this image
          // two images from different nodes should be the same
          if (!images.get(ri.getTxId()).equals(ri)) {
            throw new IOException(
                "Images received from different nodes do not match: "
                    + images.get(ri.getTxId()) + " vs: " + ri);
          }
        } else {
          // store image
          images.put(ri.getTxId(), ri);
        }
      }
    }
    List<RemoteImage> result = Lists.newArrayList();
    for (RemoteImage ri : images.values()) {
      result.add(ri);
    }
    // we need to sort the images
    Collections.sort(result);

    return new RemoteImageManifest(result);
  }

  /**
   * Get latest image committed to the underlying journal nodes.
   */
  @Override
  public FSImageFile getLatestImage() throws IOException {
    List<RemoteImage> images = getImageManifest(HdfsConstants.INVALID_TXID)
        .getImages();

    // nothing available
    if (images.size() == 0) {
      return null;
    }
    return new FSImageFile(null, null, images.get(images.size() - 1).getTxId(),
        this);
  }

  /**
   * Get input stream to one of the nodes for given txid.
   */
  @Override
  public ImageInputStream getImageInputStream(long txid) throws IOException {
    URLImageInputStream stream = loggers.getImageInputStream(txid,
        httpConnectReadTimeoutMs);
    if (stream == null) {
      throw new IOException("Cannot obtain input stream for image: " + txid);
    }
    return new ImageInputStream(txid, stream, stream.getImageDigest(),
        stream.toString(), stream.getSize());
  }

  @Override
  public RemoteStorageState analyzeJournalStorage() throws IOException {
    return analyzeStorageInternal(false);
  }
  
  @Override
  public RemoteStorageState analyzeImageStorage() throws IOException {
    return analyzeStorageInternal(true);
  }
    
  private RemoteStorageState analyzeStorageInternal(boolean image) throws IOException {
    QuorumCall<AsyncLogger, GetStorageStateProto> call = image ? loggers
        .analyzeImageStorage() : loggers.analyzeJournalStorage();
    try {
      // we want all responses!
      call.waitFor(loggers.size(), loggers.size(), 0, dirTransitionTimeoutMs,
          "analyze storage");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for " + 
            (image ? "analyzeImageStorage()" : "analyzeJournalStorage()") + " response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for " +
            (image ? "analyzeImageStorage()" : "analyzeJournalStorage()") + " response");
    }
    
    if (call.countExceptions() > 0) {
      call.throwQuorumException("Could not analyze one or more JournalNodes");
    }
    
    // iterate through responses and figure out the state
    StorageState state = null;
    StorageInfo storageInfo = null;
    
    for (GetStorageStateProto r : call.getResults().values()) {
      if (state == null) {
        state = r.getStorageState();
      } else {
        if (state != r.getStorageState()) {
          state = StorageState.INCONSISTENT;
          LOG.warn("Inconsistent state detected: "
              + Arrays.toString(call.getResults().values().toArray()));
          return new RemoteStorageState(state, storageInfo);
        }
      }

      if (storageInfo == null) {
        storageInfo = r.getStorageInfo();
      } else {
        if (!storageInfo.equals(r.getStorageInfo())) {
          state = StorageState.INCONSISTENT;
          LOG.warn("Inconsistent state detected: "
              + Arrays.toString(call.getResults().values().toArray()));
          return new RemoteStorageState(state, storageInfo);
        }
      }
    }
    
    // return the state
    return new RemoteStorageState(state, storageInfo);
  }

  @Override
  public URI getURI() throws IOException {
    return uri;
  }
}

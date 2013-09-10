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
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalRequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetImageManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalImageServlet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
@InterfaceAudience.Private
public class IPCLoggerChannel implements AsyncLogger {

  public static final Log LOG = LogFactory.getLog(IPCLoggerChannel.class);
  
  private final Configuration conf;
  protected final InetSocketAddress addr;
  private QJournalProtocol proxy;

  private final ListeningExecutorService executor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private long committedTxId = HdfsConstants.INVALID_TXID;
  
  private final String journalId;
  private final byte[] journalIdBytes;
  private final NamespaceInfo nsInfo;
  private int httpPort = -1;
  
  private final IPCLoggerChannelMetrics metrics;
  
  /**
   * The number of bytes of edits data still in the queue.
   */
  private int queuedEditsSizeBytes = 0;
  
  /**
   * The highest txid that has been successfully logged on the remote JN.
   */
  private long highestAckedTxId = 0;

  /**
   * Nanotime of the last time we successfully journaled some edits
   * to the remote node.
   */
  private long lastAckNanos = 0;

  /**
   * Nanotime of the last time that committedTxId was update. Used
   * to calculate the lag in terms of time, rather than just a number
   * of txns.
   */
  private long lastCommitNanos = 0;
  
  /**
   * The maximum number of bytes that can be pending in the queue.
   * This keeps the writer from hitting OOME if one of the loggers
   * starts responding really slowly. Eventually, the queue
   * overflows and it starts to treat the logger as having errored.
   */
  private final int queueSizeLimitBytes;

  /**
   * If this logger misses some edits, or restarts in the middle of
   * a segment, the writer won't be able to write any more edits until
   * the beginning of the next segment. Upon detecting this situation,
   * the writer sets this flag to true to avoid sending useless RPCs.
   */
  private boolean outOfSync = false;
  
  /**
   * Stopwatch which starts counting on each heartbeat that is sent
   */
  private Stopwatch lastHeartbeatStopwatch = new Stopwatch();
  
  private static final long HEARTBEAT_INTERVAL_MILLIS = 1000;
  
  private static final long WARN_JOURNAL_MILLIS_THRESHOLD = 1000;
  
  static final Factory FACTORY = new AsyncLogger.Factory() {
    @Override
    public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr) {
      return new IPCLoggerChannel(conf, nsInfo, journalId, addr);
    }
  };

  public IPCLoggerChannel(Configuration conf,
      NamespaceInfo nsInfo,
      String journalId,
      InetSocketAddress addr) {
    this.conf = conf;
    this.nsInfo = nsInfo;
    this.journalId = journalId;
    this.journalIdBytes = QuorumJournalManager
        .journalIdStringToBytes(journalId);
    this.addr = addr;
    
    this.queueSizeLimitBytes = 1024 * 1024 * conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT);
    
    executor = MoreExecutors.listeningDecorator(
        createExecutor());
    
    metrics = IPCLoggerChannelMetrics.create(this);
  }
  
  @Override
  public synchronized void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  @Override
  public synchronized void setCommittedTxId(long txid, boolean force) {
    if (!force && txid < committedTxId) {
      throw new IllegalArgumentException(
          "Trying to move committed txid backwards in client, old:"
              + committedTxId + " new: " + txid);
    }
    this.committedTxId = txid;
    if (force && this.highestAckedTxId > txid) {
      this.highestAckedTxId = txid;
    }
    if (!force) {
      this.lastCommitNanos = System.nanoTime();
      metrics.setCurrentLagTransactions(getLagTxns());
      metrics.setCurrentLagTimeMicros(getLagTimeMicros());
    }
  }
  
  @Override
  public void close() {
    // No more tasks may be submitted after this point.
    executor.shutdown();
    if (proxy != null) {
      // TODO: this can hang for quite some time if the client
      // is currently in the middle of a call to a downed JN.
      // We should instead do this asynchronously, and just stop
      // making any more calls after this point (eg clear the queue)
      try {
        RPC.stopProxy(proxy);
      } catch (Exception e) {
        LOG.error("error: ", e);
      }
    }
  }
  
  protected QJournalProtocol getProxy() throws IOException {
    if (proxy != null)
      return proxy;
    proxy = createProxy();
    return proxy;
  }
  
  protected QJournalProtocol createProxy() throws IOException {
    final Configuration confCopy = new Configuration(conf);
    // Need to set NODELAY or else batches larger than MTU can trigger
    // 40ms nagling delays.
    confCopy.setBoolean("ipc.client.tcpnodelay", true);
    long connectionTimeout = conf.getLong(
        JournalConfigKeys.DFS_QJOURNAL_CONNECT_TIMEOUT_KEY, Long.MAX_VALUE);
    return (QJournalProtocol) RPC.waitForProxy(QJournalProtocol.class,
        QJournalProtocol.versionID, addr, confCopy, connectionTimeout, 0);
  }
  
  
  /**
   * Separated out for easy overriding in tests.
   */
  @VisibleForTesting
  protected ExecutorService createExecutor() {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Logger channel to " + addr)
          .setUncaughtExceptionHandler(
              UncaughtExceptionHandlers.systemExit())
          .build());
  }
  
  @Override
  public URL buildURLToFetchLogs(long segmentTxId, long position) {
    Preconditions.checkArgument(segmentTxId > -1,
        "Invalid segment: %s", segmentTxId);
    Preconditions.checkState(httpPort != -1,
        "HTTP port not set yet");
        
    try {
      String path = GetJournalEditServlet.buildPath(
          journalId, segmentTxId, nsInfo, position);
      return new URL("http", addr.getAddress().getHostAddress(), httpPort, path.toString());
    } catch (MalformedURLException e) {
      // should never get here.
      throw new IllegalStateException(e);
    }
  }

  private synchronized RequestInfo createReqInfo() {
    checkEpoch();
    return new RequestInfo(journalIdBytes, epoch, ipcSerial++,
        committedTxId);
  }
  
  private synchronized JournalRequestInfo createJournalReqInfo(
      long segmentTxId, long firstTxdId, int numTxns, byte[] records) {
    checkEpoch();
    return new JournalRequestInfo(journalIdBytes, epoch, ipcSerial++,
        committedTxId, segmentTxId, firstTxdId, numTxns, records);
  }
  
  private void checkEpoch() {
    if (epoch <= 0) {
      throw new IllegalStateException("bad epoch: " + epoch);
    }
  }

  @VisibleForTesting
  synchronized long getNextIpcSerial() {
    return ipcSerial;
  }

  public synchronized int getQueuedEditsSize() {
    return queuedEditsSizeBytes;
  }
  
  public InetSocketAddress getRemoteAddress() {
    return addr;
  }

  /**
   * @return true if the server has gotten out of sync from the client,
   * and thus a log roll is required for this logger to successfully start
   * logging more edits.
   */
  public synchronized boolean isOutOfSync() {
    return outOfSync;
  }
  
  @VisibleForTesting
  void waitForAllPendingCalls() throws InterruptedException {
    try {
      executor.submit(new Runnable() {
        @Override
        public void run() {
        }
      }).get();
    } catch (ExecutionException e) {
      // This can't happen!
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ListenableFuture<Boolean> isJournalFormatted() {
    return executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return getProxy().isJournalFormatted(journalIdBytes);
      }
    });
  }

  @Override
  public ListenableFuture<GetJournalStateResponseProto> getJournalState() {
    return executor.submit(new Callable<GetJournalStateResponseProto>() {
      @Override
      public GetJournalStateResponseProto call() throws IOException {
        GetJournalStateResponseProto ret =
            getProxy().getJournalState(journalIdBytes);
        httpPort = ret.getHttpPort();
        return ret;
      }
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final long epoch) {
    return executor.submit(new Callable<NewEpochResponseProto>() {
      @Override
      public NewEpochResponseProto call() throws IOException {
        return getProxy().newEpoch(journalIdBytes, nsInfo, epoch);
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data) {
    try {
      reserveQueueSpace(data.length);
    } catch (LoggerTooFarBehindException e) {
      // log the exception, because immediateFailedFuture will log nothing here.
      LOG.warn("Edits queue overflows when trying to write txns " + 
                firstTxnId + "-" + (firstTxnId + numTxns - 1), e);
      return Futures.immediateFailedFuture(e);
    }
    
    // When this batch is acked, we use its submission time in order
    // to calculate how far we are lagging.
    final long submitNanos = System.nanoTime();
    
    ListenableFuture<Void> ret = null;
    try {
      ret = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          throwIfOutOfSync();

          boolean isDebugEnabled = LOG.isDebugEnabled();
          long rpcSendTimeNanos = System.nanoTime();
          try {
            if (isDebugEnabled) {
              LOG.debug("Prepare writing to remote journal " + IPCLoggerChannel.this + ". " +
                  "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1));
            }
            getProxy().journal(createJournalReqInfo(
                segmentTxId, firstTxnId, numTxns, data));
            if (isDebugEnabled) {
              LOG.debug("Finish writing to remote journal " + IPCLoggerChannel.this + ". " +
                  "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1) +
                  ". Spent time (micro time): " + ((System.nanoTime() - rpcSendTimeNanos) / 1000));
            }
          } catch (IOException e) {
            QuorumJournalManager.LOG.warn(
                "Remote journal " + IPCLoggerChannel.this + " failed to " +
                "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1) +
                ". Will try to write to this JN again after the next " +
                "log roll.", e); 
            synchronized (IPCLoggerChannel.this) {
              outOfSync = true;
              metrics.setOutOfSync(outOfSync);
            }
            throw e;
          } finally {
            long rpcTime = DFSUtil.getElapsedTimeMicroSeconds(rpcSendTimeNanos);
            long endToEndTime = DFSUtil.getElapsedTimeMicroSeconds(submitNanos);
            metrics.addWriteEndToEndLatency(endToEndTime);
            metrics.addWriteRpcLatency(rpcTime);
            if (rpcTime / 1000 > WARN_JOURNAL_MILLIS_THRESHOLD) {
              QuorumJournalManager.LOG.warn(
                  "Took " + (rpcTime / 1000) + "ms to send a batch of " +
                  numTxns + " edits (firstTxnId " + firstTxnId + ", " + data.length + " bytes) to " +
                  "remote journal " + IPCLoggerChannel.this);
            }
          }
          synchronized (IPCLoggerChannel.this) {
            highestAckedTxId = firstTxnId + numTxns - 1;
            lastAckNanos = submitNanos;
            metrics.setCurrentLagTransactions(getLagTxns());
            metrics.setCurrentLagTimeMicros(getLagTimeMicros());
          }
          return null;
        }
      });
    } finally {
      if (ret == null) {
        // it didn't successfully get submitted,
        // so adjust the queue size back down.
        unreserveQueueSpace(data.length);
      } else {
        // It was submitted to the queue, so adjust the length
        // once the call completes, regardless of whether it
        // succeeds or fails.
        Futures.addCallback(ret, new FutureCallback<Void>() {
          @Override
          public void onFailure(Throwable t) {
            unreserveQueueSpace(data.length);
          }

          @Override
          public void onSuccess(Void t) {
            unreserveQueueSpace(data.length);
          }
        });
      }
    }
    return ret;
  }

  private void throwIfOutOfSync()
      throws JournalOutOfSyncException, IOException {
    if (isOutOfSync()) {
      // Even if we're out of sync, it's useful to send an RPC
      // to the remote node in order to update its lag metrics, etc.
      heartbeatIfNecessary();
      throw new JournalOutOfSyncException(
          "Journal disabled until next roll");
    } 
    metrics.setOutOfSync(outOfSync);
  }

  /**
   * When we've entered an out-of-sync state, it's still useful to periodically
   * send an empty RPC to the server, such that it has the up to date
   * committedTxId. This acts as a sanity check during recovery, and also allows
   * that node's metrics to be up-to-date about its lag.
   * 
   * In the future, this method may also be used in order to check that the
   * current node is still the current writer, even if no edits are being
   * written.
   */
  private void heartbeatIfNecessary() throws IOException {
    if (lastHeartbeatStopwatch.elapsedMillis() > HEARTBEAT_INTERVAL_MILLIS ||
        !lastHeartbeatStopwatch.isRunning()) {
      try {
        getProxy().heartbeat(createReqInfo());
      } finally {
        // Don't send heartbeats more often than the configured interval,
        // even if they fail.
        lastHeartbeatStopwatch.reset().start();
      }
    }
  }

  private synchronized void reserveQueueSpace(int size)
      throws LoggerTooFarBehindException {
    Preconditions.checkArgument(size >= 0);
    if (queuedEditsSizeBytes + size > queueSizeLimitBytes &&
        queuedEditsSizeBytes > 0) {
      throw new LoggerTooFarBehindException("The edits pending queues to " +
        IPCLoggerChannel.this + " overflows. Required size: " + (queuedEditsSizeBytes + size) + 
        ", limited size: " + queueSizeLimitBytes);
    }
    queuedEditsSizeBytes += size;
    metrics.setCurrentQueuedEditsSizeBytes(queuedEditsSizeBytes);
  }
  
  private synchronized void unreserveQueueSpace(int size) {
    Preconditions.checkArgument(size >= 0);
    queuedEditsSizeBytes -= size;
    metrics.setCurrentQueuedEditsSizeBytes(queuedEditsSizeBytes);
  }

  @Override
  public ListenableFuture<Void> transitionImage(final NamespaceInfo nsInfo,
      final Transition transition, final StartupOption startOpt) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().transitionImage(journalIdBytes, nsInfo, transition, startOpt);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> transitionJournal(final NamespaceInfo nsInfo,
      final Transition transition, final StartupOption startOpt) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().transitionJournal(journalIdBytes, nsInfo, transition, startOpt);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> startLogSegment(final long txid) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().startLogSegment(createReqInfo(), txid);
        synchronized (IPCLoggerChannel.this) {
          if (outOfSync) {
            outOfSync = false;
            QuorumJournalManager.LOG.info(
                "Restarting previously-stopped writes to " +
                IPCLoggerChannel.this + " in segment starting at txid " +
                txid);
          }
          metrics.setOutOfSync(outOfSync);
        }
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> finalizeLogSegment(
      final long startTxId, final long endTxId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        throwIfOutOfSync();
        
        getProxy().finalizeLogSegment(createReqInfo(), startTxId, endTxId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> purgeLogsOlderThan(final long minTxIdToKeep) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().purgeLogsOlderThan(createReqInfo(), minTxIdToKeep);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      final long fromTxnId) {
    return executor.submit(new Callable<RemoteEditLogManifest>() {
      @Override
      public RemoteEditLogManifest call() throws IOException {
        GetEditLogManifestResponseProto ret = getProxy().getEditLogManifest(
            journalIdBytes, fromTxnId);
        // Update the http port, since we need this to build URLs to any of the
        // returned logs.
        httpPort = ret.getHttpPort();
        return new RemoteEditLogManifest(ret.getLogs());
      }
    });
  }

  @Override
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      final long segmentTxId) {
    return executor.submit(new Callable<PrepareRecoveryResponseProto>() {
      @Override
      public PrepareRecoveryResponseProto call() throws IOException {
        if (httpPort < 0) {
          // If the HTTP port hasn't been set yet, force an RPC call so we know
          // what the HTTP port should be.
          httpPort = getProxy().getJournalState(journalIdBytes).getHttpPort();
        }
        return getProxy().prepareRecovery(createReqInfo(), segmentTxId);
      }
    });
  }

  @Override
  public ListenableFuture<Void> acceptRecovery(
      final SegmentStateProto log, final String url) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().acceptRecovery(createReqInfo(), log, url);
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return InetAddresses.toAddrString(addr.getAddress()) + ':' + addr.getPort();
  }
  
  @Override
  public String toHTMLString() {
    // (F) logger is disabled
    // (A) logger is active
    return InetAddresses.toAddrString(addr.getAddress())
        + ':'
        + addr.getPort()
        + (isOutOfSync() ? "<font color=red><b>(F)</b></font>"
            : "<font color=green><b>(A)</b></font>");
  }

  @Override
  public synchronized void appendHtmlReport(StringBuilder sb) {
    sb.append("Written txid ").append(highestAckedTxId);
    long behind = getLagTxns();
    if (behind > 0) {
      if (lastAckNanos != 0) {
        long lagMicros = getLagTimeMicros();
        sb.append(" (" + behind + " txns/" + lagMicros + " micros behind)");
      } else {
        sb.append(" (never written");
      }
    }
    if (outOfSync) {
      sb.append(" (will re-sync on next segment)");
    }
  }
  
  private synchronized long getLagTxns() {
    return Math.max(committedTxId - highestAckedTxId, 0);
  }
  
  private synchronized long getLagTimeMicros() {
    return Math.max((lastCommitNanos - lastAckNanos) / 1000, 0);
  }
  
  @Override
  public ListenableFuture<GetStorageStateProto> analyzeJournalStorage() {
    return executor.submit(new Callable<GetStorageStateProto>() {
      @Override
      public GetStorageStateProto call() throws Exception {
        return getProxy().analyzeJournalStorage(journalIdBytes);
      }
    });
  }

  @Override
  public ListenableFuture<Void> saveDigestAndRenameCheckpointImage(
      final long txid, final MD5Hash digest) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().saveDigestAndRenameCheckpointImage(createReqInfo(), txid,
            digest);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<RemoteImageManifest> getImageManifest(
      final long fromTxnId) {
    return executor.submit(new Callable<RemoteImageManifest>() {
      @Override
      public RemoteImageManifest call() throws IOException {
        GetImageManifestResponseProto ret = getProxy().getImageManifest(
            journalIdBytes, fromTxnId);
        return new RemoteImageManifest(ret.getImages());
      }
    });
  }
  
  /**
   * Build url to fetch image from the journal node to which this logger channel
   * is attached.
   */
  @Override
  public URL buildURLToFetchImage(long txid) {
    Preconditions.checkArgument(txid >= -1, "Invalid segment: %s", txid);
    Preconditions.checkState(httpPort != -1, "HTTP port not set yet");

    try {
      // for now we disable throttling for image downloads
      String path = GetJournalImageServlet.buildPath(journalId, txid, nsInfo,
          true);
      return new URL("http", addr.getAddress().getHostAddress(), httpPort,
          path.toString());
    } catch (MalformedURLException e) {
      // should never get here.
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ListenableFuture<GetStorageStateProto> analyzeImageStorage() {
    return executor.submit(new Callable<GetStorageStateProto>() {
      @Override
      public GetStorageStateProto call() throws Exception {
        return getProxy().analyzeImageStorage(journalIdBytes);
      }
    });
  }
  
  @Override
  public ListenableFuture<Boolean> isImageFormatted() {
    return executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return getProxy().isImageFormatted(journalIdBytes);
      }
    });
  }
}

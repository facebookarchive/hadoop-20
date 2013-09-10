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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.ArrayOutputStream;
import org.apache.hadoop.util.InjectionHandler;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;


/**
 * Manages a collection of Journals. None of the methods are synchronized, it is
 * assumed that FSEditLog methods, that use this class, use proper
 * synchronization.
 */
public class JournalSet implements JournalManager {
  
  static final public Comparator<EditLogInputStream>
  EDIT_LOG_INPUT_STREAM_COMPARATOR = new Comparator<EditLogInputStream>() {
    @Override
    public int compare(EditLogInputStream a, EditLogInputStream b) {
      return ComparisonChain.start().
        compare(a.getFirstTxId(), b.getFirstTxId()).
        compare(b.getLastTxId(), a.getLastTxId()).
        result();
    }
  };
  
  static final int NO_REDUNDANT_REQUIRED = 0;
  
  // buffer used for serializing operations when writing to journals
  private final ArrayOutputStream tempWriteBuffer = new ArrayOutputStream(1024);

  static final Log LOG = LogFactory.getLog(FSEditLog.class);
  
  // executor for syncing transactions
  private final ExecutorService executor; 
  
  private int minimumNumberOfJournals;
  private int minimumNumberOfNonLocalJournals;
  private final NNStorage storage;
  private final FSImage image;
  
  private final NameNodeMetrics metrics;
  
  /**
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   */
  public static class JournalAndStream {
    private final JournalManager journal;
    private boolean disabled = false;
    private EditLogOutputStream stream;
    private boolean required = false;
    private boolean shared = false;
    private boolean remote = false;
    
    public JournalAndStream(JournalManager manager, boolean required,
        boolean shared, boolean remote) {
      this.journal = manager;
      this.required = required;
      this.shared = shared;
      this.remote = remote;
    }

    public void startLogSegment(long txId) throws IOException {
      if(stream != null) {
        throw new IOException("Stream should not be initialized");
      }
      if (disabled) {
        FSEditLog.LOG.info("Restoring journal " + this);
      }
      disabled = false;
      InjectionHandler.processEventIO(
          InjectionEvent.JOURNALANDSTREAM_STARTLOGSEGMENT, required);
      stream = journal.startLogSegment(txId);
    }

    /**
     * Closes the stream, also sets it to null.
     */
    public void closeStream() throws IOException {
      if (stream == null) return;
      stream.close();
      stream = null;
    }

    /**
     * Close the Journal and Stream
     */
    public void close() throws IOException {
      closeStream();

      journal.close();
    }
    
    /**
     * Aborts the stream, also sets it to null.
     */
    public void abort() {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
    }

    boolean isActive() {
      return stream != null;
    }
    
    /**
     * Should be used outside JournalSet only for testing.
     */
    EditLogOutputStream getCurrentStream() {
      return stream;
    }
    
    @Override
    public String toString() {
      return "JournalAndStream(mgr=" + journal +
        ", " + "stream=" + stream + ", required=" + required + ")";
    }
    
    public String toStringShort() {
      return journal.toString();
    }
    
    public String toHTMLString() {
      return journal.toHTMLString();
    }
    
    public String generateHTMLReport() {
      return stream == null ? "" : stream.generateHtmlReport();
    }

    void setCurrentStreamForTests(EditLogOutputStream stream) {
      this.stream = stream;
    }
    
    JournalManager getManager() {
      return journal;
    }

    private boolean isDisabled() {
      return disabled;
    }

    private void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
    
    public boolean isResourceAvailable() {
      return !isDisabled();
    }
    
    public boolean isRequired() {
      return required;
    }
    
    public boolean isShared() {
      return shared;
    }
    
    public boolean isRemote() {
      return remote;
    }
  }
  
  private List<JournalAndStream> journals = new ArrayList<JournalAndStream>();
  
  private volatile boolean forceJournalCheck = false;

  JournalSet(Configuration conf, FSImage image, NNStorage storage,
      int numJournals, NameNodeMetrics metrics) {
    minimumNumberOfJournals
      = conf.getInt("dfs.name.edits.dir.minimum", 1);
    minimumNumberOfNonLocalJournals 
      = conf.getInt("dfs.name.edits.dir.minimum.nonlocal", 0);
    this.image = image;
    this.storage = storage;
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("JournalSet Worker %d")
            .build();
    this.executor = Executors.newFixedThreadPool(numJournals,
        namedThreadFactory);
    this.metrics = metrics;
  }
  
  @Override
  public EditLogOutputStream startLogSegment(final long txId) throws IOException {
    mapJournalsAndReportErrorsParallel(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId);
      }
    }, "starting log segment " + txId);
    return new JournalSetOutputStream();
  }
  
  @Override
  public void finalizeLogSegment(final long firstTxId, final long lastTxId)
      throws IOException {
    mapJournalsAndReportErrorsParallel(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.closeStream();
          jas.getManager().finalizeLogSegment(firstTxId, lastTxId);
        }
      }
    }, "finalize log segment " + firstTxId + ", " + lastTxId);
  }
   
  @Override
  public void close() throws IOException {
    mapJournalsAndReportErrorsParallel(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.close();
      }
    }, "close journal");
    executor.shutdown();
  }
  
  /**
   * Selects input streams starting with fromTxnId from this journal set.
   * First, we get the streams from all of our JournalManager objects.  
   * Then we chain them (group them) into groups corresponding to log segments.
   * Each group will be represented by a single redundant stream, with a 
   * number of underlying streams (each from a single Journal Manager).
   * 
   * @param validateInProgressSegments - validate non-finalized segments
   * @param fromTxnId the first transaction id we want to read
   * @throws IOException
   */
  @Override
  synchronized public void selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean validateInProgressSegments)
      throws IOException {
    selectInputStreams(streams, fromTxId, inProgressOk,
        validateInProgressSegments, NO_REDUNDANT_REQUIRED);
  }
  
  /**
   * Selects input streams. Returns true if each stream meets min redundancy,
   * false otherwise.
   */
  synchronized public boolean selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean validateInProgressSegments,
      int minRedundancy) throws IOException {
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (JournalAndStream jas : journals) {
      if (jas.isDisabled()) {
        LOG.info("Skipping jas " + jas + " since it's disabled");
        continue;
      }
      try {
        jas.getManager().selectInputStreams(allStreams, fromTxId, inProgressOk,
            validateInProgressSegments);
      } catch (IOException ioe) {
        LOG.warn("Unable to determine input streams from " + jas.getManager()
            + ". Skipping.", ioe);
      }
    }
    return chainAndMakeRedundantStreams(streams, allStreams, fromTxId,
        inProgressOk, minRedundancy);
  }
  
  /**
   * Check if any journal manager has unfinalized segments.
   * @param fromTxId starting txid
   */
  synchronized boolean hasUnfinalizedSegments(long fromTxId) {
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    for (JournalAndStream jas : journals) {
      if (jas.isDisabled()) {
        continue;
      }
      try {
        // get all streams, including inProgress ones.
        jas.getManager().selectInputStreams(streams, fromTxId, true, false);
        for (EditLogInputStream elis : streams) {
          if (elis.isInProgress()) {
            // we found an input stream that is in progress
            return true;
          }
        }
      } catch (IOException ioe) {
        LOG.warn("Unable to determine input streams from " + jas.getManager()
            + ". Skipping.", ioe);
      }
    }
    // all streams are finalized
    return false;
  }
  
  /**
   * Chain the input stream, by grouping them according to the start
   * transaction id. For each group, create a redundant input stream.
   * (Each log segment will have one redundant input stream, with
   * multiple underlying actual stream.) Add the resulting redundant
   * streams to allStreams
   * 
   * @param outStreams - input streams
   * @param allStreams - result
   * @param fromTxId - discard any older streams than from TxId
   * @param inProgressOk - if true, add inProgress segments
   * @param minRedundancy check if each stream is sufficiently redundant
   * 
   * @return true if redundancy is violated
   */
  public static boolean chainAndMakeRedundantStreams(
      Collection<EditLogInputStream> outStreams,
      PriorityQueue<EditLogInputStream> allStreams, long fromTxId,
      boolean inProgressOk, int minRedundancy) throws IOException {
    // We want to group together all the streams that start on the same start
    // transaction ID.  To do this, we maintain an accumulator (acc) of all
    // the streams we've seen at a given start transaction ID.  When we see a
    // higher start transaction ID, we select a stream from the accumulator and
    // clear it.  Then we begin accumulating streams with the new, higher start
    // transaction ID.
    LinkedList<EditLogInputStream> acc = new LinkedList<EditLogInputStream>();
    EditLogInputStream elis;
    boolean redundancyViolated = false;
    
    while ((elis = allStreams.poll()) != null) {
      
      // check if there is any inconsistency here
      // at startup all segments should be finalized and recovered
      // so we should fail if we get to this point at startup
      if (!inProgressOk && elis.isInProgress()) {
        LOG.fatal("Found in progress stream");
        throw new IOException("In progress streams not allowed");
      }
      
      if (acc.isEmpty()) {
        acc.add(elis);
      } else {
        long accFirstTxId = acc.get(0).getFirstTxId();
        if (accFirstTxId == elis.getFirstTxId()) {
          if ((elis.getLastTxId() > acc.get(0).getLastTxId())
              || (elis.getLastTxId() == acc.get(0).getLastTxId() && isLocalJournal(elis
                  .getJournalManager()))) {
            // prefer local journals for reading
            // or journals with more transactions
            // this can happen only when tailing in progress logs
            acc.add(0, elis);
          } else {
            acc.add(elis);
          }
        } else if (accFirstTxId < elis.getFirstTxId()) {
          // check acc for min Redundancy
          if (minRedundancy > 0 && acc.size() < minRedundancy) {
            // we have fewer copies than required
            // on startup, we want to force to save the namespace
            redundancyViolated = true;
          }
          outStreams.add(new RedundantEditLogInputStream(acc, fromTxId,
              inProgressOk));
          acc.clear();
          acc.add(elis);
        } else if (accFirstTxId > elis.getFirstTxId()) {
          throw new RuntimeException("sorted set invariants violated!  " +
              "Got stream with first txid " + elis.getFirstTxId() +
              ", but the last firstTxId was " + accFirstTxId);
        }
      }
    }
    if (!acc.isEmpty()) {
      // check acc for min Redundancy
      if (minRedundancy > 0 && acc.size() < minRedundancy) {
        // we have fewer copies than required
        // on startup, we want to force to save the namespace
        redundancyViolated = true;
      }
      outStreams.add(new RedundantEditLogInputStream(acc, fromTxId,
          inProgressOk));
      acc.clear();
    }
    return redundancyViolated;
  }

  /**
   * Check if the given journal is local.
   */
  private static boolean isLocalJournal(JournalManager jm) {
    if (jm == null || (!(jm instanceof FileJournalManager))) {
      return false;
    }
    return NNStorage.isPreferred(StorageLocationType.LOCAL,
        ((FileJournalManager) jm).getStorageDirectory());
  }

  /**
   * Returns true if there are no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   * 
   * @return True if there no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   */
  public boolean isEmpty() {
    return journals.size() == 0;
  }
  
  /**
   * Called when some journals experience an error in some operation.
   */
  private void disableAndReportErrorOnJournals(
      List<JournalAndStream> badJournals, String status) throws IOException {
    if (badJournals == null || badJournals.isEmpty()) {
      if (forceJournalCheck) {
        // check status here, because maybe some other operation
        // (e.g., rollEditLog failed and disabled journals) but this
        // was missed by logSync() exit runtime
        forceJournalCheck = false;
        checkJournals(status);
      }
      return; // nothing to do
    }
 
    for (JournalAndStream j : badJournals) {
      LOG.error("Disabling journal " + j);
      j.abort();
      j.setDisabled(true);
      
      // report errors on storage directories as well for FJMs
      if (j.journal instanceof FileJournalManager) {
        FileJournalManager fjm = (FileJournalManager) j.journal;
        // pass image to handle image managers
        storage.reportErrorsOnDirectory(fjm.getStorageDirectory(), image);
      }

      // report error on shared journal/image managers
      if (j.journal instanceof ImageManager) {
        ImageManager im = (ImageManager) j.journal;
        im.setImageDisabled(true);
      }
    }
    // update image manager metrics
    if (image != null) {
      image.updateImageMetrics();
    }
    checkJournals(status);
  }

  /**
   * Implementations of this interface encapsulate operations that can be
   * iteratively applied on all the journals. For example see
   * {@link JournalSet#mapJournalsAndReportErrors}.
   */
  interface JournalClosure {
    /**
     * The operation on JournalAndStream.
     * @param jas Object on which operations are performed.
     * @throws IOException
     */
    public void apply(JournalAndStream jas) throws IOException;
  }

  /**
   * Apply the given operation across all of the journal managers, disabling
   * any for which the closure throws an IOException.
   * @param closure {@link JournalClosure} object encapsulating the operation.
   * @param status message used for logging errors (e.g. "opening journal")
   * @throws IOException If the operation fails on all the journals.
   */
  private void mapJournalsAndReportErrors(
      JournalClosure closure, String status) throws IOException{
    List<JournalAndStream> badJAS = null;
    for (JournalAndStream jas : journals) {
      try {
        closure.apply(jas);
      } catch (Throwable t) {
        if (badJAS == null)
          badJAS = new LinkedList<JournalAndStream>();
        LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
        badJAS.add(jas);
      }
    }
    disableAndReportErrorOnJournals(badJAS, status);
  }
  
  /**
   * Apply the given operation across all of the journal managers, disabling
   * any for which the closure throws an IOException. Do it in parallel.
   * @param closure {@link JournalClosure} object encapsulating the operation.
   * @param status message used for logging errors (e.g. "opening journal")
   * @throws IOException If the operation fails on all the journals.
   */
  private void mapJournalsAndReportErrorsParallel(JournalClosure closure,
      String status) throws IOException {

    // set-up calls
    List<Future<JournalAndStream>> jasResponeses = new ArrayList<Future<JournalAndStream>>(
        journals.size());

    for (JournalAndStream jas : journals) {
      jasResponeses.add(executor.submit(new JournalSetWorker(jas, closure,
          status)));
    }

    List<JournalAndStream> badJAS = null;

    // iterate through responses
    for (Future<JournalAndStream> future : jasResponeses) {
      JournalAndStream jas = null;
      try {
        jas = future.get();
      } catch (ExecutionException e) {
        throw new IOException("This should never happen!!!", e);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted whe performing journal operations",
            e);
      }
      if (jas == null)
        continue;

      // the worker returns the journal if the operation failed
      if (badJAS == null)
        badJAS = new LinkedList<JournalAndStream>();

      badJAS.add(jas);
    }
    disableAndReportErrorOnJournals(badJAS, status);
  }
  
  /**
   * Get the number of available journals.
   */
  void updateJournalMetrics() {
    if (metrics == null) {
      return;
    }
    int failedJournals = 0;
    for(JournalAndStream jas : journals) {
      if(jas.isDisabled()) {
        failedJournals++;
      }
    }
    metrics.journalsFailed.set(failedJournals);
  }
  
  /**
   * Returns number of all journals (enabled and disabled).
   */
  protected int getNumberOfJournals() {
    return journals.size();
  }
  
  /**
   * Checks if the number of journals available is not below
   * minimum. Only invoked at errors.
   */
  protected int checkJournals(String status) throws IOException {
    boolean abort = false;
    int journalsAvailable = 0;
    int nonLocalJournalsAvailable = 0;
    for(JournalAndStream jas : journals) {
      if(jas.isDisabled() && jas.isRequired()) {
        abort = true;
      } else if (jas.isResourceAvailable()) {
        journalsAvailable++;
        if (jas.isRemote() || jas.isShared()) {
          nonLocalJournalsAvailable++;
        }
      }
    }
    // update metrics
    updateJournalMetrics();
    if (abort || journalsAvailable < minimumNumberOfJournals
        || nonLocalJournalsAvailable < minimumNumberOfNonLocalJournals) {
      forceJournalCheck = true;
      String message = status + " failed for too many journals, minimum: "
          + minimumNumberOfJournals + " current: " + journalsAvailable
          + ", non-local: " 
          + minimumNumberOfNonLocalJournals + " current: " + nonLocalJournalsAvailable;
      LOG.error(message);
      throw new IOException(message);
    }
    return journalsAvailable;
  }
  
  /**
   * Checks if the shared journal (if present) available)
   */
  protected boolean isSharedJournalAvailable() throws IOException {
    for(JournalAndStream jas : journals) {
      if(jas.isShared() && jas.isResourceAvailable()) {
        return true;
      } 
    }
    return false;
  }
  
  protected void updateNamespaceInfo(StorageInfo si) {
    for (JournalAndStream jas : journals) {
      JournalManager jm = jas.getManager();
      if (jm instanceof QuorumJournalManager) {
        ((QuorumJournalManager)jm).updateNamespaceInfo(si);
      }
    }
  }
  
  /**
   * An implementation of EditLogOutputStream that applies a requested method on
   * all the journals that are currently active.
   */
  private class JournalSetOutputStream extends EditLogOutputStream {

    JournalSetOutputStream() throws IOException {
      super();
    }

    @Override
    public void write(final FSEditLogOp op)
        throws IOException {    
      // serialize operation first
      Writer.writeOp(op, tempWriteBuffer);
      writeRawOp(tempWriteBuffer.getBytes(), 0, tempWriteBuffer.size(), op.txid);
    }
    
    @Override
    public void writeRawOp(final byte[] data, final int offset,
        final int length, final long txid) throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().writeRawOp(data, offset, length, txid);
          }
        }
      }, "write raw op");
    }
    
    @Override
    public void writeRaw(final byte[] data, final int offset, final int length)
        throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().writeRaw(data, offset, length);
          }
        }
      }, "write bytes");
    }

    @Override
    public void create() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().create();
          }
        }
      }, "create");
    }

    @Override
    public void close() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.closeStream();
        }
      }, "close");
    }

    @Override
    public void abort() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.abort();
        }
      }, "abort");
    }

    @Override
    public void setReadyToFlush() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().setReadyToFlush();
          }
        }
      }, "setReadyToFlush");
    }

    @Override
    protected void flushAndSync(final boolean durable) throws IOException {
      mapJournalsAndReportErrorsParallel(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flushAndSync(durable);
          }
        }
      }, "flushAndSync");
    }
    
    @Override
    public void flush() throws IOException {
      mapJournalsAndReportErrorsParallel(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flush();
          }
        }
      }, "flush");
    }
    
    @Override
    public boolean shouldForceSync() {
      for (JournalAndStream js : journals) {
        if (js.isActive() && js.getCurrentStream().shouldForceSync()) {
          return true;
        }
      }
      return false;
    }
    
    @Override
    public long getNumSync() {
      for (JournalAndStream jas : journals) {
        if (jas.isActive()) {
          return jas.getCurrentStream().getNumSync();
        }
      }
      return 0;
    }

    //TODO what is the name of the journalSet?
    @Override
    public String getName() {
      return "JournalSet: ";
    }

    @Override
    public long length() throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }
  }
  
  List<JournalAndStream> getAllJournalStreams() {
    return journals;
  }

  List<JournalManager> getJournalManagers() {
    List<JournalManager> jList = new ArrayList<JournalManager>();
    for (JournalAndStream j : journals) {
      jList.add(j.getManager());
    }
    return jList;
  }

  FSImage getImage() {
    return image;
  }

  void add(JournalManager j, boolean required, boolean shared, boolean remote) {
    JournalAndStream jas = new JournalAndStream(j, required, shared, remote);
    journals.add(jas);
    // update journal metrics
    updateJournalMetrics();
  }
  
  void remove(JournalManager j) {
    JournalAndStream jasToRemove = null;
    for (JournalAndStream jas: journals) {
      if (jas.getManager().equals(j)) {
        jasToRemove = jas;
        break;
      }
    }
    if (jasToRemove != null) {
      jasToRemove.abort();
      journals.remove(jasToRemove);
    }
    // update journal metrics
    updateJournalMetrics();
  }

  @Override
  public void purgeLogsOlderThan(final long minTxIdToKeep) throws IOException {
    mapJournalsAndReportErrorsParallel(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().purgeLogsOlderThan(minTxIdToKeep);
      }
    }, "purgeLogsOlderThan " + minTxIdToKeep);
  }

  @Override
  public void setCommittedTxId(final long txid, final boolean force)
      throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().setCommittedTxId(txid, force);
      }
    }, "txid " + txid + " " + force);
  }

  
  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    mapJournalsAndReportErrorsParallel(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().recoverUnfinalizedSegments();
      }
    }, "recoverUnfinalizedSegments");
  }
  
  /**
   * Return a manifest of what edit logs are available. All available
   * edit logs are returned starting from the transaction id passed,
   * including inprogress segments.
   * 
   * @param fromTxId Starting transaction id to read the logs.
   * @return RemoteEditLogManifest object.
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId) {
    // Collect RemoteEditLogs available from each FileJournalManager
    List<RemoteEditLog> allLogs = new ArrayList<RemoteEditLog>();
    for (JournalAndStream j : journals) {
      JournalManager jm = j.getManager();
      try {
        allLogs.addAll(jm.getEditLogManifest(fromTxId).getLogs());
      } catch (Throwable t) {
        LOG.warn("Cannot list edit logs in " + jm, t);
      }
    }

    // Group logs by their starting txid
    ImmutableListMultimap<Long, RemoteEditLog> logsByStartTxId =
      Multimaps.index(allLogs, RemoteEditLog.GET_START_TXID);
    long curStartTxId = fromTxId;

    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    while (true) {
      ImmutableList<RemoteEditLog> logGroup = logsByStartTxId.get(curStartTxId);
      if (logGroup.isEmpty()) {
        // we have a gap in logs - for example because we recovered some old
        // storage directory with ancient logs. Clear out any logs we've
        // accumulated so far, and then skip to the next segment of logs
        // after the gap.
        SortedSet<Long> startTxIds = new TreeSet<Long>(logsByStartTxId.keySet());
        startTxIds = startTxIds.tailSet(curStartTxId);
        if (startTxIds.isEmpty()) {
          break;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found gap in logs at " + curStartTxId + ": " +
                "not returning previous logs in manifest.");
          }
          logs.clear();
          curStartTxId = startTxIds.first();
          continue;
        }
      }

      // Find the one that extends the farthest forward
      RemoteEditLog bestLog = Collections.max(logGroup);
      logs.add(bestLog);
      // And then start looking from after that point
      curStartTxId = bestLog.getEndTxId() + 1;
      if (curStartTxId == 0)
        break;
    }
    RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated manifest for logs since " + fromTxId + ":"
          + ret);      
    }
    return ret;
  }

  /**
   * Add sync times to the buffer.
   */
  String getSyncTimes() {
    StringBuilder buf = new StringBuilder();
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        buf.append(jas.getCurrentStream().getTotalSyncTime());
        buf.append(" ");
      }
    }
    return buf.toString();
  }
  
  @Override
  public void transitionJournal(StorageInfo nsInfo, Transition transition,
      StartupOption startOpt) throws IOException {
    // The iteration is done by FSEditLog itself
    throw new UnsupportedOperationException();
  }
  
  /**
   * Transition the non-file journals.
   */
  public void transitionNonFileJournals(StorageInfo nsInfo, boolean checkEmpty,
      Transition transition, StartupOption startOpt)
      throws IOException {
    for (JournalManager jm : getJournalManagers()) {
      if (!(jm instanceof FileJournalManager)) {
        if (checkEmpty && jm.hasSomeJournalData()) {
          LOG.warn("Journal " + jm + " is not empty.");
          continue;
        }
        LOG.info(transition + ": " + jm);
        jm.transitionJournal(nsInfo, transition, startOpt);
      }
    }
  }
  
  @Override
  public boolean hasSomeJournalData() throws IOException {
    // This is called individually on the underlying journals,
    // not on the JournalSet.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean hasSomeImageData() throws IOException {
    // This is called individually on the underlying journals,
    // not on the JournalSet.
    throw new UnsupportedOperationException();
  }
  
  /**
   * Get input stream from the given journal starting at txid.
   * Does not perform validation of the streams.
   * 
   * This should only be used for tailing inprogress streams!!!
   */
  public static EditLogInputStream getInputStream(JournalManager jm, long txid)
      throws IOException {
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    jm.selectInputStreams(streams, txid, true, false);
    if (streams.size() < 1) {
      throw new IOException("Cannot obtain stream for txid: " + txid);
    }
    Collections.sort(streams, JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    
    // we want the "oldest" available stream
    if (txid == HdfsConstants.INVALID_TXID) {
      return streams.get(0);
    }
    
    // we want a specific stream
    for (EditLogInputStream elis : streams) {
      if (elis.getFirstTxId() == txid) {
        return elis;
      }
    }
    // we cannot obtain the stream
    throw new IOException("Cannot obtain stream for txid: " + txid);
  }
  
  @Override
  public String toHTMLString() {
    return this.toString();
  }

  @Override
  public boolean hasImageStorage() {
    // we handle all journals in FSImage, withouth employing JournalSet
    throw new UnsupportedOperationException();
  }
  
  /**
   * Return all non-file journal managers.
   */
  public List<JournalManager> getNonFileJournalManagers() {
    List<JournalManager> list = new ArrayList<JournalManager>();
    for (JournalManager jm : getJournalManagers()) {
      if (!(jm instanceof FileJournalManager)) {
        list.add(jm);
      }
    }
    return list;
  }

  @Override
  public RemoteStorageState analyzeJournalStorage() {
    // this is done directly in FSImage
    throw new UnsupportedOperationException();
  }
}

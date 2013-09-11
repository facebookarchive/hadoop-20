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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * The server-side metrics for a journal from the JournalNode's perspective.
 */
class JournalMetrics implements Updater {
  private static Log LOG = LogFactory.getLog(JournalMetrics.class);
  final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsRecord metricsRecord;

  private final Journal journal;

  public MetricsTimeVaryingLong batchesWrittenWhileLagging;
  public MetricsTimeVaryingLong batchesWritten;
  public MetricsTimeVaryingLong bytesWritten;
  public MetricsTimeVaryingLong txnsWritten;
  private MetricsTimeVaryingRate syncTime;

  public MetricsLongValue lastWriterEpoch;
  public MetricsLongValue lastPromisedEpoch;
  public MetricsLongValue lastWrittenTxId;
  public MetricsLongValue currentTxnsLag;
  
  // http getJournal
  public MetricsTimeVaryingLong numGetJournalDoGet;
  public MetricsTimeVaryingLong numGetImageDoGet;
  public MetricsTimeVaryingLong sizeGetJournalDoGet;

  JournalMetrics(Journal journal) {
    this.journal = journal;

    // Create a record for NameNode metrics
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "journalnode");
    String journalId = journal.getJournalId();
    metricsRecord.setTag("journalid", journalId);
    metricsContext.registerUpdater(this);

    batchesWrittenWhileLagging = new MetricsTimeVaryingLong(
        "batchesWrittenWhileLagging_" + journalId, registry,
        "batchesWrittenWhileLagging");
    batchesWritten = new MetricsTimeVaryingLong("batchesWritten_" + journalId,
        registry, "batchesWritten");
    bytesWritten = new MetricsTimeVaryingLong("bytesWritten_" + journalId,
        registry, "bytesWritten");
    txnsWritten = new MetricsTimeVaryingLong("txnsWritten_" + journalId,
        registry, "txnsWritten");
    syncTime = new MetricsTimeVaryingRate("syncTimes_" + journalId, registry);

    lastWriterEpoch = new MetricsLongValue("lastWriterEpoch_" + journalId,
        registry);
    lastPromisedEpoch = new MetricsLongValue("lastPromisedEpoch_" + journalId,
        registry);
    lastWrittenTxId = new MetricsLongValue("lastWrittenTxId_" + journalId,
        registry);
    currentTxnsLag = new MetricsLongValue("currentTxnsLag_" + journalId,
        registry);
    
    // http related metrics
    numGetJournalDoGet = new MetricsTimeVaryingLong("numGetEditsServletDoGet_"
        + journalId, registry);
    numGetImageDoGet = new MetricsTimeVaryingLong("numGetImageServletDoGet_"
        + journalId, registry);
    sizeGetJournalDoGet = new MetricsTimeVaryingLong(
        "numListPathsServletDoGet_" + journalId, registry);

    LOG.info("Initializing JournalNodeMeterics using context object:"
        + metricsContext.getClass().getName());
  }

  public static JournalMetrics create(Journal j) {
    return new JournalMetrics(j);
  }

  String getName() {
    return "Journal-" + journal.getJournalId();
  }

  public void setLastWrittenTxId(long txid) {
    lastWrittenTxId.set(txid);
  }

  public void setCurrentTxnsLag(long value) {
    currentTxnsLag.set(value);
  }

  void addSync(long us) {
    syncTime.inc(us);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
}

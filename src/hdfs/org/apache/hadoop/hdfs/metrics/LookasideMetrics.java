package org.apache.hadoop.hdfs.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsLongValue;

import org.apache.hadoop.hdfs.LookasideCache;

public class LookasideMetrics implements Updater {
  public MetricsRegistry registry = new MetricsRegistry();

  public MetricsLongValue numAddCache =
    new MetricsLongValue("numAddCache", registry);
  public MetricsLongValue numAddNewCache =
    new MetricsLongValue("numAddNewCache", registry);
  public MetricsLongValue numAddExistingCache =
    new MetricsLongValue("numAddExistingCache", registry);
  public MetricsLongValue numRenameCache =
    new MetricsLongValue("numRenameCache", registry);
  public MetricsLongValue numRemoveCache =
    new MetricsLongValue("numRemoveCache", registry);
  public MetricsLongValue numEvictCache =
    new MetricsLongValue("numEvictCache", registry);
  public MetricsLongValue numGetAttempts =
    new MetricsLongValue("numGetAttempts", registry);
  public MetricsLongValue numGetHits =
    new MetricsLongValue("numGetHits", registry);

  /**
   * This is a class that is used to record metrics locally by a
   * LookasideCache implementation. No locking is essential,
   * all counters are declared volatile.
   */
  public static class LocalMetrics {
    public volatile long numAdd;  // total attempts to add elements
    public volatile long numAddNew;  // num attempts that added a new element
    public volatile long numAddExisting;  // addCache, already existing
    public volatile long numRename; // number of renames in cache
    public volatile long numRemove; // number or removeCache calls
    public volatile long numEvict;  // number of itesm evicted from cache
    public volatile long numGetAttempts; // total cache get attempts
    public volatile long numGetHits;     // num of hits in cache

    public LocalMetrics() {
    }

    // a copy and zero-out constructor
    public LocalMetrics(LocalMetrics old) {
      this.numAdd = old.numAdd; old.numAdd = 0;
      this.numAddNew = old.numAddNew; old.numAddNew = 0;
      this.numAddExisting = old.numAddExisting; old.numAddExisting = 0;
      this.numRename = old.numRename; old.numRename = 0;
      this.numRemove = old.numRemove; old.numRemove = 0;
      this.numEvict = old.numEvict; old.numEvict = 0;
      this.numGetAttempts = old.numGetAttempts; old.numGetAttempts = 0;
      this.numGetHits = old.numGetHits; old.numGetHits = 0;
    }
    
    public void reset() {
      this.numAdd = 0;
      this.numAddNew = 0;
      this.numAddExisting = 0;
      this.numRename = 0;
      this.numRemove = 0;
      this.numEvict = 0;
      this.numGetAttempts = 0;
      this.numGetHits = 0;
    }
  }

  private long numLsCalls = 0;
  private static Log log = LogFactory.getLog(LookasideMetrics.class);
  final MetricsRecord metricsRecord;

  public LookasideMetrics() {
    // Create a record for LookasideCache metrics
    MetricsContext metricsContext = MetricsUtil.getContext("lookasideCache");
    metricsRecord = MetricsUtil.createRecord(metricsContext,
                                             "LookasideFileSystem");
    metricsContext.registerUpdater(this);

  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {

    // get all statistic from the LooksideCache object,
    // this does not need any locking.
    LocalMetrics stats = LookasideCache.copyZeroLocalMetrics();

    synchronized (this) {
      if (stats != null) {
        this.numAddCache.set(stats.numAdd);
        this.numAddNewCache.set(stats.numAddNew);
        this.numAddExistingCache.set(stats.numAddExisting);
        this.numRenameCache.set(stats.numRename);
        this.numRemoveCache.set(stats.numRemove);
        this.numEvictCache.set(stats.numEvict);
        this.numGetAttempts.set(stats.numGetAttempts);
        this.numGetHits.set(stats.numGetHits);

        // push it
        this.numAddCache.pushMetric(this.metricsRecord);
        this.numAddNewCache.pushMetric(this.metricsRecord);
        this.numAddExistingCache.pushMetric(this.metricsRecord);
        this.numRenameCache.pushMetric(this.metricsRecord);
        this.numRemoveCache.pushMetric(this.metricsRecord);
        this.numEvictCache.pushMetric(this.metricsRecord);
        this.numGetAttempts.pushMetric(this.metricsRecord);
        this.numGetHits.pushMetric(this.metricsRecord);
      }
    }
    metricsRecord.update();
  }
}

package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;


public class FairSchedulerMetricsInst implements Updater {

  private final MetricsRecord metricsRecord;
  private final MetricsContext context = MetricsUtil.getContext("mapred");
  private final Map<String, MetricsRecord> poolToMetricsRecord;

  private long updatePeriod = 0;
  private long lastUpdateTime = 0;

  private int numPreemptMaps = 0;
  private long preemptMapWasteTime = 0L;
  private int numPreemptReduces = 0;
  private long preemptReduceWasteTime = 0L;
  private long updateThreadRunTime = 0;
  
  List<PoolInfo> poolInfos = new ArrayList<PoolInfo>();

  public FairSchedulerMetricsInst(FairScheduler scheduler, Configuration conf) {
    // Create a record for map-reduce metrics
    metricsRecord = MetricsUtil.createRecord(context, "fairscheduler");
    poolToMetricsRecord = new HashMap<String, MetricsRecord>();
    context.registerUpdater(this);

    updatePeriod = conf.getLong("mapred.fairscheduler.metric.update.period",
                                5 * 1000);  // default period is 5 seconds.
  }

  @Override
  public void doUpdates(MetricsContext context) {
    long now = JobTracker.getClock().getTime();
    if (now - lastUpdateTime > updatePeriod) {
      updateMetrics();
      lastUpdateTime = now;
    }
    updateCounters();
    metricsRecord.update();
    for (MetricsRecord mr : poolToMetricsRecord.values()) {
      mr.update();
    }
  }

  @SuppressWarnings("deprecation")
  public synchronized void preemptMap(TaskAttemptID taskAttemptID, long startTime) {
    ++numPreemptMaps;
    preemptMapWasteTime = JobTracker.getClock().getTime() - startTime;
  }

  @SuppressWarnings("deprecation")
  public synchronized void preemptReduce(TaskAttemptID taskAttemptID, long startTime) {
    ++numPreemptReduces;
    preemptReduceWasteTime = JobTracker.getClock().getTime() - startTime;
  }
  
  public synchronized void setUpdateThreadRunTime(long runTime) {
    updateThreadRunTime = runTime;
  }

  private void updateCounters() {
    synchronized (this) {
      metricsRecord.incrMetric("num_preempt_maps", numPreemptMaps);
      metricsRecord.incrMetric("num_preempt_reduces", numPreemptReduces);
      metricsRecord.incrMetric("map_preempt_waste_time", preemptMapWasteTime);
      metricsRecord.incrMetric("reduce_preempt_waste_time", preemptReduceWasteTime);
      metricsRecord.setMetric("update_thread_run_time", updateThreadRunTime);

      numPreemptMaps = 0;
      numPreemptReduces = 0;
      preemptMapWasteTime = 0L;
      preemptReduceWasteTime = 0L;
    }
  }

  private void updateMetrics() {

    int numActivePools = 0;
    int numStarvedPools = 0;
    int numStarvedJobs = 0;
    int totalMinReduces = 0;
    int totalMaxReduces = 0;
    int totalMinMaps = 0;
    int totalMaxMaps = 0;
    int totalRunningJobs = 0;
    long nonConfiguredFirstMapWaitTime = 0;
    long nonConfiguredFirstReduceWaitTime = 0;
    int nonConfiguredJobs = 0;

    synchronized (this) {
      for (PoolInfo info: poolInfos) {
        numStarvedJobs += info.numStarvedJobs;
        totalRunningJobs += info.runningJobs;
        totalMinMaps += info.minMaps;
        totalMinReduces += info.minReduces;
        if (info.maxMaps != Integer.MAX_VALUE) {
          totalMaxMaps += info.maxMaps;
        }
        if (info.maxReduces != Integer.MAX_VALUE) {
          totalMaxReduces += info.maxReduces;
        }
        if (info.isActive()) {
          ++numActivePools;
        }
        if (info.isStarved()) {
          ++numStarvedPools;
        }
        if (!info.isConfiguredPool) {
          nonConfiguredFirstMapWaitTime += info.totalFirstMapWaitTime;
          nonConfiguredFirstReduceWaitTime += info.totalFirstReduceWaitTime;
          nonConfiguredJobs += info.runningJobs;
        }
      }
      for (PoolInfo info : poolInfos) {
        submitPoolMetrics(info);
      }
    }
    long nonConfiguredAvgFirstMapWaitTime =
      nonConfiguredJobs == 0 ? 0:
      nonConfiguredFirstMapWaitTime / nonConfiguredJobs;
    long nonConfiguredAvgFirstReduceWaitTime =
      nonConfiguredJobs == 0 ? 0:
      nonConfiguredFirstReduceWaitTime / nonConfiguredJobs;

    metricsRecord.setMetric("num_active_pools", numActivePools);
    metricsRecord.setMetric("num_starved_pools", numStarvedPools);
    metricsRecord.setMetric("num_starved_jobs", numStarvedJobs);
    metricsRecord.setMetric("num_running_jobs", totalRunningJobs);
    metricsRecord.setMetric("total_min_maps", totalMinMaps);
    metricsRecord.setMetric("total_max_maps", totalMaxMaps);
    metricsRecord.setMetric("total_min_reduces", totalMinReduces);
    metricsRecord.setMetric("total_max_reduces", totalMaxReduces);
    metricsRecord.setMetric("adhoc_avg_first_map_wait_time", nonConfiguredAvgFirstMapWaitTime);
    metricsRecord.setMetric("adhoc_avg_first_reduce_wait_time", nonConfiguredAvgFirstReduceWaitTime);
  }

  private void submitPoolMetrics(PoolInfo info) {
    String pool = info.poolName.toLowerCase();
    MetricsRecord record = poolToMetricsRecord.get(pool);
    if (record == null) {
      record = MetricsUtil.createRecord(context, "pool-" + pool);
      FairScheduler.LOG.info("Create metrics record for pool:" + pool);
      poolToMetricsRecord.put(pool, record);
    }
    record.setMetric("min_map", info.minMaps);
    record.setMetric("min_reduce", info.minReduces);
    record.setMetric("max_map", info.maxMaps);
    record.setMetric("max_reduce", info.maxReduces);
    record.setMetric("running_map", info.runningMaps);
    record.setMetric("running_reduce", info.runningReduces);
    record.setMetric("runnable_map", info.runnableMaps);
    record.setMetric("runnable_reduce", info.runnableReduces);
  }

  static class PoolInfo {

    final String poolName;
    final boolean isConfiguredPool;
    final int runningJobs;
    final int minMaps;
    final int minReduces;
    final int maxMaps;
    final int maxReduces;
    final int runningMaps;
    final int runningReduces;
    final int runnableMaps;
    final int runnableReduces;
    final int numStarvedJobs;
    final long totalFirstMapWaitTime;
    final long totalFirstReduceWaitTime;

    PoolInfo(
        String poolName,
        boolean isConfiguredPool,
        int runningJobs,
        int minMaps,
        int minReduces,
        int maxMaps,
        int maxReduces,
        int runningMaps,
        int runningReduces,
        int runnableMaps,
        int runnableReduces,
        int numStarvedJobs,
        long totalFirstMapWaitTime,
        long totalFirstReduceWaitTime
        ) {
      this.poolName = poolName;
      this.isConfiguredPool = isConfiguredPool;
      this.runningJobs = runningJobs;
      this.minMaps = minMaps;
      this.minReduces = minReduces;
      this.maxMaps = maxMaps;
      this.maxReduces = maxReduces;
      this.runningMaps = runningMaps;
      this.runningReduces = runningReduces;
      this.runnableMaps = runnableMaps;
      this.runnableReduces = runnableReduces;
      this.numStarvedJobs = numStarvedJobs;
      this.totalFirstMapWaitTime = totalFirstMapWaitTime;
      this.totalFirstReduceWaitTime = totalFirstReduceWaitTime;
    }

    boolean isActive() {
      // A pool is active if
      // 1. It has a running job. Or
      // 2. It is defined in pool config file
      return !(runningJobs == 0 && minMaps == 0 && minReduces == 0 &&
          maxMaps == Integer.MAX_VALUE && maxReduces == Integer.MAX_VALUE &&
          runningMaps == 0 && runningReduces == 0);
    }

    boolean isStarved() {
      return ((runnableMaps > minMaps && runningMaps < minMaps) ||
              (runnableReduces > minReduces && runningReduces < minReduces));
    }
  }

  public synchronized void setPoolInfos(List<PoolInfo> poolInfos) {
    this.poolInfos = poolInfos;
  }
}

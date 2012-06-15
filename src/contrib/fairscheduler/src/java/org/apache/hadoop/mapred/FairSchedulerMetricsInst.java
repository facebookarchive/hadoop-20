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

  /** Used to find admission control metrics */
  private final JobInitializer jobInitializer;

  List<PoolInfo> poolInfos = new ArrayList<PoolInfo>();

  /**
   * Simple object to store the necessary admission control data
   */
  static class AdmissionControlData {
    private final int totalTasks;
    private final int softTaskLimit;
    private final int hardTaskLimit;

    /**
     * Only constructor.
     *
     * @param totalTasks Total tasks inited
     * @param softTaskLimit Soft task limit
     * @param hardTaskLimit Hard task limit
     */
    AdmissionControlData(
        int totalTasks, int softTaskLimit, int hardTaskLimit) {
      this.totalTasks = totalTasks;
      this.softTaskLimit = softTaskLimit;
      this.hardTaskLimit = hardTaskLimit;
    }

    public int getTotalTasks() {
      return totalTasks;
    }

    public int getSoftTaskLimit() {
      return softTaskLimit;
    }

    public int getHardTaskLimit() {
      return hardTaskLimit;
    }
  }

  public FairSchedulerMetricsInst(FairScheduler scheduler, Configuration conf) {
    // Create a record for map-reduce metrics
    metricsRecord = MetricsUtil.createRecord(context, "fairscheduler");
    poolToMetricsRecord = new HashMap<String, MetricsRecord>();
    context.registerUpdater(this);

    updatePeriod = conf.getLong("mapred.fairscheduler.metric.update.period",
                                5 * 1000);  // default period is 5 seconds.
    jobInitializer = scheduler.getJobInitializer();
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
    // Update the number with admission control stats
    AdmissionControlData admissionControlData =
        jobInitializer.getAdmissionControlData();
    metricsRecord.setMetric("inited_total_tasks",
        admissionControlData.totalTasks);
    metricsRecord.setMetric("inited_soft_task_limit",
        admissionControlData.softTaskLimit);
    metricsRecord.setMetric("inited_hard_task_limit",
        admissionControlData.hardTaskLimit);

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
    List<PoolMetadata> poolMetadataList = new ArrayList<PoolMetadata>();

    synchronized (this) {
      for (PoolInfo info: poolInfos) {
        PoolMetadata poolMetadata = new PoolMetadata(info.poolName);
        poolMetadata.addResourceMetadata("map",
            new ResourceMetadata(
                info.poolName, info.minMaps, info.maxMaps,
                info.runningMaps, info.runnableMaps));
        poolMetadata.addResourceMetadata("reduce",
            new ResourceMetadata(
                info.poolName, info.minReduces, info.maxReduces,
                info.runningReduces, info.runnableReduces));
        poolMetadataList.add(poolMetadata);

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
    PoolFairnessCalculator.calculateFairness(poolMetadataList, metricsRecord);
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
    metricsRecord.setMetric("adhoc_avg_first_map_wait_ms", nonConfiguredAvgFirstMapWaitTime);
    metricsRecord.setMetric("adhoc_avg_first_reduce_wait_ms", nonConfiguredAvgFirstReduceWaitTime);
  }

  private void submitPoolMetrics(PoolInfo info) {
    MetricsRecord record = poolToMetricsRecord.get(info.poolName);
    if (record == null) {
      record = MetricsUtil.createRecord(context, "pool-" + info.poolName);
      FairScheduler.LOG.info("Create metrics record for pool:" + info.poolName);
      poolToMetricsRecord.put(info.poolName, record);
    }
    record.setMetric("min_map", info.minMaps);
    record.setMetric("min_reduce", info.minReduces);
    record.setMetric("max_map", info.maxMaps);
    record.setMetric("max_reduce", info.maxReduces);
    record.setMetric("running_map", info.runningMaps);
    record.setMetric("running_reduce", info.runningReduces);
    record.setMetric("runnable_map", info.runnableMaps);
    record.setMetric("runnable_reduce", info.runnableReduces);
    record.setMetric("inited_tasks", info.initedTasks);
    record.setMetric("max_inited_tasks", info.maxInitedTasks);
    int runningJobs = info.runningJobs;
    record.setMetric("avg_first_map_wait_ms",
        (runningJobs == 0) ? 0 : info.totalFirstMapWaitTime / runningJobs);
    record.setMetric("avg_first_reduce_wait_ms",
        (runningJobs == 0) ? 0 : info.totalFirstReduceWaitTime / runningJobs);
  }

  static class PoolInfo {

    private final String poolName;
    private final boolean isConfiguredPool;
    private final int runningJobs;
    private final int minMaps;
    private final int minReduces;
    private final int maxMaps;
    private final int maxReduces;
    private final int runningMaps;
    private final int runningReduces;
    private final int runnableMaps;
    private final int runnableReduces;
    private final int initedTasks;
    private final int maxInitedTasks;
    private final int numStarvedJobs;
    private final long totalFirstMapWaitTime;
    private final long totalFirstReduceWaitTime;

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
        int initedTasks,
        int maxInitedTasks,
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
      this.initedTasks = initedTasks;
      this.maxInitedTasks = maxInitedTasks;
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

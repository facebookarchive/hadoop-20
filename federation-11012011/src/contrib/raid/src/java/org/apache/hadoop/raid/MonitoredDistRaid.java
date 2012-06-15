package org.apache.hadoop.raid;

public class MonitoredDistRaid {
  private long monitoredTimestampMillis;
  private DistRaid raidJob;

  /*
   * Constructor - receives the actual job as parameter
   */
  public MonitoredDistRaid(DistRaid job) {
    raidJob = job;
    monitoredTimestampMillis = RaidNode.now();
  }

  /*
   * Returns the internal "TTL" timer for this job
   */
  public long getMonitoredTimestampMillis() {
    return monitoredTimestampMillis;
  }

  /*
   * Returns the raid job being monitored
   */
  public DistRaid getRaidJob() {
    return raidJob;
  }
}

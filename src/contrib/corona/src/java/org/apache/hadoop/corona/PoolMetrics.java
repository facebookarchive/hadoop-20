package org.apache.hadoop.corona;

import java.util.LinkedHashMap;

public class PoolMetrics {
  final String poolId;
  final String type;
  final LinkedHashMap<MetricName, Long> counters;

  public enum MetricName {
    SESSIONS("Sessions"),
    GRANTED("Granted"),
    REQUESTED("Requested"),
    SHARE("Share"),
    MIN("Min"),
    MAX("Max"),
    WEIGHT("Weight"),
    STARVING("StarvingTime");
    
    final private String title;
    MetricName(String title) {
      this.title = title;
    }
    public String toString() {
      return title;
    }
  }
  public PoolMetrics(String poolId, String type) {
    this.poolId = poolId;
    this.type = type;
    this.counters = new LinkedHashMap<MetricName, Long>();
  }
  public void setCounter(MetricName name, long value) {
    counters.put(name, value);
  }
  public long getCounter(MetricName name) {
    return counters.get(name);
  }
}

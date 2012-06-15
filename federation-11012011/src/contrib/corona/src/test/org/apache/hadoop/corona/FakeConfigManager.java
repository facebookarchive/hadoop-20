package org.apache.hadoop.corona;

import java.util.HashMap;
import java.util.Map;


/**
 * A fake {@link ConfigManager} that allows set the configuration manually
 */
public class FakeConfigManager extends ConfigManager {

  private final Map<String, Object> config = new HashMap<String, Object>();
  private static final String SEPARATOR = "" + ((char)1);

  private enum PROPERTY {
    MAX, MIN, WEIGHT, PREEMPTED_TASKS_MAX_RUNNING_TIME,
    COMPARATOR, SHARE_STARVING_RATIO, STARVING_TIME_FOR_SHARE,
    STARVING_TIME_FOR_MIN, LOCALITY_WAIT;

    final private static String POSTFIX = "" + ((char)2);
    @Override
    public String toString() {
      return super.toString() + POSTFIX;
    }
  }

  public FakeConfigManager() {
  }

  @Override
  public void start() {
  }

  @Override
  public void close() {
  }


  public void setMaximum(String name, String type, int val) {
    String key = PROPERTY.MAX + name + SEPARATOR + type;
    config.put(key, val);
  }

  @Override
  public int getMaximum(String name, String type) {
    String key = PROPERTY.MAX + name + SEPARATOR + type;
    if (!config.containsKey(key)) {
      return Integer.MAX_VALUE;
    }
    return (Integer)config.get(key);
  }

  public void setMinimum(String name, String type, int val) {
    String key = PROPERTY.MIN + name + SEPARATOR + type;
    config.put(key, val);
  }

  @Override
  public int getMinimum(String name, String type) {
    String key = PROPERTY.MIN + name + SEPARATOR + type;
    if (!config.containsKey(key)) {
      return 0;
    }
    return (Integer)config.get(key);
  }

  public void setWeight(String name, double val) {
    String key = PROPERTY.WEIGHT + name;
    config.put(key, val);
  }

  @Override
  public double getWeight(String name) {
    String key = PROPERTY.WEIGHT + name;
    if (!config.containsKey(key)) {
      return 1.0;
    }
    return (Integer)config.get(key);
  }

  public void setComparator(String name, ScheduleComparator val) {
    String key = PROPERTY.COMPARATOR + name;
    config.put(key, val);
  }

  @Override
  public ScheduleComparator getComparator(String name) {
    String key = PROPERTY.COMPARATOR + name;
    if (!config.containsKey(key)) {
      return ScheduleComparator.FIFO;
    }
    return (ScheduleComparator)config.get(key);
  }

  public void setPreemptedTaskMaxRunningTime(long val) {
    config.put(PROPERTY.PREEMPTED_TASKS_MAX_RUNNING_TIME.toString(), val);
  }

  @Override
  public long getPreemptedTaskMaxRunningTime() {
    String key = PROPERTY.PREEMPTED_TASKS_MAX_RUNNING_TIME.toString();
    if (!config.containsKey(key)) {
      return Long.MAX_VALUE;
    }
    return (Long)config.get(key);
  }

  public void setShareStarvingRatio(double val) {
    config.put(PROPERTY.SHARE_STARVING_RATIO.toString(), val);
  }

  @Override
  public double getShareStarvingRatio() {
    String key = PROPERTY.SHARE_STARVING_RATIO.toString();
    if (!config.containsKey(key)) {
      return 0.5;
    }
    return (Double)config.get(key);
  }

  public void setStarvingTimeForShare(long val) {
    config.put(PROPERTY.STARVING_TIME_FOR_SHARE.toString(), val);
  }

  @Override
  public long getStarvingTimeForShare() {
    String key = PROPERTY.STARVING_TIME_FOR_SHARE.toString();
    if (!config.containsKey(key)) {
      return Long.MAX_VALUE;
    }
    return (Long)config.get(key);
  }

  public void setStarvingTimeForMinimum(long val) {
    config.put(PROPERTY.STARVING_TIME_FOR_MIN.toString(), val);
  }

  @Override
  public long getStarvingTimeForMinimum() {
    String key = PROPERTY.STARVING_TIME_FOR_MIN.toString();
    if (!config.containsKey(key)) {
      return Long.MAX_VALUE;
    }
    return (Long)config.get(key);
  }

  public void setLocalityWait(String type, LocalityLevel level, long val) {
    String key = PROPERTY.LOCALITY_WAIT + type + SEPARATOR + level; 
    config.put(key, val);
  }

  @Override
  public long getLocalityWait(String type, LocalityLevel level) {
    String key = PROPERTY.LOCALITY_WAIT + type + SEPARATOR + level; 
    if (!config.containsKey(key)) {
      return 0L;
    }
    return (Long)config.get(key);
  }
}

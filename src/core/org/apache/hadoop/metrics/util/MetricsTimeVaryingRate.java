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
package org.apache.hadoop.metrics.util;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The MetricsTimeVaryingRate class is for a rate based metric that
 * naturally varies over time (e.g. time taken to create a file).
 * The rate is averaged at each interval heart beat (the interval
 * is set in the metrics config file).
 * This class also keeps track of the min and max rates along with 
 * a method to reset the min-max.
 *
 */
public class MetricsTimeVaryingRate extends MetricsBase {

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");

  static class Metrics {
    int numOperations = 0;
    long time = 0;  // total time or average time

    void set(final Metrics resetTo) {
      numOperations = resetTo.numOperations;
      time = resetTo.time;
    }
    
    void reset() {
      numOperations = 0;
      time = 0;
    }
  }
  
  static class MinMax {
    long minTime = -1;
    long maxTime = 0;
    
    void set(final MinMax newVal) {
      minTime = newVal.minTime;
      maxTime = newVal.maxTime;
    }
    
    void reset() {
      minTime = -1;
      maxTime = 0;
    }
    void update(final long time) { // update min max
      minTime = (minTime == -1) ? time : Math.min(minTime, time);
      minTime = Math.min(minTime, time);
      maxTime = Math.max(maxTime, time);
    }
  }
  private Metrics currentData;
  private Metrics previousIntervalData;
  private MinMax minMax;
  /** Print the min/max? */
  private final boolean printMinMax;
  final private ReentrantLock lock;
  private MinMax previousIntervalMinMax;

  /**
   * Constructor - create a new metric
   * @param name the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public MetricsTimeVaryingRate(final String name,
                                final MetricsRegistry registry) {
    this(name, registry, NO_DESCRIPTION, true);
  }

  /**
   * Constructor - create a new metric
   * @param name the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * @param description - description of the metric
   */
  public MetricsTimeVaryingRate(final String name, final MetricsRegistry
      registry, final String description) {
    this(name, registry, description, true);
  }

  /**
   * Constructor - create a new metric
   * @param name the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * @param description - description of the metric
   * @param printMinMax - Print the min-max for this metric?
   */
  public MetricsTimeVaryingRate(final String name, final MetricsRegistry
      registry, final String description, final boolean printMinMax) {
    super(name, description);
    currentData = new Metrics();
    previousIntervalData = new Metrics();
    minMax = new MinMax();
    previousIntervalMinMax = new MinMax();
    this.printMinMax = printMinMax;
    lock = new ReentrantLock(false); 
    registry.add(name, this);
  }

   /**
   * Increment the metrics for numOps operations
   * @param numOps - number of operations
   * @param time - time for numOps operations
   */
  public void inc(final int numOps, final long time) {
    lock.lock();
    try {
      currentData.numOperations += numOps;
      currentData.time += time;
      long timePerOps = time/numOps;
      minMax.update(timePerOps);
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Increment the metrics for one operation
   * @param time for one operation
   */
  public void inc(final long time) {
    lock.lock();
    try {
      currentData.numOperations++;
      currentData.time += time;
      minMax.update(time);
    } finally {
      lock.unlock();
    }
  }

  private void intervalHeartBeat() {
    lock.lock();
    try {
      previousIntervalData.numOperations = currentData.numOperations;
      previousIntervalData.time = (currentData.numOperations == 0) ?
                              0 : currentData.time / currentData.numOperations;
      currentData.reset();
      previousIntervalMinMax.maxTime = minMax.maxTime;
      previousIntervalMinMax.minTime = minMax.minTime;
    } finally {
      lock.unlock();
    }
     
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #getPreviousIntervalAverageTime()} and
   * {@link #getPreviousIntervalNumOps()}
   *
   * @param mr metrics record.  If null, simply interval heart beat only.
   */
  public void pushMetric(final MetricsRecord mr) {
    lock.lock();
    try {
      intervalHeartBeat();
      try {
        if (mr != null) {
          mr.incrMetric(getName() + "_num_ops", getPreviousIntervalNumOps());
          mr.setMetric(getName() + "_avg_time", getPreviousIntervalAverageTime());
          if (printMinMax) {
            mr.setMetric(getName() + "_min", getMinTime());
            mr.setMetric(getName() + "_max", getMaxTime());
            resetMinMax();
          }
        }
      } catch (Exception e) {
        LOG.info("pushMetric failed for " + getName() + "\n" +
            StringUtils.stringifyException(e));
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * The number of operations in the previous interval
   * @return - ops in prev interval
   */
  public int getPreviousIntervalNumOps() { 
    lock.lock();
    try {
      return previousIntervalData.numOperations;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * The number of operations in the current interval
   * @return - ops in current interval
   */
  public int getCurrentIntervalNumOps() {
    lock.lock();
    try {
      return currentData.numOperations;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The average rate of an operation in the previous interval
   * @return - the average rate.
   */
  public long getPreviousIntervalAverageTime() {
    lock.lock();
    try {
      return previousIntervalData.time;
    } finally {
      lock.unlock();
    }
    
  } 
  
  /**
   * The min time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return min time for an operation
   */
  public long getMinTime() {
    lock.lock();
    try {
      if (printMinMax) {
        return previousIntervalMinMax.minTime;
      }
      return minMax.minTime;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * The max time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return max time for an operation
   */
  public long getMaxTime() {
    lock.lock();
    try {
      if (printMinMax) {
        return previousIntervalMinMax.maxTime;
      }
      return minMax.maxTime;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Reset the min max values
   */
  public void resetMinMax() {
    lock.lock();
    try {
      minMax.reset();
    } finally {
      lock.unlock();
    }
  }
}

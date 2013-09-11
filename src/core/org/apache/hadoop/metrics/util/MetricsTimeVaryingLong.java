package org.apache.hadoop.metrics.util;

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

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The MetricsTimeVaryingLong class is for a metric that naturally
 * varies over time (e.g. number of files created). The metrics is accumulated
 * over an interval (set in the metrics config file); the metrics is
 *  published at the end of each interval and then 
 * reset to zero. Hence the counter has the value in the current interval. 
 * 
 * Note if one wants a time associated with the metric then use
 * @see org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
 *
 */
public class MetricsTimeVaryingLong extends MetricsBase{

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");
 
  private long currentValue;
  private long previousIntervalValue;
  final private ReentrantLock lock;
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public MetricsTimeVaryingLong(final String nam, MetricsRegistry registry, final String description) {
    super(nam, description);
    currentValue = 0;
    previousIntervalValue = 0;
    lock = new ReentrantLock(false); 
    registry.add(nam, this);
}
  
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public MetricsTimeVaryingLong(final String nam, MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);
  }
  
  /**
   * Inc metrics for incr vlaue
   * @param incr - number of operations
   */
  public void inc(final long incr) {
    lock.lock();
    try {
      currentValue += incr;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Inc metrics by one
   */
  public void inc() {
    lock.lock();
    try {
      currentValue++;
    } finally {
      lock.unlock();
    }
  }

  private void intervalHeartBeat() {
    lock.lock();
    try {
     previousIntervalValue = currentValue;
     currentValue = 0;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #previousIntervalValue}
   *
   * @param mr
   */
  public void pushMetric(final MetricsRecord mr) {
    lock.lock();
    try {
      intervalHeartBeat();
      try {
        mr.incrMetric(getName(), getPreviousIntervalValue());
      } catch (Exception e) {
        LOG.info("pushMetric failed for " + getName() + "\n" +
            StringUtils.stringifyException(e));
      }
    } finally {
      lock.unlock();
    }
  }
  
  
  /**
   * The Value at the Previous interval
   * @return prev interval value
   */
  public long getPreviousIntervalValue() { 
    lock.lock();
    try {
      return previousIntervalValue;
    } finally {
      lock.unlock();
    }
  } 
  
  /**
   * The Value at the current interval
   * @return prev interval value
   */
  public synchronized long getCurrentIntervalValue() { 
    lock.lock();
    try {
      return currentValue;
    } finally {
      lock.unlock();
    }
  } 
}

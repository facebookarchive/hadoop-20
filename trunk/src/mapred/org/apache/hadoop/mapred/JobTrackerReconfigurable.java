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

package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;

public class JobTrackerReconfigurable extends ReconfigurableBase {
  public static final String MAX_TRACKER_BLACKLISTS_PROPERTY =
    "mapred.max.tracker.blacklists";
  public static final int DEFAULT_MAX_TRACKER_BLACKLISTS = 4;
  public static final String MAX_UNIQUE_COUNTER_NAMES =
    "mapred.jobtracker.max.unique.counter.names";
  public static final int DEFAULT_MAX_UNIQUE_COUNTER_NAMES = 1000 * 1000;
  /** Initial refresh ms timeout for a job */
  public static final String INITIAL_JOB_REFRESH_TIMEOUT_MS_PROPERTY =
      "mapred.jobtracker.job.refresh.timeout.ms";
  /** Default refresh timeout in ms*/
  public static final long DEFAULT_INITIAL_JOB_REFRESH_TIMEOUT_MS = 5000L;

  /** Class logger  */
  private static final Log LOG =
      LogFactory.getLog(JobTrackerReconfigurable.class);
  /** Initial job refresh timeout in milliseconds */
  private volatile long initialJobRefreshTimeoutMs =
      DEFAULT_INITIAL_JOB_REFRESH_TIMEOUT_MS;
  private volatile int maxTrackerBlacklists = DEFAULT_MAX_TRACKER_BLACKLISTS;
  private volatile int maxUniqueCounterNames = DEFAULT_MAX_UNIQUE_COUNTER_NAMES;

  List<String> reconfigurableProperties = new ArrayList<String>();

  @SuppressWarnings("deprecation")
  private JobConf jobConf;

  @SuppressWarnings("deprecation")
  public JobTrackerReconfigurable(JobConf jobConf) {
    super(jobConf);
    this.jobConf = jobConf;
    maxTrackerBlacklists = getConf().getInt(
      MAX_TRACKER_BLACKLISTS_PROPERTY, maxTrackerBlacklists);
    reconfigurableProperties.add(MAX_TRACKER_BLACKLISTS_PROPERTY);
    reconfigurableProperties.add(MAX_UNIQUE_COUNTER_NAMES);
    // Just need to make this reconfigurable. The next job will pick it up.
    reconfigurableProperties.add(JobConf.MAPRED_MAX_TRACKER_FAILURES_PROPERTY);
    reconfigurableProperties.add(INITIAL_JOB_REFRESH_TIMEOUT_MS_PROPERTY);
  }

  @Override
  public Collection<String> getReconfigurableProperties() {
    return reconfigurableProperties;
  }

  @Override
  protected void reconfigurePropertyImpl(String property, String newVal)
    throws ReconfigurationException {
    try {
      if (property.equals(MAX_TRACKER_BLACKLISTS_PROPERTY)) {
        LOG.info("changing maxTrackerBlacklists to " + newVal +
            " from " + initialJobRefreshTimeoutMs);
        maxTrackerBlacklists = Integer.parseInt(newVal);
      } else if (property.equals(MAX_UNIQUE_COUNTER_NAMES)) {
        LOG.info("changing maxUniqueCounterNames to " + newVal +
            " from " + initialJobRefreshTimeoutMs);
        maxUniqueCounterNames = Integer.parseInt(newVal);
      } else if (property.equals(INITIAL_JOB_REFRESH_TIMEOUT_MS_PROPERTY)) {
        LOG.info("changing initialJobRefreshTimeoutMs to " + newVal +
            " from " + initialJobRefreshTimeoutMs);
        initialJobRefreshTimeoutMs = Long.parseLong(newVal);
      }
    } catch (NumberFormatException e) {
      LOG.warn("reconfigurePropertyImpl: Invalid property " + property +
          " or newVal " + newVal);
    }
  }

  /**
   * The maximum number of blacklists for a tracker after which the
   * tracker could be blacklisted across all jobs.
   */
  public int getMaxBlacklistsPerTracker() {
    return maxTrackerBlacklists;
  }

  /**
   * The maximum number of unique counter names kept. \see CounterNames.
   */
  public int getMaxUniqueCounterNames() {
    return maxUniqueCounterNames;
  }

  public long getInitialJobRefreshTimeoutMs() {
    return initialJobRefreshTimeoutMs;
  }

  @SuppressWarnings("deprecation")
  public JobConf getJobConf() {
    return jobConf;
  }
}

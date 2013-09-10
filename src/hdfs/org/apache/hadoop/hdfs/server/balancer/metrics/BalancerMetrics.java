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
package org.apache.hadoop.hdfs.server.balancer.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

import java.net.InetSocketAddress;

/**
 * This class is for maintaining the various Balancer activity statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #bytesMoved}.inc()
 */

public class BalancerMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  public MetricsRegistry registry = new MetricsRegistry();
  private BalancerActivityMBean balancerActivityMBean;

  public MetricsTimeVaryingLong bytesMoved = new MetricsTimeVaryingLong("bytes_moved", registry);
  public MetricsTimeVaryingLong blocksFetched = new MetricsTimeVaryingLong("blocks_fetched",
      registry);
  public MetricsTimeVaryingLong blocksFetchedAndDropped = new MetricsTimeVaryingLong
      ("blocks_fetched_and_dropped", registry);

  public BalancerMetrics(Configuration conf, InetSocketAddress namenodeAddress) {
    String sessionId = conf.get("session.id");
    JvmMetrics.init("Balancer", sessionId);
    balancerActivityMBean = new BalancerActivityMBean(registry, conf, namenodeAddress);

    MetricsContext context = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(context, "balancer");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }

  public void shutdown() {
    if (balancerActivityMBean != null)
      balancerActivityMBean.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
}

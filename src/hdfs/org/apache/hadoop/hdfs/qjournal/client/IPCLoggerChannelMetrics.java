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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import com.google.common.collect.Maps;

/**
 * The metrics for a journal from the writer's perspective.
 */
class IPCLoggerChannelMetrics implements Updater {

  private volatile IPCLoggerChannel ch;
  private final MetricsRecord metricsRecord;
  final MetricsRegistry registry = new MetricsRegistry();

  private MetricsTimeVaryingRate writeEndToEndLatency;
  private MetricsTimeVaryingRate writeRpcLatency;

  private MetricsLongValue currentQueuedEditsSizeBytes;
  private MetricsLongValue currentLagTransactions;
  private MetricsLongValue currentLagTimeMicros;
  private MetricsIntValue isOutOfSync;

  /**
   * In the case of the NN transitioning between states, edit logs are closed
   * and reopened. Thus, the IPCLoggerChannel instance that writes to a given
   * JournalNode may change over the lifetime of the process. However, metrics2
   * doesn't have a function to unregister a set of metrics and fails if a new
   * metrics class is registered with the same name as the existing one. Hence,
   * we have to maintain our own registry ("multiton") here, so that we have
   * exactly one metrics instance per JournalNode, and switch out the pointer to
   * the underlying IPCLoggerChannel instance.
   */
  private static final Map<String, IPCLoggerChannelMetrics> REGISTRY = Maps
      .newHashMap();

  private IPCLoggerChannelMetrics(IPCLoggerChannel ch,
      MetricsRecord metricRecords, String name) {
    this.ch = ch;
    this.metricsRecord = metricRecords;

    writeEndToEndLatency = new MetricsTimeVaryingRate("writeEndToEndLatency_"
        + name, registry);
    writeRpcLatency = new MetricsTimeVaryingRate("writeRpcLatency_" + name,
        registry);

    currentQueuedEditsSizeBytes = new MetricsLongValue(
        "currentQueuedEditsSizeBytes_" + name, registry);
    currentLagTransactions = new MetricsLongValue("currentLagTransactions_"
        + name, registry);
    currentLagTimeMicros = new MetricsLongValue("currentLagTimeMicros_" + name,
        registry);
    isOutOfSync = new MetricsIntValue("isOutOfSync_" + name, registry);
  }

  private void setChannel(IPCLoggerChannel ch) {
    assert ch.getRemoteAddress().equals(this.ch.getRemoteAddress());
    this.ch = ch;
  }

  static IPCLoggerChannelMetrics create(IPCLoggerChannel ch) {
    String name = getName(ch);

    synchronized (REGISTRY) {
      IPCLoggerChannelMetrics m = REGISTRY.get(name);
      if (m != null) {
        m.setChannel(ch);
      } else {
        MetricsContext metricsContext = MetricsUtil.getContext("dfs");
        MetricsRecord metricsRecord = MetricsUtil.createRecord(metricsContext,
            "loggerchannel");
        metricsRecord.setTag("loggerchannel", name);
        m = new IPCLoggerChannelMetrics(ch, metricsRecord, name);
        metricsContext.registerUpdater(m);
        REGISTRY.put(name, m);
      }
      return m;
    }
  }

  private static String getName(IPCLoggerChannel ch) {
    InetSocketAddress addr = ch.getRemoteAddress();
    String addrStr = addr.getAddress().getHostAddress();

    // IPv6 addresses have colons, which aren't allowed as part of
    // MBean names. Replace with '.'
    addrStr = addrStr.replace(':', '.');
    
    return "qjmchannel-" + addrStr + "-" + addr.getPort();
  }

  public void setOutOfSync(boolean value) {
    isOutOfSync.set(value ? 1 : 0);
  }

  public void setCurrentLagTimeMicros(long value) {
    currentLagTimeMicros.set(value);
  }

  public void setCurrentLagTransactions(long value) {
    currentLagTransactions.set(value);
  }

  public void setCurrentQueuedEditsSizeBytes(long value) {
    currentQueuedEditsSizeBytes.set(value);
  }

  public void addWriteEndToEndLatency(long micros) {
    writeEndToEndLatency.inc(micros);
  }

  public void addWriteRpcLatency(long micros) {
    writeRpcLatency.inc(micros);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {

      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
}

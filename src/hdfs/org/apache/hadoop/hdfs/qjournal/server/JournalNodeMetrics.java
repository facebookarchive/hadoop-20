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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class JournalNodeMetrics implements Updater {
  public static final Log LOG = 
      LogFactory.getLog(JournalNodeMetrics.class);
  private MetricsRecord metricsRecord;
  private JournalNodeActivityMBean journalNodeActivityMBean;
  public MetricsRegistry registry = new MetricsRegistry();
  
  // ServerCore metrics
  public MetricsIntValue numJournals =
      new MetricsIntValue("numJournals", registry,
          "The number of clients currently registered on the server.");
 
  public JournalNodeMetrics(Configuration conf, String serverId) {
    String sessionId = conf.get("session.id"); 
    JvmMetrics.init("JournalNode", sessionId);
    
    journalNodeActivityMBean = new JournalNodeActivityMBean(registry,
        "" + serverId);
    
    MetricsContext context = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(context, "journalnode");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);

    LOG.info("Initializing JournalNodeMetrics using context object:" +
        context.getClass().getName() + " and record: " +
        metricsRecord.getClass().getCanonicalName());
  }
  
  
  public void shutdown() {
    if (journalNodeActivityMBean != null) 
      journalNodeActivityMBean.shutdown();
  }
  
  
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
  
  public void resetAllMinMax() {
    
  }
}

/*
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
package org.apache.hadoop.corona;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

class ClusterManagerMetrics implements Updater{

  static final String CONTEXT_NAME = "clustermanager";
  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry = new MetricsRegistry();
  private final Map<String, MetricsTimeVaryingLong> typeToResourceRequested;
  private final Map<String, MetricsTimeVaryingLong> typeToResourceGranted;
  private final Map<String, MetricsTimeVaryingLong> typeToResourceRevoked;
  private final Map<String, MetricsTimeVaryingLong> typeToResourceReleased;
  private final Map<String, MetricsIntValue> typeToPendingCount;
  private final Map<String, MetricsIntValue> typeToRunningCount;
  private final Map<String, MetricsIntValue> typeToTotalSlots;
  private final Map<String, MetricsIntValue> typeToFreeSlots;
  private final MetricsIntValue aliveNodes;
  private final Map<SessionStatus, MetricsTimeVaryingInt> sessionStatusToMetrics;
  private final MetricsIntValue numRunningSessions;
  private final MetricsTimeVaryingInt totalSessionCount;

  public void setPendingRequestCount(String resourceType, int pending) {
    typeToPendingCount.get(resourceType).set(pending);
  }

  public void setRunningRequestCount(String resourceType, int running) {
    typeToRunningCount.get(resourceType).set(running);
  }

  public void setTotalSlots(String resourceType, int totalSlots) {
    typeToTotalSlots.get(resourceType).set(totalSlots);
  }

  public void setFreeSlots(String resourceType, int freeSlots) {
    typeToFreeSlots.get(resourceType).set(freeSlots);
  }

  public void requestResource(String type) {
    typeToResourceRequested.get(type).inc();
  }

  public void releaseResource(String type) {
    typeToResourceReleased.get(type).inc();
  }

  public void grantResource(String type) {
    typeToResourceGranted.get(type).inc();
  }

  public void revokeResource(String type) {
    typeToResourceRevoked.get(type).inc();
  }


  public void setAliveNodes(int numAlive) {
    aliveNodes.set(numAlive);
  }

  public void setNumRunningSessions(int num) {
    numRunningSessions.set(num);
  }

  public void sessionStart() {
    totalSessionCount.inc();
  }

  public void sessionEnd(SessionStatus finishState) {
    if (sessionStatusToMetrics.containsKey(finishState)) {
      sessionStatusToMetrics.get(finishState).inc();
    } else {
      throw new IllegalArgumentException("Invalid end state " + finishState);
    }
  }

  public ClusterManagerMetrics(Collection<String> types) {
    context = MetricsUtil.getContext(CONTEXT_NAME);
    metricsRecord = MetricsUtil.createRecord(context, CONTEXT_NAME);
    context.registerUpdater(this);
    typeToResourceRequested = createTypeToResourceCountMap(types, "requested");
    typeToResourceGranted = createTypeToResourceCountMap(types, "granted");
    typeToResourceRevoked = createTypeToResourceCountMap(types, "revoked");
    typeToResourceReleased = createTypeToResourceCountMap(types, "released");
    typeToPendingCount = createTypeToCountMap(types, "pending");
    typeToRunningCount = createTypeToCountMap(types, "running");
    typeToTotalSlots = createTypeToCountMap(types, "total");
    typeToFreeSlots = createTypeToCountMap(types, "free");
    sessionStatusToMetrics = createSessionStatusToMetricsMap();
    aliveNodes = new MetricsIntValue("alive_nodes", registry);
    numRunningSessions = new MetricsIntValue("num_running_sessions", registry);
    totalSessionCount = new MetricsTimeVaryingInt("total_sessions", registry);
  }

  private Map<SessionStatus, MetricsTimeVaryingInt>
      createSessionStatusToMetricsMap() {
    Map<SessionStatus, MetricsTimeVaryingInt> m =
      new HashMap<SessionStatus, MetricsTimeVaryingInt>();
    for (SessionStatus endState: new SessionStatus[]{
                                            SessionStatus.SUCCESSFUL,
                                            SessionStatus.KILLED,
                                            SessionStatus.FAILED,
                                            SessionStatus.TIMED_OUT}) {
      String name = endState.toString().toLowerCase() + "_sessions";
      m.put(endState, new MetricsTimeVaryingInt(name, registry));
    }
    return m;
  }

  private Map<String, MetricsIntValue> createTypeToCountMap(
      Collection<String> resourceTypes, String actionType) {
    Map<String, MetricsIntValue> m = new HashMap<String, MetricsIntValue>();
    for (String t : resourceTypes) {
      String name = (actionType + "_" + t).toLowerCase();
      MetricsIntValue value = new MetricsIntValue(name, registry);
      m.put(t, value);
    }
    return m;
  }

  private Map<String, MetricsTimeVaryingLong> createTypeToResourceCountMap(
      Collection<String> resourceTypes, String actionType) {
    Map<String, MetricsTimeVaryingLong> m =
        new HashMap<String, MetricsTimeVaryingLong>();
    for (String t : resourceTypes) {
      String name = (actionType + "_" + t).toLowerCase();
      MetricsTimeVaryingLong value = new MetricsTimeVaryingLong(name, registry);
      m.put(t, value);
    }
    return m;
  }

  @Override
  public void doUpdates(MetricsContext context) {
    // Not synchronized on the ClusterManagerMetrics object.
    // The list of metrics in the registry is modified only in the constructor.
    // And pushMetrics() is thread-safe.
    for (MetricsBase m : registry.getMetricsList()) {
      m.pushMetric(metricsRecord);
    }

    metricsRecord.update();
  }
}

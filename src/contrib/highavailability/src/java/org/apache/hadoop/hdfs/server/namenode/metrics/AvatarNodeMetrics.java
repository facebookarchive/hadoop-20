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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

public class AvatarNodeMetrics {

  private final static String namePref = "av";

  NameNodeMetrics metrics;

  // 0/1 metric telling if the datanodes are being ignored
  public MetricsIntValue ignoreDataNodes;
  // how many datanodes are ignored
  public MetricsTimeVaryingLong numIgnoredDatanodes;

  // for incremental block report, how many blocks are reported by datanodes
  public MetricsTimeVaryingLong numReportedBlocks;
  // how many of these are sent for retry
  public MetricsTimeVaryingLong numRetryBlocks;

  // monitor state of the cleaner thread
  public MetricsTimeVaryingLong numCleanerThreadExceptions;

  // number of checkpoint and ingest failures
  public MetricsIntValue numIngestFailures;
  public MetricsIntValue numCheckpointFailures;

  public NameNodeMetrics getNameNodeMetrics() {
    return metrics;
  }

  public AvatarNodeMetrics(NameNodeMetrics metrics) {
    this.metrics = metrics;

    ignoreDataNodes = new MetricsIntValue(namePref + "IgnoreDatanodes",
        metrics.registry, "Ignoring datanodes");
    numIgnoredDatanodes = new MetricsTimeVaryingLong(namePref
        + "NumIgnoredDatanodes", metrics.registry,
        "Number of ignored datanodes");

    numReportedBlocks = new MetricsTimeVaryingLong(namePref
        + "NumReportedBlocks", metrics.registry,
        "Blocks reported through incremental block reports");
    numRetryBlocks = new MetricsTimeVaryingLong(namePref + "NumRetryBlocks",
        metrics.registry, "Blocks retried for incremental block reports");

    numCleanerThreadExceptions = new MetricsTimeVaryingLong(namePref
        + "NumCleanerThreadExceptions", metrics.registry,
        "Exceptions when clearing deletion queues");

    numIngestFailures = new MetricsIntValue(namePref + "NumIngestFailures",
        metrics.registry, "Number of ingest failures");
    numCheckpointFailures = new MetricsIntValue(namePref
        + "NumCheckpointFailures", metrics.registry,
        "Number of checkpoint failures");
  }
}

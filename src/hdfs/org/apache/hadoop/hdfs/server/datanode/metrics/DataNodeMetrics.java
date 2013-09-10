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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.CountingLogger.ErrorCounter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;


/**
 * 
 * This class is for maintaining  the various DataNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
public class DataNodeMetrics implements Updater, ErrorCounter {
  private final MetricsRecord metricsRecord;
  private DataNodeActivityMBean datanodeActivityMBean;
  public MetricsRegistry registry = new MetricsRegistry();

  public MetricsTimeVaryingInt loggedErrors =
      new MetricsTimeVaryingInt("logged_errors", registry);
  public MetricsTimeVaryingInt loggedWarnings =
      new MetricsTimeVaryingInt("logged_warnings", registry);

  public MetricsTimeVaryingLong bytesWritten = 
                      new MetricsTimeVaryingLong("bytes_written", registry);
  public MetricsTimeVaryingLong bytesRead = 
                      new MetricsTimeVaryingLong("bytes_read", registry);
  public MetricsTimeVaryingInt blocksWritten = 
                      new MetricsTimeVaryingInt("blocks_written", registry);
  public MetricsTimeVaryingInt blocksRead = 
                      new MetricsTimeVaryingInt("blocks_read", registry);
  public MetricsTimeVaryingInt blocksReplicated =
                      new MetricsTimeVaryingInt("blocks_replicated", registry);
  public MetricsTimeVaryingInt blocksRemoved =
                       new MetricsTimeVaryingInt("blocks_removed", registry);
  public MetricsTimeVaryingInt blocksVerified = 
                        new MetricsTimeVaryingInt("blocks_verified", registry);
  public MetricsTimeVaryingInt blockVerificationFailures =
                       new MetricsTimeVaryingInt("block_verification_failures", registry);
  public MetricsTimeVaryingInt opFailures =
      new MetricsTimeVaryingInt("operation_failures", registry);
  public MetricsTimeVaryingInt blockReadFailures =
      new MetricsTimeVaryingInt("block_read_failures", registry);
  public MetricsTimeVaryingInt dataXceiverConnFailures =
      new MetricsTimeVaryingInt("data_xceiver_connection_failures", registry);
  
  public MetricsTimeVaryingInt readsFromLocalClient = 
                new MetricsTimeVaryingInt("reads_from_local_client", registry);
  public MetricsTimeVaryingInt readsFromRemoteClient = 
                new MetricsTimeVaryingInt("reads_from_remote_client", registry);
  public MetricsTimeVaryingInt writesFromLocalClient = 
              new MetricsTimeVaryingInt("writes_from_local_client", registry);
  public MetricsTimeVaryingInt writesFromRemoteClient = 
              new MetricsTimeVaryingInt("writes_from_remote_client", registry);
  
  public MetricsTimeVaryingInt xceiverCount =
          new MetricsTimeVaryingInt("xceiver_count", registry);
  public MetricsTimeVaryingInt xceiverCountExceeded =
          new MetricsTimeVaryingInt("xceiver_count_exceeded", registry);
  public MetricsTimeVaryingInt volumeFailures = 
          new MetricsTimeVaryingInt("volumeFailures", registry, "The number of volume failures");
  
  public MetricsTimeVaryingInt cachedFileHandlerCount =
          new MetricsTimeVaryingInt("cached_file_handler_count", registry);
  
  public MetricsTimeVaryingRate readBlockOp = 
                new MetricsTimeVaryingRate("readBlockOp", registry);
  public MetricsTimeVaryingRate writeBlockOp = 
                new MetricsTimeVaryingRate("writeBlockOp", registry);
  public MetricsTimeVaryingRate appendBlockOp = 
                new MetricsTimeVaryingRate("appendBlockOp", registry);
  public MetricsTimeVaryingRate readMetadataOp = 
                new MetricsTimeVaryingRate("readMetadataOp", registry);
  public MetricsTimeVaryingRate blockChecksumOp = 
                new MetricsTimeVaryingRate("blockChecksumOp", registry);
  public MetricsTimeVaryingRate copyBlockOp = 
                new MetricsTimeVaryingRate("copyBlockOp", registry);
  public MetricsTimeVaryingRate replaceBlockOp = 
                new MetricsTimeVaryingRate("replaceBlockOp", registry);
  public MetricsTimeVaryingRate heartbeats = 
                    new MetricsTimeVaryingRate("heartBeats", registry);
  public MetricsTimeVaryingRate blockReports = 
                    new MetricsTimeVaryingRate("blockReports", registry);

  public MetricsTimeVaryingRate bytesReadLatency = 
                      new MetricsTimeVaryingRate("bytes_read_latency", registry);
  public MetricsTimeVaryingRate bytesWrittenLatency =
                      new MetricsTimeVaryingRate("bytes_writ_latency", registry);
  public MetricsTimeVaryingRate receiveBlockLatency =
                      new MetricsTimeVaryingRate("receive_block_latency", registry);
  public MetricsTimeVaryingRate receiveAndWritePacketLatency =
                      new MetricsTimeVaryingRate("receive_and_write_packet_latency", registry);
  public MetricsTimeVaryingRate writePacketLatency =
                      new MetricsTimeVaryingRate("write_packet_latency", registry);
  public MetricsTimeVaryingRate syncFileRangeLatency =
                      new MetricsTimeVaryingRate("sync_file_range_latency", registry);
  public MetricsTimeVaryingLong slowWritePacketNumOps =
                      new MetricsTimeVaryingLong("slow_write_packet_num_ops", registry);
  public MetricsTimeVaryingRate mirrorWritePacketLatency =
                      new MetricsTimeVaryingRate("mirror_write_packet_latency", registry);
  public MetricsTimeVaryingLong slowMirrorWritePacketNumOps =
                      new MetricsTimeVaryingLong("slow_mirror_write_packet_num_ops", registry);
  public MetricsTimeVaryingRate readPacketLatency =
                      new MetricsTimeVaryingRate("read_packet_latency", registry);
  public MetricsTimeVaryingRate largeReadsToBufRate =
                      new MetricsTimeVaryingRate("blockReceiverLargeReadsToBuf_rate", registry);

  public MetricsTimeVaryingRate smallReadsToBufRate =
                      new MetricsTimeVaryingRate("blockReceiverSmallReadsToBuf_rate", registry);

  // This is kind of a hack of the MetricsTimeVaryingRate class. Its being used
  // to keep track of the average number of bytes read.
  public MetricsTimeVaryingRate readToBufBytesRead =
                      new MetricsTimeVaryingRate("blockReceiverreadToBufBytesRead", registry);

  public MetricsTimeVaryingRate bytesWrittenRate =
                      new MetricsTimeVaryingRate("bytes_written_rate", registry);
  public MetricsTimeVaryingRate bytesReadRate =
                      new MetricsTimeVaryingRate("bytes_read_rate", registry);

  public MetricsIntValue threadActiveness = new MetricsIntValue("thread_alive",
      registry);
  
  public DataNodeMetrics(Configuration conf, String storageId) {
    String sessionId = conf.get("session.id"); 
    // Initiate reporting of Java VM metrics
    JvmMetrics.init("DataNode", sessionId);
    

    // Now the MBean for the data node
    datanodeActivityMBean = new DataNodeActivityMBean(registry, storageId);
    
    // Create record for DataNode metrics
    MetricsContext context = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(context, "datanode");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }
  
  public void shutdown() {
    if (datanodeActivityMBean != null) 
      datanodeActivityMBean.shutdown();
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
  public void resetAllMinMax() {
    readBlockOp.resetMinMax();
    writeBlockOp.resetMinMax();
    appendBlockOp.resetMinMax();
    readMetadataOp.resetMinMax();
    blockChecksumOp.resetMinMax();
    copyBlockOp.resetMinMax();
    replaceBlockOp.resetMinMax();
    heartbeats.resetMinMax();
    blockReports.resetMinMax();
    threadActiveness.set(0);
  }

  @Override
  public void errorInc() {
    loggedErrors.inc();
  }

  @Override
  public void warnInc() {
    loggedWarnings.inc();
  }
}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various NameNode activity statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #syncs}.inc()
 *
 */
public class NameNodeMetrics implements Updater {
    private static Log log = LogFactory.getLog(NameNodeMetrics.class);
    private final MetricsRecord metricsRecord;
    public MetricsRegistry registry = new MetricsRegistry();
    
    private NameNodeActivtyMBean namenodeActivityMBean;
    
    public MetricsTimeVaryingLong numReportedCorruptReplicas =
               new MetricsTimeVaryingLong("CorruptReplicasReported", registry);
    public MetricsTimeVaryingLong numFilesCreated =
                          new MetricsTimeVaryingLong("FilesCreated", registry);
    public MetricsTimeVaryingLong numFilesAppended =
                          new MetricsTimeVaryingLong("FilesAppended", registry);
    public MetricsTimeVaryingLong numGetBlockLocations =
                    new MetricsTimeVaryingLong("GetBlockLocations", registry);
    public MetricsTimeVaryingLong numFilesRenamed =
                    new MetricsTimeVaryingLong("FilesRenamed", registry);
    public MetricsTimeVaryingLong numFilesMerged = 
                    new MetricsTimeVaryingLong("FileMerged", registry);
    public MetricsTimeVaryingLong numFilesDeleted =
                    new MetricsTimeVaryingLong("FilesDeleted", registry,
                    "The number of files/dirs deleted by delete or rename operations");
    public MetricsTimeVaryingLong numFilesRaided = 
                    new MetricsTimeVaryingLong("FileRaided", registry,
                    "The number of files raided by raidFile operations");
    public MetricsTimeVaryingLong numGetListingOps =
                    new MetricsTimeVaryingLong("GetListingOps", registry);
    public MetricsTimeVaryingLong numCreateFileOps =
                    new MetricsTimeVaryingLong("CreateFileOps", registry);
    public MetricsTimeVaryingLong numDeleteFileOps =
                          new MetricsTimeVaryingLong("DeleteFileOps", registry);
    public MetricsTimeVaryingLong numRaidFileOps = 
                          new MetricsTimeVaryingLong("RaidFileOps", registry);
    public MetricsTimeVaryingLong numFileInfoOps =
                          new MetricsTimeVaryingLong("FileInfoOps", registry);
    public MetricsTimeVaryingLong numAddBlockOps =
                          new MetricsTimeVaryingLong("AddBlockOps", registry);
    public MetricsTimeVaryingLong numSetReplication =
                          new MetricsTimeVaryingLong("SetReplication", registry);
    public MetricsTimeVaryingLong numSetPermission =
                          new MetricsTimeVaryingLong("SetPermission", registry);
    public MetricsTimeVaryingLong numSetOwner =
                          new MetricsTimeVaryingLong("SetOwner", registry);
    public MetricsTimeVaryingLong numAbandonBlock =
                          new MetricsTimeVaryingLong("numAbandonBlock", registry);
    public MetricsTimeVaryingLong numCompleteFile =
                          new MetricsTimeVaryingLong("numCompleteFile", registry);
    public MetricsTimeVaryingLong numReportBadBlocks =
                          new MetricsTimeVaryingLong("numReportBadBlocks", registry);
    public MetricsTimeVaryingLong numNextGenerationStamp =
                          new MetricsTimeVaryingLong("numNextGenerationStamp", registry);
    public MetricsTimeVaryingLong numMkdirs =
                          new MetricsTimeVaryingLong("numMkdirs", registry);
    public MetricsTimeVaryingLong numRenewLease =
                          new MetricsTimeVaryingLong("numRenewLease", registry);
    public MetricsTimeVaryingLong numSaveNamespace =
                          new MetricsTimeVaryingLong("numSaveNamespace", registry);
    public MetricsTimeVaryingLong numRefreshNodes =
                          new MetricsTimeVaryingLong("numRefreshNodes", registry);
    public MetricsTimeVaryingLong numSetQuota =
                          new MetricsTimeVaryingLong("numSetQuota", registry);
    public MetricsTimeVaryingLong numFsync =
                          new MetricsTimeVaryingLong("numFsync", registry);
    public MetricsTimeVaryingLong numSetTimes =
                          new MetricsTimeVaryingLong("numSetTimes", registry);
    public MetricsTimeVaryingLong numRegister =
                          new MetricsTimeVaryingLong("numRegister", registry);
    public MetricsTimeVaryingLong numHeartbeat =
                          new MetricsTimeVaryingLong("numHeartbeat", registry);
    public MetricsTimeVaryingLong numBlockReport =
                          new MetricsTimeVaryingLong("numBlockReport", registry);
    public MetricsTimeVaryingLong numBlockReceived =
                          new MetricsTimeVaryingLong("numBlockReceived", registry);
    public MetricsTimeVaryingLong numVersionRequest =
                          new MetricsTimeVaryingLong("numVersionRequest", registry);
    public MetricsTimeVaryingLong numGetContentSummary =
                          new MetricsTimeVaryingLong("numGetContentSummary", registry, 
                          "The number of get content summary operations");

    // hftp related metrics
    public MetricsTimeVaryingLong numFileDataServletDoGet =
        new MetricsTimeVaryingLong("numFileDataServletDoGet", registry);
    public MetricsTimeVaryingLong numListPathsServletDoGet =
        new MetricsTimeVaryingLong("numListPathsServletDoGet", registry);
    public MetricsTimeVaryingLong numRedirectServletDoGet =
        new MetricsTimeVaryingLong("numRedirectServletDoGet", registry);
    public MetricsTimeVaryingLong numFsckDoGet =
        new MetricsTimeVaryingLong("numFsckDoGet", registry);
    
    public MetricsTimeVaryingRate transactions =
                    new MetricsTimeVaryingRate("Transactions", registry, "Journal Transaction");
    public MetricsTimeVaryingRate syncs =
                    new MetricsTimeVaryingRate("Syncs", registry, "Journal Sync");
    public MetricsTimeVaryingLong transactionsBatchedInSync =
                    new MetricsTimeVaryingLong("JournalTransactionsBatchedInSync", registry, "Journal Transactions Batched In Sync");
    public MetricsLongValue currentTxnId =
                    new MetricsLongValue("CurrentTxId", registry, "Last Written Transaction Id");
    public MetricsTimeVaryingLong rollEditLogTime =
                    new MetricsTimeVaryingLong("RollEditLogTime", registry, "Roll Edit Log Time");
    public MetricsTimeVaryingLong rollFsImageTime =
                    new MetricsTimeVaryingLong("RollFSImageTime", registry, "Roll FSImage Time");
    
    public MetricsTimeVaryingRate blockReport =
                    new MetricsTimeVaryingRate("blockReport", registry, "Block Report");
    public MetricsIntValue safeModeTime =
                    new MetricsIntValue("SafemodeTime", registry, "Duration in SafeMode at Startup");
    public MetricsIntValue fsImageLoadTime = 
                    new MetricsIntValue("fsImageLoadTime", registry, "Time loading FS Image at Startup");
    public MetricsIntValue numBlocksCorrupted =
                    new MetricsIntValue("BlocksCorrupted", registry);
    public MetricsIntValue numBufferedTransactions =
                    new MetricsIntValue("numBufferedTransactions", registry);
    public MetricsLongValue numOverReplicatedBlocks =
                    new MetricsLongValue("numOverReplicatedBlocks", registry);
    public MetricsLongValue numRaidEncodingTasks =
                    new MetricsLongValue("numRaidEncodingTasks", registry);
    public MetricsIntValue imagesFailed = 
                    new MetricsIntValue("imagesFailed", registry, 
                        "Number of failed image directories");
    public MetricsIntValue journalsFailed = 
                    new MetricsIntValue("journalsFailed", registry, 
                        "Number of failed journals");
    public MetricsLongValue tsLastEditsRoll = new MetricsLongValue(
        "tsLastEditsRoll", registry, "Timestamp of last edits rolling");
      
    public NameNodeMetrics(Configuration conf, NameNode nameNode) {
      String sessionId = conf.get("session.id");
      // Initiate Java VM metrics
      JvmMetrics.init("NameNode", sessionId);

      
      // Now the Mbean for the name node - this alos registers the MBean
      namenodeActivityMBean = new NameNodeActivtyMBean(registry);
      
      // Create a record for NameNode metrics
      MetricsContext metricsContext = MetricsUtil.getContext("dfs");
      metricsRecord = MetricsUtil.createRecord(metricsContext, "namenode");
      metricsRecord.setTag("sessionId", sessionId);
      metricsContext.registerUpdater(this);
      log.info("Initializing NameNodeMeterics using context object:" +
                metricsContext.getClass().getName());
    }
    

    
    public void shutdown() {
      if (namenodeActivityMBean != null) 
        namenodeActivityMBean.shutdown();
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
      transactions.resetMinMax();
      syncs.resetMinMax();
      blockReport.resetMinMax();
    }
}

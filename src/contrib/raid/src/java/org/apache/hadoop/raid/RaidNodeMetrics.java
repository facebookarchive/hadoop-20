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
package org.apache.hadoop.raid;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor;

public class RaidNodeMetrics implements Updater {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.RaidNodeMetrics");
  public static final int DEFAULT_NAMESPACE_ID = 0;
  static long metricsResetInterval = 86400 * 1000; // 1 day.
  private static ConcurrentMap<Integer, RaidNodeMetrics> instances = new ConcurrentHashMap<Integer, RaidNodeMetrics>(); 
  
  // Number of files currently raided.
  public static final String filesRaidedMetric = "files_raided";
  // Number of files fixed by block fixer.
  public static final String filesFixedMetric = "files_fixed";
  // Number of failures encountered by block fixer.
  public static final String fileFixFailuresMetric = "file_fix_failures";
  // Number of failures encountered by using new code block fixer
  public static final String blockFixSimulationFailuresMetric = "block_fix_simulation_failures";
  // Number of failures encountered by using new code block fixer
  public static final String blockFixSimulationSuccessMetric = "block_fix_simulation_success";
  // Number of files that need to be fixed by block fixer.
  public static final String numFilesToFixMetric = "files_to_fix";
  // Number of files copied by block copier.
  public static final String filesCopiedMetric = "files_copied";
  // Number of failures encountered by block copier.
  public static final String fileCopyFailuresMetric = "file_copy_failures";
  // Number of files that need to be copied by block copier.
  public static final String numFilesToCopyMetric = "files_to_copy";
  // Number of failures encountered during raiding.
  public static final String raidFailuresMetric = "raid_failures";
  // Number of purged files/directories.
  public static final String entriesPurgedMetric = "entries_purged";
  // Slot-seconds used by RAID jobs
  public static final String raidSlotSecondsMetric = "raid_slot_seconds";
  // Slot-seconds used by corrupt block fixing jobs.
  public static final String blockFixSlotSecondsMetric = "blockfix_slot_seconds";
  // Slot-seconds used by decommissioning block copying jobs.
  public static final String blockCopySlotSecondsMetric = "blockcopy_slot_seconds";
  // Number of block moved because of violation of the stripe block placement
  public static final String blockMoveMetric = "block_move";
  // Number of scheduled block move
  public static final String blockMoveScheduledMetric = "block_move_scheduled";
  // Number of skipped block move
  public static final String blockMoveSkippedMetric = "block_move_skipped";
  // Number of blocks which are misplaced
  public static final String misplacedMetricHeader = "misplaced";
  // Number of corrupt files being fixed with high priority
  public static final String corruptFilesHighPriMetric = "corrupt_files_high_pri";
  // Number of corrupt files being fixed with low priority
  public static final String corruptFilesLowPriMetric = "corrupt_files_low_pri";
  // Number of files being copied off decommissioning hosts  with low priority
  public static final String decomFilesLowPriMetric = "decom_files_low_pri";
  // Number of files being copied off decommissioning hosts  with lowest priority
  public static final String decomFilesLowestPriMetric = "decom_files_lowest_pri";
  // Monitor number of misplaced blocks in a stripe
  public static final int MAX_MONITORED_MISPLACED_BLOCKS = 5;
  //Number of files which have at least one block missing
  public static final String filesWithMissingBlksMetric = "files_with_missing_blks";
  //Number of stripes using "rs" codec with certain number of blocks missing
  public static final String NumStrpsOneMissingBlkMetric = "stripes_with_one_missingBlk";
  public static final String NumStrpsTwoMissingBlkMetric = "stripes_with_two_missingBlk";
  public static final String NumStrpsThreeMissingBlkMetric = "stripes_with_three_missingBlk";
  public static final String NumStrpsFourMissingBlkMetric = "stripes_with_four_missingBlk";
  public static final String NumStrpsFiveMoreMissingBlkMetric = "stripes_with_fiveOrMore_missingBlk";
  public static final String NumFilesToFixDroppedMetric = "files_to_fix_dropped";
  public static final String numFileFixReadBytesRemoteRackMetric = "file_fix_bytes_read_remote_rack";
  
  MetricsContext context;
  private MetricsRecord metricsRecord;
  private MetricsRegistry registry = new MetricsRegistry();
  MetricsLongValue filesRaided =
    new MetricsLongValue(filesRaidedMetric, registry);
  MetricsTimeVaryingLong raidFailures =
    new MetricsTimeVaryingLong(raidFailuresMetric, registry);
  MetricsTimeVaryingLong filesFixed =
    new MetricsTimeVaryingLong(filesFixedMetric, registry);
  MetricsTimeVaryingLong fileFixFailures =
    new MetricsTimeVaryingLong(fileFixFailuresMetric, registry);
  MetricsTimeVaryingLong blockFixSimulationFailures = 
    new MetricsTimeVaryingLong(blockFixSimulationFailuresMetric, registry);
  MetricsTimeVaryingLong blockFixSimulationSuccess = 
      new MetricsTimeVaryingLong(blockFixSimulationSuccessMetric, registry);
  MetricsLongValue numFilesToFix =
    new MetricsLongValue(numFilesToFixMetric, registry);
  MetricsTimeVaryingLong filesCopied =
    new MetricsTimeVaryingLong(filesCopiedMetric, registry);
  MetricsTimeVaryingLong fileCopyFailures =
    new MetricsTimeVaryingLong(fileCopyFailuresMetric, registry);
  MetricsLongValue numFilesToCopy =
    new MetricsLongValue(numFilesToCopyMetric, registry);
  MetricsTimeVaryingLong entriesPurged =
    new MetricsTimeVaryingLong(entriesPurgedMetric, registry);
  MetricsTimeVaryingLong raidSlotSeconds =
    new MetricsTimeVaryingLong(raidSlotSecondsMetric, registry);
  MetricsTimeVaryingLong blockFixSlotSeconds =
    new MetricsTimeVaryingLong(blockFixSlotSecondsMetric, registry);
  MetricsTimeVaryingLong blockCopySlotSeconds =
    new MetricsTimeVaryingLong(blockCopySlotSecondsMetric, registry);
  MetricsTimeVaryingLong blockMove =
    new MetricsTimeVaryingLong(blockMoveMetric, registry);
  MetricsTimeVaryingLong blockMoveScheduled =
    new MetricsTimeVaryingLong(blockMoveScheduledMetric, registry);
  MetricsTimeVaryingLong blockMoveSkipped =
    new MetricsTimeVaryingLong(blockMoveSkippedMetric, registry);
  Map<String, Map<Integer, MetricsLongValue>> codecToMisplacedBlocks;
  MetricsLongValue numFilesWithMissingBlks =
      new MetricsLongValue(filesWithMissingBlksMetric, registry);
  MetricsLongValue numStrpsOneMissingBlk = 
      new MetricsLongValue(NumStrpsOneMissingBlkMetric, registry);
  MetricsLongValue numStrpsTwoMissingBlk = 
      new MetricsLongValue(NumStrpsTwoMissingBlkMetric, registry);
  MetricsLongValue numStrpsThreeMissingBlk = 
      new MetricsLongValue(NumStrpsThreeMissingBlkMetric, registry);
  MetricsLongValue numStrpsFourMissingBlk = 
      new MetricsLongValue(NumStrpsFourMissingBlkMetric, registry);
  MetricsLongValue numStrpsFiveMoreMissingBlk = 
      new MetricsLongValue(NumStrpsFiveMoreMissingBlkMetric, registry);
  MetricsLongValue numFilesToFixDropped = 
      new MetricsLongValue(NumFilesToFixDroppedMetric, registry);
  MetricsTimeVaryingLong numFileFixReadBytesRemoteRack = 
      new MetricsTimeVaryingLong(numFileFixReadBytesRemoteRackMetric, registry);
  
  Map<String, Map<RaidState, MetricsLongValue>> sourceFiles;
  Map<String, Map<RaidState, MetricsLongValue>> sourceBlocks;
  Map<String, Map<RaidState, MetricsLongValue>> sourceBytes;
  Map<String, Map<RaidState, MetricsLongValue>> sourceLogical;
  Map<String, MetricsLongValue> parityFiles;
  Map<String, MetricsLongValue> parityBlocks;
  Map<String, MetricsLongValue> parityBytes;
  Map<String, MetricsLongValue> parityLogical;


  Map<String, MetricsLongValue> corruptFiles = null;
  Map<String, MetricsLongValue> underRedundantFiles = null;

  MetricsLongValue effectiveReplicationTimes1000 =
      new MetricsLongValue("effective_replication_1000", registry);
  MetricsLongValue saving = new MetricsLongValue("saving", registry);
  Map<String, MetricsLongValue> savingForCode =
    new HashMap<String, MetricsLongValue>();

  MetricsLongValue corruptFilesHighPri =
      new MetricsLongValue(corruptFilesHighPriMetric, registry);
  MetricsLongValue corruptFilesLowPri =
      new MetricsLongValue(corruptFilesLowPriMetric, registry);
  MetricsLongValue decomFilesLowPri =
    new MetricsLongValue(decomFilesLowPriMetric, registry);
  MetricsLongValue decomFilesLowestPri =
    new MetricsLongValue(decomFilesLowestPriMetric, registry);  
  
  // LogMetrics record the metrics for every logging into scribe
  // The key of logMetrics is cluster_logType_result
  Map<String, MetricsTimeVaryingLong> logMetrics =
      new HashMap<String, MetricsTimeVaryingLong>();

  public static RaidNodeMetrics getInstance(int namespaceId) {
    RaidNodeMetrics metric = instances.get(namespaceId);
    if (metric == null) {
      metric = new RaidNodeMetrics();
      RaidNodeMetrics old = instances.putIfAbsent(namespaceId, metric);
      if (old != null) {
        metric = old;
      }
    }
    return metric;
  }
  
  public static void clearInstances() {
    instances.clear();
  }

  private RaidNodeMetrics() {
    // Create a record for raid metrics
    context = MetricsUtil.getContext("raidnode");
    metricsRecord = MetricsUtil.createRecord(context, "raidnode");
    context.registerUpdater(this);
    initPlacementMetrics();
    initSourceMetrics();
    initParityMetrics();
    LOG.info("RaidNode Metrics is initialized");
  }

  private void initPlacementMetrics() {
    codecToMisplacedBlocks = new HashMap<String, Map<Integer, MetricsLongValue>>();
    for (Codec codec : Codec.getCodecs()) {
      Map<Integer, MetricsLongValue> m = new HashMap<Integer, MetricsLongValue>();
      for (int i = 0; i < MAX_MONITORED_MISPLACED_BLOCKS; ++i) {
        m.put(i, new MetricsLongValue(misplacedMetricHeader +
                                   "_" + codec.id + "_" + i, registry));
      }
      codecToMisplacedBlocks.put(codec.id, m);
    }
  }

  private void initSourceMetrics() {
    sourceFiles = createSourceMap();
    sourceBlocks = createSourceMap();
    sourceBytes = createSourceMap();
    sourceLogical = createSourceMap();
    for (Codec codec : Codec.getCodecs()) {
      for (RaidState state : RaidState.values()) {
        String head = (codec.id + "_" + state + "_").toLowerCase();
        createSourceMetrics(sourceFiles, codec.id, state, head + "files");
        createSourceMetrics(sourceBlocks, codec.id, state, head + "blocks");
        createSourceMetrics(sourceBytes, codec.id, state, head + "bytes");
        createSourceMetrics(sourceLogical, codec.id, state, head + "logical");
      }
    }
  }

  public synchronized void initCorruptFilesMetrics(Configuration conf) {
    if (corruptFiles == null) {
      String[] dirs = DistBlockIntegrityMonitor.getCorruptMonitorDirs(conf);
      corruptFiles = new HashMap<String, MetricsLongValue>();
      for (String dir: dirs) {
        String name = dir + "_corrupt_files";
        corruptFiles.put(dir, new MetricsLongValue(name, registry));
      }
    }
  }
  
  public synchronized void initUnderRedundantFilesMetrics(Configuration conf) {
    if (underRedundantFiles == null) {
      String[] dirs = DistBlockIntegrityMonitor.getCorruptMonitorDirs(conf);
      underRedundantFiles = new HashMap<String, MetricsLongValue>();
      for (String dir: dirs) {
        String name = "under_redundant_files_" + dir;
        underRedundantFiles.put(dir, new MetricsLongValue(name, registry));
      }
      String name = "under_redundant_files_" + BlockIntegrityMonitor.OTHERS;
      underRedundantFiles.put(BlockIntegrityMonitor.OTHERS,
          new MetricsLongValue(name, registry));
    }
  }
  
  private void createSourceMetrics(
      Map<String, Map<RaidState, MetricsLongValue>> m,
      String code, RaidState state, String name) {
    Map<RaidState, MetricsLongValue> innerMap = m.get(code);
    innerMap.put(state, new MetricsLongValue(name, registry));
  }

  private Map<String, Map<RaidState, MetricsLongValue>> createSourceMap() {
    Map<String, Map<RaidState, MetricsLongValue>> result =
        new HashMap<String, Map<RaidState, MetricsLongValue>>();
    for (Codec codec : Codec.getCodecs()) {
      Map<RaidState, MetricsLongValue> m =
          new HashMap<RaidState, MetricsLongValue>();
      for (RaidState state : RaidState.values()) {
        m.put(state, null);
      }
      m = new EnumMap<RaidState, MetricsLongValue>(m);
      result.put(codec.id, m);
    }
    return result;
  }

  private void initParityMetrics() {
    parityFiles = createParityMap();
    parityBlocks = createParityMap();
    parityBytes = createParityMap();
    parityLogical = createParityMap();
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
      String head = (code + "_parity_").toLowerCase();
      createParityMetrics(parityFiles, code, head + "files");
      createParityMetrics(parityBlocks, code, head + "blocks");
      createParityMetrics(parityBytes, code, head + "bytes");
      createParityMetrics(parityLogical, code, head + "logical");
      String savingName = ("saving_" + code).toLowerCase();
      savingForCode.put(code, new MetricsLongValue(savingName, registry));
    }
  }

  private void createParityMetrics(
      Map<String, MetricsLongValue> m,
      String code, String name) {
    m.put(code, new MetricsLongValue(name, registry));
  }

  private Map<String, MetricsLongValue> createParityMap() {
    Map<String, MetricsLongValue> m =
        new HashMap<String, MetricsLongValue>();
    for (Codec codec : Codec.getCodecs()) {
      m.put(codec.id, null);
    }
    return m;
  }
  
  public MetricsRegistry getMetricsRegistry() {
    return this.registry;
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

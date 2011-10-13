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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

public class RaidNodeMetrics implements Updater {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.RaidNodeMetrics");

  static long metricsResetInterval = 86400 * 1000; // 1 day.

  private static final RaidNodeMetrics instance = new RaidNodeMetrics();

  // Number of files currently raided.
  public static final String filesRaidedMetric = "files_raided";
  // Number of files fixed by block fixer.
  public static final String filesFixedMetric = "files_fixed";
  // Number of failures encountered by block fixer.
  public static final String fileFixFailuresMetric = "file_fix_failures";
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
  // Number of RS blocks which are misplaced
  public static final String misplacedRsMetricsHeader = "misplaced_rs";
  // Number of XOR blocks which are misplaced
  public static final String misplacedXorMetricsHeader = "misplaced_xor";
  // Number of corrupt files being fixed with high priority
  public static final String corruptFilesHighPriMetric = "corrupt_files_high_pri";
  // Number of corrupt files being fixed with low priority
  public static final String corruptFilesLowPriMetric = "corrupt_files_low_pri";
  // Number of files being copied off decommissioning hosts  with low priority
  public static final String decomFilesLowPriMetric = "decom_files_low_pri";
  // Number of files being copied off decommissioning hosts  with lowest priority
  public static final String decomFilesLowestPriMetric = "decom_files_lowest_pri";
  
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
  MetricsLongValue misplacedRs[];
  MetricsLongValue misplacedXor[];

  Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> sourceFiles;
  Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> sourceBlocks;
  Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> sourceBytes;
  Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> sourceLogical;
  Map<ErasureCodeType, MetricsLongValue> parityFiles;
  Map<ErasureCodeType, MetricsLongValue> parityBlocks;
  Map<ErasureCodeType, MetricsLongValue> parityBytes;
  Map<ErasureCodeType, MetricsLongValue> parityLogical;
  MetricsLongValue effectiveReplicationTimes1000 =
      new MetricsLongValue("effective_replication_1000", registry);
  MetricsLongValue saving = new MetricsLongValue("saving", registry);
  Map<ErasureCodeType, MetricsLongValue> savingForCode =
    new HashMap<ErasureCodeType, MetricsLongValue>();

  MetricsLongValue corruptFilesHighPri =
      new MetricsLongValue(corruptFilesHighPriMetric, registry);
  MetricsLongValue corruptFilesLowPri =
      new MetricsLongValue(corruptFilesLowPriMetric, registry);
  MetricsLongValue decomFilesLowPri =
    new MetricsLongValue(decomFilesLowPriMetric, registry);
  MetricsLongValue decomFilesLowestPri =
    new MetricsLongValue(decomFilesLowestPriMetric, registry);  

  public static RaidNodeMetrics getInstance() {
    return instance;
  }

  private RaidNodeMetrics() {
    // Create a record for raid metrics
    context = MetricsUtil.getContext("raidnode");
    metricsRecord = MetricsUtil.createRecord(context, "raidnode");
    context.registerUpdater(this);
    initPlacementMetrics();
    initSourceMetrics();
    initParityMetrics();
  }

  private void initPlacementMetrics() {
    final int MAX_MONITORED_MISPLACED_BLOCKS = 5;
    misplacedRs = new MetricsLongValue[MAX_MONITORED_MISPLACED_BLOCKS];
    misplacedXor = new MetricsLongValue[MAX_MONITORED_MISPLACED_BLOCKS];
    for (int i = 0; i < misplacedRs.length; ++i) {
      misplacedRs[i] = new MetricsLongValue(misplacedRsMetricsHeader + "_" + i, registry);
    }
    for (int i = 0; i < misplacedXor.length; ++i) {
      misplacedXor[i] = new MetricsLongValue(misplacedXorMetricsHeader + "_" + i, registry);
    }
  }

  private void initSourceMetrics() {
    sourceFiles = createSourceMap();
    sourceBlocks = createSourceMap();
    sourceBytes = createSourceMap();
    sourceLogical = createSourceMap();
    for (ErasureCodeType code : ErasureCodeType.values()) {
      for (RaidState state : RaidState.values()) {
        String head = (code + "_" + state + "_").toLowerCase();
        createSourceMetrics(sourceFiles, code, state, head + "files");
        createSourceMetrics(sourceBlocks, code, state, head + "blocks");
        createSourceMetrics(sourceBytes, code, state, head + "bytes");
        createSourceMetrics(sourceLogical, code, state, head + "logical");
      }
    }
  }

  private void createSourceMetrics(
      Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> m,
      ErasureCodeType code, RaidState state, String name) {
    Map<RaidState, MetricsLongValue> innerMap = m.get(code);
    innerMap.put(state, new MetricsLongValue(name, registry));
  }

  private Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> createSourceMap() {
    Map<ErasureCodeType, Map<RaidState, MetricsLongValue>> result =
        new HashMap<ErasureCodeType, Map<RaidState, MetricsLongValue>>();
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Map<RaidState, MetricsLongValue> m =
          new HashMap<RaidState, MetricsLongValue>();
      for (RaidState state : RaidState.values()) {
        m.put(state, null);
      }
      m = new EnumMap<RaidState, MetricsLongValue>(m);
      result.put(code, m);
    }
    return new EnumMap<ErasureCodeType, Map<RaidState, MetricsLongValue>>(result);
  }

  private void initParityMetrics() {
    parityFiles = createParityMap();
    parityBlocks = createParityMap();
    parityBytes = createParityMap();
    parityLogical = createParityMap();
    for (ErasureCodeType code : ErasureCodeType.values()) {
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
      Map<ErasureCodeType, MetricsLongValue> m,
      ErasureCodeType code, String name) {
    m.put(code, new MetricsLongValue(name, registry));
  }

  private Map<ErasureCodeType, MetricsLongValue> createParityMap() {
    Map<ErasureCodeType, MetricsLongValue> m =
        new HashMap<ErasureCodeType, MetricsLongValue>();
    for (ErasureCodeType code : ErasureCodeType.values()) {
      m.put(code, null);
    }
    return new EnumMap<ErasureCodeType, MetricsLongValue>(m);
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

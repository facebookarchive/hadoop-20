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
package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.raid.Statistics.Counters;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;
import org.apache.hadoop.util.StringUtils;


public class StatisticsCollector implements Runnable {

  final static public Log LOG = LogFactory.getLog(StatisticsCollector.class);
  final static public long UPDATE_PERIOD = 20 * 60 * 1000L;
  final static public long FILES_SCANNED_LOGGING_PERIOD = 100 * 1000L;
  final static private int MAX_FILES_TO_RAID_QUEUE_SIZE = 10000000;
  final private ConfigManager configManager;
  final private Path rsParityLocation;
  final private Path xorParityLocation;
  final private FileSystem fs;
  final private Configuration conf;
  final private int numThreads;
  final private RaidNode raidNode;

  private volatile Map<ErasureCodeType, Statistics> lastRaidStatistics;
  private volatile long lastUpdateFinishTime = 0L;
  private volatile long lastUpdateUsedTime = 0L;
  private long lastUpdateStartTime = 0L;
  private volatile boolean running = true;
  private volatile long filesScanned = 0;
  
  public StatisticsCollector(RaidNode raidNode, ConfigManager configManager,
      Configuration conf)
      throws IOException {
    this.configManager = configManager;
    this.raidNode = raidNode;
    this.conf = conf;
    this.fs = new Path(Path.SEPARATOR).getFileSystem(conf);
    this.rsParityLocation = RaidNode.rsDestinationPath(conf);
    this.xorParityLocation = RaidNode.xorDestinationPath(conf);
    this.lastUpdateFinishTime = 0L;
    this.lastUpdateStartTime = 0L;
    this.lastRaidStatistics = null;
    this.numThreads = conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
  }

  @Override
  public void run() {
    while (running) {
      try {
        while (RaidNode.now() - lastUpdateStartTime < UPDATE_PERIOD) {
          Thread.sleep(UPDATE_PERIOD / 10);
        }
        Collection<PolicyInfo> allPolicies = loadPolicies();
        collect(allPolicies);
      } catch (IOException e) {
        LOG.warn("Failed to collect statistics. Retry");
      } catch (InterruptedException e) {
        LOG.info(StatisticsCollector.class + " interrupted.");
      }
    }
  }

  public void stop() {
    running = false;
  }

  public Statistics getRaidStatistics(ErasureCodeType code) {
    if (lastRaidStatistics == null) {
      return null;
    }
    return lastRaidStatistics.get(code);
  }

  public long getUpateUsedTime() {
    return lastUpdateUsedTime;
  }

  public long getLastUpdateTime() {
    return lastUpdateFinishTime;
  }

  /**
   * Get the total RAID saving in bytes
   * @return Number of bytes saved due to RAID
   */
  public long getSaving() {
    if (lastRaidStatistics == null) {
      return -1;
    }
    long saving = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      long s = lastRaidStatistics.get(code).getSaving();
      if (s == -1) {
        return -1;
      }
      saving += s;
    }
    return saving;
  }

  /**
   * Populate RAID savings by code and total.
   * @return Number of bytes saved due to RAID
   */
  public void populateSaving(RaidNodeMetrics metrics) {
    if (lastRaidStatistics == null) {
      return;
    }
    long saving = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      long s = lastRaidStatistics.get(code).getSaving();
      if (s > 0) {
        metrics.savingForCode.get(code).set(s);
        saving += s;
      }
    }
    if (saving > 0) {
      metrics.saving.set(saving);
    }
  }

  /**
   * Get the estimated toal RAID saving when policies are done
   * @return Number of bytes saved due to RAID
   */
  public long getDoneSaving() {
    if (lastRaidStatistics == null) {
      return -1;
    }
    long saving = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      long s = lastRaidStatistics.get(code).getDoneSaving();
      if (s == -1) {
        return -1;
      }
      saving += s;
    }
    return saving;
  }

  public double getEffectiveReplication() {
    if (lastRaidStatistics == null) {
      return -1;
    }
    DFSClient dfs;
    double totalPhysical;
    try {
      dfs = new DFSClient(conf);
      totalPhysical = dfs.getDiskStatus().getDfsUsed();
    } catch (IOException e) {
      return -1;
    }
    double notRaidedPhysical = totalPhysical;
    double totalLogical = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Statistics st = lastRaidStatistics.get(code);
      totalLogical += st.getSourceCounters(RaidState.RAIDED).getNumLogical();
      notRaidedPhysical -= st.getSourceCounters(RaidState.RAIDED).getNumBytes();
      notRaidedPhysical -= st.getParityCounters().getNumBytes();
    }
    totalLogical += notRaidedPhysical / dfs.getDefaultReplication();
    if (totalLogical == 0) {
      // divided by 0
      return -1;
    }
    return totalPhysical / totalLogical;
  }

  private Collection<PolicyInfo> loadPolicies() {
    configManager.reloadConfigsIfNecessary();
    Collection<PolicyInfo> allPolicyInfos = new ArrayList<PolicyInfo>();
    for (PolicyList policyList : configManager.getAllPolicies()) {
      allPolicyInfos.addAll(policyList.getAll());
    }
    return allPolicyInfos;
  }

  void collect(Collection<PolicyInfo> allPolicies) throws IOException {
    Map<ErasureCodeType, Statistics>
        codeToRaidStatistics = createEmptyStatistics();
    lastUpdateStartTime = RaidNode.now();
    filesScanned = 0;
    Statistics rsStats = codeToRaidStatistics.get(ErasureCodeType.RS);
    Statistics xorStats = codeToRaidStatistics.get(ErasureCodeType.XOR);
    collectSourceStatistics(codeToRaidStatistics, allPolicies);
    collectParityStatistics(rsParityLocation, rsStats);
    collectParityStatistics(xorParityLocation, xorStats);
    lastRaidStatistics = codeToRaidStatistics;
    populateMetrics(codeToRaidStatistics);
    long now = RaidNode.now();
    lastUpdateFinishTime = now;
    lastUpdateUsedTime = lastUpdateFinishTime - lastUpdateStartTime;
    LOG.info("Finishing collecting statistics.");
  }

  private Map<ErasureCodeType, Statistics> createEmptyStatistics() {
    Map<ErasureCodeType, Statistics> m =
        new HashMap<ErasureCodeType, Statistics>();
    for (ErasureCodeType code : ErasureCodeType.values()) {
      m.put(code, new Statistics(code, conf));
    }
    return new EnumMap<ErasureCodeType, Statistics>(m);
  }
  
  private void collectSourceStatistics(
      Map<ErasureCodeType, Statistics> codeToRaidStatistics,
      Collection<PolicyInfo> allPolicyInfos)
      throws IOException {
    long now = RaidNode.now();
    RaidState.Checker checker =
        new RaidState.Checker(allPolicyInfos, conf);
    for (PolicyInfo info : allPolicyInfos) {
      LOG.info("Collecting statistics for policy:" + info.getName() + ".");
      ErasureCodeType code = info.getErasureCode();
      Statistics statistics = codeToRaidStatistics.get(code);
      DirectoryTraversal retriver =
          DirectoryTraversal.fileRetriever(
              info.getSrcPathExpanded(), fs, numThreads, false);
      int targetReplication =
          Integer.parseInt(info.getProperty("targetReplication"));
      FileStatus file;
      List<FileStatus> filesToRaid = new ArrayList<FileStatus>();
      while ((file = retriver.next()) != DirectoryTraversal.FINISH_TOKEN) {
        boolean shouldBeRaided =
          statistics.addSourceFile(info, file, checker, now, targetReplication);
        if (shouldBeRaided &&
            filesToRaid.size() < MAX_FILES_TO_RAID_QUEUE_SIZE) {
          filesToRaid.add(file);
        }
        filesToRaid = submitRaidJobsWhenPossible(info, filesToRaid, false);
        incFileScanned();
      }
      filesToRaid = submitRaidJobsWhenPossible(info, filesToRaid, true);
      if (!filesToRaid.isEmpty()) {
        // Note that there might be some files not raided. But we don't want to
        // wait here. TriggerMonitor will take care of them later.
        LOG.info(filesToRaid.size() + " files were not raided by " +
            StatisticsCollector.class + " because of the job limit.");
      }
      LOG.info("Finish collecting statistics for policy:" + info.getName() +
          "\n" + statistics);
    }
  }

  /**
   * Raiding a given list of files
   * @param info The Raid policy
   * @param filesToRaid The list of files to raid
   * @param submitAll Should submit the whole list?
   * @return The remaining files to raid
   */
  private List<FileStatus> submitRaidJobsWhenPossible(PolicyInfo info,
      List<FileStatus> filesToRaid, boolean submitAll) {
    try {
      int maxFilesPerJob = configManager.getMaxFilesPerJob();
      int maxJobs = configManager.getMaxJobsPerPolicy();
      while (!filesToRaid.isEmpty() &&
          (submitAll || filesToRaid.size() >= maxFilesPerJob) &&
          raidNode.getRunningJobsForPolicy(info.getName()) < maxJobs) {
        int numFiles = Math.min(maxFilesPerJob, filesToRaid.size());
        raidNode.raidFiles(info, filesToRaid.subList(0, numFiles));
        filesToRaid =
            filesToRaid.subList(numFiles, filesToRaid.size());

      }
    } catch (IOException e) {
      LOG.warn("Failed to raid files for policy:" + info.getName(), e);
    }
    return filesToRaid;
  }

  private void collectParityStatistics(Path parityLocation,
      Statistics statistics) throws IOException {
    LOG.info("Collecting parity statistics in " + parityLocation + ".");
    DirectoryTraversal retriver =
        DirectoryTraversal.fileRetriever(
            Arrays.asList(parityLocation), fs, numThreads, false);
    FileStatus file;
    while ((file = retriver.next()) != DirectoryTraversal.FINISH_TOKEN) {
      statistics.addParityFile(file);
      incFileScanned();
    }
    LOG.info("Finish collecting statistics in " +
        parityLocation + "\n" + statistics);
  }

  public long getFilesScanned() {
    return filesScanned;
  }

  private void incFileScanned() {
    filesScanned += 1;
    if (filesScanned % FILES_SCANNED_LOGGING_PERIOD == 0) {
      LOG.info("Scanned " +
          StringUtils.humanReadableInt(filesScanned) + " files.");
    }
  }

  private void populateMetrics(
      Map<ErasureCodeType, Statistics> codeToRaidStatistics) {
    RaidNodeMetrics metrics = RaidNodeMetrics.getInstance();
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Counters counters = codeToRaidStatistics.get(code).getParityCounters();
      metrics.parityFiles.get(code).set(counters.getNumFiles());
      metrics.parityBlocks.get(code).set(counters.getNumBlocks());
      metrics.parityBytes.get(code).set(counters.getNumBytes());
      metrics.parityLogical.get(code).set(counters.getNumLogical());
      for (RaidState state : RaidState.values()) {
        counters = codeToRaidStatistics.get(code).getSourceCounters(state);
        metrics.sourceFiles.get(code).get(state).set(counters.getNumFiles());
        metrics.sourceBlocks.get(code).get(state).set(counters.getNumBlocks());
        metrics.sourceBytes.get(code).get(state).set(counters.getNumBytes());
        metrics.sourceLogical.get(code).get(state).set(counters.getNumLogical());
      }
    }
    double repl = getEffectiveReplication();
    populateSaving(metrics);
    if (repl != -1) {
      metrics.effectiveReplicationTimes1000.set( (long)(1000 * repl));
    }
  }

}

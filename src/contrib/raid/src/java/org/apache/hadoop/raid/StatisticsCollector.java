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

import java.io.*;
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
import org.apache.hadoop.util.StringUtils;


public class StatisticsCollector implements Runnable {

  public static final String STATS_COLLECTOR_SUBMIT_JOBS_CONFIG =
    "raid.statscollector.submit.raid.jobs";
  public static final String STATS_SNAPSHOT_FILE_KEY =
    "fs.raid.statscollector.snapshotFile";

  final static public Log LOG = LogFactory.getLog(StatisticsCollector.class);
  final static public long UPDATE_PERIOD = 20 * 60 * 1000L;
  final static public long FILES_SCANNED_LOGGING_PERIOD = 100 * 1000L;
  final static private int MAX_FILES_TO_RAID_QUEUE_SIZE = 10000000;
  final private ConfigManager configManager;
  final private FileSystem fs;
  final private Configuration conf;
  final private int numThreads;
  final private RaidNode raidNode;
  final private String snapshotFileName;

  private volatile Map<String, Statistics> lastRaidStatistics;
  private volatile long lastUpdateFinishTime = 0L;
  private volatile long lastUpdateUsedTime = 0L;
  private long lastUpdateStartTime = 0L;
  private volatile boolean running = true;
  private volatile long filesScanned = 0;
  private boolean submitRaidJobs;

  public StatisticsCollector(RaidNode raidNode, ConfigManager configManager,
      Configuration conf)
      throws IOException {
    this.configManager = configManager;
    this.raidNode = raidNode;
    this.conf = conf;
    this.fs = new Path(Path.SEPARATOR).getFileSystem(conf);
    this.lastUpdateFinishTime = 0L;
    this.lastUpdateStartTime = 0L;
    this.lastRaidStatistics = null;
    this.numThreads = conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
    this.submitRaidJobs = conf.getBoolean(STATS_COLLECTOR_SUBMIT_JOBS_CONFIG, true);
    this.snapshotFileName = conf.get(STATS_SNAPSHOT_FILE_KEY);
  }

  @Override
  public void run() {
    readStatsSnapshot();
    while (running) {
      try {
        while (RaidNode.now() - lastUpdateStartTime < UPDATE_PERIOD) {
          Thread.sleep(UPDATE_PERIOD / 10);
        }
        Collection<PolicyInfo> allPolicies = loadPolicies();
        collect(allPolicies);
        writeStatsSnapshot();
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

  public boolean writeStatsSnapshot() {
    if (snapshotFileName == null) {
      return false;
    }
    LOG.info("Checkpointing raid statistics to " + snapshotFileName);
    // Write to a temp file and then rename to prevent partial snapshots.
    String newSnapshotFile = snapshotFileName + ".tmp";
    try {
      ObjectOutputStream output = new ObjectOutputStream(
        new BufferedOutputStream(
          new FileOutputStream(newSnapshotFile)));
      try {
        output.writeObject((Long) lastUpdateStartTime);
        output.writeObject((Long) lastUpdateFinishTime);
        output.writeObject((Long) lastUpdateUsedTime);
        output.writeObject((Long) filesScanned);
        output.writeObject(lastRaidStatistics);
      } finally {
        output.close();
      }
    } catch (IOException e) {
      LOG.warn("Unable to write stats snapshot to disk: ", e);
      return false;
    }
    File tmpFile = new File(newSnapshotFile);
    File permFile = new File (snapshotFileName);
    if (!tmpFile.renameTo(permFile)) {
      LOG.warn("Unable to rename stats snapshot file.");
      return false;
    }
    return true;
  }

  public boolean readStatsSnapshot() {
    if (snapshotFileName == null) {
      return false;
    }
    try {
      LOG.info("Restoring prior raid statistics from " + snapshotFileName);
      ObjectInputStream input = new ObjectInputStream(
        new BufferedInputStream(
          new FileInputStream(snapshotFileName)));
      try {
        lastUpdateStartTime = (Long) input.readObject();
        lastUpdateFinishTime = (Long) input.readObject();
        lastUpdateUsedTime = (Long) input.readObject();
        filesScanned = (Long) input.readObject();
        lastRaidStatistics =
          (Map<String, Statistics>) input.readObject();
      } finally {
        input.close();
      }
    } catch (IOException e) {
      LOG.warn("Unable to load stats snapshot from disk.", e);
      return false;
    } catch (ClassNotFoundException e) {
      LOG.warn("Unable to parse stats snapshot read from disk.", e);
      return false;
    }
    populateMetrics(lastRaidStatistics);
    return true;
  }

  public Statistics getRaidStatistics(String code) {
    if (lastRaidStatistics == null) {
      return null;
    }
    return lastRaidStatistics.get(code);
  }

  protected Map<String, Statistics> getRaidStatisticsMap() {
    return lastRaidStatistics;
  }

  protected void clearRaidStatisticsMap() {
    lastRaidStatistics = null;
  }

  public long getUpdateUsedTime() {
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
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
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
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
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
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
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
      totalPhysical = dfs.getNSDiskStatus().getDfsUsed();
    } catch (IOException e) {
      return -1;
    }
    double notRaidedPhysical = totalPhysical;
    double totalLogical = 0;
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
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
    List<PolicyInfo> list = new ArrayList<PolicyInfo>();
    for (PolicyInfo info: configManager.getAllPolicies()) {
      if (info.getSrcPath() != null) {
        list.add(info);
      }
    }
    return list;
  }

  void collect(Collection<PolicyInfo> allPolicies) throws IOException {
    Map<String, Statistics>
        codeToRaidStatistics = createEmptyStatistics();
    lastUpdateStartTime = RaidNode.now();
    filesScanned = 0;

    collectSourceStatistics(codeToRaidStatistics, allPolicies);

    for (Codec codec : Codec.getCodecs()) {
      Statistics stats = codeToRaidStatistics.get(codec.id);
      Path parityPath = new Path(codec.parityDirectory);
      collectParityStatistics(codec, parityPath, stats);
    }
    lastRaidStatistics = codeToRaidStatistics;
    populateMetrics(codeToRaidStatistics);
    long now = RaidNode.now();
    lastUpdateFinishTime = now;
    lastUpdateUsedTime = lastUpdateFinishTime - lastUpdateStartTime;
    LOG.info("Finishing collecting statistics.");
  }

  private Map<String, Statistics> createEmptyStatistics() {
    Map<String, Statistics> m =
        new HashMap<String, Statistics>();
    for (Codec codec : Codec.getCodecs()) {
      if (codec.isDirRaid) {
        m.put(codec.id, new DirectoryStatistics(codec, conf));
      } else {
        m.put(codec.id, new Statistics(codec, conf));
      }
    }
    return m;
  }

  private void collectSourceStatistics(
      Map<String, Statistics> codeToRaidStatistics,
      Collection<PolicyInfo> allPolicyInfos)
      throws IOException {
    long now = RaidNode.now();
    RaidState.Checker checker =
        new RaidState.Checker(allPolicyInfos, conf);
    for (PolicyInfo info : allPolicyInfos) {
      LOG.info("Collecting statistics for policy:" + info.getName() + ".");
      String code = info.getCodecId();
      Codec codec = Codec.getCodec(code);
      Statistics statistics = codeToRaidStatistics.get(code);
      DirectoryTraversal retriever;
      
      if (codec.isDirRaid) {
        // leaf directory retriever
        retriever = DirectoryTraversal.directoryRetriever(
            info.getSrcPathExpanded(), fs, numThreads, false, true, true);
      } else {
        retriever = DirectoryTraversal.fileRetriever(
              info.getSrcPathExpanded(), fs, numThreads, false, true);
      }
      int targetReplication =
          Integer.parseInt(info.getProperty("targetReplication"));
      FileStatus file;
      List<FileStatus> filesToRaid = new ArrayList<FileStatus>();
      while ((file = retriever.next()) != DirectoryTraversal.FINISH_TOKEN) {
        boolean shouldBeRaided =
          statistics.addSourceFile(fs, info, file, checker, now, 
              targetReplication);
        if (shouldBeRaided &&
            filesToRaid.size() < MAX_FILES_TO_RAID_QUEUE_SIZE) {
          filesToRaid.add(file);
        }
        filesToRaid = submitRaidJobsWhenPossible(info, filesToRaid, false);
        incFileScanned(file);
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
    if (!submitRaidJobs || !info.getShouldRaid()) {
      return filesToRaid;
    }
    try {
      int maxFilesPerJob = configManager.getMaxFilesPerJob();
      int maxJobs = configManager.getMaxJobsPerPolicy();
      while (!filesToRaid.isEmpty() &&
          (submitAll || filesToRaid.size() >= maxFilesPerJob) &&
          raidNode.getRunningJobsForPolicy(info.getName()) < maxJobs) {
        int numFiles = Math.min(maxFilesPerJob, filesToRaid.size());
        LOG.info("Invoking raidFiles with " + numFiles + " files");
        raidNode.raidFiles(info, filesToRaid.subList(0, numFiles));
        filesToRaid =
            filesToRaid.subList(numFiles, filesToRaid.size());

      }
    } catch (IOException e) {
      LOG.warn("Failed to raid files for policy:" + info.getName(), e);
    }
    return filesToRaid;
  }

  private void collectParityStatistics(Codec codec, Path parityLocation,
      Statistics statistics) throws IOException {
    LOG.info("Collecting parity statistics in " + parityLocation + ".");
    DirectoryTraversal retriever =
            DirectoryTraversal.fileRetriever(
            Arrays.asList(parityLocation), fs, numThreads, false, true);
    FileStatus file;
    while ((file = retriever.next()) != DirectoryTraversal.FINISH_TOKEN) {
      statistics.addParityFile(file);
      incFileScanned(file);
    }
    LOG.info("Finish collecting statistics in " +
        parityLocation + "\n" + statistics);
  }

  public long getFilesScanned() {
    return filesScanned;
  }

  private void incFileScanned(FileStatus file) throws IOException {
    if (file.isDir()) {
      filesScanned += fs.listStatus(file.getPath()).length;
    } else {
      filesScanned += 1;
    }
    
    if (filesScanned % FILES_SCANNED_LOGGING_PERIOD == 0) {
      LOG.info("Scanned " +
          StringUtils.humanReadableInt(filesScanned) + " files.");
    }
  }

  private void populateMetrics(
      Map<String, Statistics> codeToRaidStatistics) {
    RaidNodeMetrics metrics = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
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

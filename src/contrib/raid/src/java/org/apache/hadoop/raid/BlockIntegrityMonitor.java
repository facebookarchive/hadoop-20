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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;


/**
 * contains the core functionality of the block integrity monitor
 *
 * configuration options:
 * raid.blockfix.classname         - the class name of the integrity monitor 
 *                                   implementation to use
 *
 * raid.blockfix.interval          - interval between checks for lost files
 *
 * raid.blockfix.read.timeout      - read time out
 *
 * raid.blockfix.write.timeout     - write time out
 */
public abstract class BlockIntegrityMonitor extends Configured {

  public static final String BLOCKFIX_CLASSNAME = "raid.blockfix.classname";
  public static final String BLOCKCHECK_INTERVAL = "raid.blockfix.interval";
  public static final String CORRUPTFILECOUNT_INTERVAL = "raid.corruptfilecount.interval";
  public static final String BLOCKFIX_READ_TIMEOUT = 
    "raid.blockfix.read.timeout";
  public static final String BLOCKFIX_WRITE_TIMEOUT = 
    "raid.blockfix.write.timeout";
  // If a file has replication at least this, we can assume its not raided.
  public static final String NOT_RAIDED_REPLICATION =
    "raid.blockfix.noraid.replication";

  public static final long DEFAULT_BLOCKFIX_INTERVAL = 60 * 1000; // 1 min
  public static final long DEFAULT_CORRUPTFILECOUNT_INTERVAL = 600 * 1000; //10min
  public static final short DEFAULT_NOT_RAIDED_REPLICATION = 3;

  public static BlockIntegrityMonitor createBlockIntegrityMonitor(
      Configuration conf) throws ClassNotFoundException {
    try {
      // default to distributed integrity monitor
      Class<?> blockFixerClass =
        conf.getClass(BLOCKFIX_CLASSNAME, DistBlockIntegrityMonitor.class);
      if (!BlockIntegrityMonitor.class.isAssignableFrom(blockFixerClass)) {
        throw new ClassNotFoundException("not an implementation of " +
        		"blockintegritymonitor");
      }
      Constructor<?> constructor =
        blockFixerClass.getConstructor(new Class[] {Configuration.class} );
      return (BlockIntegrityMonitor) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new ClassNotFoundException("cannot construct integritymonitor", e);
    } catch (InstantiationException e) {
      throw new ClassNotFoundException("cannot construct integritymonitor", e);
    } catch (IllegalAccessException e) {
      throw new ClassNotFoundException("cannot construct integritymonitor", e);
    } catch (InvocationTargetException e) {
      throw new ClassNotFoundException("cannot construct integritymonitor", e);
    }
  }


  private long numFilesFixed = 0;
  private long numFileFixFailures = 0;
  private long numFilesCopied = 0;
  private long numFileCopyFailures = 0;
  private long numBlockFixSimulationFailures = 0;
  private long numBlockFixSimulationSuccess = 0;
  private long numFilesToFixDropped = 0;
  
  public volatile boolean running = true;

  // interval between checks for lost files
  protected long blockCheckInterval;
  protected long corruptFileCountInterval;
  protected short notRaidedReplication;

  public BlockIntegrityMonitor(Configuration conf) {
    super(conf);
    blockCheckInterval =
      getConf().getLong(BLOCKCHECK_INTERVAL, DEFAULT_BLOCKFIX_INTERVAL);
    corruptFileCountInterval = 
      getConf().getLong(CORRUPTFILECOUNT_INTERVAL, DEFAULT_CORRUPTFILECOUNT_INTERVAL);
    notRaidedReplication = (short) getConf().getInt(
      NOT_RAIDED_REPLICATION, DEFAULT_NOT_RAIDED_REPLICATION);
  }
  
  /**
   * Returns the number of new code file fixing verification success
   */
  public synchronized long getNumBlockFixSimulationSuccess() {
    return numBlockFixSimulationSuccess;
  }
  
  /**
   * Increments the number of new code file fixing verification success
   */
  protected synchronized void incrNumBlockFixSimulationSuccess(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).
      blockFixSimulationSuccess.inc(incr);
    numBlockFixSimulationSuccess += incr;
  }
  
  /**
   * Returns the number of new code file fixing verification failures
   */
  public synchronized long getNumBlockFixSimulationFailures() {
    return numBlockFixSimulationFailures;
  }
  
  /**
   * Increments the number of new code file fixing verification failures
   */
  protected synchronized void incrNumBlockFixSimulationFailures(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).
      blockFixSimulationFailures.inc(incr);
    numBlockFixSimulationFailures += incr;
  }

  /**
   * Returns the number of corrupt file fixing failures.
   */
  public synchronized long getNumFileFixFailures() {
    return numFileFixFailures;
  }

  /**
   * Increments the number of corrupt file fixing failures.
   */
  protected synchronized void incrFileFixFailures() {
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).fileFixFailures.inc();
    numFileFixFailures++;
  }
  
  /**
   * Increments the number of corrupt file fixing failures.
   */
  protected synchronized void incrFileFixFailures(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).fileFixFailures.inc(incr);
    numFileFixFailures += incr;
  }
  
   
  /**
   * Returns the number of corrupt files that have been fixed by this
   * integrity monitor.
   */
  public synchronized long getNumFilesFixed() {
    return numFilesFixed;
  }

  /**
   * Increments the number of corrupt files that have been fixed by this 
   * integrity monitor.
   */
  protected synchronized void incrFilesFixed() {
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).filesFixed.inc();
    numFilesFixed++;
  }
  
  /**
   * Increments the number of corrupt files that have been fixed by this 
   * integrity monitor.
   */
  protected synchronized void incrFilesFixed(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).filesFixed.inc(incr);
    numFilesFixed += incr;
  }
  
  /**
   * Returns the number of decommissioning file copy failures.
   */
  public synchronized long getNumFileCopyFailures() {
    return numFileCopyFailures;
  }

  /**
   * Increments the number of decommissioning file copy failures.
   */
  protected synchronized void incrFileCopyFailures(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }

    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).fileCopyFailures.inc(incr);
    numFileCopyFailures += incr;
  }
  
  /**
   * Returns the number of decommissioning files that have been copied by this
   * integrity monitor.
   */
  public synchronized long getNumFilesCopied() {
    return numFilesCopied;
  }

  /**
   * Increments the number of decommissioning files that have been copied by  
   * this integrity monitor.
   */
  protected synchronized void incrFilesCopied(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("Cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).filesCopied.inc(incr);
    numFilesCopied += incr;
  }

  static boolean isSourceFile(String p) {
    for (Codec codec: Codec.getCodecs()) {
      if (p.startsWith(codec.getParityPrefix())) {
        return false;
      }
    }
    return true;
  }

  static boolean doesParityDirExist(FileSystem parityFs, String path)
      throws IOException {
    // Check if it is impossible to have a parity file. We check if the
    // parent directory of the lost file exists under a parity path.
    // If the directory does not exist, the parity file cannot exist.
    Path fileRaidParent = new Path(path).getParent();
    Path dirRaidParent = (fileRaidParent != null)? fileRaidParent.getParent(): null;
    boolean parityCanExist = false;
    for (Codec codec: Codec.getCodecs()) {
      Path parityDir = null;
      if (codec.isDirRaid) {
        if (dirRaidParent == null) 
          continue;
        parityDir = (dirRaidParent.depth() == 0)?
          new Path(codec.getParityPrefix()):
          new Path(codec.getParityPrefix(),
              RaidNode.makeRelative(dirRaidParent));
      } else {
        parityDir = (fileRaidParent.depth() == 0)?
          new Path(codec.getParityPrefix()):
          new Path(codec.getParityPrefix(),
              RaidNode.makeRelative(fileRaidParent));
      }
      if (parityFs.exists(parityDir)) {
        parityCanExist = true;
        break;
      }
    }
    return parityCanExist;
  }

  void filterUnreconstructableSourceFiles(FileSystem parityFs, 
      Iterator<String> it)
      throws IOException {
    while (it.hasNext()) {
      String p = it.next();
      if (isSourceFile(p) &&
          !doesParityDirExist(parityFs, p)) {
          it.remove();
      }
    }
  }


  public abstract Status getAggregateStatus();

  public static class Status {
    final int highPriorityFiles;
    final int lowPriorityFiles;
    final int lowestPriorityFiles;
    final List<JobStatus> jobs;
    final List<JobStatus> simFailJobs;
    final List<String> highPriorityFileNames;
    final long lastUpdateTime;

    protected Status(int highPriorityFiles, int lowPriorityFiles,
        int lowestPriorityFiles,
        List<JobStatus> jobs, List<String> highPriorityFileNames,
        List<JobStatus> simFailJobs) {
      this.highPriorityFiles = highPriorityFiles;
      this.lowPriorityFiles = lowPriorityFiles;
      this.lowestPriorityFiles = lowestPriorityFiles;
      this.jobs = jobs;
      this.simFailJobs = simFailJobs;
      this.highPriorityFileNames = highPriorityFileNames;
      this.lastUpdateTime = RaidNode.now();
    }

    @Override
    public String toString() {
      String result = BlockIntegrityMonitor.class.getSimpleName() + " Status:";
      result += " HighPriorityFiles:" + highPriorityFiles;
      result += " LowPriorityFiles:" + lowPriorityFiles;
      result += " LowestPriorityFiles:" + lowPriorityFiles;
      result += " Jobs:" + jobs.size();
      result += " Sim Fail Jobs: " + simFailJobs.size();
      return result;
    }

    public String toHtml(int numCorruptToReport) {
      long now = RaidNode.now();
      String html = "";
      html += tr(td("High Priority Corrupted Files") + td(":") +
                 td(StringUtils.humanReadableInt(highPriorityFiles)));
      html += tr(td("Low Priority Corrupted Files") + td(":") +
                 td(StringUtils.humanReadableInt(lowPriorityFiles)));
      html += tr(td("Lowest Priority Corrupted Files") + td(":") +
                 td(StringUtils.humanReadableInt(lowestPriorityFiles)));
      html += tr(td("Running Jobs") + td(":") +
                 td(jobs.size() + ""));
      html += tr(td("Simulated Failed Jobs") + td(":") + 
                 td(simFailJobs.size() + ""));
      html += tr(td("Last Update") + td(":") +
                 td(StringUtils.formatTime(now - lastUpdateTime) + " ago"));
      html = JspUtils.tableSimple(html);
      if (numCorruptToReport <= 0) {
        return html;
      }
      
      if (simFailJobs.size() > 0) {
        String jobTable = tr(JobStatus.htmlRowHeader());
        for (JobStatus job : simFailJobs) {
          jobTable += tr(job.htmlRow());
        }
        jobTable = JspUtils.table(jobTable);
        html += "<br><h4>Simulated Fail Jobs:</h4><br>" + jobTable;
      }

      if (jobs.size() > 0) {
        String jobTable = tr(JobStatus.htmlRowHeader());
        for (JobStatus job : jobs) {
          jobTable += tr(job.htmlRow());
        }
        jobTable = JspUtils.table(jobTable);
        html += "<br>" + jobTable;
      }

      numCorruptToReport = Math.min(numCorruptToReport, highPriorityFileNames.size());
      if (numCorruptToReport > 0) {
        String highPriFilesTable = "";
        highPriFilesTable += tr(td("High Priority Corrupted Files") +
                                td(":") + td(highPriorityFileNames.get(0)));
        for (int i = 1; i < numCorruptToReport; ++i) {
          highPriFilesTable += tr(td("") + td(":") +
                                  td(highPriorityFileNames.get(i)));
        }
        highPriFilesTable = JspUtils.tableSimple(highPriFilesTable);
        html += "<br>" + highPriFilesTable;
      }

      return html;
    }
  }

  public static class JobStatus {
    final String id;
    final String name;
    final String url;
    JobStatus(JobID id, String name, String url) {
      this.id = id == null ? "" : id.toString();
      this.name = name == null ? "" : name;
      this.url = url == null ? "" : url;
    }
    @Override
    public String toString() {
      return "id:" + id + " name:" + name + " url:" + url;
    }
    public static String htmlRowHeader() {
      return td("JobID") + td("JobName");
    }
    public String htmlRow() {
      return td(JspUtils.link(id, url)) + td(name);
    }
  }
  private static String td(String s) {
    return JspUtils.td(s);
  }
  private static String tr(String s) {
    return JspUtils.tr(s);
  }
  
  public abstract Runnable getCorruptionMonitor();
  public abstract Runnable getDecommissioningMonitor();
  public abstract Runnable getCorruptFileCounter();
}


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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.raid.BlockReconstructor.CorruptBlockReconstructor;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker.LostFileInfo;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.RaidUtils.RaidInfo;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * distributed block integrity monitor, uses parity to reconstruct lost files
 *
 * configuration options
 * raid.blockfix.filespertask       - number of files to reconstruct in a single
 *                                    map reduce task (i.e., at one mapper node)
 *
 * raid.blockfix.fairscheduler.pool - the pool to use for MR jobs
 *
 * raid.blockfix.maxpendingjobs    - maximum number of MR jobs
 *                                    running simultaneously
 */
public class DistBlockIntegrityMonitor extends BlockIntegrityMonitor {
  public final static String[] BLOCKFIXER_MAPREDUCE_KEYS = {
    "mapred.job.tracker",
    "cm.server.address",
    "cm.server.http.address",
    "mapred.job.tracker.corona.proxyaddr",
    "corona.proxy.job.tracker.rpcaddr",
    "corona.system.dir",
    "mapred.temp.dir"
  };
  public final static String BLOCKFIXER = "blockfixer";

  private static final String IN_FILE_SUFFIX = ".in";
  private static final String PART_PREFIX = "part-";
  static final Pattern LIST_CORRUPT_FILE_PATTERN =
      Pattern.compile("blk_-*\\d+\\s+(.*)");
  static final Pattern LIST_DECOMMISSION_FILE_PATTERN = 
      Pattern.compile("blk_-*\\d+\\s+(.*)"); // For now this is the same because of how dfsck generates output 
  private static final String FILES_PER_TASK = 
    "raid.blockfix.filespertask";
  public static final String MAX_PENDING_JOBS =
    "raid.blockfix.maxpendingjobs";
  private static final String HIGH_PRI_SCHEDULER_OPTION =     
    "raid.blockfix.highpri.scheduleroption";        
  private static final String LOW_PRI_SCHEDULER_OPTION =        
    "raid.blockfix.lowpri.scheduleroption";     
  private static final String LOWEST_PRI_SCHEDULER_OPTION =     
    "raid.blockfix.lowestpri.scheduleroption";
  private static final String MAX_FIX_TIME_FOR_FILE =
    "raid.blockfix.max.fix.time.for.file";
  private static final String LOST_FILES_LIMIT =
    "raid.blockfix.corruptfiles.limit";
  private static final String RAIDNODE_BLOCK_FIXER_SCAN_NUM_THREADS_KEY =
    "raid.block.fixer.scan.threads";
  private static final int DEFAULT_BLOCK_FIXER_SCAN_NUM_THREADS = 5;
  private int blockFixerScanThreads = DEFAULT_BLOCK_FIXER_SCAN_NUM_THREADS;
  // The directories checked by the corrupt file monitor, seperate by comma
  public static final String RAIDNODE_BLOCK_FIX_SUBMISSION_INTERVAL_KEY = 
    "raid.block.fix.submission.interval";
  private static final long DEFAULT_BLOCK_FIX_SUBMISSION_INTERVAL = 5 * 1000;
  public static final String RAIDNODE_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL_KEY = 
      "raid.block.fix.scan.submission.interval";
  private static final long DEFAULT_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL = 5 * 1000;
  public static final String RAIDNODE_MAX_NUM_DETECTION_TIME_COLLECTED_KEY =
    "raid.max.num.detection.time.collected";
  public static final int DEFAULT_RAIDNODE_MAX_NUM_DETECTION_TIME_COLLECTED = 100;
  public enum UpdateNumFilesDropped {
    SET,
    ADD
  };

  // default number of files to reconstruct in a task
  private static final long DEFAULT_FILES_PER_TASK = 10L;

  private static final int TASKS_PER_JOB = 50;

  // default number of files to reconstruct simultaneously
  private static final long DEFAULT_MAX_PENDING_JOBS = 100L;

  private static final long DEFAULT_MAX_FIX_TIME_FOR_FILE =
    4 * 60 * 60 * 1000;  // 4 hrs.

  private static final int DEFAULT_LOST_FILES_LIMIT = 200000;
  public static final String FAILED_FILE = "failed";
  public static final String SIMULATION_FAILED_FILE = "simulation_failed";
 
  protected static final Log LOG = LogFactory.getLog(DistBlockIntegrityMonitor.class);
  
  private static final String CORRUPT_FILE_DETECT_TIME = "corrupt_detect_time";

  // number of files to reconstruct in a task
  private long filesPerTask;

  // number of files to reconstruct simultaneously
  final private long maxPendingJobs;

  final private long maxFixTimeForFile;

  final private int lostFilesLimit;

  private static final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  
  private Worker corruptionWorker = new CorruptionWorker();
  private Worker decommissioningWorker = new DecommissioningWorker();
  private Runnable corruptFileCounterWorker = new CorruptFileCounter();

  static enum RaidCounter {
    FILES_SUCCEEDED, FILES_FAILED, FILES_NOACTION,
    BLOCK_FIX_SIMULATION_FAILED, BLOCK_FIX_SIMULATION_SUCCEEDED, 
    FILE_FIX_NUM_READBYTES_REMOTERACK
  }
  
  static enum CorruptFileStatus {
    POTENTIALLY_CORRUPT,
    RAID_UNRECOVERABLE,
    NOT_RAIDED_UNRECOVERABLE,
    NOT_EXIST,
    RECOVERABLE
  }
  
  static enum Priority {
    HIGH  (HIGH_PRI_SCHEDULER_OPTION,   2),
    LOW   (LOW_PRI_SCHEDULER_OPTION,    1),
    LOWEST(LOWEST_PRI_SCHEDULER_OPTION, 0);
    
    public final String configOption;
    private final int underlyingValue;
    
    private Priority(String s, int value) {
      configOption = s;
      underlyingValue = value;
    }
    
    public boolean higherThan (Priority other) {
      return (underlyingValue > other.underlyingValue);
    }
  }
  
  static public class TrackingUrlInfo {
    String trackingUrl;
    long insertTime;
    public TrackingUrlInfo(String newUrl, long newTime) {
      trackingUrl = newUrl;
      insertTime = newTime;
    }
  }
  
  /**
   * Hold information about a failed file with task id
   */
  static public class FailedFileInfo {
    String taskId;
    LostFileInfo fileInfo;
    public FailedFileInfo(String newTaskId, LostFileInfo newFileInfo) {
      this.taskId = newTaskId;
      this.fileInfo = newFileInfo;
    }
  }

  public DistBlockIntegrityMonitor(Configuration conf) throws Exception {
    super(conf);
    filesPerTask = DistBlockIntegrityMonitor.getFilesPerTask(getConf());
    maxPendingJobs = DistBlockIntegrityMonitor.getMaxPendingJobs(getConf());
    maxFixTimeForFile = DistBlockIntegrityMonitor.getMaxFixTimeForFile(getConf());
    lostFilesLimit = DistBlockIntegrityMonitor.getLostFilesLimit(getConf());
  }
  
  public static void updateBlockFixerMapreduceConfigs(Configuration conf, String suffix) {
    for (String configKey: BLOCKFIXER_MAPREDUCE_KEYS) {
      String newKey = configKey + "." + suffix;
      String value = conf.get(newKey);
      if (value != null) {
        conf.set(configKey, value);
      }
    }
  }

  /**
   * determines how many files to reconstruct in a single task
   */ 
  protected static long getFilesPerTask(Configuration conf) {
    return conf.getLong(FILES_PER_TASK, 
                        DEFAULT_FILES_PER_TASK);

  }
  /**
   * determines how many files to reconstruct simultaneously
   */ 
  protected static long getMaxPendingJobs(Configuration conf) {
    return conf.getLong(MAX_PENDING_JOBS,
                        DEFAULT_MAX_PENDING_JOBS);
  }

  protected static long getMaxFixTimeForFile(Configuration conf) {
    return conf.getLong(MAX_FIX_TIME_FOR_FILE,
                        DEFAULT_MAX_FIX_TIME_FOR_FILE);
  }

  protected static int getLostFilesLimit(Configuration conf) {
    return conf.getInt(LOST_FILES_LIMIT, DEFAULT_LOST_FILES_LIMIT);
  }
  
  // Return true if succeed to start one job
  public static Job startOneJob(Worker newWorker, 
      Priority pri, Set<String> jobFiles, long detectTime, 
      AtomicLong numFilesSubmitted, AtomicLong lastCheckingTime,
      long maxPendingJobs) 
          throws IOException, InterruptedException, ClassNotFoundException {
    if (lastCheckingTime != null) {
      lastCheckingTime.set(System.currentTimeMillis());
    }
    String startTimeStr = dateFormat.format(new Date());
    String jobName = newWorker.JOB_NAME_PREFIX + "." + newWorker.jobCounter + 
        "." + pri + "-pri" + "." + startTimeStr;
    Job job = null;
    synchronized(jobFiles) {
      if (jobFiles.size() == 0) {
        return null;
      }
      newWorker.jobCounter++;
      
      synchronized(newWorker.jobIndex) {
        if (newWorker.jobIndex.size() >= maxPendingJobs) {
          // full 
          return null;
        }
        job = newWorker.startJob(jobName, jobFiles, pri, detectTime);
      }
      numFilesSubmitted.addAndGet(jobFiles.size());
      jobFiles.clear();
      
    }
    return job;
  }
  
  public abstract class Worker implements Runnable {

    protected Map<String, LostFileInfo> fileIndex = Collections.synchronizedMap(
      new HashMap<String, LostFileInfo>());
    protected Map<JobID, TrackingUrlInfo> idToTrakcingUrlMap = 
        Collections.synchronizedMap(new HashMap<JobID, TrackingUrlInfo>());
    protected Map<Job, List<LostFileInfo>> jobIndex =
      Collections.synchronizedMap(new HashMap<Job, List<LostFileInfo>>());
    protected Map<Job, List<FailedFileInfo>> failJobIndex =
        new HashMap<Job, List<FailedFileInfo>>();
    protected Map<Job, List<FailedFileInfo>> simFailJobIndex =
      new HashMap<Job, List<FailedFileInfo>>();

    private long jobCounter = 0;
    private AtomicInteger numJobsRunning = new AtomicInteger(0);
    
    protected AtomicLong numFilesDropped = new AtomicLong(0);
    
    volatile BlockIntegrityMonitor.Status lastStatus = null;
    AtomicLong recentNumFilesSucceeded = new AtomicLong();
    AtomicLong recentNumFilesFailed = new AtomicLong();
    AtomicLong recentSlotSeconds = new AtomicLong();
    AtomicLong recentNumBlockFixSimulationSucceeded = new AtomicLong();
    AtomicLong recentNumBlockFixSimulationFailed = new AtomicLong();
    AtomicLong recentNumReadBytesRemoteRack = new AtomicLong();
    Map<String, Long> recentLogMetrics = 
        Collections.synchronizedMap(new HashMap<String, Long>());
    
    private static final int POOL_SIZE = 2;
    private final ExecutorService executor = 
        Executors.newFixedThreadPool(POOL_SIZE);
    private static final int DEFAULT_CHECK_JOB_TIMEOUT_SEC = 600; //10 mins

    protected final Log LOG;
    protected final Class<? extends BlockReconstructor> RECONSTRUCTOR_CLASS;
    protected final String JOB_NAME_PREFIX;

    protected Worker(Log log, 
        Class<? extends BlockReconstructor> rClass, 
        String prefix) {

      this.LOG = log;
      this.RECONSTRUCTOR_CLASS = rClass;
      this.JOB_NAME_PREFIX = prefix;
      Path workingDir = new Path(prefix);
      try {
        FileSystem fs = workingDir.getFileSystem(getConf());
        // Clean existing working dir
        fs.delete(workingDir, true);
      } catch (IOException ioe) {
        LOG.warn("Get exception when cleaning " + workingDir, ioe);
      }
    }
    
    public void shutdown() {
    }


    /**
     * runs the worker periodically
     */
    public void run() {
      try {
        while (running) {
          try {
            updateStatus();
            checkAndReconstructBlocks();
          } catch (InterruptedException ignore) {
            LOG.info("interrupted");
          } catch (Exception e) {
            // log exceptions and keep running
            LOG.error(StringUtils.stringifyException(e));
          } catch (Error e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
          }
  
          try {
            Thread.sleep(blockCheckInterval);
          } catch (InterruptedException ignore) {
            LOG.info("interrupted");
          }
        }
      } finally {
        shutdown();
      }
    }

    /**
     * checks for lost blocks and reconstructs them (if any)
     */
    void checkAndReconstructBlocks() throws Exception {
      checkJobsWithTimeOut(DEFAULT_CHECK_JOB_TIMEOUT_SEC);
      int size = jobIndex.size();
      if (size >= maxPendingJobs) {
        LOG.info("Waiting for " + size + " pending jobs");
        return;
      }

      FileSystem fs = new Path("/").getFileSystem(getConf());
      Map<String, Integer> lostFiles = getLostFiles(fs);
      long detectTime = System.currentTimeMillis();
      computePrioritiesAndStartJobs(fs, lostFiles, detectTime);
    }

    /**
     * Handle a failed job.
     */
    private void failJob(Job job) {
      // assume no files have been reconstructed
      LOG.error("Job " + job.getID() + "(" + job.getJobName() +
      ") finished (failed)");
      // We do not change metrics here since we do not know for sure if file
      // reconstructing failed.
      for (LostFileInfo fileInfo: jobIndex.get(job)) {
        boolean failed = true;
        addToMap(job, job.getID().toString(), fileInfo, failJobIndex);
        fileInfo.finishJob(job.getJobName(), failed);
      }
      numJobsRunning.decrementAndGet();
    }
    
    private void addToMap(Job job, String taskId, LostFileInfo fileInfo, 
        Map<Job, List<FailedFileInfo>> index) {
      List<FailedFileInfo> failFiles = null;
      if (!index.containsKey(job)) {
        failFiles = new ArrayList<FailedFileInfo>();
        index.put(job, failFiles);
      } else {
        failFiles = index.get(job);
      }
      failFiles.add(new FailedFileInfo(taskId, fileInfo));
    }

    /**
     * Handle a successful job.
     */
    private void succeedJob(Job job, long filesSucceeded, long filesFailed)
    throws IOException {
      String jobName = job.getJobName();
      LOG.info("Job " + job.getID() + "(" + jobName +
      ") finished (succeeded)");
      // we have to look at the output to check which files have failed
      HashMap<String, String> failedFiles = getFailedFiles(job);
      for (LostFileInfo fileInfo: jobIndex.get(job)) {
        String filePath = fileInfo.getFile().toString();
        String failedFilePath = 
            DistBlockIntegrityMonitor.FAILED_FILE + "," +
            filePath;
        String simulatedFailedFilePath =
            DistBlockIntegrityMonitor.SIMULATION_FAILED_FILE + "," + 
            filePath;
        if (failedFiles.containsKey(simulatedFailedFilePath)) {
          String taskId = failedFiles.get(simulatedFailedFilePath);
          addToMap(job, taskId, fileInfo, simFailJobIndex);
          LOG.error("Simulation failed file: " + fileInfo.getFile());
        }
        if (failedFiles.containsKey(failedFilePath)) {
          String taskId = failedFiles.get(failedFilePath);
          addToMap(job, taskId, fileInfo, failJobIndex);
          boolean failed = true;
          fileInfo.finishJob(jobName, failed);
        } else {
          // call succeed for files that have succeeded or for which no action
          // was taken
          boolean failed = false;
          fileInfo.finishJob(jobName, failed);
        }
      }
      // report succeeded files to metrics
      this.recentNumFilesSucceeded.addAndGet(filesSucceeded);
      this.recentNumFilesFailed.addAndGet(filesFailed);
      if (filesSucceeded > 0) {
        lastSuccessfulFixTime = System.currentTimeMillis();
      }
      numJobsRunning.decrementAndGet();
    }
    
    /**
     * Check the jobs with timeout
     */
    void checkJobsWithTimeOut(int timeoutSec) 
        throws ExecutionException {
      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          checkJobs();
          return true;
        }
      });
      try {
        future.get(timeoutSec, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // ignore this.
        LOG.warn("Timeout when checking jobs' status.");
      } catch (InterruptedException e) {
        // ignore this.
        LOG.warn("checkJobs function is interrupted.");
      }
      if (!future.isDone()) {
        future.cancel(true);
      }
    }

    /**
     * checks if jobs have completed and updates job and file index
     * returns a list of failed files for restarting
     */
    void checkJobs() throws IOException {
      List<Job> nonRunningJobs = new ArrayList<Job>();
      synchronized(jobIndex) {
        Iterator<Job> jobIter = jobIndex.keySet().iterator();
        while(jobIter.hasNext()) {
          Job job = jobIter.next();
  
          try {
            if (job.isComplete()) {
              Counters ctrs = job.getCounters();
              if (ctrs != null) {
                // If we got counters, perform extra validation.
                this.recentSlotSeconds.addAndGet(ctrs.findCounter(
                    JobInProgress.Counter.SLOTS_MILLIS_MAPS).getValue() / 1000);
                
                long filesSucceeded =
                    ctrs.findCounter(RaidCounter.FILES_SUCCEEDED) != null ?
                      ctrs.findCounter(RaidCounter.FILES_SUCCEEDED).getValue() : 0;
                long filesFailed =
                    ctrs.findCounter(RaidCounter.FILES_FAILED) != null ?
                      ctrs.findCounter(RaidCounter.FILES_FAILED).getValue() : 0;
                long filesNoAction =
                    ctrs.findCounter(RaidCounter.FILES_NOACTION) != null ?
                      ctrs.findCounter(RaidCounter.FILES_NOACTION).getValue() : 0;
                long blockFixSimulationFailed = 
                    ctrs.findCounter(RaidCounter.BLOCK_FIX_SIMULATION_FAILED) != null?
                      ctrs.findCounter(RaidCounter.BLOCK_FIX_SIMULATION_FAILED).getValue() : 0;
                long blockFixSimulationSucceeded = 
                    ctrs.findCounter(RaidCounter.BLOCK_FIX_SIMULATION_SUCCEEDED) != null?
                      ctrs.findCounter(RaidCounter.BLOCK_FIX_SIMULATION_SUCCEEDED).getValue() : 0;
                this.recentNumBlockFixSimulationFailed.addAndGet(blockFixSimulationFailed);
                this.recentNumBlockFixSimulationSucceeded.addAndGet(blockFixSimulationSucceeded);
                long fileFixNumReadBytesRemoteRack = 
                    ctrs.findCounter(RaidCounter.FILE_FIX_NUM_READBYTES_REMOTERACK) != null ?
                      ctrs.findCounter(RaidCounter.FILE_FIX_NUM_READBYTES_REMOTERACK).getValue() : 0;
                this.recentNumReadBytesRemoteRack.addAndGet(fileFixNumReadBytesRemoteRack);
                CounterGroup counterGroup = ctrs.getGroup(LogUtils.LOG_COUNTER_GROUP_NAME);
                for (Counter ctr: counterGroup) {
                  Long curVal = ctr.getValue();
                  if (this.recentLogMetrics.containsKey(ctr.getName())) {
                    curVal += this.recentLogMetrics.get(ctr.getName());
                  }
                  this.recentLogMetrics.put(ctr.getName(), curVal);
                }
                
                int files = jobIndex.get(job).size();
                
                if (job.isSuccessful() &&
                    (filesSucceeded + filesFailed + filesNoAction ==
                      ((long) files))) {
                  // job has processed all files
                  succeedJob(job, filesSucceeded, filesFailed);
                } else {
                  failJob(job);
                }
              } else {
                long filesSucceeded = jobIndex.get(job).size();
                long filesFailed = 0;
                if (job.isSuccessful()) {
                  succeedJob(job, filesSucceeded, filesFailed);
                } else {
                  failJob(job);
                }
              }
              jobIter.remove();
              nonRunningJobs.add(job);
            } else {
              LOG.info("Job " + job.getID() + "(" + job.getJobName()
                  + " still running");
            }
          } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
            failJob(job);
            jobIter.remove();
            nonRunningJobs.add(job);
            try {
              job.killJob();
            } catch (Exception ee) {
              LOG.error(StringUtils.stringifyException(ee));
            }
          }
        }
      }
      purgeFileIndex();
      cleanupNonRunningJobs(nonRunningJobs);
    }

    /**
     * Delete (best-effort) the input and output directories of jobs.
     * @param nonRunningJobs
     */
    private void cleanupNonRunningJobs(List<Job> nonRunningJobs) {
      for (Job job: nonRunningJobs) {
        Path outDir = null;
        try {
          outDir = SequenceFileOutputFormat.getOutputPath(job);
          outDir.getFileSystem(getConf()).delete(outDir, true);
        } catch (IOException e) {
          LOG.warn("Could not delete output dir " + outDir, e);
        }
        Path[] inDir = null;
        try {
          // We only create one input directory.
          inDir = ReconstructionInputFormat.getInputPaths(job);
          inDir[0].getFileSystem(getConf()).delete(inDir[0], true);
        } catch (IOException e) {
          LOG.warn("Could not delete input dir " + inDir[0], e);
        }
      }
    }


    /**
     * determines which files have failed for a given job
     */
    private HashMap<String, String> getFailedFiles(Job job) throws IOException {
      HashMap<String, String> failedFiles = new HashMap<String, String>();

      Path outDir = SequenceFileOutputFormat.getOutputPath(job);
      FileSystem fs  = outDir.getFileSystem(getConf());
      if (!fs.getFileStatus(outDir).isDir()) {
        throw new IOException(outDir.toString() + " is not a directory");
      }

      FileStatus[] files = fs.listStatus(outDir);

      for (FileStatus f: files) {
        Path fPath = f.getPath();
        if ((!f.isDir()) && (fPath.getName().startsWith(PART_PREFIX))) {
          LOG.info("opening " + fPath.toString());
          SequenceFile.Reader reader = 
            new SequenceFile.Reader(fs, fPath, getConf());

          Text key = new Text();
          Text value = new Text();
          while (reader.next(key, value)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("key: " + key.toString() + " , value: " + value.toString());
            }
            failedFiles.put(key.toString(), value.toString());
          }
          reader.close();
        }
      }
      return failedFiles;
    }


    /**
     * purge expired jobs from the file index
     */
    private void purgeFileIndex() {
      Iterator<String> fileIter = fileIndex.keySet().iterator();
      long now = System.currentTimeMillis();
      while(fileIter.hasNext()) {
        String file = fileIter.next();
        if (fileIndex.get(file).isTooOld(now)) {
          fileIter.remove();
        }
      }
      Iterator<TrackingUrlInfo> tuiIter = 
          this.idToTrakcingUrlMap.values().iterator();
      while (tuiIter.hasNext()) {
        TrackingUrlInfo tui = tuiIter.next();
        if (System.currentTimeMillis() - tui.insertTime > maxWindowTime) {
          tuiIter.remove();
        }
      }
    }

    // Start jobs for all the lost files.
    public void startJobs(Map<String, Priority> filePriorities, long detectTime)
    throws IOException, InterruptedException, ClassNotFoundException {
      AtomicLong numFilesSubmitted = new AtomicLong(0);
      for (Priority pri : Priority.values()) {
        Set<String> jobFiles = new HashSet<String>();
        for (Map.Entry<String, Priority> entry: filePriorities.entrySet()) {
          // Check if file priority matches the current round.
          if (entry.getValue() != pri) {
            continue;
          }
          jobFiles.add(entry.getKey());
          // Check if we have hit the threshold for number of files in a job.
          if (jobFiles.size() == filesPerTask * TASKS_PER_JOB) {
            boolean succeed = startOneJob(this, pri, jobFiles, detectTime,
                numFilesSubmitted, null, maxPendingJobs) != null;
            if (!succeed) {
              this.numFilesDropped.set(filePriorities.size() -
                  numFilesSubmitted.get());
              LOG.debug("Submitted a job with max number of files allowed. " + 
                        "Num files dropped is " + this.numFilesDropped.get());
              return;
            }
          }
        }
        if (jobFiles.size() > 0) {
          boolean succeed = startOneJob(this, pri, jobFiles, detectTime,
              numFilesSubmitted, null, maxPendingJobs) != null;
          if (!succeed) {
            this.numFilesDropped.set(filePriorities.size() -
                numFilesSubmitted.get());
            LOG.debug("Submitted a job with max number of files allowed. " + 
                      "Num files dropped is " + this.numFilesDropped.get());
            return;
          }
        }
      }
      this.numFilesDropped.set(filePriorities.size() - numFilesSubmitted.get());
    }

    /**
     * creates and submits a job, updates file index and job index
     */
    private Job startJob(String jobName, Set<String> lostFiles, Priority priority,
        long detectTime)
    throws IOException, InterruptedException, ClassNotFoundException {
      Path inDir = new Path(JOB_NAME_PREFIX + "/in/" + jobName);
      Path outDir = new Path(JOB_NAME_PREFIX + "/out/" + jobName);
      List<String> filesInJob = createInputFile(
          jobName, inDir, lostFiles);
      if (filesInJob.isEmpty()) return null;

      Configuration jobConf = new Configuration(getConf());
      DistBlockIntegrityMonitor.updateBlockFixerMapreduceConfigs(jobConf, BLOCKFIXER);
      RaidUtils.parseAndSetOptions(jobConf, priority.configOption);
      Job job = new Job(jobConf, jobName);
      job.getConfiguration().set(CORRUPT_FILE_DETECT_TIME, Long.toString(detectTime));
      configureJob(job, this.RECONSTRUCTOR_CLASS);
      job.setJarByClass(getClass());
      job.setMapperClass(ReconstructionMapper.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(ReconstructionInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      ReconstructionInputFormat.setInputPaths(job, inDir);
      SequenceFileOutputFormat.setOutputPath(job, outDir);
      

      submitJob(job, filesInJob, priority);
      List<LostFileInfo> fileInfos =
        updateFileIndex(jobName, filesInJob, priority);
      // The implementation of submitJob() need not update jobIndex.
      // So check if the job exists in jobIndex before updating jobInfos.
      if (jobIndex.containsKey(job)) {
        jobIndex.put(job, fileInfos);
      }
      numJobsRunning.incrementAndGet();
      return job;
    }

    void submitJob(Job job, List<String> filesInJob, Priority priority)
    throws IOException, InterruptedException, ClassNotFoundException {
      LOG.info("Submitting job");
      DistBlockIntegrityMonitor.this.submitJob(job, filesInJob, priority, this.jobIndex,
          this.idToTrakcingUrlMap);
    }

    /**
     * inserts new job into file index and job index
     */
    private List<LostFileInfo> updateFileIndex(
        String jobName, List<String> lostFiles, Priority priority) {
      List<LostFileInfo> fileInfos = new ArrayList<LostFileInfo>();

      for (String file: lostFiles) {
        LostFileInfo fileInfo = fileIndex.get(file);
        if (fileInfo != null) {
          fileInfo.addJob(jobName, priority);
        } else {
          fileInfo = new LostFileInfo(file, jobName, priority);
          fileIndex.put(file, fileInfo);
        }
        fileInfos.add(fileInfo);
      }
      return fileInfos;
    }

    /**
     * creates the input file (containing the names of the files to be 
     * reconstructed)
     */
    private List<String> createInputFile(String jobName, Path inDir,
        Set<String> lostFiles) throws IOException {
      Path file = new Path(inDir, jobName + IN_FILE_SUFFIX);
      FileSystem fs = file.getFileSystem(getConf());
      SequenceFile.Writer fileOut = SequenceFile.createWriter(fs, getConf(), file,
          LongWritable.class,
          Text.class);
      long index = 0L;

      List<String> filesAdded = new ArrayList<String>();
      int count = 0;
      for (String lostFileName: lostFiles) {
        fileOut.append(new LongWritable(index++), new Text(lostFileName));
        filesAdded.add(lostFileName);
        count++;

        if (index % filesPerTask == 0) {
          fileOut.sync(); // create sync point to make sure we can split here
        }
      }

      fileOut.close();
      return filesAdded;
    }
  
    /**
     * Update {@link lastStatus} so that it can be viewed from outside
     */
    protected void updateStatus() {
      int highPriorityFiles = 0;
      int lowPriorityFiles = 0;
      int lowestPriorityFiles = 0;
      List<JobStatus> jobs = new ArrayList<JobStatus>();
      List<JobStatus> failJobs = new ArrayList<JobStatus>();
      List<JobStatus> simFailJobs = new ArrayList<JobStatus>();
      List<String> highPriorityFileNames = new ArrayList<String>();
      for (Map.Entry<String, LostFileInfo> e : fileIndex.entrySet()) {
        String fileName = e.getKey();
        LostFileInfo fileInfo = e.getValue();
        Priority pri = fileInfo.getHighestPriority();
        if (pri == Priority.HIGH) {
          highPriorityFileNames.add(fileName);
          highPriorityFiles++;
        } else if (pri == Priority.LOW){
          lowPriorityFiles++;
        } else if (pri == Priority.LOWEST) {
          lowestPriorityFiles++;
        }
      }
      synchronized(jobIndex) {
        for (Job job : jobIndex.keySet()) {
          String url = job.getTrackingURL();
          String name = job.getJobName();
          JobID jobId = job.getID();
          jobs.add(new BlockIntegrityMonitor.JobStatus(jobId, name, url,
              jobIndex.get(job), null));
        }
        for (Job job : failJobIndex.keySet()) {
          String url = job.getTrackingURL();
          String name = job.getJobName();
          JobID jobId = job.getID();
          failJobs.add(new BlockIntegrityMonitor.JobStatus(jobId, name, url,
              null, failJobIndex.get(job)));
        }
        for (Job simJob : simFailJobIndex.keySet()) {
          String url = simJob.getTrackingURL();
          String name = simJob.getJobName();
          JobID jobId = simJob.getID();
          simFailJobs.add(new BlockIntegrityMonitor.JobStatus(jobId, name, url,
              null, simFailJobIndex.get(simJob)));
        }
      }
      lastStatus = new BlockIntegrityMonitor.Status(highPriorityFiles, lowPriorityFiles,
          lowestPriorityFiles, jobs, highPriorityFileNames, failJobs, simFailJobs);
      updateRaidNodeMetrics();
    }
    
    public Status getStatus() {
      return lastStatus;
    }

    abstract void computePrioritiesAndStartJobs(
        FileSystem fs, Map<String, Integer> lostFiles, long detectTime)
            throws IOException, InterruptedException, ClassNotFoundException;

    protected abstract Map<String, Integer> getLostFiles(FileSystem fs) throws IOException;

    protected abstract void updateRaidNodeMetrics();
    
    /**
     * hold information about a lost file that is being reconstructed
     */
    class LostFileInfo {

      private String file;
      private List<String> jobNames;  // Jobs reconstructing this file.
      private boolean done;
      private List<Priority> priorities;
      private long insertTime;

      public LostFileInfo(String file, String jobName, Priority priority) {
        this.file = file;
        this.jobNames = new ArrayList<String>();
        this.priorities = new ArrayList<Priority>();
        this.done = false;
        this.insertTime = System.currentTimeMillis();
        addJob(jobName, priority);
      }

      public boolean isTooOld(long now) {
        return now - insertTime > maxFixTimeForFile;
      }

      public boolean isDone() {
        return done;
      }

      public void addJob(String jobName, Priority priority) {
        this.jobNames.add(jobName);
        this.priorities.add(priority);
      }

      public Priority getHighestPriority() {
        Priority max = Priority.LOWEST;
        for (Priority p: priorities) {
          if (p.higherThan(max)) max = p;
        }
        return max;
      }

      public String getFile() {
        return file;
      }

      /**
       * Updates state with the completion of a job. If all jobs for this file
       * are done, the file index is updated.
       */
      public void finishJob(String jobName, boolean failed) {
        int idx = jobNames.indexOf(jobName);
        if (idx == -1) return;
        jobNames.remove(idx);
        priorities.remove(idx);
        LOG.info("reconstructing " + file +
            (failed ? " failed in " : " succeeded in ") +
            jobName);
        if (jobNames.isEmpty()) {
          // All jobs dealing with this file are done,
          // remove this file from the index
          LostFileInfo removed = fileIndex.remove(file);
          if (removed == null) {
            LOG.error("trying to remove file not in file index: " + file);
          }
          done = true;
        }
      }
    }

    public String getTrackingUrl(JobID jobId) {
      TrackingUrlInfo tui = this.idToTrakcingUrlMap.get(jobId); 
      if (tui == null) {
        return "";
      } else {
        return tui.trackingUrl;
      }
    }
  }
  
  /**
   * CorruptFileCounter is a periodical running daemon that keeps running raidfsck 
   * to get the number of the corrupt files under the give directories defined by 
   * RAIDNODE_CORRUPT_FILE_COUNTER_DIRECTORIES_KEY
   * @author weiyan
   *
   */
  public class CorruptFileCounter implements Runnable {
    private long filesWithMissingBlksCnt = 0;
    private Map<String, long[]> numStrpWithMissingBlksMap = new HashMap<String, long[]>();
    private Object counterMapLock = new Object();
    private long numNonRaidedMissingBlocks = 0;

    public CorruptFileCounter() {
      for (Codec codec : Codec.getCodecs()) {
        this.numStrpWithMissingBlksMap.put(codec.id,
            new long[codec.stripeLength + codec.parityLength]);
      }
    }

    public void run() {
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID)
        .initCorruptFilesMetrics(getConf());
      while (running) {
        TreeMap<String, Long> newUnRecoverableCounterMap = new TreeMap<String, Long>();
        Map<String, Long> newRecoverableCounterMap = new HashMap<String, Long>();
        long newfilesWithMissingBlksCnt = 0;
        String srcDir = "/";
        try {
          ByteArrayOutputStream bout = new ByteArrayOutputStream();
          PrintStream ps = new PrintStream(bout, true);
          RaidShell shell = new RaidShell(getConf(), ps);
          int res = ToolRunner.run(shell, new String[] { "-fsck", srcDir,
              "-count", "-retNumStrpsMissingBlks" });
          shell.close();
          ByteArrayInputStream bin = new ByteArrayInputStream(
              bout.toByteArray());
          BufferedReader reader = new BufferedReader(new InputStreamReader(bin));
          String line = reader.readLine();
          if (line == null) {
            throw new IOException("Raidfsck fails without output");
          }
          Long corruptCount = Long.parseLong(line);
          LOG.info("The number of corrupt files under " + srcDir + " is "
              + corruptCount);
          newUnRecoverableCounterMap.put(srcDir, corruptCount);
          line = reader.readLine();
          if (line == null) {
            throw new IOException("Raidfsck did not print number "
                + "of files with missing blocks");
          }

          // get files with Missing Blks
          // fsck with '-count' prints this number in line2
          long incfilesWithMissingBlks = Long.parseLong(line);
          LOG.info("The number of files with missing blocks under " + srcDir
              + " is " + incfilesWithMissingBlks);

          long numRecoverableFiles = incfilesWithMissingBlks - corruptCount;
          newRecoverableCounterMap.put(srcDir, numRecoverableFiles);
          approximateNumRecoverableFiles = numRecoverableFiles;

          // Add filesWithMissingBlks and numStrpWithMissingBlks only for "/"
          // dir to avoid duplicates
          Map<String, long[]> newNumStrpWithMissingBlksMap = new HashMap<String, long[]>();
          newfilesWithMissingBlksCnt += incfilesWithMissingBlks;
          // read the array for num stripes with missing blocks

          line = reader.readLine();
          if (line == null) {
            throw new IOException("Raidfsck did not print the number of "
                + "missing blocks in non raided files");
          }
          long numNonRaided = Long.parseLong(line);

          for (int i = 0; i < Codec.getCodecs().size(); i++) {
            line = reader.readLine();
            if (line == null) {
              throw new IOException("Raidfsck did not print the missing "
                  + "block info for codec at index " + i);
            }

            Codec codec = Codec.getCodec(line);
            long[] incNumStrpWithMissingBlks = new long[codec.stripeLength
                                                        + codec.parityLength];
            for (int j = 0; j < incNumStrpWithMissingBlks.length; j++) {
              line = reader.readLine();
              if (line == null) {
                throw new IOException("Raidfsck did not print the array "
                          + "for number stripes with missing blocks for index "
                          + j);
              }
              incNumStrpWithMissingBlks[j] = Long.parseLong(line);
              LOG.info("The number of stripes with missing blocks at index"
                        + j + "under" + srcDir + " is "
                        + incNumStrpWithMissingBlks[j]);
            }
            newNumStrpWithMissingBlksMap.put(codec.id, incNumStrpWithMissingBlks);
          }
          synchronized (counterMapLock) {
            this.numNonRaidedMissingBlocks = numNonRaided;
            for (String codeId : newNumStrpWithMissingBlksMap.keySet()) {
              numStrpWithMissingBlksMap.put(codeId,
              newNumStrpWithMissingBlksMap.get(codeId));
            }
          }
          reader.close();
          bin.close();
        } catch (Exception e) {
          LOG.error("Fail to count the corrupt files under " + srcDir, e);
        }
        synchronized (counterMapLock) {
          this.filesWithMissingBlksCnt = newfilesWithMissingBlksCnt;
        }
        updateRaidNodeMetrics();
        if (!running) {
          break;
        }
        try {
          Thread.sleep(corruptFileCountInterval);
        } catch (InterruptedException ignore) {
          LOG.info("interrupted");
        }
      }
    }

    public long getNumNonRaidedMissingBlks() {
      synchronized (counterMapLock) {
        return this.numNonRaidedMissingBlocks;
      }
    }

    public long getFilesWithMissingBlksCnt() {
      synchronized (counterMapLock) {
        return filesWithMissingBlksCnt;
      }
    }

    public long[] getNumStrpWithMissingBlksRS() {
      synchronized (counterMapLock) {
        return numStrpWithMissingBlksMap.get("rs");
      }
    }

    protected void updateRaidNodeMetrics() {
      RaidNodeMetrics rnm = RaidNodeMetrics
        .getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);

      synchronized (counterMapLock) {
        rnm.numFilesWithMissingBlks.set(this.filesWithMissingBlksCnt);
        long[] numStrpWithMissingBlksRS = this.numStrpWithMissingBlksMap
          .get("rs");

        if (numStrpWithMissingBlksRS != null) {
          rnm.numStrpsOneMissingBlk.set(numStrpWithMissingBlksRS[0]);
          rnm.numStrpsTwoMissingBlk.set(numStrpWithMissingBlksRS[1]);
          rnm.numStrpsThreeMissingBlk.set(numStrpWithMissingBlksRS[2]);
          rnm.numStrpsFourMissingBlk.set(numStrpWithMissingBlksRS[3]);

          long tmp_sum = 0;
          for (int idx = 4; idx < numStrpWithMissingBlksRS.length; idx++) {
            tmp_sum += numStrpWithMissingBlksRS[idx];
          }
          rnm.numStrpsFiveMoreMissingBlk.set(tmp_sum);
        }
      }
    }

    public String getMissingBlksHtmlTable() {
      synchronized (counterMapLock) {
        return RaidUtils.getMissingBlksHtmlTable(
            this.numNonRaidedMissingBlocks, this.numStrpWithMissingBlksMap);
      }
    }
  }
  
  /**
   * Get the lost blocks numbers per stripe in the source file.
   */
  private Map<Integer, Integer> getLostStripes(
              Configuration conf, FileStatus stat, FileSystem fs) 
                  throws IOException {
    Map<Integer, Integer> lostStripes = new HashMap<Integer, Integer>();
    RaidInfo raidInfo = RaidUtils.getFileRaidInfo(stat, conf);
    if (raidInfo.codec == null) {
      // Can not find the parity file, the file is not raided.
      return lostStripes;
    }
    Codec codec = raidInfo.codec;
    
    if (codec.isDirRaid) {
      RaidUtils.collectDirectoryCorruptBlocksInStripe(conf, 
          (DistributedFileSystem)fs, raidInfo, 
          stat, lostStripes);
    } else {
      RaidUtils.collectFileCorruptBlocksInStripe((DistributedFileSystem)fs, 
          raidInfo, stat, lostStripes);
    }
    return lostStripes;
  }
  
  public class CorruptFile {
    public String path;
    public long detectTime;
    public volatile int numCorrupt;
    public volatile CorruptFileStatus fileStatus;
    public volatile long lastSubmitTime;
    public CorruptFile(String newPath, int newNumCorrupt, long newDetectTime) {
      this.path = newPath;
      this.numCorrupt = newNumCorrupt;
      this.fileStatus = CorruptFileStatus.POTENTIALLY_CORRUPT;
      this.lastSubmitTime = System.currentTimeMillis();
      this.detectTime = newDetectTime;
    }
    
    public String toString() {
      return fileStatus.name();
    }
  }
  
  public class MonitorSet {
    public ConcurrentHashMap<String, CorruptFile> toScanFiles;
    public ExecutorService executor;
    public BlockingQueue<Runnable> scanningQueue;
    public MonitorSet(final String monitorDir) {
      this.scanningQueue = new LinkedBlockingQueue<Runnable>();
      ThreadFactory factory = new ThreadFactory() {
        final AtomicInteger numThreads = new AtomicInteger();
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("BlockFix-Scanner-" + monitorDir + "-" + 
            numThreads.getAndIncrement());
          return t;
        }
      };
      this.executor = new ThreadPoolExecutor(blockFixerScanThreads,
          blockFixerScanThreads, 0L, TimeUnit.MILLISECONDS, scanningQueue,
          factory);
      this.toScanFiles = new ConcurrentHashMap<String, CorruptFile>();
    }
  }

  public class CorruptionWorker extends Worker {
    public static final String RAIDNODE_JOB_SUBMIT_NUM_THREADS_KEY = 
        "raid.job.submit.num.threads";
    public static final int RAIDNODE_JOB_SUBMIT_NUM_THREADS_DEFAULT = 5;
    public String[] corruptMonitorDirs = null;
    public HashMap<String, MonitorSet> monitorSets;
    public final String OTHERS = "others";
    public HashMap<Priority, HashSet<String>> jobFilesMap;
    public HashMap<Priority, AtomicLong> lastCheckingTimes;
    public AtomicLong numFilesSubmitted = new AtomicLong(0);
    public AtomicLong totalFilesToSubmit = new AtomicLong(0);
    private long blockFixSubmissionInterval =
        DEFAULT_BLOCK_FIX_SUBMISSION_INTERVAL;
    private long blockFixScanSubmissionInterval = 
        DEFAULT_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL;
    private int maxNumDetectionTime;
    // Collection of recent X samples;
    private long[] detectionTimeCollection;
    private int currPos;
    private long totalDetectionTime;
    private long totalCollecitonSize;
    private ExecutorService jobSubmitExecutor;
    private BlockingQueue<Runnable> jobSubmitQueue;
    private int jobSubmitThreads = RAIDNODE_JOB_SUBMIT_NUM_THREADS_DEFAULT;

    public CorruptionWorker() {
      super(LogFactory.getLog(CorruptionWorker.class), 
          CorruptBlockReconstructor.class, 
          "blockfixer");
      blockFixerScanThreads = getConf().getInt(
          RAIDNODE_BLOCK_FIXER_SCAN_NUM_THREADS_KEY,
          DEFAULT_BLOCK_FIXER_SCAN_NUM_THREADS);
      this.blockFixSubmissionInterval = getConf().getLong(
          RAIDNODE_BLOCK_FIX_SUBMISSION_INTERVAL_KEY,
          DEFAULT_BLOCK_FIX_SUBMISSION_INTERVAL);
      this.blockFixScanSubmissionInterval = getConf().getLong(
          RAIDNODE_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL_KEY, 
          DEFAULT_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL);
      this.corruptMonitorDirs = DistBlockIntegrityMonitor.getCorruptMonitorDirs(
          getConf());
      this.monitorSets = new HashMap<String, MonitorSet>();
      for (String monitorDir : this.corruptMonitorDirs) {
        this.monitorSets.put(monitorDir, new MonitorSet(monitorDir));
      }
      this.monitorSets.put(OTHERS, new MonitorSet(OTHERS));
      this.jobFilesMap = new HashMap<Priority, HashSet<String>>();
      lastCheckingTimes = new HashMap<Priority, AtomicLong>();
      for (Priority priority: Priority.values()) {
        this.jobFilesMap.put(priority, new HashSet<String>());
        this.lastCheckingTimes.put(priority, new AtomicLong(System.currentTimeMillis()));
      }
      this.maxNumDetectionTime = getConf().getInt(
          RAIDNODE_MAX_NUM_DETECTION_TIME_COLLECTED_KEY, 
          DEFAULT_RAIDNODE_MAX_NUM_DETECTION_TIME_COLLECTED);
      detectionTimeCollection = new long[maxNumDetectionTime];
      this.totalCollecitonSize = 0;
      this.totalDetectionTime = 0;
      this.currPos = 0;
      this.jobSubmitThreads = getConf().getInt(RAIDNODE_JOB_SUBMIT_NUM_THREADS_KEY,
          RAIDNODE_JOB_SUBMIT_NUM_THREADS_DEFAULT);
      this.jobSubmitQueue = new LinkedBlockingQueue<Runnable>();
      ThreadFactory factory = new ThreadFactory() {
        final AtomicInteger numThreads = new AtomicInteger();
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("BlockFix-Job-Submit-" + numThreads.getAndIncrement());
          return t;
        }
      };
      this.jobSubmitExecutor = new ThreadPoolExecutor(this.jobSubmitThreads,
          this.jobSubmitThreads, 0L, TimeUnit.MILLISECONDS, this.jobSubmitQueue,
          factory);
    }
    
    public void putDetectionTime(long detectionTime) {
      synchronized(detectionTimeCollection) {
        long oldVal = detectionTimeCollection[currPos]; 
        detectionTimeCollection[currPos] = detectionTime;
        totalDetectionTime += detectionTime - oldVal;
        currPos++;
        if (currPos == maxNumDetectionTime) {
          currPos = 0;
        }
        if (totalCollecitonSize < maxNumDetectionTime) {
          totalCollecitonSize++;
        }
      }
    }
    
    public double getNumDetectionsPerSec() {
      synchronized(detectionTimeCollection) {
        if (totalCollecitonSize == 0) {
          return 0;
        } else {
          return ((double)totalCollecitonSize)*1000/totalDetectionTime
              * blockFixerScanThreads;
        }
      }
    }

    @Override
    protected Map<String, Integer> getLostFiles(FileSystem fs) throws IOException {
      Map<String, Integer> lostFiles = new HashMap<String, Integer>();
      RemoteIterator<Path> cfb = fs.listCorruptFileBlocks(new Path("/"));
      while (cfb.hasNext()) {
        String lostFile = cfb.next().toString();
        Integer count = lostFiles.get(lostFile);
        if (count == null) {
          lostFiles.put(lostFile, 1);
        } else {
          lostFiles.put(lostFile, count+1);
        }
      }
      LOG.info("ListCorruptFileBlocks returned " + lostFiles.size() + " files");
      RaidUtils.filterTrash(getConf(), lostFiles.keySet().iterator());
      LOG.info("getLostFiles returning " + lostFiles.size() + " files");

      return lostFiles;
    }
    
    public void addToScanSet(String p, int numCorrupt, String monitorDir,
        ConcurrentHashMap<String, CorruptFile> newScanSet, FileSystem fs,
        long detectTime)
            throws IOException {
      CorruptFile cf = new CorruptFile(p, numCorrupt, detectTime);
      MonitorSet monitorSet = monitorSets.get(monitorDir);
      CorruptFile oldCf = monitorSet.toScanFiles.get(p);
      FileCheckRunnable fcr = new FileCheckRunnable(cf, monitorSet, fs,
          detectTime, this);
      if (oldCf == null) {
        newScanSet.put(p, cf);
        monitorSet.toScanFiles.put(p,  cf);
        // Check the file
        cf.lastSubmitTime = System.currentTimeMillis();
        monitorSet.executor.submit(fcr);
      } else {
        if (oldCf.numCorrupt == numCorrupt) {
          newScanSet.put(p, oldCf);
          if (System.currentTimeMillis() - oldCf.lastSubmitTime >
              this.blockFixScanSubmissionInterval) {
            // if a block hasn't been checked for a while, check it again.
            oldCf.lastSubmitTime = System.currentTimeMillis();
            monitorSet.executor.submit(fcr);
          }
        } else {
          cf.detectTime = oldCf.detectTime;
          newScanSet.put(p, cf);
          cf.lastSubmitTime = System.currentTimeMillis();
          monitorSet.executor.submit(fcr);  
        }
      }
    }
    
    /**
     * In JobSubmitRunnable, a mapreduce job to fix files under tmpJobFiles will
     * be created and submitted to mapreduce cluster. 
     * If it fails to do that, numFilesDropped will be updated and files under
     * tmpJobFiles will be move back to the original jobFiles so that they could
     * be fixed in the next job.
     */
    public class JobSubmitRunnable implements Runnable {
      private final Priority priority;
      private final HashSet<String> tmpJobFiles;
      private final HashSet<String> jobFiles;
      private final long detectTime;
      private final AtomicLong lastCheckingTime;
      private final UpdateNumFilesDropped type;
      public JobSubmitRunnable(Priority newPriority, HashSet<String> tmpJobFiles, 
          HashSet<String> originalJobFiles, long newDetectTime, 
          AtomicLong newLastCheckingTime, UpdateNumFilesDropped newType) {
        this.priority = newPriority;
        this.tmpJobFiles = tmpJobFiles;
        this.jobFiles = originalJobFiles;
        this.detectTime = newDetectTime;
        this.lastCheckingTime = newLastCheckingTime;
        this.type = newType;
      }
      
      public void run() {
        boolean succeed = false;
        try {
          succeed = startOneJob(CorruptionWorker.this, priority, tmpJobFiles, detectTime,
              numFilesSubmitted, lastCheckingTime, maxPendingJobs) != null;
        } catch (Throwable ex) {
          LOG.error("Get Error in blockSubmitRunnable", ex);
        } finally {
          if (!succeed) {
            if (type == UpdateNumFilesDropped.SET) {
              numFilesDropped.set(tmpJobFiles.size());
            } else if (type == UpdateNumFilesDropped.ADD) {
              numFilesDropped.addAndGet(tmpJobFiles.size());
            } else {
              LOG.error("Hit an unexpected type:" + type.name());
            }
            // add back to original job files
            synchronized(jobFiles) {
              this.jobFiles.addAll(tmpJobFiles);
            }
          }
        }
      }
    }
    
    // Return used time 
    public long addToJobFilesMap(
        HashMap<Priority, HashSet<String>> jobFilesMap,
        Priority priority, String path, long detectTime)
            throws IOException, InterruptedException, ClassNotFoundException {
      long startTime = System.currentTimeMillis();
      HashSet<String> jobFiles = jobFilesMap.get(priority);
      
      synchronized(jobFiles) {
        if (!jobFiles.add(path)) {
          return System.currentTimeMillis() - startTime;
        }
        totalFilesToSubmit.incrementAndGet();
        // Check if we have hit the threshold for number of files in a job.
      
        AtomicLong lastCheckingTime = lastCheckingTimes.get(priority);
        if ((jobFiles.size() >= filesPerTask * TASKS_PER_JOB)) {
          // Collect enough files
          this.asyncSubmitJob(jobFiles, priority, detectTime,
              UpdateNumFilesDropped.ADD);
        } else if (System.currentTimeMillis() - lastCheckingTime.get()
            > this.blockFixSubmissionInterval && jobFiles.size() > 0) {
          // Wait enough time
          this.asyncSubmitJob(jobFiles, priority, detectTime,
              UpdateNumFilesDropped.SET);
        }
      }
      return System.currentTimeMillis() - startTime;
    }
    
    @Override
    public void shutdown() {
      for (MonitorSet ms : monitorSets.values()) {
        ms.executor.shutdownNow();
      }
      this.jobSubmitExecutor.shutdownNow();
    }
    
    public Map<String, Map<CorruptFileStatus, Long>> getCounterMap() {
      TreeMap<String, Map<CorruptFileStatus, Long>> results = 
          new TreeMap<String, Map<CorruptFileStatus, Long>>();
      for (String monitorDir: monitorSets.keySet()) {
        MonitorSet ms = monitorSets.get(monitorDir);
        HashMap<CorruptFileStatus, Long> counters =
            new HashMap<CorruptFileStatus, Long>();
        for (CorruptFileStatus cfs: CorruptFileStatus.values()) {
          counters.put(cfs, 0L);
        }
        for (CorruptFile cf: ms.toScanFiles.values()) {
          Long counter = counters.get(cf.fileStatus);
          if (counter == null) {
            counter = 0L;
          }
          counters.put(cf.fileStatus, counter + 1);
        }
        results.put(monitorDir, counters);
      }
      return results;
    }
    
    public ArrayList<CorruptFile> getCorruptFileList(String monitorDir, 
        CorruptFileStatus cfs) { 
      ArrayList<CorruptFile> corruptFiles = new ArrayList<CorruptFile>();
      MonitorSet ms = monitorSets.get(monitorDir);
      if (ms == null) {
        return corruptFiles;
      }
      for (CorruptFile cf: ms.toScanFiles.values()) {
        if (cf.fileStatus == cfs) {
          corruptFiles.add(cf);
        }
      }
      return corruptFiles;
    }
    
    public Map<String, Map<CorruptFileStatus, Long>> getCorruptFilesCounterMap() {
      return this.getCounterMap();
    }
    
    public class FileCheckRunnable implements Runnable {
      CorruptFile corruptFile;
      MonitorSet monitorSet;
      FileSystem fs;
      CorruptionWorker worker;
      long detectTime;
      
      public FileCheckRunnable(CorruptFile newCorruptFile,
          MonitorSet newMonitorSet,
          FileSystem newFs, long newDetectTime, CorruptionWorker newWorker) {
        corruptFile = newCorruptFile;
        monitorSet = newMonitorSet;
        fs = newFs;
        detectTime = newDetectTime;
        worker = newWorker;
      }
      
      public void run() {
        long startTime = System.currentTimeMillis();
        try {
          if (corruptFile.numCorrupt <=0) {
            // Not corrupt
            return;
          }
          ConcurrentHashMap<String, CorruptFile> toScanFiles = 
              monitorSet.toScanFiles;
          // toScanFiles could be switched before the task get executed
          CorruptFile cf = toScanFiles.get(corruptFile.path);
          if (cf == null || cf.numCorrupt != corruptFile.numCorrupt) {
            // Not exist or doesn't match
            return;
          }
          FileStatus stat = null;
          try {
            stat = fs.getFileStatus(new Path(corruptFile.path));
          } catch (FileNotFoundException fnfe) {
            cf.fileStatus = CorruptFileStatus.NOT_EXIST;
            return;
          }
          Codec codec = BlockIntegrityMonitor.isParityFile(corruptFile.path);
          long addJobTime = 0;
          if (codec == null) {
            if (stat.getReplication() >= notRaidedReplication) {
              cf.fileStatus = CorruptFileStatus.NOT_RAIDED_UNRECOVERABLE;
              return;
            }
            if (BlockIntegrityMonitor.doesParityDirExist(fs, corruptFile.path)) {
              Priority priority = Priority.LOW;
              if (stat.getReplication() > 1) {
                // If we have a missing block when replication > 1, it is high pri.
                priority = Priority.HIGH;
              } else {
                // Replication == 1. Assume Reed Solomon parity exists.
                // If we have more than one missing block when replication == 1, then
                // high pri.
                priority = (corruptFile.numCorrupt > 1) ? Priority.HIGH : Priority.LOW;
              }
              LostFileInfo fileInfo = fileIndex.get(corruptFile.path);
              if (fileInfo == null || priority.higherThan(
                  fileInfo.getHighestPriority())) {
                addJobTime = addToJobFilesMap(jobFilesMap, priority,
                    corruptFile.path, detectTime);
              }
            }
          } else {
            // Dikang: for parity files, we use the total numbers for now.
            Priority priority = (corruptFile.numCorrupt > 1) ?
                Priority.HIGH : (codec.parityLength == 1)? Priority.HIGH: Priority.LOW;
            LostFileInfo fileInfo = fileIndex.get(corruptFile.path);
            if (fileInfo == null || priority.higherThan(
                fileInfo.getHighestPriority())) {
              addJobTime = addToJobFilesMap(jobFilesMap, priority, corruptFile.path,
                  detectTime);
            }
          }
          boolean isFileCorrupt = RaidShell.isFileCorrupt((DistributedFileSystem)fs,
              stat, false, getConf(), null, null);
          if (isFileCorrupt) {
            cf.fileStatus = CorruptFileStatus.RAID_UNRECOVERABLE;
          } else {
            cf.fileStatus = CorruptFileStatus.RECOVERABLE;
          }
          long elapseTime = System.currentTimeMillis() - startTime - addJobTime;
          worker.putDetectionTime(elapseTime);
        } catch (Exception e) {
          LOG.error("Get Exception ", e);
        }
      }
    }
    
    /**
     * Acquire a lock and dump files of jobFiles into a tmpJobFiles
     * Then it clears the jobFiles and submits a jobSubmitRunnable to the thread pool 
     * to submit a mapreduce job in the background. 
     * No need to wait for job submission to finish.
     */
    void asyncSubmitJob(HashSet<String> jobFiles, Priority pri,
        long detectTime, UpdateNumFilesDropped type) throws IOException {
      synchronized(jobFiles) {
        if (jobFiles.size() == 0)
          return;
        HashSet<String> tmpJobFiles = new HashSet<String>();
        tmpJobFiles.addAll(jobFiles);
        jobFiles.clear();
        JobSubmitRunnable jsr = new JobSubmitRunnable(pri, tmpJobFiles,
            jobFiles, detectTime, lastCheckingTimes.get(pri), type);
        this.jobSubmitExecutor.submit(jsr);
      }
    }

    @Override
    // Compute integer priority and start jobs. Urgency is indicated by higher numbers.
    void computePrioritiesAndStartJobs(
        FileSystem fs, Map<String, Integer> corruptFiles, long detectTime)
            throws IOException, InterruptedException, ClassNotFoundException {

      HashMap<String, ConcurrentHashMap<String, CorruptFile>>
          newToScanSet = new HashMap<String, ConcurrentHashMap<String,
          CorruptFile>>();
      // Include "others"
      for (String monitorDir: this.monitorSets.keySet()) {
        newToScanSet.put(monitorDir, new ConcurrentHashMap<String,
            CorruptFile>());
      }
      numFilesSubmitted.set(0);
      totalFilesToSubmit.set(0);
      for (Iterator<String> it = corruptFiles.keySet().iterator(); it.hasNext(); ) {
        String p = it.next();
        int numCorrupt = corruptFiles.get(p);
        // Filter through monitor dirs
        boolean match = false;
        for (String monitorDir: this.corruptMonitorDirs) {
          if (p.startsWith(monitorDir)) {
            match = true;
            addToScanSet(p, numCorrupt, monitorDir, newToScanSet.get(monitorDir),
                fs, detectTime);
          }
        }
        if (match == false) {
          addToScanSet(p, numCorrupt, OTHERS, newToScanSet.get(OTHERS), fs,
              detectTime);
        }
      }
      // switch to new toScanSet
      for (String monitorDir : this.monitorSets.keySet()) {
        MonitorSet ms = this.monitorSets.get(monitorDir);
        ms.toScanFiles = newToScanSet.get(monitorDir);
      }
      for (Priority pri : Priority.values()) {
        HashSet<String> jobFiles = jobFilesMap.get(pri);
        if (System.currentTimeMillis() - lastCheckingTimes.get(pri).get()
            > this.blockFixSubmissionInterval && jobFiles.size() > 0) {
          this.asyncSubmitJob(jobFiles, pri, detectTime,
              UpdateNumFilesDropped.SET);
        }
      }
    }

    @Override
    protected void updateRaidNodeMetrics() {
      RaidNodeMetrics rnm = RaidNodeMetrics.getInstance(
          RaidNodeMetrics.DEFAULT_NAMESPACE_ID); 
      
      rnm.corruptFilesHighPri.set(lastStatus.highPriorityFiles);
      rnm.corruptFilesLowPri.set(lastStatus.lowPriorityFiles);
      rnm.numFilesToFix.set(this.fileIndex.size());
      rnm.numFilesToFixDropped.set(this.numFilesDropped.get());
      
      // Flush statistics out to the RaidNode
      incrFilesFixed(this.recentNumFilesSucceeded.get());
      incrFileFixFailures(this.recentNumFilesFailed.get());
      incrNumBlockFixSimulationFailures(this.recentNumBlockFixSimulationFailed.get());
      incrNumBlockFixSimulationSuccess(this.recentNumBlockFixSimulationSucceeded.get());
      incrFileFixReadBytesRemoteRack(this.recentNumReadBytesRemoteRack.get());
      LogUtils.incrLogMetrics(this.recentLogMetrics);

      rnm.blockFixSlotSeconds.inc(this.recentSlotSeconds.get());
      this.recentNumFilesSucceeded.set(0);
      this.recentNumFilesFailed.set(0);
      this.recentSlotSeconds.set(0);
      this.recentNumBlockFixSimulationFailed.set(0);
      this.recentNumBlockFixSimulationSucceeded.set(0);
      this.recentNumReadBytesRemoteRack.set(0);
      this.recentLogMetrics.clear();
      
      Map<String, Map<CorruptFileStatus, Long>> corruptFilesCounterMap = 
          this.getCounterMap();
      if (rnm.corruptFiles == null) {
        return;
      }
      for (String dir: this.corruptMonitorDirs) {
        if (corruptFilesCounterMap.containsKey(dir) &&
            rnm.corruptFiles.containsKey(dir)) {
          Map<CorruptFileStatus, Long> maps = corruptFilesCounterMap.get(dir);
          Long raidUnrecoverable = maps.get(CorruptFileStatus.RAID_UNRECOVERABLE);
          Long notRaidUnrecoverable = maps.get(
              CorruptFileStatus.NOT_RAIDED_UNRECOVERABLE);
          if (raidUnrecoverable == null) {
            raidUnrecoverable = 0L;
          }
          if (notRaidUnrecoverable == null) {
            notRaidUnrecoverable = 0L;
          }
          rnm.corruptFiles.get(dir).set(raidUnrecoverable + notRaidUnrecoverable);
        } else {
          rnm.corruptFiles.get(dir).set(-1L);
        }
      }
    }
  }

  public class DecommissioningWorker extends Worker {

    DecommissioningWorker() {
      super(LogFactory.getLog(DecommissioningWorker.class), 
          BlockReconstructor.DecommissioningBlockReconstructor.class, 
          "blockcopier");
    }


    /**
     * gets a list of decommissioning files from the namenode
     * and filters out files that are currently being regenerated or
     * that were recently regenerated
     */
    @Override
    protected Map<String, Integer> getLostFiles(FileSystem fs) throws IOException {
      return DistBlockIntegrityMonitor.this.getLostFiles(LIST_DECOMMISSION_FILE_PATTERN,
          new String[]{"-list-corruptfileblocks",
          "-list-decommissioningblocks",
          "-limit",
          new Integer(lostFilesLimit).toString()});
    }

    @Override
    void computePrioritiesAndStartJobs(
        FileSystem fs, Map<String, Integer> decommissioningFiles, long detectTime)
            throws IOException, InterruptedException, ClassNotFoundException {

      Map<String, Priority> fileToPriority =
          new HashMap<String, Priority>(decommissioningFiles.size());

      for (String file : decommissioningFiles.keySet()) {

        // Replication == 1. Assume Reed Solomon parity exists.
        // Files with more than 4 blocks being decommissioned get a bump.
        // Otherwise, copying jobs have the lowest priority. 
        Priority priority = ((decommissioningFiles.get(file)
            > Codec.getCodec("rs").parityLength) ? 
            Priority.LOW : Priority.LOWEST);

        LostFileInfo fileInfo = fileIndex.get(file);
        if (fileInfo == null || priority.higherThan(fileInfo.getHighestPriority())) {
          fileToPriority.put(file, priority);
        }
      }
      LOG.info("Found " + fileToPriority.size() + " new lost files");

      startJobs(fileToPriority, detectTime);
    }

    @Override
    protected void updateRaidNodeMetrics() {
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).decomFilesLowPri.set(lastStatus.highPriorityFiles);
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).decomFilesLowestPri.set(lastStatus.lowPriorityFiles);
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).numFilesToCopy.set(fileIndex.size());

      incrFilesCopied(recentNumFilesSucceeded.get());
      incrFileCopyFailures(recentNumFilesFailed.get());
      incrNumBlockFixSimulationFailures(this.recentNumBlockFixSimulationFailed.get());
      incrNumBlockFixSimulationSuccess(this.recentNumBlockFixSimulationSucceeded.get());
      LogUtils.incrLogMetrics(this.recentLogMetrics);
      
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).blockCopySlotSeconds.inc(recentSlotSeconds.get());

      // Reset temporary values now that they've been flushed
      recentNumFilesSucceeded.set(0);
      recentNumFilesFailed.set(0);
      recentSlotSeconds.set(0);
      recentNumBlockFixSimulationFailed.set(0);
      recentNumBlockFixSimulationSucceeded.set(0);
      recentLogMetrics.clear();
    }

  }


  // ---- Methods which can be overridden by tests ----

  /**
   * Gets a list of lost files from the name node via DFSck
   * 
   * @param pattern A pattern matching a single file in DFSck's output
   * @param dfsckArgs Arguments to pass to DFSck
   * @return A map of lost files' filenames to num lost blocks for that file 
   */
  protected Map<String, Integer> getLostFiles(
      Pattern pattern, String[] dfsckArgs) throws IOException {

    Map<String, Integer> lostFiles = new HashMap<String, Integer>();
    BufferedReader reader = getLostFileReader(dfsckArgs);
    String line = reader.readLine(); // remove the header line
    while ((line = reader.readLine()) != null) {
      Matcher m = pattern.matcher(line);
      if (!m.find()) {
        continue;
      }
      
      String fileName = m.group(1).trim();
      Integer numLost = lostFiles.get(fileName);
      numLost = numLost == null ? 0 : numLost;
      numLost += 1;
      lostFiles.put(fileName, numLost);
    } 
    LOG.info("FSCK returned " + lostFiles.size() + " files with args " +
        Arrays.toString(dfsckArgs));
    RaidUtils.filterTrash(getConf(), lostFiles.keySet().iterator());
    LOG.info("getLostFiles returning " + lostFiles.size() + " files with args " +
        Arrays.toString(dfsckArgs));
    return lostFiles;
  }

  private BufferedReader getLostFileReader(String[] dfsckArgs) 
      throws IOException {

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bout, true);
    DFSck dfsck = new DFSck(getConf(), ps);
    try {
      dfsck.run(dfsckArgs);
    } catch (Exception e) {
      throw new IOException(e);
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    return new BufferedReader(new InputStreamReader(bin));
  }

  public void configureJob(Job job, 
      Class<? extends BlockReconstructor> reconstructorClass) {

    ((JobConf)job.getConfiguration()).setUser(RaidNode.JOBUSER);
    ((JobConf)job.getConfiguration()).setClass(
        ReconstructionMapper.RECONSTRUCTOR_CLASS_TAG, 
        reconstructorClass,
        BlockReconstructor.class);
  }

  void submitJob(Job job, List<String> filesInJob, Priority priority, 
      Map<Job, List<LostFileInfo>> jobIndex, Map<JobID, TrackingUrlInfo> 
      idToTrackingUrlMap) throws IOException, InterruptedException, 
          ClassNotFoundException {
    job.submit();
    LOG.info("Job " + job.getID() + "(" + job.getJobName() +
        ") started");
    jobIndex.put(job, null);
    idToTrackingUrlMap.put(job.getID(),
        new TrackingUrlInfo(job.getTrackingURL(), System.currentTimeMillis()));
  }

  /**
   * returns the number of map reduce jobs running
   */
  public int jobsRunning() {
    return (corruptionWorker.numJobsRunning.get() 
        + decommissioningWorker.numJobsRunning.get());
  }

  static class ReconstructionInputFormat
  extends SequenceFileInputFormat<LongWritable, Text> {

    protected static final Log LOG = 
        LogFactory.getLog(ReconstructionMapper.class);

    /**
     * splits the input files into tasks handled by a single node
     * we have to read the input files to do this based on a number of 
     * items in a sequence
     */
    @Override
    public List <InputSplit> getSplits(JobContext job) 
        throws IOException {
      long filesPerTask = DistBlockIntegrityMonitor.getFilesPerTask(job.getConfiguration());

      Path[] inPaths = getInputPaths(job);

      List<InputSplit> splits = new ArrayList<InputSplit>();

      long fileCounter = 0;

      for (Path inPath: inPaths) {

        FileSystem fs = inPath.getFileSystem(job.getConfiguration());      

        if (!fs.getFileStatus(inPath).isDir()) {
          throw new IOException(inPath.toString() + " is not a directory");
        }

        FileStatus[] inFiles = fs.listStatus(inPath);

        for (FileStatus inFileStatus: inFiles) {
          Path inFile = inFileStatus.getPath();

          if (!inFileStatus.isDir() &&
              (inFile.getName().equals(job.getJobName() + IN_FILE_SUFFIX))) {

            fileCounter++;
            SequenceFile.Reader inFileReader = 
                new SequenceFile.Reader(fs, inFile, job.getConfiguration());

            long startPos = inFileReader.getPosition();
            long counter = 0;

            // create an input split every filesPerTask items in the sequence
            LongWritable key = new LongWritable();
            Text value = new Text();
            try {
              while (inFileReader.next(key, value)) {
                if (counter % filesPerTask == filesPerTask - 1L) {
                  splits.add(new FileSplit(inFile, startPos, 
                      inFileReader.getPosition() - 
                      startPos,
                      null));
                  startPos = inFileReader.getPosition();
                }
                counter++;
              }

              // create input split for remaining items if necessary
              // this includes the case where no splits were created by the loop
              if (startPos != inFileReader.getPosition()) {
                splits.add(new FileSplit(inFile, startPos,
                    inFileReader.getPosition() - startPos,
                    null));
              }
            } finally {
              inFileReader.close();
            }
          }
        }
      }

      LOG.info("created " + splits.size() + " input splits from " +
          fileCounter + " files");

      return splits;
    }

    /**
     * indicates that input file can be split
     */
    @Override
    public boolean isSplitable (JobContext job, Path file) {
      return true;
    }
  }

  /**
   * Mapper for reconstructing stripes with lost blocks
   */
  static class ReconstructionMapper
  extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Log LOG = 
        LogFactory.getLog(ReconstructionMapper.class);

    public static final String RECONSTRUCTOR_CLASS_TAG =
        "hdfs.blockintegrity.reconstructor";
    private BlockReconstructor reconstructor;
    public RaidProtocol raidnode;
    private UnixUserGroupInformation ugi;
    RaidProtocol rpcRaidnode;
    private long detectTimeInput;
    private String taskId;

    void initializeRpc(Configuration conf, InetSocketAddress address) throws IOException {
      try {
        this.ugi = UnixUserGroupInformation.login(conf, true);
      } catch (LoginException e) {
        throw (IOException)(new IOException().initCause(e));
      }

      this.rpcRaidnode = RaidShell.createRPCRaidnode(address, conf, ugi);
      this.raidnode = RaidShell.createRaidnode(rpcRaidnode);
    }

    @Override
    protected void setup(Context context) 
        throws IOException, InterruptedException {

      super.setup(context);

      Configuration conf = context.getConfiguration();
      taskId = conf.get("mapred.task.id");
      Codec.initializeCodecs(conf);
      initializeRpc(conf, RaidNode.getAddress(conf));

      Class<? extends BlockReconstructor> reconstructorClass = 
          context.getConfiguration().getClass(RECONSTRUCTOR_CLASS_TAG, 
                                            null, 
                                            BlockReconstructor.class);
      
      if (reconstructorClass == null) {
        LOG.error("No class supplied for reconstructor " +
                "(prop " + RECONSTRUCTOR_CLASS_TAG + ")");
        context.progress();
        return;
      }

      // We dynamically instantiate the helper based on the helperClass member
      try {
        Constructor<? extends BlockReconstructor> ctor =
            reconstructorClass.getConstructor(new Class[]{Configuration.class});

        reconstructor = ctor.newInstance(conf);

      } catch (Exception ex) {
        throw new IOException("Could not instantiate a block reconstructor " +
                          "based on class " + reconstructorClass, ex);
      }
      
      detectTimeInput = Long.parseLong(conf.get("corrupt_detect_time"));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      RPC.stopProxy(rpcRaidnode);
    }

    /**
     * Reconstruct a stripe
     */
    @Override
    public void map(LongWritable key, Text fileText, Context context)
      throws IOException, InterruptedException {
      long sTime = System.currentTimeMillis();
      String fileStr = fileText.toString();
      Path file = new Path(fileStr);
      String prefix = "[" + fileStr + "]      ";
      LOG.info("");
      LOG.info(prefix + "============================= BEGIN =============================");
      LOG.info(prefix + "Reconstruct File: " + fileStr);
      LOG.info(prefix + "Block Missing Detection Time: " +
          dateFormat.format(detectTimeInput));
      long waitTime = sTime - detectTimeInput;
      LOG.info(prefix + "Scheduling Time: " + (waitTime/1000) + " seconds");
      FileSystem fs = file.getFileSystem(context.getConfiguration());
      LogUtils.logWaitTimeMetrics(waitTime, getMaxPendingJobs(
          context.getConfiguration()), 
          getFilesPerTask(context.getConfiguration()),
          LOGTYPES.FILE_FIX_WAITTIME,
          fs,
          context);
      long recoveryTime = -1;

      try {
        boolean reconstructed = reconstructor.reconstructFile(file, context);
        if (reconstructed) {
          recoveryTime = System.currentTimeMillis() - detectTimeInput;
          context.getCounter(RaidCounter.FILES_SUCCEEDED).increment(1L);
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.SUCCESS, 0, null,
              file, -1, LOGTYPES.OFFLINE_RECONSTRUCTION_FILE, 
              fs, null, context, recoveryTime); 
          LOG.info(prefix + "File Reconstruction Time: " + 
              ((System.currentTimeMillis() - sTime)/1000) + " seconds");
          LOG.info(prefix + "Total Recovery Time: " + 
              (recoveryTime/1000) + " seconds");
        } else {
          LOG.info(prefix + "File has already been fixed, No action");
          context.getCounter(RaidCounter.FILES_NOACTION).increment(1L);
        }
      } catch (Throwable e) {
        LOG.error(prefix + "Reconstructing file " + file + " failed", e);
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0, null,
            file, -1, LOGTYPES.OFFLINE_RECONSTRUCTION_FILE, 
            fs, e, context, -1);
        recoveryTime = Integer.MAX_VALUE;
        // report file as failed
        context.getCounter(RaidCounter.FILES_FAILED).increment(1L);
        String outkey = DistBlockIntegrityMonitor.FAILED_FILE + "," + fileStr;
        context.write(new Text(outkey), new Text(taskId));
      } finally {
        if (recoveryTime > 0) {
          // Send recoveryTime to raidnode
          try {
            raidnode.sendRecoveryTime(fileStr, recoveryTime, taskId);
          } catch (Exception e) {
            LOG.error(prefix + "Failed to send recovery time ", e);
          }
        }
        LOG.info(prefix + "============================= END =============================");
        LOG.info("");
      }
      context.progress();
    }
  }

  /**
   * Get the status of the entire block integrity monitor.
   * The status returned represents the aggregation of the statuses of all the 
   * integrity monitor's components.
   * 
   * @return The status of the block integrity monitor 
   */
  @Override
  public BlockIntegrityMonitor.Status getAggregateStatus() {
    Status fixer = corruptionWorker.getStatus();
    Status copier = decommissioningWorker.getStatus();

    List<JobStatus> jobs = new ArrayList<JobStatus>();
    List<JobStatus> simFailedJobs = new ArrayList<JobStatus>();
    List<JobStatus> failedJobs = new ArrayList<JobStatus>();
    List<String> highPriFileNames = new ArrayList<String>();
    int numHighPriFiles = 0;
    int numLowPriFiles = 0;
    int numLowestPriFiles = 0;
    if (fixer != null) {
      jobs.addAll(fixer.jobs);
      simFailedJobs.addAll(fixer.simFailJobs);
      failedJobs.addAll(fixer.failJobs);
      if (fixer.highPriorityFileNames != null) {
        highPriFileNames.addAll(fixer.highPriorityFileNames);
      }
      numHighPriFiles += fixer.highPriorityFiles;
      numLowPriFiles += fixer.lowPriorityFiles;
      numLowestPriFiles += fixer.lowestPriorityFiles;
    }
    if (copier != null) {
      jobs.addAll(copier.jobs);
      simFailedJobs.addAll(copier.simFailJobs);
      failedJobs.addAll(copier.failJobs);
      if (copier.highPriorityFileNames != null) {
        highPriFileNames.addAll(copier.highPriorityFileNames);
      }
      numHighPriFiles += copier.highPriorityFiles;
      numLowPriFiles += copier.lowPriorityFiles;
      numLowestPriFiles += copier.lowestPriorityFiles;
    }

    return new Status(numHighPriFiles, numLowPriFiles, numLowestPriFiles,
                      jobs, highPriFileNames,failedJobs, simFailedJobs);
  }
  
  public Worker getCorruptionMonitor() {
    return this.corruptionWorker;
  }

  @Override
  public Worker getDecommissioningMonitor() {
    return this.decommissioningWorker;
  }
  
  @Override
  public Runnable getCorruptFileCounter() {
    return this.corruptFileCounterWorker;
  }
}

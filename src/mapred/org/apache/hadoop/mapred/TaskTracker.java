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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.management.ObjectName;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationServlet;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.http.NettyMapOutputHttpServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerPathDeletionContext;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.MRAsyncDiskService;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.PulseChecker;
import org.apache.hadoop.util.PulseCheckable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.apache.hadoop.util.DataDirFileReader;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
public class TaskTracker extends ReconfigurableBase
             implements MRConstants, TaskUmbilicalProtocol, Runnable, PulseCheckable {

  /**
   * @deprecated
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY =
    "mapred.tasktracker.vmem.reserved";
  /**
   * @deprecated
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY =
     "mapred.tasktracker.pmem.reserved";

  static final String MAP_USERLOG_RETAIN_SIZE =
      "mapreduce.cluster.map.userlog.retain-size";
  static final String REDUCE_USERLOG_RETAIN_SIZE =
      "mapreduce.cluster.reduce.userlog.retain-size";
  static final String CHECK_TASKTRACKER_BUILD_VERSION =
      "mapreduce.tasktracker.build.version.check";
  static final String LOG_FINISHED_TASK_COUNTERS =
      "mapreduce.tasktracker.log.finished.task.counters";
  static final String FINISHED_TASK_COUNTERS_LOG_FORMAT = 
      "mapreduce.tasktracker.log.finished.task.counters.format";
  
  static final long LOCALIZE_TASK_TIMEOUT = 10 * 60 * 1000L;
  static final long WAIT_FOR_DONE = 3 * 1000;
  // Port for jetty http server, not netty http server
  protected int httpPort;

  static enum State {NORMAL, STALE, INTERRUPTED, DENIED}

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  public static final Log LOG =
    LogFactory.getLog(TaskTracker.class);

  public static final String MR_CLIENTTRACE_FORMAT =
        "src: %s" +     // src IP
        ", dest: %s" +  // dst IP
        ", bytes: %s" + // byte count
        ", op: %s" +    // operation
        ", cliID: %s" +  // task id
        ", duration: %s"; // duration
  public static final Log ClientTraceLog =
    LogFactory.getLog(TaskTracker.class.getName() + ".clienttrace");

  volatile boolean running = true;
  volatile long lastHeartbeat = 0;

  private LocalDirAllocator localDirAllocator = null;
  String taskTrackerName;
  String localHostname;
  String localHostAddress = null;
  InetSocketAddress jobTrackAddr;

  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  InterTrackerProtocol jobClient;

  // last heartbeat response recieved
  short heartbeatResponseId = -1;

  static final String TASK_CLEANUP_SUFFIX = ".cleanup";

  /*
   * This is the last 'status' report sent by this tracker to the JobTracker.
   *
   * If the rpc call succeeds, this 'status' is cleared-out by this tracker;
   * indicating that a 'fresh' status report be generated; in the event the
   * rpc calls fails for whatever reason, the previous status report is sent
   * again.
   */
  TaskTrackerStatus status = null;

  // The system-directory on HDFS where job files are stored
  Path systemDirectory = null;

  // The filesystem where job files are stored
  FileSystem systemFS = null;

  private HttpServer server;
  /**
   * If netty will be used for map outputs, then this is the first port
   * it tries to bind to. If binding fails, it will retry with increasing
   * port numbers.
   */
  public static String NETTY_MAPOUTPUT_HTTP_PORT =
    "mapred.task.tracker.netty.http.port";
  public static String NETTY_MAPOUTPUT_USE = "mapred.task.tracker.netty.use";
  private NettyMapOutputHttpServer nettyMapOutputServer = null;
  /** If this port is -1, it is not being used */
  protected int nettyMapOutputHttpPort = -1;

  volatile boolean shuttingDown = false;

  Map<TaskAttemptID, TaskInProgress> tasks = new HashMap<TaskAttemptID, TaskInProgress>();
  /**
   * Map from taskId -> TaskInProgress.
   */
  protected Map<TaskAttemptID, TaskInProgress> runningTasks = null;
  Map<JobID, RunningJob> runningJobs = null;
  volatile int mapTotal = 0;
  volatile int reduceTotal = 0;
  boolean justStarted = true;
  boolean justInited = true;

  /** Keeps track of the last N milliseconds to refill a map slot */
  private final Queue<Integer> mapSlotRefillMsecsQueue =
    new LinkedList<Integer>();
  /** Keeps track of the last N milliseconds to refill a reduce slot */
  private final Queue<Integer> reduceSlotRefillMsecsQueue =
    new LinkedList<Integer>();
  /** Maximum length of the map or reduce refill queues */
  private int maxRefillQueueSize;
  /** Configuration key for max refill queue size */
  public static final String MAX_REFILL_QUEUE_SIZE =
    "mapred.maxRefillQueueSize";
  /** Default max refill size of 50 */
  public static final int DEFAULT_MAX_REFILL_QUEUE_SIZE = 50;

  // Mark reduce tasks that are shuffling to rollback their events index

  //dir -> DF
  Map<String, DF> localDirsDf = new HashMap<String, DF>();
  long minSpaceStart = 0;
  //must have this much space free to start new tasks
  boolean acceptNewTasks = true;
  long minSpaceKill = 0;
  //if we run under this limit, kill one task
  //and make sure we never receive any new jobs
  //until all the old tasks have been cleaned up.
  //this is if a machine is so full it's only good
  //for serving map output to the other nodes

  static Random r = new Random();
  private static final String SUBDIR = "taskTracker";
  private static final String CACHEDIR = "archive";
  private static final String JOBCACHE = "jobcache";
  private static final String OUTPUT = "output";
  protected JobConf originalConf;
  protected JobConf fConf;
  private long lastLocalDirsReloadTimeStamp = 0;
  private String[] localDirsList = null;
  /** Max map slots used for scheduling */
  private int maxMapSlots;
  /** Max reduce slots used for scheduling */
  private int maxReduceSlots;
  /** Actual max map slots that can be used for metrics */
  private int actualMaxMapSlots;
  /** Actual max reduce slots that can be used for metrics */
  private int actualMaxReduceSlots;

  private int failures;

  private FileSystem localFs;

  // Performance-related config knob to send an out-of-band heartbeat
  // on task completion
  static final String TT_OUTOFBAND_HEARBEAT =
    "mapreduce.tasktracker.outofband.heartbeat";
  private volatile boolean oobHeartbeatOnTaskCompletion;
  static final String TT_FAST_FETCH = "mapred.tasktracker.events.fastfetch";
  private volatile boolean fastFetch = false;
  public static final String TT_PROFILE_ALL_TASKS = "mapred.tasktracker.profile.alltasks";
  private volatile boolean profileAllTasks = false;

  // Track number of completed tasks to send an out-of-band heartbeat
  protected IntWritable finishedCount = new IntWritable(0);

  private MapEventsFetcherThread mapEventsFetcher;
  // A store of map task completion events. The mapping is a TCE to a weak
  // reference of the same TCE. Given a TCE, this enables fetching of a stored
  // equivalent TCE while allowing the GC to remove non-referenced instances.
  // It's useful for saving memory when there are multiple TT in the same JVM
  // that fetch task completion events for the same job, e.g. simulation.
  // This is the same idea as using String.intern() to reduce overall memory
  // usage.
  private static WeakHashMap<TaskCompletionEvent,
                             WeakReference<TaskCompletionEvent>>
    taskCompletionEventsStore =
    new WeakHashMap<TaskCompletionEvent, WeakReference<TaskCompletionEvent>>();
  // Vars to control the task completion event store. See above.
  public static final String MAPRED_TASKTRACKER_TCE_STORE_PROPERTY =
      "mapred.tasktracker.task.completion.event.store";
  private boolean useTaskCompletionEventsStore = false;

  int workerThreads;
  CleanupQueue directoryCleanupThread;
  volatile JvmManager jvmManager;

  private long previousCounterUpdate = 0;

  private TaskMemoryManagerThread taskMemoryManager;
  private boolean taskMemoryManagerEnabled = true;
  private long totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long reduceSlotSizeMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;

  // conf for Corona Memory CGroup. The default is false
  public static final 
    String MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY = 
    "mapred.tasktracker.cgroup.mem";
  public static final 
    boolean DEFAULT_MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY = false;
  private boolean taskMemoryControlGroupEnabled = false;
  
  private TaskTrackerMemoryControlGroup ttMemCgroup;
  
  private CGroupMemoryWatcher cgroupMemoryWatcher;

  // conf for CPU CGroup. The default is flase.
  public static final 
    String MAPRED_TASKTRACKER_CGROUP_CPU_ENABLE_PROPERTY = 
    "mapred.tasktracker.cgroup.cpu";
  public static final 
    boolean DEFAULT_MAPRED_TASKTRACKER_CGROUP_CPU_ENABLE_PROPERTY = false;
  private boolean taskCPUControlGroupEnabled = false;

  private TaskTrackerCPUControlGroup ttCPUCgroup;

  private TaskLogsMonitor taskLogsMonitor;

  public static final String MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY =
      "mapred.tasktracker.memory_calculator_plugin";
  protected ResourceCalculatorPlugin resourceCalculatorPlugin = null;

  /**
   * the minimum interval between jobtracker polls
   */
  private volatile int heartbeatInterval = HEARTBEAT_INTERVAL_MIN;
  /**
   * Number of maptask completion events locations to poll for at one time
   */
  private int probe_sample_size = 500;

  private IndexCache indexCache;

  private MRAsyncDiskService asyncDiskService;

  private ObjectName versionBeanName;

  /**
  * Handle to the specific instance of the {@link TaskController} class
  */
  private TaskController taskController;

  /**
   * Handle to the specific instance of the {@link NodeHealthCheckerService}
   */
  private NodeHealthCheckerService healthChecker;

  /*
   * A list of commitTaskActions for whom commit response has been received
   */
  protected List<TaskAttemptID> commitResponses =
            Collections.synchronizedList(new ArrayList<TaskAttemptID>());
  
  private static final String DISK_OUT_OF_SPACE_KEY1 = "space";
  private static final String DISK_OUT_OF_SPACE_KEY2 = "quota";
  
  private AtomicLong taskTrackerRSSMem = new AtomicLong();
  
  private Map<Integer, Integer> killedTaskRssBuckets = new HashMap<Integer, Integer>();
  private static final int KILLED_TASKS_RSS_BUCKETS_NUM = 18;

  protected ShuffleServerMetrics shuffleServerMetrics;
  /** This class contains the methods that should be used for metrics-reporting
   * the specific metrics for shuffle. The TaskTracker is actually a server for
   * the shuffle and hence the name ShuffleServerMetrics.
   */
  public class ShuffleServerMetrics implements Updater {
    private MetricsRecord shuffleMetricsRecord = null;
    private int serverHandlerBusy = 0;
    private long outputBytes = 0;
    private int failedOutputs = 0;
    private int successOutputs = 0;
    private int missingOutputs = 0;
    private int httpQueueLen = 0;
    private ThreadPoolExecutor nettyWorkerThreadPool = null;
    ShuffleServerMetrics(JobConf conf) {
      MetricsContext context = MetricsUtil.getContext("mapred");
      shuffleMetricsRecord =
                           MetricsUtil.createRecord(context, "shuffleOutput");
      this.shuffleMetricsRecord.setTag("sessionId", conf.getSessionId());
      context.registerUpdater(this);
    }
    synchronized void serverHandlerBusy() {
      ++serverHandlerBusy;
    }
    synchronized void serverHandlerFree() {
      --serverHandlerBusy;
    }
    public synchronized void outputBytes(long bytes) {
      outputBytes += bytes;
    }
    synchronized void failedOutput() {
      ++failedOutputs;
    }
    synchronized void successOutput() {
      ++successOutputs;
    }
    synchronized void missingOutput() {
      ++missingOutputs;
    }
    synchronized void setHttpQueueLen(int queueLen) {
      this.httpQueueLen = queueLen;
    }
    synchronized void setNettyWorkerThreadPool(ThreadPoolExecutor workerThreadPool) {
      this.nettyWorkerThreadPool = workerThreadPool;
    }
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        if (workerThreads != 0) {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent",
              100*((float)serverHandlerBusy/workerThreads));
        } else {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent", 0);
        }
        shuffleMetricsRecord.setMetric("shuffle_queue_len", httpQueueLen);
        shuffleMetricsRecord.incrMetric("shuffle_output_bytes",
                                        outputBytes);
        shuffleMetricsRecord.incrMetric("shuffle_failed_outputs",
                                        failedOutputs);
        shuffleMetricsRecord.incrMetric("shuffle_success_outputs",
                                        successOutputs);
        shuffleMetricsRecord.incrMetric("shuffle_missing_outputs",
                                        missingOutputs);
        // Netty map output metrics
        if (nettyWorkerThreadPool != null) {
          shuffleMetricsRecord.setMetric("netty_mapoutput_activecount",
              nettyWorkerThreadPool.getActiveCount());
          shuffleMetricsRecord.setMetric("netty_mapoutput_poolsize",
              nettyWorkerThreadPool.getPoolSize());
          shuffleMetricsRecord.setMetric("netty_mapoutput_maximumpoolsize",
              nettyWorkerThreadPool.getMaximumPoolSize());
          shuffleMetricsRecord.setMetric("netty_mapoutput_largestpoolsize",
              nettyWorkerThreadPool.getLargestPoolSize());
          shuffleMetricsRecord.setMetric("netty_mapoutput_taskcount",
              nettyWorkerThreadPool.getTaskCount());
        }
        outputBytes = 0;
        failedOutputs = 0;
        successOutputs = 0;
        missingOutputs = 0;
      }
      shuffleMetricsRecord.update();
    }
  }

  private TaskTrackerInstrumentation myInstrumentation = null;

  public TaskTrackerInstrumentation getTaskTrackerInstrumentation() {
    return myInstrumentation;
  }

  /**
   * A list of tips that should be cleaned up.
   */
  protected BlockingQueue<TaskTrackerAction> tasksToCleanup =
    new LinkedBlockingQueue<TaskTrackerAction>();

  /**
   * Variables related to running a simulated tasktracker where map/reduce tasks
   * finish without doing any actual work. Saves CPU and memory as no JVM is
   * launched.
   */
  // Whether the task tracker is running in the simulation mode
  private boolean simulatedTaskMode = false;
  // A thread that simulates the running of map/reduce tasks
  protected SimulatedTaskRunner simulatedTaskRunner = null;

  // Conf var name for whether TT should run in simulation mode
  protected static String TT_SIMULATED_TASKS =
      "mapred.tasktracker.simulated.tasks";
  // Conf var name for how long the simulated task should take to finish in ms
  // Applies to both maps and reduces.
  protected static String TT_SIMULATED_TASK_RUNNING_TIME =
      "mapred.tasktracker.simulated.tasks.running.time";

  /**
	 * Purge old user logs, and clean up half of the logs if the number of logs
	 * exceed the limit.
	 */
  private Thread logCleanupThread =
    new Thread(new LogCleanupThread(TaskLog.getUserLogDir()), "logCleanup");

  class LogCleanupThread implements Runnable {
    File logDir;

    public LogCleanupThread(File logDir) {
      this.logDir = logDir;
    }

    public void run() {
      long logCleanupIntervalTime = fConf.getLong("mapred.userlog.cleanup.interval", 300000);
      int logsRetainHours = fConf.getInt("mapred.userlog.retain.hours", 24);
      int logsNumberLimit = fConf.getInt("mapred.userlog.files.limit", 20000);
      while(true) {
        try{
          TaskLog.cleanup(logDir, logsRetainHours, logsNumberLimit);
          Thread.sleep(logCleanupIntervalTime);
        } catch (Throwable except) {
          LOG.warn("Error in cleanup thread ", except);
        }
      }
    }
  }

  /**
   * A daemon-thread that pulls tips off the list of things to cleanup.
   */
  private Thread taskCleanupThread =
    new Thread(new Runnable() {
        public void run() {
          while (true) {
            try {
              TaskTrackerAction action = tasksToCleanup.take();
              if (action instanceof KillJobAction) {
                purgeJob((KillJobAction) action);
              } else if (action instanceof KillTaskAction) {
                TaskInProgress tip;
                KillTaskAction killAction = (KillTaskAction) action;
                synchronized (TaskTracker.this) {
                  tip = tasks.get(killAction.getTaskID());
                }
                LOG.info("Received KillTaskAction for task: " +
                         killAction.getTaskID());
                purgeTask(tip, false);
              } else {
                LOG.error("Non-delete action given to cleanup thread: "
                          + action);
              }
            } catch (Throwable except) {
              LOG.warn(StringUtils.stringifyException(except));
            }
          }
        }
      }, "taskCleanup");

  TaskController getTaskController() {
    return taskController;
  }

  private RunningJob addTaskToJob(JobID jobId,
                                  TaskInProgress tip) throws IOException {
    synchronized (runningJobs) {
      RunningJob rJob = null;
      if (!runningJobs.containsKey(jobId)) {
        rJob = createRunningJob(jobId, tip);
        rJob.localized = false;
        rJob.tasks = new HashSet<TaskInProgress>();
        runningJobs.put(jobId, rJob);
      } else {
        rJob = runningJobs.get(jobId);
      }
      synchronized (rJob) {
        rJob.tasks.add(tip);
      }
      runningJobs.notify(); //notify the fetcher thread
      return rJob;
    }
  }

  protected RunningJob createRunningJob(JobID jobId, TaskInProgress tip)
      throws IOException {
    return new RunningJob(jobId, this.jobClient, null);
  }

  private void removeTaskFromJob(JobID jobId, TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rjob = runningJobs.get(jobId);
      if (rjob == null) {
        LOG.warn("Task " + tip.getTask().getTaskID() +
          " being deleted from unknown job " + jobId);
      } else {
        synchronized (rjob) {
          rjob.tasks.remove(tip);
        }
      }
    }
  }
  protected synchronized void removeRunningTask(TaskAttemptID attemptID) {
    runningTasks.remove(attemptID);
  }

  protected List<TaskAttemptID> getRunningTasksForJob(JobID jobId) {
    List<TaskAttemptID> running = new ArrayList<TaskAttemptID>();
    synchronized (this) {
      for (TaskAttemptID attemptId: runningTasks.keySet()) {
        if (jobId.equals(attemptId.getJobID())) {
          running.add(attemptId);
        }
      }
    }
    return running;
  }

  public IndexRecord getIndexInformation(String mapId, int reduce,
      Path fileName) throws IOException {
    return indexCache.getIndexInformation(mapId, reduce, fileName);
  }

  TaskLogsMonitor getTaskLogsMonitor() {
    return this.taskLogsMonitor;
  }

  void setTaskLogsMonitor(TaskLogsMonitor t) {
    this.taskLogsMonitor = t;
  }

  static String getCacheSubdir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.CACHEDIR;
  }

  static String getJobCacheSubdir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.JOBCACHE;
  }

  static String getLocalJobDir(String jobid) {
	return getJobCacheSubdir() + Path.SEPARATOR + jobid;
  }

  static String getLocalTaskDir(String jobid, String taskid) {
	return getLocalTaskDir(jobid, taskid, false) ;
  }

  public static String getIntermediateOutputDir(String jobid, String taskid) {
	return getLocalTaskDir(jobid, taskid)
           + Path.SEPARATOR + TaskTracker.OUTPUT ;
  }

  static String getLocalTaskDir(String jobid,
                                String taskid,
                                boolean isCleanupAttempt) {
	String taskDir = getLocalJobDir(jobid) + Path.SEPARATOR + taskid;
	if (isCleanupAttempt) {
      taskDir = taskDir + TASK_CLEANUP_SUFFIX;
	}
	return taskDir;
  }

  String getPid(TaskAttemptID tid) {
    TaskInProgress tip = tasks.get(tid);
    if (tip != null) {
      return jvmManager.getPid(tip.getTaskRunner());
    }
    return null;
  }
  
  long getTaskCPUMSecs(TaskAttemptID tid) {
    TaskInProgress tip = tasks.get(tid);
    if (tip != null) {
      return tip.getStatus().getCounters().getCounter(Task.Counter.CPU_MILLISECONDS);
    }
    
    return 0L;
  }

  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(TaskUmbilicalProtocol.class.getName())) {
      return TaskUmbilicalProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol for task tracker: " +
                            protocol);
    }
  }

  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /**
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  protected synchronized void initialize(JobConf conf) throws IOException {
    maxRefillQueueSize =
        conf.getInt(MAX_REFILL_QUEUE_SIZE, DEFAULT_MAX_REFILL_QUEUE_SIZE);
    if (maxRefillQueueSize <= 0) {
      throw new RuntimeException("Illegal value for " +
          MAX_REFILL_QUEUE_SIZE + " " + maxRefillQueueSize);
    }
    this.originalConf = conf;
    // use configured nameserver & interface to get local hostname
    this.fConf = new JobConf(conf);
    localFs = FileSystem.getLocal(fConf);
    if (fConf.get("slave.host.name") != null) {
      this.localHostname = fConf.get("slave.host.name");
    }
    if (localHostname == null) {
      this.localHostname =
      DNS.getDefaultHost
      (fConf.get("mapred.tasktracker.dns.interface","default"),
       fConf.get("mapred.tasktracker.dns.nameserver","default"));
    }
  
    try {
      java.net.InetAddress inetAddress = java.net.InetAddress.getByName(this.localHostname);
      if (inetAddress != null) {
        this.localHostAddress = inetAddress.getHostAddress();
      }
      else {
        LOG.info("Unable to get IPaddress for " + this.localHostname);
      }
    } catch (UnknownHostException e) {
      LOG.info("Unable to get IPaddress for " + this.localHostname);
    }

    Class<? extends ResourceCalculatorPlugin> clazz =
        fConf.getClass(MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
            null, ResourceCalculatorPlugin.class);
    resourceCalculatorPlugin =
      (ResourceCalculatorPlugin) ResourceCalculatorPlugin
            .getResourceCalculatorPlugin(clazz, fConf);
    LOG.info("Using ResourceCalculatorPlugin : " + resourceCalculatorPlugin);
    int numCpuOnTT = resourceCalculatorPlugin.getNumProcessors();
    maxMapSlots = getMaxSlots(fConf, numCpuOnTT, TaskType.MAP);
    maxReduceSlots = getMaxSlots(fConf, numCpuOnTT, TaskType.REDUCE);
    actualMaxMapSlots =
      getMaxActualSlots(fConf, numCpuOnTT, TaskType.MAP);
    actualMaxReduceSlots =
      getMaxActualSlots(fConf, numCpuOnTT, TaskType.REDUCE);
    LOG.info("Num cpus = " + numCpuOnTT +
             ", max map slots = " + maxMapSlots +
             ", max reduce slots = " + maxReduceSlots +
             ", actual max map slots = " + actualMaxMapSlots +
             ", actual max reduce slots = " + actualMaxReduceSlots);

    // Check local disk, start async disk service, and clean up all
    // local directories.
    checkLocalDirs(getLocalDirsFromConf(this.fConf));
    asyncDiskService = new MRAsyncDiskService(FileSystem.getLocal(fConf),
        getLocalDirsFromConf(fConf), fConf);
    asyncDiskService.cleanupAllVolumes();
    DistributedCache.purgeCache(fConf, asyncDiskService);
    versionBeanName = VersionInfo.registerJMX("TaskTracker");

    // Clear out state tables
    this.tasks.clear();
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.runningJobs = new TreeMap<JobID, RunningJob>();
    this.mapTotal = 0;
    this.reduceTotal = 0;
    this.acceptNewTasks = true;
    this.status = null;

    this.minSpaceStart = this.fConf.getLong("mapred.local.dir.minspacestart", 0L);
    this.minSpaceKill = this.fConf.getLong("mapred.local.dir.minspacekill", 0L);
    //tweak the probe sample size (make it a function of numCopiers)
    probe_sample_size = this.fConf.getInt("mapred.tasktracker.events.batchsize", 500);

    // Set up TaskTracker instrumentation
    this.myInstrumentation = createInstrumentation(this, fConf);

    // bind address
    String address =
      NetUtils.getServerAddress(fConf,
                                "mapred.task.tracker.report.bindAddress",
                                "mapred.task.tracker.report.port",
                                "mapred.task.tracker.report.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();

    this.jvmManager = new JvmManager(this);

    // Set service-level authorization security policy
    if (this.fConf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider =
        (PolicyProvider)(ReflectionUtils.newInstance(
            this.fConf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
                MapReducePolicyProvider.class, PolicyProvider.class),
            this.fConf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(this.fConf, policyProvider));
    }

    // RPC initialization
    int max = actualMaxMapSlots > actualMaxReduceSlots ?
                       actualMaxMapSlots : actualMaxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    int numHandlers = 2 * max;
    LOG.info("Starting RPC server with " + numHandlers + " handlers");
    this.taskReportServer =
      RPC.getServer(this, bindAddress, tmpPort, numHandlers, false, this.fConf);
    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set("mapred.task.tracker.report.address",
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + localHostname + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);

    this.justInited = true;
    this.running = true;

    taskMemoryControlGroupEnabled = fConf.getBoolean(
        MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY,
        DEFAULT_MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY);
    if(taskMemoryControlGroupEnabled) {
      ttMemCgroup = new TaskTrackerMemoryControlGroup(fConf);
    }
    
    taskCPUControlGroupEnabled = fConf.getBoolean(
            MAPRED_TASKTRACKER_CGROUP_CPU_ENABLE_PROPERTY,
            DEFAULT_MAPRED_TASKTRACKER_CGROUP_CPU_ENABLE_PROPERTY);
    if(taskCPUControlGroupEnabled)
      ttCPUCgroup = new TaskTrackerCPUControlGroup(fConf, actualMaxMapSlots + actualMaxReduceSlots);
    
    initializeMemoryManagement();
    cgroupMemoryWatcher = new CGroupMemoryWatcher(this);
    cgroupMemoryWatcher.start();

    setTaskLogsMonitor(new TaskLogsMonitor(getMapUserLogRetainSize(),
        getReduceUserLogRetainSize()));
    getTaskLogsMonitor().start();

    this.indexCache = new IndexCache(this.fConf);

    pulseChecker = PulseChecker.create(this, "TaskTracker");

    heartbeatMonitor = new HeartbeatMonitor(this.fConf);
    mapLauncher =
      new TaskLauncher(TaskType.MAP, maxMapSlots, actualMaxMapSlots);
    reduceLauncher =
      new TaskLauncher(TaskType.REDUCE, maxReduceSlots, actualMaxReduceSlots);
    mapLauncher.start();
    reduceLauncher.start();
    Class<? extends TaskController> taskControllerClass
                          = fConf.getClass("mapred.task.tracker.task-controller",
                                            DefaultTaskController.class,
                                            TaskController.class);
    taskController = (TaskController)ReflectionUtils.newInstance(
                                                      taskControllerClass, fConf);

    //setup and create jobcache directory with appropriate permissions
    taskController.setup();

    //Start up node health checker service.
    if (shouldStartHealthMonitor(this.fConf)) {
      startHealthMonitor(this.fConf);
    }

    oobHeartbeatOnTaskCompletion =
      fConf.getBoolean(TT_OUTOFBAND_HEARBEAT, false);
    fastFetch = fConf.getBoolean(TT_FAST_FETCH, false);

    // Setup the launcher if in simulation mode
    simulatedTaskMode = fConf.getBoolean(TT_SIMULATED_TASKS, false);
    LOG.info("simulatedTaskMode = " + simulatedTaskMode);
    long simulatedTaskRunningTime =
        fConf.getLong(TT_SIMULATED_TASK_RUNNING_TIME, 20000);
    if (simulatedTaskMode) {
      simulatedTaskRunner =
        new SimulatedTaskRunner(simulatedTaskRunningTime, this);
      simulatedTaskRunner.start();
    }
    // Setup task completion event store
    useTaskCompletionEventsStore = fConf.getBoolean(
        MAPRED_TASKTRACKER_TCE_STORE_PROPERTY, false);
    profileAllTasks = fConf.getBoolean(TT_PROFILE_ALL_TASKS, false);
  }

  protected String getLocalHostname() {
    return localHostname;
  }

  protected String getLocalHostAddress() {
    return localHostAddress;
  }

  protected TaskUmbilicalProtocol getUmbilical(
    TaskInProgress tip ) throws IOException {
    return this;
  }

  protected void cleanupUmbilical(TaskUmbilicalProtocol t) {
    return;
  }

  public boolean getProfileAllTasks() {
    return profileAllTasks;
  }

  protected void initializeMapEventFetcher() {
    // start the thread that will fetch map task completion events
    this.mapEventsFetcher = new MapEventsFetcherThread();
    mapEventsFetcher.setDaemon(true);
    mapEventsFetcher.setName(
        "Map-events fetcher for all reduce tasks on " + taskTrackerName);
    mapEventsFetcher.start();
  }

  public static Class<?>[] getInstrumentationClasses(Configuration conf) {
    return conf.getClasses("mapred.tasktracker.instrumentation",
        TaskTrackerMetricsInst.class);
  }

  public static void setInstrumentationClass(
    Configuration conf, Class<? extends TaskTrackerInstrumentation> t) {
    conf.setClass("mapred.tasktracker.instrumentation",
        t, TaskTrackerInstrumentation.class);
  }

  public static TaskTrackerInstrumentation createInstrumentation(
      TaskTracker tt, Configuration conf) {
    try {
      Class<?>[] instrumentationClasses = getInstrumentationClasses(conf);
      if (instrumentationClasses.length == 0) {
        LOG.error("Empty string given for mapred.tasktracker.instrumentation" +
            " property -- will use default instrumentation class instead");
        return new TaskTrackerMetricsInst(tt);
      } else if (instrumentationClasses.length == 1) {
        // Just one instrumentation class given; create it directly
        Class<?> cls = instrumentationClasses[0];
        java.lang.reflect.Constructor<?> c =
          cls.getConstructor(new Class[] {TaskTracker.class} );
        return (TaskTrackerInstrumentation) c.newInstance(tt);
      } else {
        // Multiple instrumentation classes given; use a composite object
        List<TaskTrackerInstrumentation> instrumentations =
          new ArrayList<TaskTrackerInstrumentation>();
        for (Class<?> cls: instrumentationClasses) {
          java.lang.reflect.Constructor<?> c =
            cls.getConstructor(new Class[] {TaskTracker.class} );
          TaskTrackerInstrumentation inst =
            (TaskTrackerInstrumentation) c.newInstance(tt);
          instrumentations.add(inst);
        }
        return new CompositeTaskTrackerInstrumentation(tt, instrumentations);
      }
    } catch(Exception e) {
      // Reflection can throw lots of exceptions -- handle them all by
      // falling back on the default.
      LOG.error("Failed to initialize TaskTracker metrics", e);
      return new TaskTrackerMetricsInst(tt);
    }
  }

  /**
   * Removes all contents of temporary storage.  Called upon
   * startup, to remove any leftovers from previous run.
   *
   * Use MRAsyncDiskService.moveAndDeleteAllVolumes instead.
   * @see org.apache.hadoop.util.MRAsyncDiskService#cleanupAllVolumes()
   */
  @Deprecated
  public void cleanupStorage() throws IOException {
    this.fConf.deleteLocalFiles();
  }

  // Object on wait which MapEventsFetcherThread is going to wait.
  private Object waitingOn = new Object();

  private class MapEventsFetcherThread extends Thread {

    public List <FetchStatus> reducesInShuffle() {
      List <FetchStatus> fList = new ArrayList<FetchStatus>();
      for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
        RunningJob rjob = item.getValue();
        JobID jobId = item.getKey();
        FetchStatus f;
        synchronized (rjob) {
          f = rjob.getFetchStatus();
          for (TaskInProgress tip : rjob.tasks) {
            Task task = tip.getTask();
            if (!task.isMapTask()) {
              if (((ReduceTask)task).getPhase() ==
                  TaskStatus.Phase.SHUFFLE) {
                if (rjob.getFetchStatus() == null) {
                  //this is a new job; we start fetching its map events
                  f = new FetchStatus(
                      jobId, ((ReduceTask)task).getNumMaps(), rjob);
                  rjob.setFetchStatus(f);
                }
                f = rjob.getFetchStatus();
                fList.add(f);
                break; //no need to check any more tasks belonging to this
              }
            }
          }
        }
      }
      //at this point, we have information about for which of
      //the running jobs do we need to query the jobtracker for map
      //outputs (actually map events).
      return fList;
    }

    @Override
    public void run() {
      LOG.info("Starting thread: " + this.getName());

      while (running) {
        try {
          List <FetchStatus> fList = null;
          synchronized (runningJobs) {
            while (((fList = reducesInShuffle()).size()) == 0) {
              try {
                runningJobs.wait();
              } catch (InterruptedException e) {
                LOG.info("Shutting down: " + this.getName());
                return;
              }
            }
          }
          // now fetch all the map task events for all the reduce tasks
          // possibly belonging to different jobs
          boolean fetchAgain = false; //flag signifying whether we want to fetch
                                      //immediately again.
          for (FetchStatus f : fList) {
            long currentTime = System.currentTimeMillis();
            try {
              //the method below will return true when we have not
              //fetched all available events yet
              if (f.fetchMapCompletionEvents(currentTime)) {
                fetchAgain = true;
              }
            } catch (Exception e) {
              LOG.warn(
                       "Ignoring exception that fetch for map completion" +
                       " events threw for " + f.jobId + " threw: ", e);
            }
            if (!running) {
              break;
            }
          }
          synchronized (waitingOn) {
            try {
              if (!fetchAgain) {
                waitingOn.wait(heartbeatInterval);
              }
            } catch (InterruptedException ie) {
              LOG.info("Shutting down: " + this.getName());
              return;
            }
          }
        } catch (Exception e) {
          LOG.info("Ignoring exception ", e);
        }
      }
    }
  }

  public class FetchStatus {
    /** The next event ID that we will start querying the JobTracker from*/
    public IntWritable fromEventId;
    /** This is the cache of map events for a given job */
    private List<TaskCompletionEvent> allMapEvents;
    /** What jobid this fetchstatus object is for*/
    private JobID jobId;
    private long lastFetchTime;
    private boolean fetchAgain;
    private RunningJob rJob;

    public FetchStatus(JobID jobId, int numMaps, RunningJob rJob) {
      this.fromEventId = new IntWritable(0);
      this.jobId = jobId;
      this.allMapEvents = new ArrayList<TaskCompletionEvent>(numMaps);
      this.rJob = rJob;
    }

    /**
     * Reset the events obtained so far.
     */
    public void reset() {
      // Note that the sync is first on fromEventId and then on allMapEvents
      synchronized (fromEventId) {
        synchronized (allMapEvents) {
          fromEventId.set(0); // set the new index for TCE
          allMapEvents.clear();
        }
      }
    }

    public TaskCompletionEvent[] getMapEvents(int fromId, int max) {

      TaskCompletionEvent[] mapEvents =
        TaskCompletionEvent.EMPTY_ARRAY;
      boolean notifyFetcher = false;
      synchronized (allMapEvents) {
        if (allMapEvents.size() > fromId) {
          int actualMax = Math.min(max, (allMapEvents.size() - fromId));
          List <TaskCompletionEvent> eventSublist =
            allMapEvents.subList(fromId, actualMax + fromId);
          mapEvents = eventSublist.toArray(mapEvents);
        } else {
          // Notify Fetcher thread.
          notifyFetcher = true;
          // Go to the jobtracker right away
          fetchAgain = TaskTracker.this.fastFetch;
        }
      }
      if (notifyFetcher) {
        synchronized (waitingOn) {
          waitingOn.notify();
        }
      }
      return mapEvents;
    }

    public boolean fetchMapCompletionEvents(long currTime) throws IOException {
      if (!fetchAgain && (currTime - lastFetchTime) < heartbeatInterval) {
        return false;
      }
      int currFromEventId = 0;
      synchronized (fromEventId) {
        currFromEventId = fromEventId.get();
        List <TaskCompletionEvent> recentMapEvents =
          queryJobTracker(fromEventId, jobId, rJob.getJobClient());
        synchronized (allMapEvents) {
          allMapEvents.addAll(recentMapEvents);
        }
        lastFetchTime = currTime;
        if (fromEventId.get() - currFromEventId >= probe_sample_size) {
          //return true when we have fetched the full payload, indicating
          //that we should fetch again immediately (there might be more to
          //fetch
          fetchAgain = true;
          return true;
        }
      }
      fetchAgain = false;
      return false;
    }
  }

  private static LocalDirAllocator lDirAlloc =
                              new LocalDirAllocator("mapred.local.dir");

  // intialize the job directory
  private JobConf localizeJob(TaskInProgress tip) throws IOException {
    Path localJarFile = null;
    Task t = tip.getTask();
    JobID jobId = t.getJobID();
    Path jobFile = new Path(t.getJobFile());
    // Get sizes of JobFile and JarFile
    // sizes are -1 if they are not present.
    FileStatus status = null;
    long jobFileSize = -1;
    try {
      status = systemFS.getFileStatus(jobFile);
      jobFileSize = status.getLen();
    } catch(FileNotFoundException fe) {
      jobFileSize = -1;
    }
    Path localJobFile = lDirAlloc.getLocalPathForWrite(
                                    getLocalJobDir(jobId.toString())
                                    + Path.SEPARATOR + "job.xml",
                                    jobFileSize, fConf);
    RunningJob rjob = addTaskToJob(jobId, tip);
    synchronized (rjob.localizationLock) {
      if (rjob.localized == false) {
        // Actually start the job localization IO.
        FileSystem localFs = FileSystem.getLocal(fConf);
        // this will happen on a partial execution of localizeJob.
        // Sometimes the job.xml gets copied but copying job.jar
        // might throw out an exception
        // we should clean up and then try again
        Path jobDir = localJobFile.getParent();
        if (localFs.exists(jobDir)){
          LOG.warn("Deleting pre-existing jobDir: " + jobDir
              + " when localizeJob for tip " + tip);
          localFs.delete(jobDir, true);
          boolean b = localFs.mkdirs(jobDir);
          if (!b)
            throw new IOException("Not able to create job directory "
                + jobDir.toString());
        }
        systemFS.copyToLocalFile(jobFile, localJobFile);
        boolean cleanup = false;
        JobConf localJobConf = null;
        Path bigParamPath = null;
        if (fConf.getBoolean("mapred.input.dir.cleanup", true)) {
          bigParamPath = new Path(jobDir, "bigParam");
          int threshold = fConf.getInt("mapred.input.dir.cleanup.threshold", 1000);
          localJobConf = new JobConf(localJobFile, bigParamPath, localFs, threshold);
        } else {
          localJobConf = new JobConf(localJobFile);
        } 

        // create the 'work' directory
        // job-specific shared directory for use as scratch space
        Path workDir = lDirAlloc.getLocalPathForWrite(
            (getLocalJobDir(jobId.toString())
                + Path.SEPARATOR + "work"), fConf);
        if (!localFs.mkdirs(workDir)) {
          throw new IOException("Mkdirs failed to create "
              + workDir.toString());
        }
        System.setProperty("job.local.dir", workDir.toString());
        localJobConf.set("job.local.dir", workDir.toString());

        // copy Jar file to the local FS and unjar it.
        String jarFile = localJobConf.getJar();
        long jarFileSize = -1;
        boolean changeLocalJobConf = jarFile != null;
        if (changeLocalJobConf) {
          boolean shared =
            localJobConf.getBoolean("mapred.cache.shared.enabled", false);

          // If sharing is turned on, we already have the jarFileSize, so we
          // don't have to make another RPC call to NameNode
          Path jarFilePath = new Path(jarFile);
          if (shared) {
            try {
              jarFileSize =
                Long.parseLong(DistributedCache.
                    getSharedArchiveLength(localJobConf)[0]);
            } catch (NullPointerException npe) {
              jarFileSize = -1;
            }
          } else {
            try {
              status = systemFS.getFileStatus(jarFilePath);
              jarFileSize = status.getLen();
            } catch(FileNotFoundException fe) {
              jarFileSize = -1;
            }
          }
          // Here we check for and we check five times the size of jarFileSize
          // to accommodate for unjarring the jar file in work directory
          localJarFile = new Path(lDirAlloc.getLocalPathForWrite(
              getLocalJobDir(jobId.toString())
              + Path.SEPARATOR + "jars",
              5 * jarFileSize, fConf), "job.jar");
          if (!localFs.mkdirs(localJarFile.getParent())) {
            throw new IOException("Mkdirs failed to create jars directory ");
          }

          if (!shared) {
            // we copy the job jar to the local disk and unjar it
            // for the shared case - this is done inside TaskRunner
            systemFS.copyToLocalFile(jarFilePath, localJarFile);
            RunJar.unJar(new File(localJarFile.toString()),
                new File(localJarFile.getParent().toString()));
          }
          localJobConf.setJar(localJarFile.toString());
        }
        reconfigureLocalJobConf(localJobConf, localJobFile, tip, changeLocalJobConf);
        synchronized (rjob) {
          rjob.keepJobFiles = ((localJobConf.getKeepTaskFilesPattern() != null) ||
              localJobConf.getKeepFailedTaskFiles());
          rjob.jobConf = localJobConf;
          taskController.initializeJob(jobId);
          rjob.localized = true;
        }
      } else {
        // Even if job is localized we must update umbilical addresses
        // Only when the remote job tracker address get changed we will update the XML file. 
        // Refer to the logic of reconfigureLocalJobConf which override this function in 
        // CoronaTaskTracker.
        JobConf localJobConf = new JobConf(localJobFile);
        reconfigureLocalJobConf(localJobConf, localJobFile, tip, false);
      }
    }
    return new JobConf(rjob.jobConf);
  }

  protected void reconfigureLocalJobConf(
      JobConf localJobConf, Path localJobFile, TaskInProgress tip, boolean changed)
      throws IOException {
    if (!changed) {
      return;
    }
    OutputStream out = localFs.create(localJobFile);
    try {
      localJobConf.writeXml(out);
    } finally {
      out.close();
    }
  }

  public synchronized void shutdown() throws IOException {
    shuttingDown = true;
    close();
  }
  /**
   * Close down the TaskTracker and all its components.  We must also shutdown
   * any running tasks or threads, and cleanup disk space.  A new TaskTracker
   * within the same process space might be restarted, so everything must be
   * clean.
   */
  public synchronized void close() throws IOException {
    //
    // Kill running tasks.  Do this in a 2nd vector, called 'tasksToClose',
    // because calling jobHasFinished() may result in an edit to 'tasks'.
    //
    TreeMap<TaskAttemptID, TaskInProgress> tasksToClose =
      new TreeMap<TaskAttemptID, TaskInProgress>();
    tasksToClose.putAll(tasks);
    for (TaskInProgress tip : tasksToClose.values()) {
      tip.jobHasFinished(false);
    }

    this.running = false;

    if (pulseChecker != null) {
      pulseChecker.shutdown();
    }

    if (versionBeanName != null) {
      MBeanUtil.unregisterMBean(versionBeanName);
    }

    // Clear local storage
    if (asyncDiskService != null) {
      // Clear local storage
      asyncDiskService.cleanupAllVolumes();

      // Shutdown all async deletion threads with up to 10 seconds of delay
      asyncDiskService.shutdown();
      try {
        if (!asyncDiskService.awaitTermination(10000)) {
          asyncDiskService.shutdownNow();
          asyncDiskService = null;
        }
      } catch (InterruptedException e) {
        asyncDiskService.shutdownNow();
        asyncDiskService = null;
      }
    }

    // Shutdown the fetcher thread
    if (this.mapEventsFetcher != null) {
      this.mapEventsFetcher.interrupt();
    }

    // Stop the launchers
    this.mapLauncher.interrupt();
    this.reduceLauncher.interrupt();
    if (this.heartbeatMonitor != null) {
      this.heartbeatMonitor.interrupt();
    }

    // Stop memory manager thread
    if (this.taskMemoryManager != null) {
      this.taskMemoryManager.shutdown();
    }

    // Stop cgroup memory watcher
    this.cgroupMemoryWatcher.shutdown();

    // All tasks are killed. So, they are removed from TaskLog monitoring also.
    // Interrupt the monitor.
    getTaskLogsMonitor().interrupt();

    jvmManager.stop();

    // shutdown RPC connections
    RPC.stopProxy(jobClient);

    // wait for the fetcher thread to exit
    for (boolean done = false; !done; ) {
      try {
        if (this.mapEventsFetcher != null) {
          this.mapEventsFetcher.join();
        }
        done = true;
      } catch (InterruptedException e) {
      }
    }

    if (taskReportServer != null) {
      taskReportServer.stop();
      taskReportServer = null;
    }
    if (healthChecker != null) {
      //stop node health checker service
      healthChecker.stop();
      healthChecker = null;
    }

    if (this.server != null) {
      try {
        LOG.info("Shutting down StatusHttpServer");
        this.server.stop();
        LOG.info("Shutting down Netty MapOutput Server");
        if (this.nettyMapOutputServer != null) {
          this.nettyMapOutputServer.stop();
        }
      } catch (Exception e) {
        LOG.warn("Exception shutting down TaskTracker", e);
      }
    }
  }

  /**
   * Start with the local machine name, and the default JobTracker
   */
  public TaskTracker(JobConf conf) throws IOException {
    // Default is to use netty over jetty
    boolean useNetty = conf.getBoolean(NETTY_MAPOUTPUT_USE, true);
    this.shuffleServerMetrics = new ShuffleServerMetrics(conf);
    if (useNetty) {
      initNettyMapOutputHttpServer(conf);
    }
    initHttpServer(conf, useNetty);
    LOG.info("Http port " + httpPort +
              ", netty map output http port " + nettyMapOutputHttpPort +
              ", use netty = " + useNetty);
    initJobClient(conf);
    initialize(conf);
    initializeMapEventFetcher();
  }

  protected void initJobClient(JobConf conf) throws IOException {
    this.jobTrackAddr = JobTracker.getAddress(conf);
    this.jobClient = (InterTrackerProtocol)
    RPC.waitForProxy(InterTrackerProtocol.class,
        InterTrackerProtocol.versionID,
        jobTrackAddr,conf);
  }

  protected void initNettyMapOutputHttpServer(JobConf conf) throws IOException {
    int nettyHttpPort = conf.getInt(NETTY_MAPOUTPUT_HTTP_PORT, 0);
    NettyMapOutputAttributes attributes = new NettyMapOutputAttributes(
      conf, this, FileSystem.getLocal(conf),
      new LocalDirAllocator("mapred.local.dir"), shuffleServerMetrics);
    nettyMapOutputServer = new NettyMapOutputHttpServer(nettyHttpPort);
    nettyMapOutputServer.init(conf);
    shuffleServerMetrics.setNettyWorkerThreadPool(
      nettyMapOutputServer.getWorkerThreadPool());
    HttpMapOutputPipelineFactory pipelineFactory =
        new HttpMapOutputPipelineFactory(attributes, nettyHttpPort);
    this.nettyMapOutputHttpPort = nettyMapOutputServer.start(
      conf, pipelineFactory);
  }

  protected void initHttpServer(JobConf conf,
      boolean useNettyMapOutputs) throws IOException {

    String infoAddr =
      NetUtils.getServerAddress(conf,
                                "tasktracker.http.bindAddress",
                                "tasktracker.http.port",
                                "mapred.task.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String httpBindAddress = infoSocAddr.getHostName();
    int httpPort = infoSocAddr.getPort();
    server = new HttpServer("task", httpBindAddress, httpPort,
        httpPort == 0, conf);
    workerThreads = conf.getInt("tasktracker.http.threads", 40);
    server.setThreads(1, workerThreads);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator("mapred.local.dir");
    server.setAttribute("task.tracker", this);
    server.setAttribute("local.file.system", local);
    server.setAttribute("conf", conf);
    server.setAttribute("log", LOG);
    server.setAttribute("localDirAllocator", localDirAllocator);
    server.setAttribute("shuffleServerMetrics", shuffleServerMetrics);
    server.setAttribute(ReconfigurationServlet.
                        CONF_SERVLET_RECONFIGURABLE_PREFIX + "/ttconfchange",
                        TaskTracker.this);
    server.setAttribute("nettyMapOutputHttpPort", nettyMapOutputHttpPort);
    server.addInternalServlet("reconfiguration", "/ttconfchange",
                                ReconfigurationServlet.class);
    server.addInternalServlet(
      "mapOutput", "/mapOutput", MapOutputServlet.class);
    server.addInternalServlet("taskLog", "/tasklog", TaskLogServlet.class);
    server.start();
    this.httpPort = server.getPort();
    checkJettyPort();
  }

  /**
   * Blank constructor. Only usable by tests.
   */
  TaskTracker() {
    server = null;
  }

  /**
   * Configuration setter method for use by tests.
   */
  void setConf(JobConf conf) {
    fConf = conf;
  }

  public boolean isJettyLowOnThreads() {
    return server.isLowOnThreads();
  }

  public int getJettyQueueSize() {
    return server.getQueueSize();
  }

  public int getJettyThreads() {
    return server.getThreads();
  }

  protected void checkJettyPort() throws IOException {
    //See HADOOP-4744
    int port = server.getPort();
    if (port < 0) {
      shuttingDown = true;
      throw new IOException("Jetty problem. Jetty didn't bind to a " +
      		"valid port");
    }
  }

  // This method is stubbed for testing only
  public void startLogCleanupThread() throws IOException {
      logCleanupThread.setDaemon(true);
      logCleanupThread.start();
  }

  protected void startCleanupThreads() throws IOException {
    taskCleanupThread.setDaemon(true);
    taskCleanupThread.start();
    logCleanupThread.setDaemon(true);
    logCleanupThread.start();
    directoryCleanupThread = new CleanupQueue();
  }

  /**
   * The connection to the JobTracker, used by the TaskRunner
   * for locating remote files.
   */
  public InterTrackerProtocol getJobClient() {
    return jobClient;
  }

  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }

  /**
   * Given a TaskCompletionEvent, it checks the store and returns an equivalent 
   * copy that can be used instead. If not in the store, it adds it to the store 
   * and returns the same supplied TaskCompletionEvent. If the caller uses the
   * stored copy, we have an opportunity to save memory.
   * @param t the TaskCompletionEvent to check in the store. If not in the store
   * add it
   * @return the equivalent TaskCompletionEvent to use instead. May be the same
   * as the one passed in.
   */
  private static TaskCompletionEvent getTceFromStore(TaskCompletionEvent t) {
    // Use the store so that we can save memory in simulations where there
    // are multiple task trackers in memory
    synchronized(taskCompletionEventsStore) {
      WeakReference<TaskCompletionEvent> e =
          taskCompletionEventsStore.get(t);
      // If it's not in the store, then put it in
      if (e == null) {
        taskCompletionEventsStore.put(t,
            new WeakReference<TaskCompletionEvent>(t));
        return t;
      }
      // It might be in the map, but the actual item might have been GC'ed
      // just after we got it from the map
      TaskCompletionEvent tceFromStore = e.get();
      if (tceFromStore == null) {
        taskCompletionEventsStore.put(t,
            new WeakReference<TaskCompletionEvent>(t));
        return t;
      }
      return tceFromStore;
    }
  }

  /** Queries the job tracker for a set of outputs ready to be copied
   * @param fromEventId the first event ID we want to start from, this is
   * modified by the call to this method
   * @param jobClient the job tracker
   * @return a set of locations to copy outputs from
   * @throws IOException
   */
  private List<TaskCompletionEvent> queryJobTracker(IntWritable fromEventId,
                                                    JobID jobId,
                                                    InterTrackerProtocol jobClient)
    throws IOException {
    if (jobClient == null) {
      List<TaskCompletionEvent> empty = Collections.emptyList();
      return empty;
    }
    TaskCompletionEvent t[] = jobClient.getTaskCompletionEvents(
                                                                jobId,
                                                                fromEventId.get(),
                                                                probe_sample_size);
    //we are interested in map task completion events only. So store
    //only those
    List <TaskCompletionEvent> recentMapEvents =
      new ArrayList<TaskCompletionEvent>();
    for (int i = 0; i < t.length; i++) {
      if (t[i].isMap) {
        if (useTaskCompletionEventsStore) {
          // Try to get it from a store so that we don't have duplicate instances
          // in memory in the same JVM. This could happen if there are multiple TT's
          // and different reduce tasks from the same job are running in each TT.
          recentMapEvents.add(getTceFromStore(t[i]));
        } else {
          recentMapEvents.add(t[i]);
        }
      }
    }
    fromEventId.set(fromEventId.get() + t.length);
    return recentMapEvents;
  }

  private class HeartbeatMonitor extends Thread {
    public static final long DEFAULT_HEARTBEAT_GAP = 30 * 60 * 1000;  // 30 min.
    final private long maxHeartbeatGap;

    public HeartbeatMonitor(Configuration conf) {
      maxHeartbeatGap =
        conf.getLong("mapred.tasktraker.maxheartbeatgap", DEFAULT_HEARTBEAT_GAP);
    }

    @Override
    public void run() {
      LOG.info("Starting HeartbeatMonitor");
      boolean forceExit = false;
      long gap = 0;
      while (running && !shuttingDown) {
        long now = System.currentTimeMillis();
        gap = now - lastHeartbeat;
        if (gap > maxHeartbeatGap) {
          forceExit = true;
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
      if (forceExit) {
        LOG.fatal("No heartbeat for " + gap + " msec, TaskTracker has to die");
        ReflectionUtils.logThreadInfo(LOG, "No heartbeat", 1);
        System.exit(-1);
      } else {
        LOG.info("Stopping HeartbeatMonitor, running=" + running +
          ", shuttingDown=" + shuttingDown);
      }
    }
  }

  /**
   * Main service loop.  Will stay in this loop forever.
   */
  private State offerService() throws Exception {
    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time or
          // until there are empty slots to schedule tasks
          synchronized (finishedCount) {
            if (finishedCount.get() == 0) {
              finishedCount.wait(waitTime);
            }
            finishedCount.set(0);
          }
        }

        // If the TaskTracker is just starting up:
        // 1. Verify the buildVersion
        // 2. Get the system directory & filesystem
        if(justInited) {
          String jobTrackerBV = jobClient.getBuildVersion();
          if(doCheckBuildVersion() &&
             !VersionInfo.getBuildVersion().equals(jobTrackerBV)) {
            String msg = "Shutting down. Incompatible buildVersion." +
            "\nJobTracker's: " + jobTrackerBV +
            "\nTaskTracker's: "+ VersionInfo.getBuildVersion();
            LOG.error(msg);
            try {
              jobClient.reportTaskTrackerError(taskTrackerName, null, msg);
            } catch(Exception e ) {
              LOG.info("Problem reporting to jobtracker: " + e);
            }
            return State.DENIED;
          }

          String dir = jobClient.getSystemDir();
          if (dir == null) {
            throw new IOException("Failed to get system directory");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(fConf);
        }

        boolean sendCounters = false;
        if (now > (previousCounterUpdate + COUNTER_UPDATE_INTERVAL)) {
          sendCounters = true;
          previousCounterUpdate = now;
        }

        status = updateTaskTrackerStatus(
            sendCounters, status, runningTasks.values(), jobTrackAddr);

        // Send heartbeat only when there is at least one task in progress
        HeartbeatResponse heartbeatResponse = transmitHeartBeat(
            jobClient, heartbeatResponseId, status);

        // The heartbeat got through successfully!
        // Force a rebuild of 'status' on the next iteration
        status = null;
        heartbeatResponseId = heartbeatResponse.getResponseId();


        final boolean firstHeartbeat = (lastHeartbeat == 0);
        // Note the time when the heartbeat returned, use this to decide when to send the
        // next heartbeat
        lastHeartbeat = System.currentTimeMillis();
        // Start the heartbeat monitor after the first heartbeat.
        if (firstHeartbeat) {
          heartbeatMonitor.start();
        }

        TaskTrackerAction[] actions = heartbeatResponse.getActions();
        if(LOG.isDebugEnabled()) {
          LOG.debug("Got heartbeatResponse from JobTracker with responseId: " +
                    heartbeatResponse.getResponseId() + " and " +
                    ((actions != null) ? actions.length : 0) + " actions");
        }
        if (reinitTaskTracker(actions)) {
          return State.STALE;
        }

        // resetting heartbeat interval from the response.
        heartbeatInterval = heartbeatResponse.getHeartbeatInterval();
        justStarted = false;
        justInited = false;
        if (actions != null){
          for(TaskTrackerAction action: actions) {
            if (action instanceof LaunchTaskAction) {
              addToTaskQueue((LaunchTaskAction)action);
            } else if (action instanceof CommitTaskAction) {
              CommitTaskAction commitAction = (CommitTaskAction)action;
              if (!commitResponses.contains(commitAction.getTaskID())) {
                LOG.info("Received commit task action for " +
                          commitAction.getTaskID());
                commitResponses.add(commitAction.getTaskID());
              }
            } else {
              tasksToCleanup.put(action);
            }
          }
        }
        markUnresponsiveTasks();
        killOverflowingTasks();

        //we've cleaned up, resume normal operation
        if (!acceptNewTasks && isIdle()) {
          acceptNewTasks=true;
        }
        //The check below may not be required every iteration but we are
        //erring on the side of caution here. We have seen many cases where
        //the call to jetty's getLocalPort() returns different values at
        //different times. Being a real paranoid here.
        checkJettyPort();
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (DiskErrorException de) {
        String msg = "Exiting task tracker for disk error:\n" +
          StringUtils.stringifyException(de);
        LOG.error(msg);
        synchronized (this) {
          jobClient.reportTaskTrackerError(taskTrackerName,
                                           "DiskErrorException", msg);
        }
        return State.STALE;
      } catch (RemoteException re) {
        String reClass = re.getClassName();
        if (DisallowedTaskTrackerException.class.getName().equals(reClass)) {
          LOG.info("Tasktracker disallowed by JobTracker.");
          return State.DENIED;
        }
      } catch (Exception except) {
        LOG.error("Caught exception: ", except);
      }
    }

    return State.NORMAL;
  }

  /**
   * Build and transmit the heart beat to the JobTracker
   * @param jobClient The jobTracker RPC handle
   * @param heartbeatResponseId Last heartbeat response received
   * @param status TaskTrackerStatus to transmit
   * @return false if the tracker was unknown
   * @throws IOException
   */
  protected HeartbeatResponse transmitHeartBeat(
      InterTrackerProtocol jobClient, short heartbeatResponseId,
      TaskTrackerStatus status) throws IOException {
    //
    // Check if we should ask for a new Task
    //
    boolean askForNewTask;
    long localMinSpaceStart;
    synchronized (this) {
      askForNewTask =
        ((status.countOccupiedMapSlots() < maxMapSlots ||
          status.countOccupiedReduceSlots() < maxReduceSlots) &&
         acceptNewTasks);
      localMinSpaceStart = minSpaceStart;
    }
    if (askForNewTask) {
      checkLocalDirs(getLocalDirsFromConf(fConf));
      askForNewTask = enoughFreeSpace(localMinSpaceStart);
      gatherResourceStatus(status);
    }
    //add node health information

    TaskTrackerHealthStatus healthStatus = status.getHealthStatus();
    synchronized (this) {
      if (healthChecker != null) {
        healthChecker.setHealthStatus(healthStatus);
      } else {
        healthStatus.setNodeHealthy(true);
        healthStatus.setLastReported(0L);
        healthStatus.setHealthReport("");
      }
    }
    //
    // Xmit the heartbeat
    //
    HeartbeatResponse heartbeatResponse = jobClient.heartbeat(status,
                                                              justStarted,
                                                              justInited,
                                                              askForNewTask,
                                                              heartbeatResponseId);

    synchronized (this) {
      for (TaskStatus taskStatus : status.getTaskReports()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            !taskStatus.inTaskCleanupPhase()) {
          if (taskStatus.getIsMap()) {
            mapTotal--;
          } else {
            reduceTotal--;
          }
          try {
            myInstrumentation.completeTask(taskStatus.getTaskID());
          } catch (MetricsException me) {
            LOG.warn("Caught: " + StringUtils.stringifyException(me));
          }
          removeRunningTask(taskStatus.getTaskID());
          
          //
          // When the task attempt has entered the finished state
          // we log the counters to task log for future use
          // load the counters into Scuba, Scriber and Hive
          //
          if (fConf.getBoolean(LOG_FINISHED_TASK_COUNTERS, true)) {
            // for log format, 0 means json, else means name and value pair
            String logHeader = "TaskCountersLogged " + taskStatus.getTaskID() + " " +
              taskStatus.getFinishTime()/1000 + " ";
            if (fConf.getInt(FINISHED_TASK_COUNTERS_LOG_FORMAT, 0) == 0) {
              LOG.warn(
                  logHeader + taskStatus.getCounters().makeJsonString());
            } else {
              LOG.warn(
                  logHeader + taskStatus.getCounters().makeCompactString());
            }
          }
        }
      }

      // Clear transient status information which should only
      // be sent once to the JobTracker
      for (TaskInProgress tip: runningTasks.values()) {
        tip.getStatus().clearStatus();
      }
    }

    return heartbeatResponse;
  }

  protected TaskTrackerStatus updateTaskTrackerStatus(
      boolean sendCounters, TaskTrackerStatus oldStatus,
      Collection<TaskInProgress> tips, InetSocketAddress jobTrackerAddr) {

    //
    // Check if the last heartbeat got through...
    // if so then build the heartbeat information for the JobTracker;
    // else resend the previous status information.
    //
    if (oldStatus == null) {
      synchronized (this) {
        return new TaskTrackerStatus(taskTrackerName, localHostname,
                                     httpPort,
                                     cloneAndResetRunningTaskStatuses(
                                       tips, sendCounters),
                                     failures,
                                     maxMapSlots,
                                     maxReduceSlots);
      }
    }
    LOG.info("Resending 'status' to '" + jobTrackAddr.getHostName() +
        "' with reponseId '" + heartbeatResponseId);
    return oldStatus;
  }

  public Boolean isAlive() {
    long timeSinceHearbeat = System.currentTimeMillis() - lastHeartbeat;
    long expire = fConf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);

    if (timeSinceHearbeat > expire) {
      return false;
    }

    return true;
  }

  private void gatherResourceStatus(TaskTrackerStatus status)
      throws IOException {
    long freeDiskSpace = getDiskSpace(true);
    long totVmem = getTotalVirtualMemoryOnTT();
    long totPmem = getTotalPhysicalMemoryOnTT();
    long availableVmem = resourceCalculatorPlugin.getAvailableVirtualMemorySize();
    long availablePmem = resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
    long cumuCpuTime = resourceCalculatorPlugin.getCumulativeCpuTime();
    long cpuFreq = resourceCalculatorPlugin.getCpuFrequency();
    int numCpu = resourceCalculatorPlugin.getNumProcessors();
    float cpuUsage = resourceCalculatorPlugin.getCpuUsage();

    status.getResourceStatus().setAvailableSpace(freeDiskSpace);
    status.getResourceStatus().setTotalVirtualMemory(totVmem);
    status.getResourceStatus().setTotalPhysicalMemory(totPmem);
    status.getResourceStatus().setAvailableVirtualMemory(availableVmem);
    status.getResourceStatus().setAvailablePhysicalMemory(availablePmem);
    status.getResourceStatus().setMapSlotMemorySizeOnTT(
        mapSlotMemorySizeOnTT);
    status.getResourceStatus().setReduceSlotMemorySizeOnTT(
        reduceSlotSizeMemoryOnTT);
    status.getResourceStatus().setCumulativeCpuTime(cumuCpuTime);
    status.getResourceStatus().setCpuFrequency(cpuFreq);
    status.getResourceStatus().setNumProcessors(numCpu);
    status.getResourceStatus().setCpuUsage(cpuUsage);
  }

  protected boolean doCheckBuildVersion() {
    return fConf.getBoolean(CHECK_TASKTRACKER_BUILD_VERSION, true);
  }

  long getMapUserLogRetainSize() {
    return fConf.getLong(MAP_USERLOG_RETAIN_SIZE, -1);
  }

  void setMapUserLogRetainSize(long retainSize) {
    fConf.setLong(MAP_USERLOG_RETAIN_SIZE, retainSize);
  }

  long getReduceUserLogRetainSize() {
    return fConf.getLong(REDUCE_USERLOG_RETAIN_SIZE, -1);
  }

  void setReduceUserLogRetainSize(long retainSize) {
    fConf.setLong(REDUCE_USERLOG_RETAIN_SIZE, retainSize);
  }

  /**
   * Returns the MRAsyncDiskService object for async deletions.
   */
  public MRAsyncDiskService getAsyncDiskService() {
    return asyncDiskService;
  }

  /**
   * Return the total virtual memory available on this TaskTracker.
   * @return total size of virtual memory.
   */
  long getTotalVirtualMemoryOnTT() {
    return totalVirtualMemoryOnTT;
  }

  /**
   * Return the total physical memory available on this TaskTracker.
   * @return total size of physical memory.
   */
  long getTotalPhysicalMemoryOnTT() {
    return totalPhysicalMemoryOnTT;
  }

  long getTotalMemoryAllottedForTasksOnTT() {
    return totalMemoryAllottedForTasks;
  }

  /**
   * Check if the jobtracker directed a 'reset' of the tasktracker.
   *
   * @param actions the directives of the jobtracker for the tasktracker.
   * @return <code>true</code> if tasktracker is to be reset,
   *         <code>false</code> otherwise.
   */
  private boolean reinitTaskTracker(TaskTrackerAction[] actions) {
    if (actions != null) {
      for (TaskTrackerAction action : actions) {
        if (action.getActionId() ==
            TaskTrackerAction.ActionType.REINIT_TRACKER) {
          LOG.info("Recieved RenitTrackerAction from JobTracker");
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Kill any tasks that have not reported progress in the last X seconds.
   */
  protected synchronized void markUnresponsiveTasks() throws IOException {
    long now = System.currentTimeMillis();
    for (TaskInProgress tip: runningTasks.values()) {
      if (tip.getRunState() == TaskStatus.State.RUNNING ||
          tip.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          tip.isCleaningup()) {
        // Check the per-job timeout interval for tasks;
        // an interval of '0' implies it is never timed-out
        long jobTaskTimeout = tip.getTaskTimeout();
        if (jobTaskTimeout == 0) {
          continue;
        }

        // Check if the task has not reported progress for a
        // time-period greater than the configured time-out
        long timeSinceLastReport = now - tip.getLastProgressReport();
        if (timeSinceLastReport > jobTaskTimeout && !tip.wasKilled) {
          String msg =
            "Task " + tip.getTask().getTaskID() + " failed to report status for "
            + (timeSinceLastReport / 1000) + " seconds. Killing!";
          LOG.info(tip.getTask().getTaskID() + ": " + msg);
          ReflectionUtils.logThreadInfo(LOG, "lost task", 30);
          tip.reportDiagnosticInfo(msg);
          myInstrumentation.timedoutTask(tip.getTask().getTaskID());
          purgeTask(tip, true);
        }
      }
    }
  }

  private static PathDeletionContext[] buildPathDeletionContexts(FileSystem fs,
      Path[] paths) {
    int i = 0;
    PathDeletionContext[] contexts = new PathDeletionContext[paths.length];

    for (Path p : paths) {
      contexts[i++] = new PathDeletionContext(fs, p.toUri().getPath());
    }
    return contexts;
  }

  static PathDeletionContext[] buildTaskControllerPathDeletionContexts(
      FileSystem fs, Path[] paths, Task task, boolean isWorkDir,
      TaskController taskController)
      throws IOException {
    int i = 0;
    PathDeletionContext[] contexts =
                          new TaskControllerPathDeletionContext[paths.length];

    for (Path p : paths) {
      contexts[i++] = new TaskControllerPathDeletionContext(fs, p, task,
                          isWorkDir, taskController);
    }
    return contexts;
  }

  /**
   * The task tracker is done with this job, so we need to clean up.
   * @param action The action with the job
   * @throws IOException
   */
  protected synchronized void purgeJob(KillJobAction action) throws IOException {
    JobID jobId = action.getJobID();
    LOG.info("Received 'KillJobAction' for job: " + jobId);
    RunningJob rjob = null;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);
    }

    if (rjob == null) {
      if (LOG.isDebugEnabled()) {
        // We cleanup the job on all tasktrackers in the cluster
        // so there is a good chance it never ran a single task from it
        LOG.debug("Unknown job " + jobId + " being deleted.");
      }
    } else {
      synchronized (rjob) {
        // Add this tips of this job to queue of tasks to be purged
        for (TaskInProgress tip : rjob.tasks) {
          tip.jobHasFinished(false);
          Task t = tip.getTask();
          if (t.isMapTask()) {
            indexCache.removeMap(tip.getTask().getTaskID().toString());
          }
          
          // Remove it from the runningTasks
          if (this.runningTasks.containsKey(t.getTaskID())) {
            LOG.info("Remove " + t.getTaskID() + " from runningTask by purgeJob");
            this.runningTasks.remove(t.getTaskID());
          }
        }
        // Delete the job directory for this
        // task if the job is done/failed
        if (!rjob.keepJobFiles){
          PathDeletionContext[] contexts = buildPathDeletionContexts(localFs,
              getLocalFiles(fConf, getLocalJobDir(rjob.getJobID().toString())));
          directoryCleanupThread.addToQueue(contexts);
        }
        // Remove this job
        rjob.tasks.clear();
      }
    }

    synchronized(runningJobs) {
      runningJobs.remove(jobId);
    }

  }


  /**
   * Remove the tip and update all relevant state.
   *
   * @param tip {@link TaskInProgress} to be removed.
   * @param wasFailure did the task fail or was it killed?
   */
  private void purgeTask(TaskInProgress tip, boolean wasFailure)
  throws IOException {
    if (tip != null) {
      LOG.info("About to purge task: " + tip.getTask().getTaskID());

      // Remove the task from running jobs,
      // removing the job if it's the last task
      removeTaskFromJob(tip.getTask().getJobID(), tip);
      tip.jobHasFinished(wasFailure);
      if (tip.getTask().isMapTask()) {
        indexCache.removeMap(tip.getTask().getTaskID().toString());
      }
    }
  }

  /** Check if we're dangerously low on disk space
   * If so, kill jobs to free up space and make sure
   * we don't accept any new tasks
   * Try killing the reduce jobs first, since I believe they
   * use up most space
   * Then pick the one with least progress
   */
  protected void killOverflowingTasks() throws IOException {
    long localMinSpaceKill;
    synchronized(this){
      localMinSpaceKill = minSpaceKill;
    }
    if (!enoughFreeSpace(localMinSpaceKill)) {
      acceptNewTasks=false;
      //we give up! do not accept new tasks until
      //all the ones running have finished and they're all cleared up
      synchronized (this) {
        TaskInProgress killMe = findTaskToKill(null);

        if (killMe!=null) {
          String msg = "Tasktracker running out of space." +
            " Killing task.";
          LOG.info(killMe.getTask().getTaskID() + ": " + msg);
          killMe.reportDiagnosticInfo(msg);
          purgeTask(killMe, false);
        }
      }
    }
  }

  /**
   * Pick a task to kill to free up memory/disk-space
   * @param tasksToExclude tasks that are to be excluded while trying to find a
   *          task to kill. If null, all runningTasks will be searched.
   * @return the task to kill or null, if one wasn't found
   */
  synchronized TaskInProgress findTaskToKill(List<TaskAttemptID> tasksToExclude) {
    TaskInProgress killMe = null;
    for (Iterator it = runningTasks.values().iterator(); it.hasNext();) {
      TaskInProgress tip = (TaskInProgress) it.next();

      if (tasksToExclude != null
          && tasksToExclude.contains(tip.getTask().getTaskID())) {
        // exclude this task
        continue;
      }

      if ((tip.getRunState() == TaskStatus.State.RUNNING ||
           tip.getRunState() == TaskStatus.State.COMMIT_PENDING) &&
          !tip.wasKilled) {

        if (killMe == null) {
          killMe = tip;

        } else if (!tip.getTask().isMapTask()) {
          //reduce task, give priority
          if (killMe.getTask().isMapTask() ||
              (tip.getTask().getProgress().get() <
               killMe.getTask().getProgress().get())) {

            killMe = tip;
          }

        } else if (killMe.getTask().isMapTask() &&
                   tip.getTask().getProgress().get() <
                   killMe.getTask().getProgress().get()) {
          //map task, only add if the progress is lower

          killMe = tip;
        }
      }
    }
    return killMe;
  }

  /**
   * Check if any of the local directories has enough
   * free space  (more than minSpace)
   *
   * If not, do not try to get a new task assigned
   * @return
   * @throws IOException
   */
  private boolean enoughFreeSpace(long minSpace) throws IOException {
    if (minSpace == 0) {
      return true;
    }
    return minSpace < getDiskSpace(true);
  }

  /**
   * Obtain the maximum disk space (free or total) in bytes for each volume
   * @param free If true returns free space, else returns capacity
   * @return disk space in bytes
   * @throws IOException
   */
  long getDiskSpace(boolean free) throws IOException {
    long biggestSeenSoFar = 0;
    String[] localDirs = getLocalDirsFromConf(fConf);
    for (int i = 0; i < localDirs.length; i++) {
      DF df = null;
      if (localDirsDf.containsKey(localDirs[i])) {
        df = localDirsDf.get(localDirs[i]);
      } else {
        df = new DF(new File(localDirs[i]), fConf);
        localDirsDf.put(localDirs[i], df);
      }
      long onThisVol = free ? df.getAvailable() : df.getCapacity();
      if (onThisVol > biggestSeenSoFar) {
        biggestSeenSoFar = onThisVol;
      }
    }

    //Should ultimately hold back the space we expect running tasks to use but
    //that estimate isn't currently being passed down to the TaskTrackers
    return biggestSeenSoFar;
  }

  /**
   * Obtain the free space on the log disk. If the log disk is not configured,
   * returns Long.MAX_VALUE
   * @return The free space available.
   * @throws IOException
   */
  long getLogDiskFreeSpace() throws IOException {
    String logDir = fConf.getLogDir();
    // If the log disk is not specified we assume it is usable.
    if (logDir == null) {
      return Long.MAX_VALUE;
    }
    DF df = localDirsDf.get(logDir);
    if (df == null) {
      df = new DF(new File(logDir), fConf);
      localDirsDf.put(logDir, df);
    }
    return df.getAvailable();
  }

  /**
   * Try to get the size of output for this task.
   * Returns -1 if it can't be found.
   * @return
   */
  long tryToGetOutputSize(TaskAttemptID taskId, JobConf conf) {

    try{
      TaskInProgress tip;
      synchronized(this) {
        tip = tasks.get(taskId);
      }
      if(tip == null)
         return -1;

      if (!tip.getTask().isMapTask() ||
          tip.getRunState() != TaskStatus.State.SUCCEEDED) {
        return -1;
      }

      MapOutputFile mapOutputFile = new MapOutputFile();
      mapOutputFile.setJobId(taskId.getJobID());
      mapOutputFile.setConf(conf);

      // In simulation mode, maps/reduces complete instantly and don't produce
      // any output
      if (this.simulatedTaskMode) {
        return 0;
      }

      Path tmp_output = null;
      try {
        tmp_output =  mapOutputFile.getOutputFile(taskId);
      } catch (DiskErrorException dex) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error getting map output of a task " + taskId, dex);
        }
      }
      if(tmp_output == null)
        return 0;
      FileSystem localFS = FileSystem.getLocal(conf);
      FileStatus stat = localFS.getFileStatus(tmp_output);
      if(stat == null)
        return 0;
      else
        return stat.getLen();
    } catch(IOException e) {
      LOG.info(e);
      return -1;
    }
  }

  private PulseChecker pulseChecker;

  private HeartbeatMonitor heartbeatMonitor;
  private TaskLauncher mapLauncher;
  private TaskLauncher reduceLauncher;
  public JvmManager getJvmManagerInstance() {
    return jvmManager;
  }

  protected void addToTaskQueue(LaunchTaskAction action) {
    if (action.getTask().isMapTask()) {
      mapLauncher.addToTaskQueue(action);
    } else {
      reduceLauncher.addToTaskQueue(action);
    }
  }

  /**
   * Simple helper class for the list of tasks to launch
   */
  private class TaskLaunchData {
    /** Time when this task in progress requested */
    final long startedMsecs = System.currentTimeMillis();
    /** Actual task in progress */
    final TaskInProgress taskInProgress;

    TaskLaunchData(TaskInProgress taskInProgress) {
      this.taskInProgress = taskInProgress;
    }
  }

  protected class TaskLauncher extends Thread {
    private IntWritable numFreeSlots;
    /** Maximum slots used for scheduling */
    private final int maxSlots;
    /** The real number of maximum slots */
    private final int actualMaxSlots;
    /** Tasks to launch in order of insert time */
    private final List<TaskLaunchData> tasksToLaunch;
    /** Used to determine which metrics to append to (map or reduce) */
    private final TaskType taskType;
    /** Keep track of the last free times for all the slots */
    private final LinkedList<Long> lastFreeMsecsQueue;

    /**
     * Constructor.
     *
     * @param taskType Type of the task (i.e. Map, Reduce)
     * @param numSlots Number of slots available for scheduling
     * @param actualNumSlots Actual number of slots on this TaskTracker
     *        (metrics)
     */
    public TaskLauncher(TaskType taskType, int numSlots, int actualNumSlots) {
      this.maxSlots = numSlots;
      this.actualMaxSlots = actualNumSlots;
      this.numFreeSlots = new IntWritable(numSlots);
      this.tasksToLaunch = new LinkedList<TaskLaunchData>();
      setDaemon(true);
      setName("TaskLauncher for " + taskType + " tasks");
      this.taskType = taskType;
      // Initialize the last free times for all the slots based on the actual
      // number of slots
      lastFreeMsecsQueue = new LinkedList<Long>();
      long currentTime = System.currentTimeMillis();
      for (int i = 0; i < actualNumSlots; ++i) {
        lastFreeMsecsQueue.add(currentTime);
      }
    }

    public void addToTaskQueue(LaunchTaskAction action) {
      synchronized (tasksToLaunch) {
        TaskInProgress tip = registerTask(action, this);
        tasksToLaunch.add(new TaskLaunchData(tip));
        tasksToLaunch.notifyAll();
      }
    }

    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }

    /**
     * Get the number of used slots for metrics
     *
     * @return Number of used slots for this TaskLauncher
     */
    public int getNumUsedSlots() {
      synchronized (numFreeSlots) {
        return maxSlots - numFreeSlots.get();
      }
    }

    public void addFreeSlots(int numSlots) {
      synchronized (numFreeSlots) {
        numFreeSlots.set(numFreeSlots.get() + numSlots);
        assert (numFreeSlots.get() <= maxSlots);
        LOG.info("addFreeSlot : " + taskType + " current free slots : " +
            numFreeSlots.get() + ", queue size : " + lastFreeMsecsQueue.size() +
            ", max queue size : " + maxSlots);

        // Create the initial timestamps for starting a slot refill
        // for these newly free slots
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < numSlots; ++i) {
          lastFreeMsecsQueue.add(currentTime);
        }
        // Due to a possible violations of using more than the actual slots,
        // we only allow the queue to grow to its maximum actual size
        if (lastFreeMsecsQueue.size() > actualMaxSlots) {
          LOG.warn("addFreeSlots: " + taskType + " lastFreeMsecsQueue is " +
              " too large (overscheduled) with " + lastFreeMsecsQueue.size() +
              " instead of " + actualMaxSlots + " slots.");
          while (lastFreeMsecsQueue.size() > actualMaxSlots) {
            lastFreeMsecsQueue.removeLast();
          }
        }
        numFreeSlots.notifyAll();
      }
    }

    /**
     * Add the update refill msecs to the metrics.  This method needs to be
     * synchronized with numFreeSlots and is currently only called in run()
     * under synchronization of numFreeSlots.
     *
     * @param usedSlots Number of slots refilled
     */
    private void updateRefillMsecs(int usedSlots) {
      long currentTime = System.currentTimeMillis();
      for (int i = 0; i < usedSlots; ++i) {
        // There should also be at least usedSlots entries in
        // lastFreeMsecsQueue, but Corona can violate this
        // principle by scheduling tasks before another task resource is
        // confirmed to have been released.
        if (lastFreeMsecsQueue.isEmpty()) {
          LOG.warn("updateRefillMsecs: Only obtained refill times for " + i +
                   " out of " + usedSlots + " slots.");
          break;
        }
        int refillMsecs = (int) (currentTime - lastFreeMsecsQueue.remove());
        if (taskType == TaskType.MAP) {
          addAveMapSlotRefillMsecs(refillMsecs);
        } else if (taskType == TaskType.REDUCE) {
          addAveReduceSlotRefillMsecs(refillMsecs);
        } else {
          throw new RuntimeException("updateRefillMsecs doesn't " +
            "suppport task type " + taskType);
        }
      }
    }

    public void run() {
      while (running) {
        try {
          TaskInProgress tip;
          TaskLaunchData taskLaunchData;
          Task task;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            taskLaunchData = tasksToLaunch.remove(0);
            //get the TIP
            tip = taskLaunchData.taskInProgress;
            task = tip.getTask();
            LOG.info("Trying to launch : " + tip.getTask().getTaskID() +
                     " which needs " + task.getNumSlotsRequired() + " slots");
          }
          //wait for free slots to run
          synchronized (numFreeSlots) {
            while (numFreeSlots.get() < task.getNumSlotsRequired()) {
              LOG.info("TaskLauncher : Waiting for " + task.getNumSlotsRequired() +
                       " to launch " + task.getTaskID() + ", currently we have " +
                       numFreeSlots.get() + " free slots");
              numFreeSlots.wait();
            }
            LOG.info("In TaskLauncher, current free slots : " + numFreeSlots.get()+
                     " and trying to launch " + tip.getTask().getTaskID() +
                     " which needs " + task.getNumSlotsRequired() + " slots");
            numFreeSlots.set(numFreeSlots.get() - task.getNumSlotsRequired());
            assert (numFreeSlots.get() >= 0);
            // Add the refill times for the used slots
            updateRefillMsecs(task.getNumSlotsRequired());
          }
          synchronized (tip) {
            //to make sure that there is no kill task action for this
            if (tip.getRunState() != TaskStatus.State.UNASSIGNED &&
                tip.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
                tip.getRunState() != TaskStatus.State.KILLED_UNCLEAN) {
              //got killed externally while still in the launcher queue
              addFreeSlots(task.getNumSlotsRequired());
              continue;
            }
            tip.slotTaken = true;
          }
          // Got a free slot. If it's in simulation mode, add it to the queue
          // so that it will be automatically marked as finished after some
          // time. Otherwise, start the task.
          if (simulatedTaskMode) {
            // Also mark the task as running. If it's not marked as running,
            // the JT may declare it as a failed launch while waiting to start
            // The task status is transmitted via heartbeat. See
            // "Error launching task" in JT. We allow KILLED_UNCLEAN tasks
            /// as that's the state of killed tasks.
            if (tip.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
              LOG.info("For running as simulation, changing run state for " +
                tip.getTask().getTaskID() + " from UNASSIGNED to RUNNING");
              tip.taskStatus.setRunState(TaskStatus.State.RUNNING);
            } else if (tip.taskStatus.getRunState() ==
                TaskStatus.State.KILLED_UNCLEAN) {
              // We can't change the run state to running as it will prevent
              // tasks from getting killed.
              LOG.info("For simulation, leaving run state same for " +
                  tip.getTask().getTaskID() + " as KILLED_UNCLEAN");
            } else {
              throw new RuntimeException("Task " + tip.getTask().getTaskID() +
                  " is not in the UNASSIGNED/KILLED_UNCLEAN state. Instead " +
                  "it's in " + tip.taskStatus
                  );
            }
            // Set the start time so we get cleaner looking tables in UI
            tip.taskStatus.setStartTime(System.currentTimeMillis());
            // This must be called here as localizeJob() is not called during
            // simulation. Without this call, the TT won't fetch map task
            // completion events for the job, and the reducers won't know when
            // they can start
            addTaskToJob(tip.getTask().getJobID(), tip);
            // Map tasks and cleanup tasks can be queued up to finish ASAP.
            // However, reduce tasks should wait until all the mappers have
            // finished. launchTask() should handle this.
            simulatedTaskRunner.launchTask(tip);

          } else {
            startNewTask(tip);
          }

          // Add metrics on how long it took to launch the task after added
          myInstrumentation.addTaskLaunchMsecs(
              System.currentTimeMillis() - taskLaunchData.startedMsecs);
        } catch (InterruptedException e) {
          if (!running)
            return; // ALL DONE
          LOG.warn ("Unexpected InterruptedException");
        } catch (Throwable th) {
          LOG.error("TaskLauncher error " +
              StringUtils.stringifyException(th));
        }
      }
    }
  }

  private TaskInProgress registerTask(LaunchTaskAction action,
      TaskLauncher launcher) {
    Task t = action.getTask();
    LOG.info("LaunchTaskAction (registerTask): " + t.getTaskID() +
             " task's state:" + t.getState());
    TaskInProgress tip = new TaskInProgress(
        t, fConf, launcher, action.getExtensible());
    synchronized (this) {
      tasks.put(t.getTaskID(), tip);
      runningTasks.put(t.getTaskID(), tip);
      boolean isMap = t.isMapTask();
      if (isMap) {
        mapTotal++;
      } else {
        reduceTotal++;
      }
    }
    return tip;
  }
  /**
   * Start a new task.
   * All exceptions are handled locally, so that we don't mess up the
   * task tracker.
   */
  private void startNewTask(TaskInProgress tip) {
    try {
      boolean launched = localizeAndLaunchTask(tip);
      if (!launched) {
        // Free the slot.
        tip.kill(true);
        tip.cleanup(true);
      }
    } catch (Throwable e) {
      String msg = ("Error initializing " + tip.getTask().getTaskID() +
                    ":\n" + StringUtils.stringifyException(e));
      LOG.error(msg, e);
      tip.reportDiagnosticInfo(msg);
      try {
        tip.kill(true);
        tip.cleanup(true);
      } catch (IOException ie2) {
        LOG.info("Error cleaning up " + tip.getTask().getTaskID() + ":\n" +
                 StringUtils.stringifyException(ie2));
      }

      // Careful!
      // This might not be an 'Exception' - don't handle 'Error' here!
      if (e instanceof Error) {
        throw ((Error) e);
      }
    }
  }

  /**
   * Localize and launch the task.
   * If it takes too long, try cancel the thread that does localization.
   * If the thread cannot be terminated, kill the JVM.
   *  The TaskTracker will die.
   *
   * We have seen localizeTask hangs TaskTracker. In this case we lose the
   * node. This method will kill tasktracker and so it can be restarted later.
   */
  private boolean localizeAndLaunchTask(final TaskInProgress tip)
      throws IOException {
    FutureTask<Boolean> task = new FutureTask<Boolean>(
        new Callable<Boolean>() {
          public Boolean call() throws IOException {
            JobConf localConf = localizeJob(tip);
            boolean launched = false;
            synchronized (tip) {
              tip.setJobConf(localConf);
              launched = tip.launchTask();
            }
            return launched;
          }
        });
    String threadName = "Localizing " + tip.getTask().toString();
    Thread thread = new Thread(task);
    thread.setName(threadName);
    thread.setDaemon(true);
    thread.start();
    boolean launched = false;
    try {
      launched = task.get(LOCALIZE_TASK_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      task.cancel(true);
      try {
        LOG.info("Wait the localizeTask thread to finish");
        thread.join(LOCALIZE_TASK_TIMEOUT);
      } catch (InterruptedException ie) {
      }
      if (thread.isAlive()) {
        LOG.error("Stacktrace of " + threadName + "\n" +
          StringUtils.stackTraceOfThread(thread));
        LOG.fatal("Cannot kill the localizeTask thread." + threadName +
          " TaskTracker has to die!!");
        System.exit(-1);
      }
      throw new IOException("TaskTracker got stuck for localized Task:" +
          tip.getTask().getTaskID(), e);
    }
    return launched;
  }

  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap,
                          JobConf conf) {
     addToMemoryManager(attemptId, isMap, conf, true);
  }

  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap,
                          JobConf conf, boolean cgroupWatcherFlag) {
    if (cgroupWatcherFlag) {
      cgroupMemoryWatcher.addTask(attemptId,
        isMap ? conf
          .getMemoryForMapTask() * 1024 * 1024L : conf
          .getMemoryForReduceTask() * 1024 * 1024L);
    }
  }

  void removeFromMemoryManager(TaskAttemptID attemptId) {
    cgroupMemoryWatcher.removeTask(attemptId);
  }

  /**
   * Notify the tasktracker to send an out-of-band heartbeat.
   */
  private void notifyTTAboutTaskCompletion() {
    if (oobHeartbeatOnTaskCompletion) {
      synchronized (finishedCount) {
        int value = finishedCount.get();
        finishedCount.set(value+1);
        finishedCount.notifyAll();
      }
    }
  }

  /**
   * The server retry loop.
   * This while-loop attempts to connect to the JobTracker.
   */
  public void run() {
    try {
      startCleanupThreads();
      try {
        // This while-loop attempts reconnects if we get network errors
        while (running && !shuttingDown) {
          try {
            State osState = offerService();
            if (osState == State.STALE || osState == State.DENIED) {
              // Shutdown TaskTracker instead of reinitialize.
              // TaskTracker should be restarted by external tools.
              LOG.error("offerService returns " + osState + ". Shutdown. ");
              break;
            }
          } catch (Exception ex) {
            if (!shuttingDown) {
              LOG.info("Lost connection to JobTracker [" +
                  jobTrackAddr + "].  Retrying...", ex);
              try {
                Thread.sleep(5000);
              } catch (InterruptedException ie) {
              }
            }
          }
        }
      } finally {
        shutdown();
      }
    } catch (IOException iex) {
      LOG.error("Got fatal exception while initializing TaskTracker", iex);
      return;
    }
  }

  ///////////////////////////////////////////////////////
  // TaskInProgress maintains all the info for a Task that
  // lives at this TaskTracker.  It maintains the Task object,
  // its TaskStatus, and the TaskRunner.
  ///////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    long lastProgressReport;
    StringBuffer diagnosticInfo = new StringBuffer();
    private TaskRunner runner;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    private JobConf defaultJobConf;
    private JobConf localJobConf;
    private boolean keepFailedTaskFiles;
    private boolean alwaysKeepTaskFiles;
    private TaskStatus taskStatus;
    private long taskTimeout;
    private String debugCommand;
    private volatile boolean slotTaken = false;
    private TaskLauncher launcher;

    private Writable extensible = null;

    public Writable getExtensible() {
      return extensible;
    }

    public TaskInProgress(Task task, JobConf conf,
        TaskLauncher launcher, Writable extensible) {
      this.task = task;
      this.launcher = launcher;
      this.lastProgressReport = System.currentTimeMillis();
      this.defaultJobConf = conf;
      this.extensible = extensible;
      localJobConf = null;
      taskStatus = TaskStatus.createTaskStatus(task.isMapTask(), task.getTaskID(),
                                               0.0f,
                                               task.getNumSlotsRequired(),
                                               task.getState(),
                                               diagnosticInfo.toString(),
                                               "initializing",
                                               getName(),
                                               task.isTaskCleanupTask() ?
                                                 TaskStatus.Phase.CLEANUP :
                                               task.isMapTask()? TaskStatus.Phase.MAP:
                                               TaskStatus.Phase.SHUFFLE,
                                               task.getCounters());
      taskTimeout = (10 * 60 * 1000);
    }

    private void localizeTask(Task task) throws IOException{

      Path localTaskDir =
        lDirAlloc.getLocalPathForWrite(
          TaskTracker.getLocalTaskDir(task.getJobID().toString(),
            task.getTaskID().toString(), task.isTaskCleanupTask()),
          defaultJobConf );

      FileSystem localFs = FileSystem.getLocal(fConf);
      if (!localFs.mkdirs(localTaskDir)) {
        throw new IOException("Mkdirs failed to create "
                    + localTaskDir.toString());
      }

      // create symlink for ../work if it already doesnt exist
      String workDir = lDirAlloc.getLocalPathToRead(
                         TaskTracker.getLocalJobDir(task.getJobID().toString())
                         + Path.SEPARATOR
                         + "work", defaultJobConf).toString();
      String link = localTaskDir.getParent().toString()
                      + Path.SEPARATOR + "work";
      File flink = new File(link);
      if (!flink.exists())
        FileUtil.symLink(workDir, link);

      // create the working-directory of the task
      Path cwd = lDirAlloc.getLocalPathForWrite(
                   getLocalTaskDir(task.getJobID().toString(),
                      task.getTaskID().toString(), task.isTaskCleanupTask())
                   + Path.SEPARATOR + MRConstants.WORKDIR,
                   defaultJobConf);
      if (!localFs.mkdirs(cwd)) {
        throw new IOException("Mkdirs failed to create "
                    + cwd.toString());
      }

      Path localTaskFile = new Path(localTaskDir, "job.xml");
      task.setJobFile(localTaskFile.toString());
      localJobConf.set("mapred.local.dir",
                       fConf.get("mapred.local.dir"));
      if (fConf.get("slave.host.name") != null) {
        localJobConf.set("slave.host.name",
                         fConf.get("slave.host.name"));
      }

      localJobConf.set("mapred.task.id", task.getTaskID().toString());
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();

      task.localizeConfiguration(localJobConf);

      Task.saveStaticResolutions(localJobConf);

      if (task.isMapTask()) {
        debugCommand = localJobConf.getMapDebugScript();
      } else {
        debugCommand = localJobConf.getReduceDebugScript();
      }
      String keepPattern = localJobConf.getKeepTaskFilesPattern();
      if (keepPattern != null) {
        alwaysKeepTaskFiles =
          Pattern.matches(keepPattern, task.getTaskID().toString());
      } else {
        alwaysKeepTaskFiles = false;
      }
      if (debugCommand != null || localJobConf.getProfileEnabled() ||
          alwaysKeepTaskFiles || keepFailedTaskFiles) {
        //disable jvm reuse
        localJobConf.setNumTasksToExecutePerJvm(1);
      }
      if (isTaskMemoryManagerEnabled()) {
        localJobConf.setBoolean("task.memory.mgmt.enabled", true);
      }
      OutputStream out = localFs.create(localTaskFile);
      try {
        localJobConf.writeXml(out);
      } finally {
        out.close();
      }
      task.setConf(localJobConf);
    }

    /**
     */
    public Task getTask() {
      return task;
    }

    public TaskRunner getTaskRunner() {
      return runner;
    }

    public synchronized void setJobConf(JobConf lconf){
      this.localJobConf = lconf;
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();
      taskTimeout = localJobConf.getLong("mapred.task.timeout",
                                         10 * 60 * 1000);
    }

    public synchronized JobConf getJobConf() {
      return localJobConf;
    }

    /**
     */
    public synchronized TaskStatus getStatus() {
      taskStatus.setDiagnosticInfo(diagnosticInfo.toString());
      if (diagnosticInfo.length() > 0) {
        diagnosticInfo = new StringBuffer();
      }

      return taskStatus;
    }

    /**
     * Kick off the task execution
     */
    public synchronized boolean launchTask() throws IOException {
      if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED ||
          this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        localizeTask(task);
        if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
          this.taskStatus.setRunState(TaskStatus.State.RUNNING);
        }
        this.runner = task.createRunner(TaskTracker.this, this);
        this.runner.start();
        this.taskStatus.setStartTime(System.currentTimeMillis());
        return true;
      } else {
        LOG.info("Not launching task: " + task.getTaskID() +
            " since it's state is " + this.taskStatus.getRunState());
        return false;
      }
    }

    boolean isCleaningup() {
   	  return this.taskStatus.inTaskCleanupPhase();
    }

    /**
     * The task is reporting its progress
     */
    public synchronized void reportProgress(TaskStatus taskStatus)
    {
      LOG.info(task.getTaskID() + " " + taskStatus.getProgress() +
          "% " + taskStatus.getStateString());
      // task will report its state as
      // COMMIT_PENDING when it is waiting for commit response and
      // when it is committing.
      // cleanup attempt will report its state as FAILED_UNCLEAN/KILLED_UNCLEAN
      if (this.done ||
          (this.taskStatus.getRunState() != TaskStatus.State.RUNNING &&
          this.taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
          !isCleaningup()) ||
          ((this.taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
           this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
           this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) &&
           taskStatus.getRunState() == TaskStatus.State.RUNNING)) {
        //make sure we ignore progress messages after a task has
        //invoked TaskUmbilicalProtocol.done() or if the task has been
        //KILLED/FAILED/FAILED_UNCLEAN/KILLED_UNCLEAN
        //Also ignore progress update if the state change is from
        //COMMIT_PENDING/FAILED_UNCLEAN/KILLED_UNCLEA to RUNNING
        LOG.info(task.getTaskID() + " Ignoring status-update since " +
                 ((this.done) ? "task is 'done'" :
                                ("runState: " + this.taskStatus.getRunState()))
                 );
        return;
      }

      this.taskStatus.statusUpdate(taskStatus);
      this.lastProgressReport = System.currentTimeMillis();
    }

    /**
     */
    public long getLastProgressReport() {
      return lastProgressReport;
    }

    /**
     */
    public TaskStatus.State getRunState() {
      return taskStatus.getRunState();
    }

    /**
     * The task's configured timeout.
     *
     * @return the task's configured timeout.
     */
    public long getTaskTimeout() {
      return taskTimeout;
    }

    /**
     * The task has reported some diagnostic info about its status
     */
    public synchronized void reportDiagnosticInfo(String info) {
      this.diagnosticInfo.append(info);
    }

    public synchronized void reportNextRecordRange(SortedRanges.Range range) {
      this.taskStatus.setNextRecordRange(range);
    }

    /**
     * The task is reporting that it's done running
     */
    public synchronized void reportDone() {
      if (isCleaningup()) {
        if (this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.FAILED);
        } else if (this.taskStatus.getRunState() ==
                   TaskStatus.State.KILLED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      } else {
        this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
      }
      this.taskStatus.setProgress(1.0f);
      this.taskStatus.setFinishTime(System.currentTimeMillis());
      this.done = true;
      // Runner may be null in the case when we are running a simulation and
      // no runner was actually launched
      if (!simulatedTaskMode) {
        jvmManager.taskFinished(runner);
        runner.signalDone();
      }
      LOG.info("Task " + task.getTaskID() + " is done.");
      LOG.info("reported output size for " + task.getTaskID() +  "  was " + taskStatus.getOutputSize());
      myInstrumentation.statusUpdate(task, taskStatus);
    }

    public boolean wasKilled() {
      return wasKilled;
    }

    /**
     * A task is reporting in as 'done'.
     *
     * We need to notify the tasktracker to send an out-of-band heartbeat.
     * If isn't <code>commitPending</code>, we need to finalize the task
     * and release the slot it's occupied.
     *
     * @param commitPending is the task-commit pending?
     */
    void reportTaskFinished(boolean commitPending) {
      if (!commitPending) {
        taskFinished();
        releaseSlot();
      }
      notifyTTAboutTaskCompletion();
    }

    /* State changes:
     * RUNNING/COMMIT_PENDING -> FAILED_UNCLEAN/FAILED/KILLED_UNCLEAN/KILLED
     * FAILED_UNCLEAN -> FAILED
     * KILLED_UNCLEAN -> KILLED
     */
    private void setTaskFailState(boolean wasFailure) {
      // go FAILED_UNCLEAN -> FAILED and KILLED_UNCLEAN -> KILLED always
      if (taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else if (taskStatus.getRunState() ==
                 TaskStatus.State.KILLED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      } else if (task.isMapOrReduce() &&
                 taskStatus.getPhase() != TaskStatus.Phase.CLEANUP) {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED_UNCLEAN);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED_UNCLEAN);
        }
      } else {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
    }

    /**
     * The task has actually finished running.
     */
    public void taskFinished() {
      long start = System.currentTimeMillis();

      //
      // Wait until task reports as done.  If it hasn't reported in,
      // wait for a second and try again.
      //
      while (!done && (System.currentTimeMillis() - start < WAIT_FOR_DONE)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }

      //
      // Change state to success or failure, depending on whether
      // task was 'done' before terminating
      //
      boolean needCleanup = false;
      synchronized (this) {
        // Remove the task from MemoryManager, if the task SUCCEEDED or FAILED.
        // KILLED tasks are removed in method kill(), because Kill
        // would result in launching a cleanup attempt before
        // TaskRunner returns; if remove happens here, it would remove
        // wrong task from memory manager.
        if (done || !wasKilled) {
          removeFromMemoryManager(task.getTaskID());
        }
        if (!done) {
          if (!wasKilled) {
            failures += 1;
            setTaskFailState(true);
            // call the script here for the failed tasks.
            if (debugCommand != null) {
              String taskStdout ="";
              String taskStderr ="";
              String taskSyslog ="";
              String jobConf = task.getJobFile();
              try {
                Map<LogName, LogFileDetail> allFilesDetails =
                    TaskLog.getAllLogsFileDetails(task.getTaskID(), false);
                // get task's stdout file
                taskStdout =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDOUT).location,
                        LogName.STDOUT);
                // get task's stderr file
                taskStderr =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDERR).location,
                        LogName.STDERR);
                // get task's syslog file
                taskSyslog =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.SYSLOG).location,
                        LogName.SYSLOG);
              } catch(IOException e){
                LOG.warn("Exception finding task's stdout/err/syslog files");
              }
              File workDir = null;
              try {
                workDir = new File(lDirAlloc.getLocalPathToRead(
                                     TaskTracker.getLocalTaskDir(
                                       task.getJobID().toString(),
                                       task.getTaskID().toString(),
                                       task.isTaskCleanupTask())
                                     + Path.SEPARATOR + MRConstants.WORKDIR,
                                     localJobConf). toString());
              } catch (IOException e) {
                LOG.warn("Working Directory of the task " + task.getTaskID() +
                		 "doesnt exist. Caught exception " +
                          StringUtils.stringifyException(e));
              }
              // Build the command
              File stdout = TaskLog.getRealTaskLogFileLocation(
                                   task.getTaskID(), TaskLog.LogName.DEBUGOUT);
              // add pipes program as argument if it exists.
              String program ="";
              String executable = Submitter.getExecutable(localJobConf);
              if ( executable != null) {
            	try {
            	  program = new URI(executable).getFragment();
            	} catch (URISyntaxException ur) {
            	  LOG.warn("Problem in the URI fragment for pipes executable");
            	}
              }
              String [] debug = debugCommand.split(" ");
              Vector<String> vargs = new Vector<String>();
              for (String component : debug) {
                vargs.add(component);
              }
              vargs.add(taskStdout);
              vargs.add(taskStderr);
              vargs.add(taskSyslog);
              vargs.add(jobConf);
              vargs.add(program);
              try {
                List<String>  wrappedCommand = TaskLog.captureDebugOut
                                                          (vargs, stdout);
                // run the script.
                try {
                  runScript(wrappedCommand, workDir);
                } catch (IOException ioe) {
                  LOG.warn("runScript failed with: " + StringUtils.
                                                      stringifyException(ioe));
                }
              } catch(IOException e) {
                LOG.warn("Error in preparing wrapped debug command");
              }

              // add all lines of debug out to diagnostics
              try {
                int num = localJobConf.getInt("mapred.debug.out.lines", -1);
                addDiagnostics(FileUtil.makeShellPath(stdout),num,"DEBUG OUT");
              } catch(IOException ioe) {
                LOG.warn("Exception in add diagnostics!");
              }

              // Debug-command is run. Do the post-debug-script-exit debug-logs
              // processing. Truncate the logs.
              getTaskLogsMonitor().addProcessForLogTruncation(
                  task.getTaskID(), Arrays.asList(task));
            }
          }
          taskStatus.setProgress(0.0f);
        }
        this.taskStatus.setFinishTime(System.currentTimeMillis());
        needCleanup = (taskStatus.getRunState() == TaskStatus.State.FAILED ||
                taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
                taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN ||
                taskStatus.getRunState() == TaskStatus.State.KILLED);
      }

      //
      // If the task has failed, or if the task was killAndCleanup()'ed,
      // we should clean up right away.  We only wait to cleanup
      // if the task succeeded, and its results might be useful
      // later on to downstream job processing.
      //
      if (needCleanup) {
        removeTaskFromJob(task.getJobID(), this);
      }
      try {
        cleanup(needCleanup);
      } catch (IOException ie) {
      }

    }


    /**
     * Runs the script given in args
     * @param args script name followed by its argumnets
     * @param dir current working directory.
     * @throws IOException
     */
    public void runScript(List<String> args, File dir) throws IOException {
      ShellCommandExecutor shexec =
              new ShellCommandExecutor(args.toArray(new String[0]), dir);
      shexec.execute();
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        throw new IOException("Task debug script exit with nonzero status of "
                              + exitCode + ".");
      }
    }

    /**
     * Add last 'num' lines of the given file to the diagnostics.
     * if num =-1, all the lines of file are added to the diagnostics.
     * @param file The file from which to collect diagnostics.
     * @param num The number of lines to be sent to diagnostics.
     * @param tag The tag is printed before the diagnostics are printed.
     */
    public void addDiagnostics(String file, int num, String tag) {
      RandomAccessFile rafile = null;
      try {
        rafile = new RandomAccessFile(file,"r");
        int no_lines =0;
        String line = null;
        StringBuffer tail = new StringBuffer();
        tail.append("\n-------------------- "+tag+"---------------------\n");
        String[] lines = null;
        if (num >0) {
          lines = new String[num];
        }
        while ((line = rafile.readLine()) != null) {
          no_lines++;
          if (num >0) {
            if (no_lines <= num) {
              lines[no_lines-1] = line;
            }
            else { // shift them up
              for (int i=0; i<num-1; ++i) {
                lines[i] = lines[i+1];
              }
              lines[num-1] = line;
            }
          }
          else if (num == -1) {
            tail.append(line);
            tail.append("\n");
          }
        }
        int n = no_lines > num ?num:no_lines;
        if (num >0) {
          for (int i=0;i<n;i++) {
            tail.append(lines[i]);
            tail.append("\n");
          }
        }
        if(n!=0)
          reportDiagnosticInfo(tail.toString());
      } catch (FileNotFoundException fnfe){
        LOG.warn("File "+file+ " not found");
      } catch (IOException ioe){
        LOG.warn("Error reading file "+file);
      } finally {
         try {
           if (rafile != null) {
             rafile.close();
           }
         } catch (IOException ioe) {
           LOG.warn("Error closing file "+file);
         }
      }
    }

    /**
     * We no longer need anything from this task, as the job has
     * finished.  If the task is still running, kill it and clean up.
     *
     * @param wasFailure did the task fail, as opposed to was it killed by
     *                   the framework
     */
    public void jobHasFinished(boolean wasFailure) throws IOException {
      // Kill the task if it is still running
      synchronized(this){
        if (getRunState() == TaskStatus.State.RUNNING ||
            getRunState() == TaskStatus.State.UNASSIGNED ||
            getRunState() == TaskStatus.State.COMMIT_PENDING ||
            isCleaningup()) {
          kill(wasFailure);
        }
      }

      // Cleanup on the finished task
      cleanup(true);
    }

    /**
     * Something went wrong and the task must be killed.
     * @param wasFailure was it a failure (versus a kill request)?
     */
    public synchronized void kill(boolean wasFailure) throws IOException {
      if (taskStatus.getRunState() == TaskStatus.State.RUNNING ||
          taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          isCleaningup()) {
        wasKilled = true;
        if (wasFailure) {
          failures += 1;
        }
        // runner could be null if task-cleanup attempt is not localized yet
        if (runner != null) {
          runner.kill();
        }
        // If the task is killed, then there is no need to finish that tip
        // anymore.
        if (TaskTracker.this.simulatedTaskMode) {
          TaskTracker.this.simulatedTaskRunner.cancel(this);
        }
        setTaskFailState(wasFailure);
      } else if (taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
        if (wasFailure) {
          failures += 1;
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
      taskStatus.setFinishTime(System.currentTimeMillis());
      removeFromMemoryManager(task.getTaskID());
      releaseSlot();
      myInstrumentation.statusUpdate(task, taskStatus);
      notifyTTAboutTaskCompletion();
    }

    private synchronized void releaseSlot() {
      if (slotTaken) {
        if (launcher != null) {
          launcher.addFreeSlots(task.getNumSlotsRequired());
        }
        slotTaken = false;
      }
    }

    /**
     * The map output has been lost.
     */
    private synchronized void mapOutputLost(String failure
                                           ) throws IOException {
      if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          taskStatus.getRunState() == TaskStatus.State.SUCCEEDED) {
        // change status to failure
        LOG.info("Reporting output lost:"+task.getTaskID());
        taskStatus.setRunState(TaskStatus.State.FAILED);
        taskStatus.setProgress(0.0f);
        reportDiagnosticInfo("Map output lost, rescheduling: " +
                             failure);
        runningTasks.put(task.getTaskID(), this);
        mapTotal++;
        myInstrumentation.statusUpdate(task, taskStatus);
      } else {
        LOG.warn("Output already reported lost:"+task.getTaskID());
      }
    }

    /**
     * We no longer need anything from this task.  Either the
     * controlling job is all done and the files have been copied
     * away, or the task failed and we don't need the remains.
     * Any calls to cleanup should not lock the tip first.
     * cleanup does the right thing- updates tasks in Tasktracker
     * by locking tasktracker first and then locks the tip.
     *
     * if needCleanup is true, the whole task directory is cleaned up.
     * otherwise the current working directory of the task
     * i.e. &lt;taskid&gt;/work is cleaned up.
     */
    void cleanup(boolean needCleanup) throws IOException {
      TaskAttemptID taskId = task.getTaskID();
      LOG.debug("Cleaning up " + taskId);

      if(taskMemoryControlGroupEnabled)
        ttMemCgroup.removeTask(taskId.toString());      

      if(taskCPUControlGroupEnabled)
        ttCPUCgroup.removeTask(taskId.toString());      

      synchronized (TaskTracker.this) {
        if (needCleanup) {
          // see if tasks data structure is holding this tip.
          // tasks could hold the tip for cleanup attempt, if cleanup attempt
          // got launched before this method.
          if (tasks.get(taskId) == this) {
            tasks.remove(taskId);
          }
        }
        synchronized (this){
          if (alwaysKeepTaskFiles ||
              (taskStatus.getRunState() == TaskStatus.State.FAILED &&
               keepFailedTaskFiles)) {
            return;
          }
        }
      }
      synchronized (this) {
        try {
          // localJobConf could be null if localization has not happened
          // then no cleanup will be required.
          if (localJobConf == null) {
            return;
          }
          String taskDir = getLocalTaskDir(task.getJobID().toString(),
                             taskId.toString(), task.isTaskCleanupTask());

          if (needCleanup) {
            if (runner != null) {
              //cleans up the output directory of the task (where map outputs
              //and reduce inputs get stored)
              runner.close();
            }
            //We don't delete the workdir
            //since some other task (running in the same JVM)
            //might be using the dir. The JVM running the tasks would clean
            //the workdir per a task in the task process itself.
            if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
              PathDeletionContext[] contexts =
                buildTaskControllerPathDeletionContexts(localFs, getLocalDirs(),
                  task, false/* not workDir */, taskController);
              directoryCleanupThread.addToQueue(contexts);
            }
            else {
              PathDeletionContext[] contexts = buildPathDeletionContexts(
                  localFs, getLocalFiles(defaultJobConf, taskDir+"/job.xml"));
              directoryCleanupThread.addToQueue(contexts);
            }
          } else {
            if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
              PathDeletionContext[] contexts =
                buildTaskControllerPathDeletionContexts(localFs, getLocalDirs(),
                  task, true /* workDir */,
                  taskController);
              directoryCleanupThread.addToQueue(contexts);
            }
          }
        } catch (Throwable ie) {
          LOG.info("Error cleaning up task runner: " +
                   StringUtils.stringifyException(ie));
        }
      }
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof TaskInProgress) &&
        task.getTaskID().equals
        (((TaskInProgress) obj).getTask().getTaskID());
    }

    @Override
    public int hashCode() {
      return task.getTaskID().hashCode();
    }
  }


  // ///////////////////////////////////////////////////////////////
  // TaskUmbilicalProtocol
  /////////////////////////////////////////////////////////////////

  /**
   * Called upon startup by the child process, to fetch Task data.
   */
  public synchronized JvmTask getTask(JvmContext context)
  throws IOException {
    JVMId jvmId = context.jvmId;
    LOG.debug("JVM with ID : " + jvmId + " asked for a task");
    // save pid of task JVM sent by child
    jvmManager.setPidToJvm(jvmId, context.pid);
    if (!jvmManager.isJvmKnown(jvmId)) {
      LOG.info("Killing unknown JVM " + jvmId);
      return new JvmTask(null, true);
    }
    RunningJob rjob = runningJobs.get(jvmId.getJobId());
    if (rjob == null) { //kill the JVM since the job is dead
      LOG.info("Killing JVM " + jvmId + " since job " + jvmId.getJobId() +
               " is dead");
      jvmManager.killJvm(jvmId);
      return new JvmTask(null, true);
    }
    TaskInProgress tip = jvmManager.getTaskForJvm(jvmId);
    if (tip == null) {
      return new JvmTask(null, false);
    }

    if(taskMemoryControlGroupEnabled) {
      long limit = getMemoryLimit(tip.getJobConf(), tip.getTask().isMapTask());
      ttMemCgroup.addTask(tip.getTask().getTaskID().toString(), context.pid, limit);
    }
    
    if(taskCPUControlGroupEnabled)
      ttCPUCgroup.addTask(tip.getTask().getTaskID().toString(), context.pid);
    
    if (tasks.get(tip.getTask().getTaskID()) != null) { //is task still present
      LOG.info("JVM with ID: " + jvmId + " given task: " +
          tip.getTask().getTaskID());
      return new JvmTask(tip.getTask(), false);
    } else {
      LOG.info("Killing JVM with ID: " + jvmId + " since scheduled task: " +
          tip.getTask().getTaskID() + " is " + tip.taskStatus.getRunState());
      return new JvmTask(null, true);
    }
  }

  /**
   * Called periodically to report Task progress, from 0.0 to 1.0.
   */
  public synchronized boolean statusUpdate(TaskAttemptID taskid,
                                              TaskStatus taskStatus)
  throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportProgress(taskStatus);
      myInstrumentation.statusUpdate(tip.getTask(), taskStatus);
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Progress from unknown child task: "+taskid);
      }
      return false;
    }
  }

  /**
   * Called when the task dies before completion, and we want to report back
   * diagnostic info
   */
  public synchronized void reportDiagnosticInfo(TaskAttemptID taskid, String info) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }

  public synchronized void reportNextRecordRange(TaskAttemptID taskid,
      SortedRanges.Range range) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportNextRecordRange(range);
    } else {
      LOG.warn("reportNextRecordRange from unknown child task: "+taskid+". " +
      		"Ignored.");
    }
  }

  /** Child checking to see if we're alive.  Normally does nothing.*/
  public synchronized boolean ping(TaskAttemptID taskid) throws IOException {
    return tasks.get(taskid) != null;
  }

  /**
   * Task is reporting that it is in commit_pending
   * and it is waiting for the commit Response
   */
  public synchronized void commitPending(TaskAttemptID taskid,
                                         TaskStatus taskStatus)
  throws IOException {
    LOG.info("Task " + taskid + " is in commit-pending," +"" +
             " task state:" +taskStatus.getRunState());
    statusUpdate(taskid, taskStatus);
    reportTaskFinished(taskid, true);
  }

  /**
   * Child checking whether it can commit
   */
  public synchronized boolean canCommit(TaskAttemptID taskid) {
    return commitResponses.contains(taskid); //don't remove it now
  }

  /**
   * The task is done.
   */
  public synchronized void done(TaskAttemptID taskid)
  throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    commitResponses.remove(taskid);
    if (tip != null) {
      tip.reportDone();
    } else {
      LOG.warn("Unknown child task done: "+taskid+". Ignored.");
    }
  }


  /**
   * A reduce-task failed to shuffle the map-outputs. Kill the task.
   */
  public synchronized void shuffleError(TaskAttemptID taskId, String message)
  throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed due to Shuffle Failure: " + message);
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      tip.reportDiagnosticInfo("Shuffle Error: " + message);
      purgeTask(tip, true);
    }
  }
  
  private boolean isDiskOutOfSpaceError(String message)
  {
    if (message.indexOf(this.DISK_OUT_OF_SPACE_KEY1) != -1 ||
        message.indexOf(this.DISK_OUT_OF_SPACE_KEY2) != -1) {
      return true;
    }
    
    return false;
  }

  /**
   * A child task had a local filesystem error. Kill the task.
   */
  public synchronized void fsError(TaskAttemptID taskId, String message)
  throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed due to FSError: " + message);
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      tip.reportDiagnosticInfo("FSError: " + message);
      purgeTask(tip, true);
    }
    if (isDiskOutOfSpaceError(message)) {
      this.myInstrumentation.diskOutOfSpaceTask(taskId);
    }
  }

  /**
   * A child task had a fatal error. Kill the task.
   */
  public synchronized void fatalError(TaskAttemptID taskId, String msg)
  throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed : " + msg);
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      tip.reportDiagnosticInfo("Error: " + msg);
      purgeTask(tip, true);
    }
  }

  public synchronized MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobId, int fromEventId, int maxLocs, TaskAttemptID id)
  throws IOException {
    TaskCompletionEvent[]mapEvents = TaskCompletionEvent.EMPTY_ARRAY;
    RunningJob rjob;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);
      if (rjob != null) {
        synchronized (rjob) {
          FetchStatus f = rjob.getFetchStatus();
          if (f != null) {
            mapEvents = f.getMapEvents(fromEventId, maxLocs);
          }
        }
      }
    }
    return new MapTaskCompletionEventsUpdate(mapEvents, false);
  }

  /////////////////////////////////////////////////////
  //  Called by TaskTracker thread after task process ends
  /////////////////////////////////////////////////////
  /**
   * The task is no longer running.  It may not have completed successfully
   */
  void reportTaskFinished(TaskAttemptID taskid, boolean commitPending) {
    TaskInProgress tip;
    synchronized (this) {
      tip = tasks.get(taskid);
    }
    if (tip != null) {
      tip.reportTaskFinished(commitPending);
    } else {
      LOG.warn("Unknown child task finished: "+taskid+". Ignored.");
    }
  }


  /**
   * A completed map task's output has been lost.
   */
  public synchronized void mapOutputLost(TaskAttemptID taskid,
                                         String errorMsg) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.mapOutputLost(errorMsg);
    } else {
      LOG.warn("Unknown child with bad map output: "+taskid+". Ignored.");
    }
  }

  /**
   *  The datastructure for initializing a job
   */
  static class RunningJob{
    private JobID jobid;
    private JobConf jobConf;
    private volatile InterTrackerProtocol jobClient;
    private Writable extensible;

    // keep this for later use
    volatile Set<TaskInProgress> tasks;
    volatile boolean localized;
    final Object localizationLock;
    boolean keepJobFiles;
    FetchStatus f;
    RunningJob(JobID jobid, InterTrackerProtocol jobClient,
        Writable extensible) {
      this.jobid = jobid;
      this.jobClient = jobClient;
      this.extensible = extensible;
      localized = false;
      localizationLock = new Object();
      tasks = new HashSet<TaskInProgress>();
      keepJobFiles = false;
    }

    JobID getJobID() {
      return jobid;
    }

    void setFetchStatus(FetchStatus f) {
      this.f = f;
    }

    FetchStatus getFetchStatus() {
      return f;
    }

    InterTrackerProtocol getJobClient() {
      return jobClient;
    }

    void setJobClient(InterTrackerProtocol jobClient) {
      this.jobClient = jobClient;
    }

    Writable getExtensible() {
      return extensible;
    }
  }

  /**
   * Get the name for this task tracker.
   * @return the string like "tracker_mymachine:50010"
   */
  public String getName() {
    return taskTrackerName;
  }

  private synchronized List<TaskStatus> cloneAndResetRunningTaskStatuses(
      Collection<TaskInProgress> tips, boolean sendCounters) {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip : tips) {
      TaskStatus status = tip.getStatus();
      status.setIncludeCounters(sendCounters);
      status.setOutputSize(tryToGetOutputSize(status.getTaskID(), fConf));
      // send counters for finished or failed tasks and commit pending tasks
      if (status.getRunState() != TaskStatus.State.RUNNING) {
        status.setIncludeCounters(true);
      }
      result.add((TaskStatus)status.clone());
      status.clearStatus();
    }
    return result;
  }
  /**
   * Get CGroupMemStat
   * @return a copy of CGroupMemStat
   */
  CGroupMemoryWatcher.CGroupMemStat getCGroupMemStat() {
    return this.cgroupMemoryWatcher.getCGroupMemStat();
  }
  /**
   * Check if a task is killed by CGroup
   */
  boolean isKilledByCGroup(TaskAttemptID tid) {
    return this.cgroupMemoryWatcher.isKilledByCGroup(tid);
  }
  /**
   * Get the list of tasks that will be reported back to the
   * job tracker in the next heartbeat cycle.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getRunningTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip: runningTasks.values()) {
      result.add(tip.getStatus());
    }
    return result;
  }

  /**
   * Get the list of stored tasks on this task tracker.
   * @return
   */
  public synchronized List<TaskStatus> getNonRunningTasks() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for(Map.Entry<TaskAttemptID, TaskInProgress> task: tasks.entrySet()) {
      if (!runningTasks.containsKey(task.getKey())) {
        result.add(task.getValue().getStatus());
      }
    }
    return result;
  }


  /**
   * Get the list of tasks from running jobs on this task tracker.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getTasksFromRunningJobs() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
      RunningJob rjob = item.getValue();
      synchronized (rjob) {
        for (TaskInProgress tip : rjob.tasks) {
          result.add(tip.getStatus());
        }
      }
    }
    return result;
  }

  /**
   * Get the default job conf for this tracker.
   */
  JobConf getJobConf() {
    return fConf;
  }

  /**
   * Check if the given local directories
   * (and parent directories, if necessary) can be created.
   * @param localDirs where the new TaskTracker should keep its local files.
   * @throws DiskErrorException if all local directories are not writable
   */
  private static void checkLocalDirs(String[] localDirs)
    throws DiskErrorException {
    boolean writable = false;

    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          DiskChecker.checkDir(new File(localDirs[i]));
          writable = true;
        } catch(DiskErrorException e) {
          LOG.warn("Task Tracker local " + e.getMessage());
        }
      }
    }

    if (!writable)
      throw new DiskErrorException(
                                   "all local directories are not writable");
  }

  /**
   * Is this task tracker idle?
   * @return has this task tracker finished and cleaned up all of its tasks?
   */
  public synchronized boolean isIdle() {
    return tasks.isEmpty() && tasksToCleanup.isEmpty();
  }

  /**
   * Start the TaskTracker, point toward the indicated JobTracker
   */
  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(TaskTracker.class, argv, LOG);
    try {
      if (argv.length == 0) {
        JobConf conf = new JobConf();
        ReflectionUtils.setContentionTracing
        (conf.getBoolean("tasktracker.contention.tracking", false));
        new TaskTracker(conf).run();
        return;
      }
      if ("-instance".equals(argv[0]) && argv.length == 2) {
        int instance = Integer.parseInt(argv[1]);
        if (instance == 0 || instance == 1) {
          JobConf conf = new JobConf();
          JobConf.overrideConfiguration(conf, instance);
          // enable the server to track time spent waiting on locks
          ReflectionUtils.setContentionTracing
          (conf.getBoolean("tasktracker.contention.tracking", false));
          new TaskTracker(conf).run();
          return;
        }
      }
      System.out.println("usage: TaskTracker [-instance <0|1>]");
      System.exit(-1);
    } catch (Throwable e) {
      LOG.error("Can not start task tracker because "+
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * This class is used in TaskTracker's Jetty to serve the map outputs
   * to other nodes.
   */
  public static class MapOutputServlet extends HttpServlet {
    private static final int MAX_BYTES_TO_READ = 64 * 1024;
    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      String mapId = request.getParameter("map");
      String reduceId = request.getParameter("reduce");
      String jobId = request.getParameter("job");

      if (jobId == null) {
        throw new IOException("job parameter is required");
      }

      if (mapId == null || reduceId == null) {
        throw new IOException("map and reduce parameters are required");
      }
      ServletContext context = getServletContext();
      TaskTracker tracker =
        (TaskTracker) context.getAttribute("task.tracker");
      // When using netty, this servlet should use an HTTP redirect to get the
      // sender to use the correct address and port for the netty server
      if (tracker.nettyMapOutputHttpPort != -1) {
        String redirectUrl = "http://" + request.getServerName() + ":" +
          tracker.nettyMapOutputHttpPort + "/mapOutput?" +
          request.getQueryString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Redirecting to " + redirectUrl + " from " +
                    request.getRequestURL() + "?" + request.getQueryString());
        }
        response.sendRedirect(redirectUrl);
        return;
      }

      int reduce = Integer.parseInt(reduceId);
      byte[] buffer = new byte[MAX_BYTES_TO_READ];
      // true iff IOException was caused by attempt to access input
      boolean isInputException = true;
      OutputStream outStream = null;
      FSDataInputStream mapOutputIn = null;

      long totalRead = 0;
      ShuffleServerMetrics shuffleMetrics =
        (ShuffleServerMetrics) context.getAttribute("shuffleServerMetrics");
      shuffleMetrics.setHttpQueueLen(tracker.getJettyQueueSize());
      long startTime = 0;
      try {
        shuffleMetrics.serverHandlerBusy();
        if(ClientTraceLog.isInfoEnabled())
          startTime = System.nanoTime();
        outStream = response.getOutputStream();
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc =
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem rfs = ((LocalFileSystem)
            context.getAttribute("local.file.system")).getRaw();

        // Index file
        Path indexFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getIntermediateOutputDir(jobId, mapId)
            + "/file.out.index", conf);

        // Map-output file
        Path mapOutputFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getIntermediateOutputDir(jobId, mapId)
            + "/file.out", conf);

        /**
         * Read the index file to get the information about where
         * the map-output for the given reducer is available.
         */
        IndexRecord info =
          tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName);

        //set the custom "from-map-task" http header to the map task from which
        //the map output data is being transferred
        response.setHeader(FROM_MAP_TASK, mapId);

        //set the custom "Raw-Map-Output-Length" http header to
        //the raw (decompressed) length
        response.setHeader(RAW_MAP_OUTPUT_LENGTH,
            Long.toString(info.rawLength));

        //set the custom "Map-Output-Length" http header to
        //the actual number of bytes being transferred
        response.setHeader(MAP_OUTPUT_LENGTH,
            Long.toString(info.partLength));

        //set the custom "for-reduce-task" http header to the reduce task number
        //for which this map output is being transferred
        response.setHeader(FOR_REDUCE_TASK, Integer.toString(reduce));

        //use the same buffersize as used for reading the data from disk
        response.setBufferSize(MAX_BYTES_TO_READ);

        /**
         * Read the data from the sigle map-output file and
         * send it to the reducer.
         */
        //open the map-output file
        mapOutputIn = rfs.open(mapOutputFileName);

        //seek to the correct offset for the reduce
        mapOutputIn.seek(info.startOffset);
        long rem = info.partLength;
        int len =
          mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        while (rem > 0 && len >= 0) {
          rem -= len;
          try {
            shuffleMetrics.outputBytes(len);
            outStream.write(buffer, 0, len);
            outStream.flush();
          } catch (IOException ie) {
            isInputException = false;
            throw ie;
          }
          totalRead += len;
          len =
            mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        }

        LOG.info("Sent out " + totalRead + " bytes for reduce: " + reduce +
                 " from map: " + mapId + " given " + info.partLength + "/" +
                 info.rawLength);

      } catch (org.mortbay.jetty.EofException eof) {

        // The Jetty EOFException is observed when the Reduce Task prematurely
        // closes a connection to a jetty server. The RT might decide to do
        // this when the expected map output size is less than its memory cache
        // limit, but cannot fetch it now because it has already fetched
        // several other map outputs to memory. In short, this is OK.
        // See MAPREDUCE-5 for details.
        LOG.info("EofException for map:" + mapId + " reduce:" + reduceId);
        shuffleMetrics.failedOutput();
        throw eof;

      } catch (IOException ie) {
        Log log = (Log) context.getAttribute("log");
        String errorMsg = ("getMapOutput(" + mapId + "," + reduceId +
                           ") failed");
        log.error(errorMsg, ie);
        if (isInputException) {
          tracker.mapOutputLost(TaskAttemptID.forName(mapId), errorMsg);
        }
        response.sendError(HttpServletResponse.SC_GONE, errorMsg);
        shuffleMetrics.failedOutput();
        throw ie;
      } finally {
        if (null != mapOutputIn) {
          mapOutputIn.close();
        }
        final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
        shuffleMetrics.serverHandlerFree();
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
                request.getLocalAddr() + ":" + request.getLocalPort(),
                request.getRemoteAddr() + ":" + request.getRemotePort(),
                totalRead, "MAPRED_SHUFFLE", mapId, endTime-startTime));
        }
      }
      outStream.close();
      shuffleMetrics.successOutput();
    }
  }

  /**
   * Pipeline for map output sending.
   */
  class HttpMapOutputPipelineFactory implements ChannelPipelineFactory {
    private final ShuffleHandler shuffleHandler;

    public HttpMapOutputPipelineFactory(
        NettyMapOutputAttributes attributes, int port) {
      this.shuffleHandler = new ShuffleHandler(attributes, port);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      return Channels.pipeline(
        new HttpRequestDecoder(),
        new HttpChunkAggregator(1 << 16),
        new HttpResponseEncoder(),
        new ChunkedWriteHandler(),
        shuffleHandler);
    }
  }

  // get the full paths of the directory in all the local disks.
  Path[] getLocalFiles(JobConf conf, String subdir) throws IOException{
    String[] localDirs = getLocalDirsFromConf(conf);
    Path[] paths = new Path[localDirs.length];
    FileSystem localFs = FileSystem.getLocal(conf);
    for (int i = 0; i < localDirs.length; i++) {
      paths[i] = new Path(localDirs[i], subdir);
      paths[i] = paths[i].makeQualified(localFs);
    }
    return paths;
  }

  // get the paths in all the local disks.
  Path[] getLocalDirs() throws IOException{
    String[] localDirs = getLocalDirsFromConf(fConf);
    Path[] paths = new Path[localDirs.length];
    FileSystem localFs = FileSystem.getLocal(fConf);
    for (int i = 0; i < localDirs.length; i++) {
      paths[i] = new Path(localDirs[i]);
      paths[i] = paths[i].makeQualified(localFs);
    }
    return paths;
  }

  FileSystem getLocalFileSystem(){
    return localFs;
  }

  int getMaxCurrentMapTasks() {
    return maxMapSlots;
  }

  int getMaxCurrentReduceTasks() {
    return maxReduceSlots;
  }

  /**
   * Metrics can use this to get the actual number of running maps
   *
   * @return Currently running maps
   */
  int getRunningMaps() {
    return mapLauncher.getNumUsedSlots();
  }

  /**
   * Metrics can use this to get the actual number of running reduces
   *
   * @return Currently running reduces
   */
  int getRunningReduces() {
    return reduceLauncher.getNumUsedSlots();
  }

  /**
   * Used for metrics only to get the actual max map tasks.
   *
   * @return Actual number of max map tasks.
   */
  int getMaxActualMapTasks() {
    return actualMaxMapSlots;
  }

  /**
   * Used for metrics only to get the actual max reduce tasks.
   *
   * @return Actual number of max reduce tasks.
   */
  int getMaxActualReduceTasks() {
    return actualMaxReduceSlots;
  }

  /**
   * Get the actual max number of tasks.  This may be different than
   * get(Max|Reduce)CurrentMapTasks() since that is the number used
   * for scheduling.  This allows the CoronaTaskTracker to return the
   * real number of resources available.
   *
   * @param conf Configuration to look for slots
   * @param numCpuOnTT Number of cpus on TaskTracker
   * @param type Type of slot
   * @return Actual number of map tasks, not what the TaskLauncher thinks.
   */
  int getMaxActualSlots(JobConf conf, int numCpuOnTT, TaskType type) {
    return getMaxSlots(conf, numCpuOnTT, type);
  }

  /**
   * Get the average time in milliseconds to refill a free map slot.  This
   * average is calculated on a rotating buffer.
   *
   * @return Average time in milliseconds to refill a free map slot.  Return -1
   *         if the value is not valid (hasn't actually refilled anything)
   */
  int getAveMapSlotRefillMsecs() {
    synchronized (mapSlotRefillMsecsQueue) {
      if (mapSlotRefillMsecsQueue.isEmpty()) {
        return -1;
      }

      int totalMapSlotRefillMsecs = 0;
      for (int refillMsecs : mapSlotRefillMsecsQueue) {
        totalMapSlotRefillMsecs += refillMsecs;
      }
      return totalMapSlotRefillMsecs / mapSlotRefillMsecsQueue.size();
    }
  }

  /**
   * Add this new refill time for the map slot refill queue.  Delete
   * the oldest value if the maximum size has been met.
   *
   * @param refillMsecs Time to refill a map slot
   */
  void addAveMapSlotRefillMsecs(int refillMsecs) {
    synchronized (mapSlotRefillMsecsQueue) {
      mapSlotRefillMsecsQueue.add(refillMsecs);
      if (mapSlotRefillMsecsQueue.size() >= maxRefillQueueSize) {
        mapSlotRefillMsecsQueue.remove();
      }
    }
  }

  /**
   * Get the average time in milliseconds to refill a free reduce slot.  This
   * average is calculated on a rotating buffer.
   *
   * @return Average time in milliseconds to refill a free reduce slot.  Return -1
   *         if the value is not valid (hasn't actually refilled anything)
   */
  int getAveReduceSlotRefillMsecs() {
    synchronized (reduceSlotRefillMsecsQueue) {
      if (reduceSlotRefillMsecsQueue.isEmpty()) {
        return -1;
      }

      int totalReduceSlotRefillMsecs = 0;
      for (int refillMsecs : reduceSlotRefillMsecsQueue) {
        totalReduceSlotRefillMsecs += refillMsecs;
      }
      return totalReduceSlotRefillMsecs / reduceSlotRefillMsecsQueue.size();
    }
  }

  /**
   * Add this new refill time for the reduce slot refill queue.  Delete
   * the oldest value if the maximum size has been met.
   *
   * @param refillMsecs Time to refill a reduce slot
   */
  void addAveReduceSlotRefillMsecs(int refillMsecs) {
    synchronized (reduceSlotRefillMsecsQueue) {
      reduceSlotRefillMsecsQueue.add(refillMsecs);
      if (reduceSlotRefillMsecsQueue.size() >= maxRefillQueueSize) {
        reduceSlotRefillMsecsQueue.remove();
      }
    }
  }

  /**
   * Is the TaskMemoryManager Enabled on this system?
   * @return true if enabled, false otherwise.
   */
  public boolean isTaskMemoryManagerEnabled() {
    return taskMemoryManagerEnabled;
  }

  public TaskMemoryManagerThread getTaskMemoryManager() {
    return taskMemoryManager;
  }

  /**
   * Normalize the negative values in configuration
   *
   * @param val
   * @return normalized val
   */
  private long normalizeMemoryConfigValue(long val) {
    if (val < 0) {
      val = JobConf.DISABLED_MEMORY_LIMIT;
    }
    return val;
  }

  /**
   * Memory-related setup
   */
  private void initializeMemoryManagement() {

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY));
    }

    totalVirtualMemoryOnTT = resourceCalculatorPlugin.getVirtualMemorySize();
    totalPhysicalMemoryOnTT = resourceCalculatorPlugin.getPhysicalMemorySize();

    mapSlotMemorySizeOnTT =
        fConf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT);
    reduceSlotSizeMemoryOnTT =
        fConf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT);
    totalMemoryAllottedForTasks =
        maxMapSlots * mapSlotMemorySizeOnTT + maxReduceSlots
            * reduceSlotSizeMemoryOnTT;
    if (totalMemoryAllottedForTasks < 0) {
      //adding check for the old keys which might be used by the administrator
      //while configuration of the memory monitoring on TT
      long memoryAllotedForSlot = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
              JobConf.DISABLED_MEMORY_LIMIT));
      long limitVmPerTask = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
              JobConf.DISABLED_MEMORY_LIMIT));
      if(memoryAllotedForSlot == JobConf.DISABLED_MEMORY_LIMIT) {
        totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
      } else {
        if(memoryAllotedForSlot > limitVmPerTask) {
          LOG.info("DefaultMaxVmPerTask is mis-configured. " +
          		"It shouldn't be greater than task limits");
          totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
        } else {
          totalMemoryAllottedForTasks = (maxMapSlots +
              maxReduceSlots) *  (memoryAllotedForSlot/(1024 * 1024));
        }
      }
    }
    if (totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT) {
      LOG.info("totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT."
          + " Thrashing might happen.");
    } else if (totalMemoryAllottedForTasks > totalVirtualMemoryOnTT) {
      LOG.info("totalMemoryAllottedForTasks > totalVirtualMemoryOnTT."
          + " Thrashing might happen.");
    }

    // start the taskMemoryManager thread only if enabled
    setTaskMemoryManagerEnabledFlag();
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager = new TaskMemoryManagerThread(this);
      taskMemoryManager.setDaemon(true);
      taskMemoryManager.start();
    }
  }

  private void setTaskMemoryManagerEnabledFlag() {
    if (!ProcfsBasedProcessTree.isAvailable()) {
      LOG.info("ProcessTree implementation is missing on this system. "
          + "TaskMemoryManager is disabled.");
      taskMemoryManagerEnabled = false;
      return;
    }

    if (fConf.get(TaskMemoryManagerThread.TT_RESERVED_PHYSICAL_MEMORY_MB) == null
        && totalMemoryAllottedForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      taskMemoryManagerEnabled = false;
      LOG.warn("TaskTracker's totalMemoryAllottedForTasks is -1 and " +
      		     "reserved physical memory is not configured. " +
               "TaskMemoryManager is disabled.");
      return;
    }

    taskMemoryManagerEnabled = true;
  }

  /**
   * Clean-up the task that TaskMemoryMangerThread requests to do so.
   * @param tid
   * @param wasFailure mark the task as failed or killed. 'failed' if true,
   *          'killed' otherwise
   * @param diagnosticMsg
   */
  synchronized void cleanUpOverMemoryTask(TaskAttemptID tid, boolean wasFailure,
      String diagnosticMsg) {
    TaskInProgress tip = runningTasks.get(tid);
    if (tip != null) {
      tip.reportDiagnosticInfo(diagnosticMsg);
      try {
        purgeTask(tip, wasFailure); // Marking it as failed/killed.
      } catch (IOException ioe) {
        LOG.warn("Couldn't purge the task of " + tid + ". Error : " + ioe);
      }
    }
  }

  /**
   * Wrapper method used by TaskTracker to check if {@link  NodeHealthCheckerService}
   * can be started
   * @param conf configuration used to check if service can be started
   * @return true if service can be started
   */
  private boolean shouldStartHealthMonitor(Configuration conf) {
    return NodeHealthCheckerService.shouldRun(conf);
  }

  /**
   * Wrapper method used to start {@link NodeHealthCheckerService} for
   * Task Tracker
   * @param conf Configuration used by the service.
   */
  private void startHealthMonitor(Configuration conf) {
    healthChecker = new NodeHealthCheckerService(conf);
    healthChecker.start();
  }

  public List<FetchStatus> reducesInShuffle() {
    if (mapEventsFetcher == null) {
      List<FetchStatus> empty = Collections.emptyList();
      return empty;
    }
    return mapEventsFetcher.reducesInShuffle();

  }

  /**
   * Obtain username from TaskId
   * @param taskId
   * @return username
   */
  public String getUserName(TaskAttemptID taskId) {
    TaskInProgress tip = tasks.get(taskId);
    if (tip != null) {
      return tip.getJobConf().getUser();
    }
    return null;
  }

  /**
   * Obtain the max number of task slots based on the configuration and CPU
   */
  protected int getMaxSlots(JobConf conf, int numCpuOnTT, TaskType type) {
    int maxSlots;
    String cpuToSlots;
    if (type == TaskType.MAP) {
      maxSlots = conf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
      cpuToSlots = conf.get("mapred.tasktracker.cpus.to.maptasks");
    } else {
      maxSlots = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2);
      cpuToSlots = conf.get("mapred.tasktracker.cpus.to.reducetasks");
    }
    if (cpuToSlots != null) {
      try {
        // Format of the configuration is
        // numCpu1:maxSlot1, numCpu2:maxSlot2, numCpu3:maxSlot3
        for (String str : cpuToSlots.split(",")) {
          String[] pair = str.split(":");
          int numCpu = Integer.parseInt(pair[0].trim());
          int max = Integer.parseInt(pair[1].trim());
          if (numCpu == numCpuOnTT) {
            maxSlots = max;
            break;
          }
        }
      } catch (Exception e) {
        LOG.warn("Error parsing number of CPU to map slots configuration", e);
      }
    }
    return maxSlots;
  }

  @Override
  public Collection<String> getReconfigurableProperties() {
    Set<String> properties = new HashSet<String>();
    properties.add(TT_FAST_FETCH);
    properties.add(TT_OUTOFBAND_HEARBEAT);
    properties.add(TT_PROFILE_ALL_TASKS);
    return properties;
  }

  @Override
  protected void reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
    if (property.equals(TT_FAST_FETCH)) {
      this.fastFetch = Boolean.valueOf(newVal);
    } else if (property.equals(TT_OUTOFBAND_HEARBEAT)) {
      this.oobHeartbeatOnTaskCompletion = Boolean.valueOf(newVal);
    } else if (property.equals(TT_PROFILE_ALL_TASKS)) {
      this.profileAllTasks = Boolean.valueOf(newVal);
    }
  }

  private long parseMemoryOption(String options) {
    for (String option : options.split("\\s+")) {
      if (option.startsWith("-Xmx")) {
        String memoryString = option.substring(4);
        // If memory string is a number, then it is in bytes.
        if (memoryString.matches(".*\\d")) {
          return Integer.valueOf(memoryString);
        } else {
          long value = Long.valueOf(memoryString.substring(0, memoryString.length() - 1));
          String unit = memoryString.substring(memoryString.length() - 1).toLowerCase();
          if (unit.equals("k")) {
            return value * 1024L;
          } else if (unit.equals("m")) {
            return value * 1048576L;
          } else if (unit.equals("g")) {
            return value * 1073741824L;
          }
        }
      }
    }
    // Couldn't find any memory option.
    return -1;
  }

  private long getMemoryLimit(JobConf jobConf, boolean isMap) throws IOException {
    long limit = isMap?
      jobConf.getInt(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY,
        TaskMemoryManagerThread.TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT) :
      jobConf.getInt(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY,
        TaskMemoryManagerThread.TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT);

    return limit * 1024 * 1024L;
  }
  
  public TaskTrackerMemoryControlGroup getTaskTrackerMemoryControlGroup() {
    return ttMemCgroup;
  }

  public int getCGroupOOM() {
    int result = 0;
    if (cgroupMemoryWatcher != null ) {
      result = cgroupMemoryWatcher.getOOMNo();
      cgroupMemoryWatcher.resetOOMNo();
    }
    return result;
  }

  String[] getLocalDirsFromConf(JobConf conf) throws IOException {
    long reloadPeriod = conf.getLong("mapred.localdir.reloadtime", 300000L);
    long currentTimeStamp = System.currentTimeMillis(); 
    if (localDirsList != null && 
      (currentTimeStamp - lastLocalDirsReloadTimeStamp) < reloadPeriod) {
      return localDirsList;
    }

    lastLocalDirsReloadTimeStamp = currentTimeStamp;
    String[] tmpLocalDirsList = null;
    String configFilePath = conf.get("mapred.localdir.confpath");
    if (configFilePath != null) {
      try {
        DataDirFileReader reader = new DataDirFileReader(configFilePath);
        String tempLocalDirs = reader.getNewDirectories();
        tmpLocalDirsList = reader.getArrayOfCurrentDataDirectories();
        if (tempLocalDirs == null) {
          LOG.warn("File is empty, using mapred.local.dir directories");
        }
      } catch (IOException e) {
        LOG.warn("Could not read file, using directories from mapred.local.dir");
      }
    }
    if (tmpLocalDirsList == null) {
      tmpLocalDirsList = conf.getLocalDirs();
    }
    localDirsList = tmpLocalDirsList;
    for (String localDir: localDirsList) {
      LOG.info("localDir: " + localDir);
    }
    return localDirsList;
  }
  
  public int getAndResetNumFailedToAddTaskToCGroup() {
    if (ttMemCgroup != null) {
      return ttMemCgroup.getAndResetNumFailedToAddTask();
    }
    
    return 0;
  }
  
  public int getAndResetAliveTaskNumInCGroup() {
    return cgroupMemoryWatcher.getAndResetAliveTaskNum();
  }
  
  public long getTaskTrackerRSSMem() {
    if (cgroupMemoryWatcher.checkWatchable() == false && taskMemoryManager == null) {
      setTaskTrackerRSSMem(resourceCalculatorPlugin.getProcResourceValues().getPhysicalMemorySize());
    }
    return this.taskTrackerRSSMem.get();
  }
  
  public void setTaskTrackerRSSMem(long val) {
    this.taskTrackerRSSMem.set(val);
  }
  
  public long getAndResetAliveTasksCPUMSecs() {
    return cgroupMemoryWatcher.getAndResetAliveTasksCPUMSecs();
  }
  
  public int getKilledTaskRssBucketsNum () {
    return KILLED_TASKS_RSS_BUCKETS_NUM;
  }
    
  public void addKilledTaskToRssBuckets(long rss) {
    int index;
    long gb = 1024*1024*1024;
    long qGb = 256*1024*1024;
    
    if (rss < gb) {
      index = 0;
    } else if (rss >= 5*gb) {
      index = getKilledTaskRssBucketsNum () - 1;
    } else {
      index = (int)((rss/gb - 1)*4 + 1 + (rss%gb)/qGb);
    }
    
    synchronized(this.killedTaskRssBuckets) {
      Integer counter = this.killedTaskRssBuckets.get(index);
      counter = (counter == null)? 1: (counter + 1);
      
      this.killedTaskRssBuckets.put(index, counter);
    }
    
  }
  
  public int getAndRestNumOfKilledTasksByRssBucket(int index) {    
    synchronized(this.killedTaskRssBuckets) {
      Integer counter = this.killedTaskRssBuckets.get(index);
      counter = (counter == null)? 0: counter;
      this.killedTaskRssBuckets.put(index, 0);
     
      return counter.intValue();
    }
  }

  public void doStackTrace(TaskAttemptID tid) {
    TaskInProgress tip = tasks.get(tid);
    if (tip != null) {
      jvmManager.doStackTrace(tip.getTaskRunner());
    }
  }
  
  public void doStackTrace(String pid) {
    TaskControllerContext context = new TaskControllerContext ();
    context.pid = pid;
    taskController.doStackTrace(context);
  }

  public boolean checkCGroupMemoryWatcherEnabled() {
    return cgroupMemoryWatcher.checkWatchable();
  }
  
  public void forceCleanTaskDir() {
    this.directoryCleanupThread.forceClean();
  }

}

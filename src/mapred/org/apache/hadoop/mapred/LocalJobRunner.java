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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.JobClient.RawSplit;
import org.apache.hadoop.util.StringUtils;

/** Implements MapReduce locally, in-process, for debugging. */ 
public class LocalJobRunner implements JobSubmissionProtocol {
  public static final Log LOG =
    LogFactory.getLog(LocalJobRunner.class);

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private volatile int map_tasks = 0;
  private volatile int reduce_tasks = 0;
  private volatile int mapperNo = 0;
  private volatile int reducerNo = 0;

  private JobTrackerInstrumentation myMetrics = null;
  private String runnerLogDir;

  private static final String jobDir =  "localRunner/";

  public static final String LOCALHOST = "127.0.0.1";
  public static final String LOCAL_RUNNER_SLOTS = "local.job.tracker.slots";
  public static final int DEFAULT_LOCAL_RUNNER_SLOTS = 4;

  public long getProtocolVersion(String protocol, long clientVersion) {
    return JobSubmissionProtocol.versionID;
  }

  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  private String computeLogDir() {
    GregorianCalendar gc = new GregorianCalendar();
    return String.format("local_%1$4d%2$02d%3$02d%4$02d%5$02d%5$02d",
                         gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1, gc
                         .get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY), gc
                         .get(Calendar.MINUTE), gc.get(Calendar.SECOND))
      + "_"
      + UUID.randomUUID().toString();
  }

  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    private JobID id;
    private JobConf job;

    private JobStatus status;
    private volatile int numSucceededMaps = 0;
    private ArrayList<TaskAttemptID> mapIds = new ArrayList<TaskAttemptID>();
    private MapOutputFile mapoutputFile;
    private JobProfile profile;
    private Path localFile;
    private FileSystem localFs;
    boolean killed = false;
    volatile boolean shutdown = false;
    boolean doSequential = true;
    
    // Current counters, including incomplete task(s)
    private Map<TaskAttemptID, Counters> currentCounters = new HashMap<TaskAttemptID, Counters>();

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }

    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    int numSlots;

    // Identifier for task.
    int taskCounter = 0;

    // A thread pool with as many threads as the number of slots.
    ExecutorService executor;

    private Map<Integer, JVMId> taskJvms = new HashMap<Integer, JVMId>();
    private Map<Integer, Task> runningTasks = new HashMap<Integer, Task>();

    Server umbilicalServer;
    int umbilicalPort;

    class TaskRunnable implements Runnable {
      private Task task;
      int id;

      TaskRunnable(Task task, int id) {
        this.task = task;
        this.id = id;
      }

      @Override
      public void run() {
        try {
          Vector<String> args = new Vector<String>();
          // Use same jvm as parent.
          File jvm =
            new File(new File(System.getProperty("java.home"), "bin"), "java");
          args.add(jvm.toString());
          // Add classpath.
          String classPath = System.getProperty("java.class.path", "");
          classPath += System.getProperty("path.separator") + currentClassPath();
          args.add("-classpath");
          args.add(classPath);

          long logSize = TaskLog.getTaskLogLength(conf);
          // Create a log4j directory for the job.
          String logDir = new File(
            System.getProperty("hadoop.log.dir")).getAbsolutePath() +
            Path.SEPARATOR + runnerLogDir +
            Path.SEPARATOR + Job.this.id;
          LOG.info("Logs for " + task.getTaskID() + " are at " + logDir);
          args.add("-Dhadoop.log.dir=" + logDir);
          args.add("-Dhadoop.root.logger=INFO,TLA");
          args.add("-Dhadoop.tasklog.taskid=" + task.getTaskID().toString());
          args.add("-Dhadoop.tasklog.totalLogFileSize=" + logSize);

          // For test code.
          if (System.getProperty("test.build.data") != null) {
            args.add("-Dtest.build.data=" +
                      System.getProperty("test.build.data"));
          }

          // Set java options.
          String javaOpts = conf.get(JobConf.MAPRED_TASK_JAVA_OPTS,
                                     JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS);
          javaOpts = javaOpts.replace("@taskid@", task.getTaskID().toString());
          String [] javaOptsSplit = javaOpts.split(" ");
          // Handle java.library.path.
          // Do we need current working directory also here?
          String libraryPath = System.getProperty("java.library.path");
          boolean hasUserLDPath = false;
          for(int i=0; i<javaOptsSplit.length ;i++) {
            if(javaOptsSplit[i].startsWith("-Djava.library.path=")) {
              javaOptsSplit[i] +=
                System.getProperty("path.separator") + libraryPath;
              hasUserLDPath = true;
              break;
            }
          }
          if(!hasUserLDPath && libraryPath != null) {
            args.add("-Djava.library.path=" + libraryPath);
          }
          for (int i = 0; i < javaOptsSplit.length; i++) {
            args.add(javaOptsSplit[i]);
          }

          // Add main class and its arguments.
          args.add(LocalChild.class.getName());  // main of Child
          args.add(LOCALHOST);
          args.add(Integer.toString(Job.this.umbilicalPort));
          args.add(task.getTaskID().toString());
          args.add(Integer.toString(id));

          ProcessBuilder pb = new ProcessBuilder(args);
          Process proc = pb.start();
          while (!Job.this.shutdown) {
            try {
              int status = proc.waitFor();
              if (conf.getBoolean("mapred.localrunner.debug", false) ||
                  status != 0) {
                if (status != 0) {
                  LOG.error("Child for " + task.getTaskID() + " exited with " +
                            status);
                }
                printStdOutErr(proc);
              }
              if (status != 0) {
                Job.this.statusUpdate(task.getTaskID(), failedStatus(task));
              } else {
                Job.this.statusUpdate(task.getTaskID(), succeededStatus(task));
                Job.this.numSucceededMaps++;
              }
              break;
            } catch (InterruptedException ie) {
            }
          }
        } catch (IOException e) {
          LOG.error("Launching task " + id + " error " + e);
          try {
            Job.this.statusUpdate(task.getTaskID(), failedStatus(task));
          } catch (IOException ie) {
          } catch (InterruptedException inte) {
          }
        } finally {
          if (task.isMapTask()) {
            LocalJobRunner.this.map_tasks -= 1;
            LocalJobRunner.this.myMetrics.completeMap(task.getTaskID());
          }
        }
      }

      private void printStdOutErr(Process proc) {
        LOG.warn("Process STDOUT");
        java.io.BufferedReader stdout = new java.io.BufferedReader(
          new java.io.InputStreamReader(proc.getInputStream()));
        String line;
        try {
          while ((line = stdout.readLine()) != null) {
            LOG.warn(line);
          }
        } catch (IOException e) {}
        LOG.warn("Process STDERR");
        java.io.BufferedReader stderr = new java.io.BufferedReader(
          new java.io.InputStreamReader(proc.getErrorStream()));
        try {
          while ((line = stderr.readLine()) != null) {
            LOG.warn(line);
          }
        } catch (IOException e) {}
      }

      private String currentClassPath() {
        Stack<String> paths = new Stack<String>();
        ClassLoader ccl = Thread.currentThread().getContextClassLoader();
        while (ccl != null) {
          for (URL u: ((URLClassLoader)ccl).getURLs()) {
            paths.push(u.getPath());
          }
          ccl = (URLClassLoader)ccl.getParent();
        }
        if (!paths.empty()) {
          String sep = System.getProperty("path.separator");
          StringBuffer appClassPath = new StringBuffer();
          while (!paths.empty()) {
            if (appClassPath.length() != 0) {
              appClassPath.append(sep);
            }
            appClassPath.append(paths.pop());
          }
          return appClassPath.toString();
        } else {
          return "";
        }
      }
    }

    public Job(JobID jobid, JobConf conf) throws IOException {
      this.doSequential =
        conf.getBoolean("mapred.localrunner.sequential", true);
      this.id = jobid;
      this.mapoutputFile = new MapOutputFile(jobid);
      this.mapoutputFile.setConf(conf);

      this.localFile = new JobConf(conf).getLocalPath(jobDir+id+".xml");
      this.localFs = FileSystem.getLocal(conf);
      persistConf(this.localFs, this.localFile, conf);

      this.job = new JobConf(localFile);
      profile = new JobProfile(job.getUser(), id, localFile.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING);

      jobs.put(id, this);

      numSlots = conf.getInt(LOCAL_RUNNER_SLOTS, DEFAULT_LOCAL_RUNNER_SLOTS);
      executor = Executors.newFixedThreadPool(numSlots);

      int handlerCount = numSlots;
      umbilicalServer =
        RPC.getServer(this, LOCALHOST, 0, handlerCount, false, conf);
      umbilicalServer.start();
      umbilicalPort = umbilicalServer.getListenerAddress().getPort();

      this.start();
    }

    JobProfile getProfile() {
      return profile;
    }

    private void persistConf(FileSystem fs, Path file, JobConf conf)
        throws IOException {
      new File(file.toUri().getPath()).delete();
      FSDataOutputStream out = FileSystem.create(
        fs, file, FsPermission.getDefault());
      conf.writeXml(out);
      out.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      JobID jobId = profile.getJobID();
      JobContext jContext = new JobContext(conf, jobId);
      OutputCommitter outputCommitter = job.getOutputCommitter();
      try {
        // split input into minimum number of splits
        RawSplit[] rawSplits = JobClient.getAndRemoveCachedSplits(jobId);
        LOG.info("Found " + rawSplits.length + " raw splits for job " + jobId);
        int numReduceTasks = job.getNumReduceTasks();
        if (numReduceTasks > 1 || numReduceTasks < 0) {
          // we only allow 0 or 1 reducer in local mode
          numReduceTasks = 1;
          job.setNumReduceTasks(1);
        }
        mapperNo = rawSplits.length;
        reducerNo = numReduceTasks;
        LOG.info("Mapper No: " + mapperNo);
        LOG.info("Reducer No: " + reducerNo);
        outputCommitter.setupJob(jContext);
        status.setSetupProgress(1.0f);
        
        for (int i = 0; i < rawSplits.length; i++) {
          if (!this.isInterrupted()) {
            TaskAttemptID mapId = new TaskAttemptID(new TaskID(jobId, true, i),0);  
            mapIds.add(mapId);
            Path taskJobFile = job.getLocalPath(jobDir + id + "_" + mapId + ".xml");
            MapTask map = new MapTask(taskJobFile.toString(),  
                                      mapId, i,
                                      rawSplits[i].getClassName(),
                                      rawSplits[i].getBytes(), 1, 
                                      job.getUser());
            JobConf localConf = new JobConf(job);
            map.localizeConfiguration(localConf);
            map.setConf(localConf);
            persistConf(this.localFs, taskJobFile, localConf);
            map.setJobFile(taskJobFile.toUri().getPath());
            map_tasks += 1;
            myMetrics.launchMap(mapId);
            // Special handling for the single mapper case.
            if (this.doSequential || rawSplits.length == 1) {
              map.run(localConf, this);
	      this.statusUpdate(map.getTaskID(), succeededStatus(map));
              numSucceededMaps++;
              myMetrics.completeMap(mapId);
              map_tasks -= 1;
            } else {
              runTask(map);
            }
          } else {
            throw new InterruptedException();
          }
        }

        // Wait for all maps to be done.
        executor.shutdown();
        try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while waiting mappers to finish");
          throw ie;
        }

        if (numSucceededMaps < rawSplits.length) {
          throw new IOException((rawSplits.length - numSucceededMaps) +
                                " maps failed");
        }

        TaskAttemptID reduceId = 
          new TaskAttemptID(new TaskID(jobId, false, 0), 0);
        try {
          if (numReduceTasks > 0) {
            // move map output to reduce input  
            for (int i = 0; i < mapIds.size(); i++) {
              if (!this.isInterrupted()) {
                TaskAttemptID mapId = mapIds.get(i);
                Path mapOut = this.mapoutputFile.getOutputFile(mapId);
                Path reduceIn = this.mapoutputFile.getInputFileForWrite(
                                  mapId.getTaskID(),reduceId,
                                  localFs.getLength(mapOut));
                if (!localFs.mkdirs(reduceIn.getParent())) {
                  throw new IOException("Mkdirs failed to create "
                      + reduceIn.getParent().toString());
                }
                if (!localFs.rename(mapOut, reduceIn))
                  throw new IOException("Couldn't rename " + mapOut);
              } else {
                throw new InterruptedException();
              }
            }
            if (!this.isInterrupted()) {
              ReduceTask reduce = new ReduceTask(localFile.toString(), 
                                                 reduceId, 0, mapIds.size(), 
                                                 1, job.getUser());
              JobConf localConf = new JobConf(job);
              reduce.localizeConfiguration(localConf);
              reduce.setConf(localConf);
              persistConf(this.localFs, this.localFile, localConf);
              reduce.setJobFile(localFile.toUri().getPath());
              reduce_tasks += 1;
              myMetrics.launchReduce(reduce.getTaskID());
              reduce.run(localConf, this);
              myMetrics.completeReduce(reduce.getTaskID());
              reduce_tasks -= 1;
              updateCounters(reduce.getTaskID(), reduce.getCounters());
            } else {
              throw new InterruptedException();
            }
          }
        } finally {
          for (TaskAttemptID mapId: mapIds) {
            this.mapoutputFile.removeAll(mapId);
          }
          if (numReduceTasks == 1) {
            this.mapoutputFile.removeAll(reduceId);
          }
        }
        // delete the temporary directory in output directory
        outputCommitter.commitJob(jContext);
        status.setCleanupProgress(1.0f);

        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.SUCCEEDED);
        }

        JobEndNotifier.localRunnerNotification(job, status);

      } catch (Throwable t) {
        try {
          outputCommitter.abortJob(jContext, JobStatus.FAILED);
        } catch (IOException ioe) {
          LOG.info("Error cleaning up job:" + id);
        }
        status.setCleanupProgress(1.0f);
        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.FAILED);
        }
        LOG.warn(id, t);

        JobEndNotifier.localRunnerNotification(job, status);

      } finally {
        this.shutdown = true;
        executor.shutdownNow();
        umbilicalServer.stop();
        try {
          localFs.delete(localFile, true);              // delete local copy
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    /**
     * Run the given task asynchronously.
     */
    void runTask(Task task) {
      JobID jobId = task.getJobID();
      boolean isMap = task.isMapTask();
      JVMId jvmId = new JVMId(jobId, isMap, taskCounter++);
      synchronized(this) {
        taskJvms.put(jvmId.getId(), jvmId);
        runningTasks.put(jvmId.getId(), task);
      }
      TaskRunnable taskRunnable = new TaskRunnable(task, jvmId.getId());
      executor.execute(taskRunnable);
    }

    // TaskUmbilicalProtocol methods

    public JvmTask getTask(JvmContext context) {
      int id = context.jvmId.getId();
      synchronized(this) {
        Task task = runningTasks.get(id);
        if (task != null) {
          return new JvmTask(task, false);
        } else {
          return new JvmTask(null, true);
        }
      }
    }

    private final Vector<TaskCompletionEvent> taskCompletionEvents = new Vector<TaskCompletionEvent>();
    private final AtomicInteger taskCompletionEventTracker = new AtomicInteger(0);

    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      LOG.info(taskStatus.getStateString());
      float taskIndex = mapIds.indexOf(taskId);
      boolean isMap = taskIndex >= 0;
      if (isMap) {
        float numTasks = mapIds.size();
        status.setMapProgress(taskIndex/numTasks + taskStatus.getProgress()/numTasks);
      } else {
        status.setReduceProgress(taskStatus.getProgress());
      }
      Counters taskCounters = taskStatus.getCounters();
      if (taskCounters != null) {
        updateCounters(taskId, taskCounters);
      }

      // Match TaskStatus.State to TaskCompletionEvent.Status
      //
      // TaskStatus.State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,
      //                   COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN}
      // TaskCompletionEvent.Status {FAILED, KILLED, SUCCEEDED, OBSOLETE,
      //                             TIPFAILED, SUCCEEDED_NO_OUTPUT}
      TaskCompletionEvent.Status taskCompletionEventStatus = null;
      boolean writeCompletionEvent = false;
      switch (taskStatus.getRunState()) {
        case SUCCEEDED:
          taskCompletionEventStatus = TaskCompletionEvent.Status.SUCCEEDED;
          writeCompletionEvent = true;
          break;
        case KILLED:
          taskCompletionEventStatus = TaskCompletionEvent.Status.KILLED;
          writeCompletionEvent = true;
          break;
        case FAILED:
          taskCompletionEventStatus = TaskCompletionEvent.Status.FAILED;
          writeCompletionEvent = true;
          break;
      }
      if(writeCompletionEvent) {
        int idWithinJob;
        if (isMap) {
          idWithinJob = (int) taskIndex;   // index of map task
        } else {
          idWithinJob = 0;                // We have only 1 reduce task.
        }
        TaskCompletionEvent taskCompletionEvent = new TaskCompletionEvent(
            taskCompletionEventTracker.getAndIncrement(),
            taskId,
            idWithinJob,
            isMap,
            taskCompletionEventStatus,
            null); // We don't need taskTrackerHttp as it's running on localhost.
        taskCompletionEvents.add(taskCompletionEvent);
      }

      return true;
    }

    /**
     * Task is reporting that it is in commit_pending
     * and it is waiting for the commit Response
     */
    public void commitPending(TaskAttemptID taskid,
                              TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskid, taskStatus);
    }

    /**
     * Updates counters corresponding to tasks.
     */ 
    private void updateCounters(TaskAttemptID taskId, Counters ctrs) {
      synchronized(currentCounters) {
        currentCounters.put(taskId, ctrs);
      }
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) {
      LOG.error("Task diagnostic info for " + taskid + " : " + trace);
    }
    
    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }

    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    public boolean canCommit(TaskAttemptID taskid) 
    throws IOException {
      return true;
    }
    
    public void done(TaskAttemptID taskId) throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        status.setMapProgress(1.0f);
      } else {
        status.setReduceProgress(1.0f);
      }
    }

    public synchronized void fsError(TaskAttemptID taskId, String message) 
    throws IOException {
      LOG.fatal("FSError: "+ message + "from task: " + taskId);
    }

    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.fatal("shuffleError: "+ message + "from task: " + taskId);
    }
    
    public synchronized void fatalError(TaskAttemptID taskId, String msg) 
    throws IOException {
      LOG.fatal("Fatal: "+ msg + "from task: " + taskId);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY,
                                               false);
    }

    public TaskCompletionEvent[] getTaskCompletionEvents(
        int fromEventId, int maxEvents) {
      TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
      synchronized (taskCompletionEvents) {
        if (taskCompletionEvents.size() > fromEventId) {
          int actualMax =
              Math.min(maxEvents, (taskCompletionEvents.size() - fromEventId));
          events = taskCompletionEvents.subList(fromEventId, actualMax + fromEventId).toArray(events);
        }
      }
      return events;
    }
  }

  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.getLocal(conf);
    this.conf = conf;
    runnerLogDir = computeLogDir();
    myMetrics = new JobTrackerMetricsInst(null, new JobConf(conf));
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  public synchronized JobID getNewJobId() {
    return new JobID("local", ++jobid);
  }

  public JobStatus submitJob(JobID jobid) throws IOException {
    return new Job(jobid, this.conf).status;
  }

  public void killJob(JobID id) {
    jobs.get(id).killed = true;
    jobs.get(id).interrupt();
  }

  public void setJobPriority(JobID id, String jp) throws IOException {
    throw new UnsupportedOperationException("Changing job priority " +
                      "in LocalJobRunner is not supported.");
  }
  
  /** Throws {@link UnsupportedOperationException} */
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
    throw new UnsupportedOperationException("Killing tasks in " +
    "LocalJobRunner is not supported");
  }

  public JobProfile getJobProfile(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.getProfile();
    else 
      return null;
  }

  public TaskReport[] getMapTaskReports(JobID id) {
    return (this.mapperNo > 0)? new TaskReport[this.mapperNo] : new TaskReport[0];
  }
  public TaskReport[] getReduceTaskReports(JobID id) {
    return (this.reducerNo > 0)? new TaskReport[this.reducerNo] : new TaskReport[0];
  }
  public TaskReport[] getCleanupTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getSetupTaskReports(JobID id) {
    return new TaskReport[0];
  }

  public JobStatus getJobStatus(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.status;
    else 
      return null;
  }
  
  public Counters getJobCounters(JobID id) {
    Job job = jobs.get(id);
    Counters total = new Counters();
    synchronized(job.currentCounters) {
      for (Counters ctrs: job.currentCounters.values()) {
        synchronized(ctrs) {
          total.incrAllCounters(ctrs);
        }
      }
    }
    return total;
  }

  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }
  
  public ClusterStatus getClusterStatus(boolean detailed) {
    return new ClusterStatus(1, 0, 0, map_tasks, reduce_tasks, 1, 1, 
                             JobTracker.State.RUNNING);
  }

  public JobStatus[] jobsToComplete() {return null;}

  public TaskCompletionEvent[] getTaskCompletionEvents(
      JobID jobid, int fromEventId, int maxEvents) throws IOException{
    Job job = jobs.get(jobid);
    if (job == null) return TaskCompletionEvent.EMPTY_ARRAY;

    return job.getTaskCompletionEvents(fromEventId, maxEvents);
  }
  
  public JobStatus[] getAllJobs() {return null;}

  
  /**
   * Returns the diagnostic information for a particular task in the given job.
   * To be implemented
   */
  public String[] getTaskDiagnostics(TaskAttemptID taskid)
      throws IOException{
    return new String [0];
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    return null;
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return null;
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return null;
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return null;
  }

  public static class LocalChild {
    public static void main(String[] args) throws Throwable {
      JobConf defaultConf = new JobConf();
      String host = args[0];
      int port = Integer.parseInt(args[1]);
      InetSocketAddress address = new InetSocketAddress(host, port);
      final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
      final int SLEEP_LONGER_COUNT = 5;
      int jvmIdInt = Integer.parseInt(args[3]);
      JVMId jvmId = new JVMId(firstTaskid.getJobID(),firstTaskid.isMap(),jvmIdInt);
      TaskUmbilicalProtocol umbilical =
        (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
            TaskUmbilicalProtocol.versionID,
            address,
            defaultConf);

      String pid = "NONE";
      JvmContext context = new JvmContext(jvmId, pid);
      Task task = null;
      try {
        JvmTask myTask = umbilical.getTask(context);
        task = myTask.getTask();
        if (myTask.shouldDie() || task == null) {
          LOG.error("Returning from local child");
          System.exit(1);
        }
        JobConf job = new JobConf(task.getJobFile());

        File userLogsDir = TaskLog.getBaseDir(task.getTaskID().toString());
        userLogsDir.mkdirs();
        System.setOut(new PrintStream(new FileOutputStream(
          new File(userLogsDir, "stdout"))));
        System.setErr(new PrintStream(new FileOutputStream(
          new File(userLogsDir, "stderr"))));

        task.setConf(job);

        task.run(job, umbilical); // run the task
      } catch (Exception exception) {
        LOG.error("Got exception " + StringUtils.stringifyException(exception));
        try {
          if (task != null) {
            umbilical.statusUpdate(task.getTaskID(), failedStatus(task));
            // do cleanup for the task
            task.taskCleanup(umbilical);
          }
        } catch (Exception e) {
        }
        System.exit(2);
      } catch (Throwable throwable) {
        LOG.error("Got throwable " + throwable);
        if (task != null) {
          Throwable tCause = throwable.getCause();
          String cause = tCause == null 
                         ? throwable.getMessage() 
                         : StringUtils.stringifyException(tCause);
          umbilical.fatalError(task.getTaskID(), cause);
        }
        System.exit(3);
      } finally {
        RPC.stopProxy(umbilical);
      }
    }
  }

  static TaskStatus succeededStatus(Task task) {
    TaskStatus taskStatus = (TaskStatus) task.taskStatus.clone();
    taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
    return taskStatus;
  }

  static TaskStatus failedStatus(Task task) {
    TaskStatus taskStatus = (TaskStatus) task.taskStatus.clone();
    taskStatus.setRunState(TaskStatus.State.FAILED);
    return taskStatus;
  }
}

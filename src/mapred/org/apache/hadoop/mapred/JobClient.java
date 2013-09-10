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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.ipc.RemoteException;

/**
 * <code>JobClient</code> is the primary interface for the user-job to interact
 * with the {@link JobTracker}.
 *
 * <code>JobClient</code> provides facilities to submit jobs, track their
 * progress, access component-tasks' reports/logs, get the Map-Reduce cluster
 * status information etc.
 *
 * <p>The job submission process involves:
 * <ol>
 *   <li>
 *   Checking the input and output specifications of the job.
 *   </li>
 *   <li>
 *   Computing the {@link InputSplit}s for the job.
 *   </li>
 *   <li>
 *   Setup the requisite accounting information for the {@link DistributedCache}
 *   of the job, if necessary.
 *   </li>
 *   <li>
 *   Copying the job's jar and configuration to the map-reduce system directory
 *   on the distributed file-system.
 *   </li>
 *   <li>
 *   Submitting the job to the <code>JobTracker</code> and optionally monitoring
 *   it's status.
 *   </li>
 * </ol></p>
 *
 * Normally the user creates the application, describes various facets of the
 * job via {@link JobConf} and then uses the <code>JobClient</code> to submit
 * the job and monitor its progress.
 *
 * <p>Here is an example on how to use <code>JobClient</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *
 *     // Specify various job-specific parameters
 *     job.setJobName("myjob");
 *
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     JobClient.runJob(job);
 * </pre></blockquote></p>
 *
 * <h4 id="JobControl">Job Control</h4>
 *
 * <p>At times clients would chain map-reduce jobs to accomplish complex tasks
 * which cannot be done via a single map-reduce job. This is fairly easy since
 * the output of the job, typically, goes to distributed file-system and that
 * can be used as the input for the next job.</p>
 *
 * <p>However, this also means that the onus on ensuring jobs are complete
 * (success/failure) lies squarely on the clients. In such situations the
 * various job-control options are:
 * <ol>
 *   <li>
 *   {@link #runJob(JobConf)} : submits the job and returns only after
 *   the job has completed.
 *   </li>
 *   <li>
 *   {@link #submitJob(JobConf)} : only submits the job, then poll the
 *   returned handle to the {@link RunningJob} to query status and make
 *   scheduling decisions.
 *   </li>
 *   <li>
 *   {@link JobConf#setJobEndNotificationURI(String)} : setup a notification
 *   on job-completion, thus avoiding polling.
 *   </li>
 * </ol></p>
 *
 * @see JobConf
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
public class JobClient extends Configured implements MRConstants, Tool  {
  private static final Log LOG = LogFactory.getLog(JobClient.class);
  public static enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED;
  private static final long MAX_JOBPROFILE_AGE = 1000 * 2;

  private static Random r = new Random();

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  private static Map<JobID, JobConf> jobConfCache =
    new HashMap<JobID, JobConf>();
  private static Map<JobID, List<RawSplit>> jobSplitCache =
    new HashMap<JobID, List<RawSplit>>();

  /** Indexes distributed cache files */
  private DistributedCacheIndex filesInCache;

  /**
   * A NetworkedJob is an implementation of RunningJob.  It holds
   * a JobProfile object to provide some info, and interacts with the
   * remote service to provide certain functionality.
   */
  class NetworkedJob implements RunningJob {
    JobProfile profile;
    JobStatus status;
    long statustime;

    // See comment for getJobStatus()
    private long MAX_STALE_STATUS_INTERVAL = 10 * 1000;
    
    /**
     * We store a JobProfile and a timestamp for when we last
     * acquired the job profile.  If the job is null, then we cannot
     * perform any of the tasks.  The job might be null if the JobTracker
     * has completely forgotten about the job.  (eg, 24 hours after the
     * job completes.)
     */
    public NetworkedJob(JobStatus job) throws IOException {
      this.status = job;
      this.profile = jobSubmitClient.getJobProfile(job.getJobID());
      this.statustime = System.currentTimeMillis();
    }

    /**
     * Some methods rely on having a recent job profile object.  Refresh
     * it, if necessary
     */
    synchronized void ensureFreshStatus() throws IOException {
      if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
        updateStatus();
      }
    }

    /** Some methods need to update status immediately. So, refresh
     * immediately
     * @throws IOException
     */
    synchronized void updateStatus() throws IOException {
      JobStatus status = jobSubmitClient.getJobStatus(profile.getJobID());
      long now = System.currentTimeMillis();
      // There is a bug where the JT occasionally returns a null status. The 
      // null status will cause clients (inc. Hive) to fail. The null status 
      // may be a transient error, so we are logging when this occurs and 
      // instead keeping the last status value. This change should be reverted 
      // following Task #917234
      
      if (status == null) {
        LOG.error("JT returned null status for job " + profile.getJobID()); 
      }

      // In case the JT continues to return the null status, that's probably the
      // right value
      if (status != null || 
          (now > this.statustime + MAX_STALE_STATUS_INTERVAL)) {
        if (status == null) {
          // If we could not get the status, assume failed.
          LOG.error("Creating failed status because getJobStatus() " + 
              "returned null");
          status = new JobStatus(profile.getJobID(), 0.0f, 0.0f, 
              0.0f, JobStatus.FAILED);
        }
        this.status = status;
        this.statustime = now;
      }
    }

    /**
     * An identifier for the job
     */
    public JobID getID() {
      return profile.getJobID();
    }

    /** @deprecated This method is deprecated and will be removed. Applications should
     * rather use {@link #getID()}.*/
    @Deprecated
    public String getJobID() {
      return profile.getJobID().toString();
    }

    /**
     * The user-specified job name
     */
    public String getJobName() {
      return profile.getJobName();
    }

    /**
     * The name of the job file
     */
    public String getJobFile() {
      return profile.getJobFile();
    }

    /**
     * A URL where the job's status can be seen
     */
    public String getTrackingURL() {
      return profile.getURL().toString();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of map work
     * completed.
     */
    public float mapProgress() throws IOException {
      ensureFreshStatus();
      return status.mapProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of reduce work
     * completed.
     */
    public float reduceProgress() throws IOException {
      ensureFreshStatus();
      return status.reduceProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of cleanup work
     * completed.
     */
    public float cleanupProgress() throws IOException {
      ensureFreshStatus();
      return status.cleanupProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of setup work
     * completed.
     */
    public float setupProgress() throws IOException {
      ensureFreshStatus();
      return status.setupProgress();
    }

    /**
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      updateStatus();
      return (status.getRunState() == JobStatus.SUCCEEDED ||
              status.getRunState() == JobStatus.FAILED ||
              status.getRunState() == JobStatus.KILLED);
    }

    /**
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      updateStatus();
      return status.getRunState() == JobStatus.SUCCEEDED;
    }

    /**
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      while (!isComplete()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }

    /**
     * Tells the service to get the state of the current job.
     */
    public synchronized int getJobState() throws IOException {
      updateStatus();
      return status.getRunState();
    }

    /**
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      jobSubmitClient.killJob(getID());
    }


    /** Set the priority of the job.
    * @param priority new priority of the job.
    */
    public synchronized void setJobPriority(String priority)
                                                throws IOException {
      jobSubmitClient.setJobPriority(getID(), priority);
    }

    /**
     * Kill indicated task attempt.
     * @param taskId the id of the task to kill.
     * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
     * it is just killed, w/o affecting job failure status.
     */
    public synchronized void killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
      jobSubmitClient.killTask(taskId, shouldFail);
    }

    /** @deprecated Applications should rather use {@link #killTask(TaskAttemptID, boolean)}*/
    @Deprecated
    public synchronized void killTask(String taskId, boolean shouldFail) throws IOException {
      killTask(TaskAttemptID.forName(taskId), shouldFail);
    }

    /**
     * Fetch task completion events from jobtracker for this job.
     */
    public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
                                                                      int startFrom) throws IOException{
      return jobSubmitClient.getTaskCompletionEvents(
                                                     getID(), startFrom, 10);
    }

    /**
     * Dump stats to screen
     */
    @Override
    public String toString() {
      try {
        updateStatus();
      } catch (IOException e) {
      }
      return "Job: " + profile.getJobID() + "\n" +
        "file: " + profile.getJobFile() + "\n" +
        "tracking URL: " + profile.getURL() + "\n" +
        "map() completion: " + status.mapProgress() + "\n" +
        "reduce() completion: " + status.reduceProgress();
    }

    /**
     * Returns the counters for this job
     */
    public Counters getCounters() throws IOException {
      return jobSubmitClient.getJobCounters(getID());
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID id) throws IOException {
      return jobSubmitClient.getTaskDiagnostics(id);
    }
  }

  JobSubmissionProtocol jobSubmitClient;
  private boolean jobSubmitClientIsProxy = false;
  private boolean isJobTrackerInProc = false;
  private Path sysDir = null;

  private FileSystem fs = null;

  /**
   * Create a job client.
   */
  public JobClient() {
  }

  /**
   * Build a job client with the given {@link JobConf}, and connect to the
   * default {@link JobTracker}.
   *
   * @param conf the job configuration.
   * @throws IOException
   */
  public JobClient(JobConf conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  /**
   * Connect to the default {@link JobTracker}.
   * The behavior based on the value of mapred.job.tracker:
   * a) "local" -> create a {@link LocalJobRunner}
   * b) valid host:port pair -> create a RPC proxy to a {@link JobTracker}
   *                            running on that machine.
   * c) anything else -> Assume an in-process job tracker will run, require
   *                     mapred.job.tracker.class to be set
   * @param conf the job configuration.
   * @throws IOException
   */
  public void init(JobConf conf) throws IOException {
    String tracker = conf.get("mapred.job.tracker", "local");
    if ("local".equals(tracker)) {
      this.jobSubmitClient = new LocalJobRunner(conf);
      isJobTrackerInProc = true;
    } else {
      if (tracker.indexOf(":") == -1) {
        // Not a host:port pair.
        // Construct a job tracker in the same process.
        try {
          Class<?> clazz = conf.getClass("mapred.job.tracker.class", null);
          if (clazz != null) {
            Constructor<?> constructor =
              clazz.getDeclaredConstructor(new Class[]{JobConf.class});
            this.jobSubmitClient =
              (JobSubmissionProtocol) constructor.newInstance(conf);
            isJobTrackerInProc = true;
          } else {
            throw new IOException(
              "In-proc job tracker class(mapred.job.tracker.class) not specified");
          }
        } catch (NoSuchMethodException e) {
          throw new IOException("cannot construct local runner", e);
        } catch (InstantiationException e) {
          throw new IOException("cannot construct local runner", e);
        } catch (IllegalAccessException e) {
          throw new IOException("cannot construct local runner", e);
        } catch (InvocationTargetException e) {
          throw new IOException("cannot construct local runner", e);
        } catch (Throwable e) {
          throw new IOException("Unknown exception", e);
        }
      } else {
        this.jobSubmitClient = createRPCProxy(JobTracker.getAddress(conf), conf);
        this.jobSubmitClientIsProxy = true;
      }
    }
  }

  private JobSubmissionProtocol createRPCProxy(InetSocketAddress addr,
      Configuration conf) throws IOException {
    return RPC.getProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, addr, getUGI(conf), conf,
        NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
  }

  /**
   * Build a job client, connect to the indicated job tracker.
   *
   * @param jobTrackAddr the job tracker to connect to.
   * @param conf configuration.
   */
  public JobClient(InetSocketAddress jobTrackAddr,
                   Configuration conf) throws IOException {
    jobSubmitClient = createRPCProxy(jobTrackAddr, conf);
  }

  /**
   * Close the <code>JobClient</code>.
   */
  public synchronized void close() throws IOException {
    if (jobSubmitClientIsProxy) {
      RPC.stopProxy(jobSubmitClient);
    }
  }

  /**
   * Get a filesystem handle.  We need this to prepare jobs
   * for submission to the MapReduce system.
   *
   * @return the filesystem handle.
   */
  public synchronized FileSystem getFs() throws IOException {
    if (this.fs == null) {
      Path sysDir = getSystemDir();
      this.fs = sysDir.getFileSystem(getConf());
    }
    return fs;
  }

  /* see if two file systems are the same or not
   *
   */
  private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch(UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    }
    else if (srcHost == null && dstHost != null) {
      return false;
    }
    else if (srcHost != null && dstHost == null) {
      return false;
    }
    //check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false;
    }
    return true;
  }

  private Path copyRemoteFiles(FileSystem jtFs, Path parentDir,
                               Path originalPath, JobConf job,
                               short replication) throws IOException {

    return copyRemoteFiles(jtFs, parentDir, originalPath, job, replication,
        null);
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  private Path copyRemoteFiles(FileSystem jtFs, Path parentDir,
                               Path originalPath, JobConf job,
                               short replication, String md5)
    throws IOException {
    //check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(job);
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }

    // This function is overloaded to support cache sharing when enabled
    if (md5 != null) {
      // Check if file already exists in cache
      Path basePath = parentDir;
      Path realPath = new Path(basePath, md5 + "_" + originalPath.getName());
      Path qualifiedRealPath = realPath.makeQualified(jtFs);
      if (filesInCache.contains(qualifiedRealPath)) {
        // We "touch" the file to update its access time
        // This is done only 10% of the time to reduce load on the namenode
        if (r.nextLong() % 10 == 0) {
          try {
            jtFs.setTimes(realPath, -1, System.currentTimeMillis());
          }
          catch (RemoteException e){
            LOG.warn("Error in setTimes", e);
          }
        }

        return qualifiedRealPath;
      }

      // This loop should not even loop most of the time
      Path newPath;
      do {
        newPath = new Path(basePath, "tmp_" + originalPath.getName() +
            r.nextLong());
      } while (jtFs.exists(newPath));

      FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, job);
      jtFs.setReplication(newPath, replication);
      jtFs.setPermission(newPath, new FsPermission(JOB_DIR_PERMISSION));

      LOG.info ("Uploading new shared jar: " + realPath.toString());

      if (!jtFs.rename(newPath, realPath)) {
        // if there are multiple clients racing to upload the new jar - only
        // one of them will succeed. Check if we failed because the file already
        // exists. if so, ignore and move on
        if (!jtFs.exists(realPath))
          throw new IOException ("Unable to upload or find shared jar: " + realPath.toString());
      }

      // Update the list
      filesInCache.add(qualifiedRealPath);

      return qualifiedRealPath;
    }

    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path

    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, job);
    jtFs.setReplication(newPath, replication);
    return jtFs.makeQualified(newPath);
  }

  private static class FileInfo {
    String md5;
    long fileLength;
    long timeStamp;

    public FileInfo(String md5, long fileLength, long timeStamp) {
      this.md5 = md5;
      this.fileLength = fileLength;
      this.timeStamp = timeStamp;
    }
  }

  Map<URI, FileInfo> fileInfo;

  /**
   * Create symlinks for the files needed for the jobs in current directory
   * @param job
   * @throws IOException
   */
  private void symLinkAndConfigureFiles(JobConf job) throws IOException {
    if (!(job.getBoolean("mapred.used.genericoptionsparser", false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. " +
               "Applications should implement Tool for the same.");
    }

    // get all the command line arguments into the
    // jobconf passed in by the user conf
    String files = job.get("tmpfiles");
    String archives = job.get("tmparchives");
    // "tmpjars" are not needed because its in the classpath

    List<String> filesToSymLink = new ArrayList<String>();
    splitAndAdd(files, filesToSymLink);
    splitAndAdd(archives, filesToSymLink);

    for (String file : filesToSymLink) {
      String target = new Path(file).toUri().getPath();
      String basename = new File(target).getName();
      String linkName = new File(".").getAbsolutePath() + File.separator + basename;
      File toLink = new File(linkName);
      if (toLink.exists()) {
        LOG.info("Symlink " + linkName + " already exists. Delete it.");
        toLink.delete();
      }
      int ret = FileUtil.symLink(target, linkName);
      LOG.info("Creating symlink " + linkName + " -> " + target +
          " returns " + ret + ".");
    }
    // Configure job name
    String originalJar = job.getJar();
    if (originalJar != null) {
      // use jar name if job is not named.
      if ("".equals(job.getJobName())) {
        job.setJobName(new Path(originalJar).getName());
      }
    }
    // Configure username
    configureUserName(job);
  }

  private void splitAndAdd(String files, List<String> filesToSymLink) {
    if (files != null) {
      String[] filenames = files.split(",");
      if (filenames != null) {
        for (String file : filenames) {
          filesToSymLink.add(file);
        }
      }
    }
  }

  /**
   * configure the jobconf of the user with the command line options of
   * -libjars, -files, -archives
   * @param conf
   * @throws IOException
   */
  private void copyAndConfigureFiles(JobConf job, Path uploadFileDir,
      boolean shared)
    throws IOException {

    if (!(job.getBoolean("mapred.used.genericoptionsparser", false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. " +
               "Applications should implement Tool for the same.");
    }

    // get all the command line arguments into the
    // jobconf passed in by the user conf
    String files = job.get("tmpfiles");
    String libjars = job.get("tmpjars");
    String archives = job.get("tmparchives");

    //
    // Figure out what fs the JobTracker is using.  Copy the
    // job to it, under a temporary name.  This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading
    // semantics.  (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    FileSystem fs = getFs();
    LOG.debug("default FileSystem: " + fs.getUri());

    // We know file system of distributed cache, initialize index
    if (filesInCache == null) {
      filesInCache = new DistributedCacheIndex(fs);
    }

    uploadFileDir = fs.makeQualified(uploadFileDir);
    uploadFileDir = new Path(uploadFileDir.toUri().getPath());
    FsPermission mapredSysPerms = new FsPermission(JOB_DIR_PERMISSION);

    if (!fs.exists(uploadFileDir)) {
      FileSystem.mkdirs(fs, uploadFileDir, mapredSysPerms);
    }
    Path filesDir = new Path(uploadFileDir, "files");
    Path archivesDir = new Path(uploadFileDir, "archives");
    Path libjarsDir = new Path(uploadFileDir, "libjars");
    short replication = (short)job.getInt("mapred.submit.replication", 10);

    fileInfo = new HashMap<URI, FileInfo>();

    String originalJar = job.getJar();

    if (originalJar != null) {
      // use jar name if job is not named.
      if ("".equals(job.getJobName())) {
        job.setJobName(new Path(originalJar).getName());
      }

      Path uploadJarPath;

      Path originalJarPath = new Path(originalJar);
      originalJarPath =
        originalJarPath.makeQualified(FileSystem.getLocal(job));

      try {
        // If sharing is turned on, we load the job jar into the distributed
        // cache

        if (shared) {
          if (!fs.exists(libjarsDir)) {
            FileSystem.mkdirs(fs, libjarsDir, mapredSysPerms);
          }

          MD5Hash md5hash = MD5Hash.digest(new
              FileInputStream(originalJarPath.toUri().getPath()));
          uploadJarPath = copyRemoteFiles(fs, libjarsDir, originalJarPath, job,
              replication, md5hash.toString());
          URI pathURI = new URI(uploadJarPath.toUri().toString());

          DistributedCache.addSharedArchiveToClassPath(uploadJarPath, job);
          fileInfo.put(pathURI, new FileInfo(md5hash.toString(),
                md5hash.getFileLength(), 0));
        } else {
          // Otherwise we copy jar to JT's filesystem
          uploadJarPath = new Path(uploadFileDir, "job.jar");
          fs.copyFromLocalFile(originalJarPath, uploadJarPath);
        }
      } catch (URISyntaxException ue) {
        // should not throw an uri exception
        throw new IOException("Failed to create uri for " + originalJar);
      }

      job.setJar(uploadJarPath.toString());
      fs.setReplication(uploadJarPath, replication);

      try {
        fs.setPermission(uploadJarPath, new FsPermission(JOB_FILE_PERMISSION));
      } catch (IOException ioe) {
        LOG.warn("Unable to set job jar permission");
      }
    } else {
      LOG.warn("No job jar file set. User classes may not be found. "+
          "See JobConf(Class) or JobConf#setJar(String).");
    }

    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem

    if (files != null) {
      if (!fs.exists(filesDir)) {
        FileSystem.mkdirs(fs, filesDir, mapredSysPerms);
      }

      String[] fileArr = files.split(",");
      for (String tmpFile: fileArr) {
        Path tmp = new Path(tmpFile);

        Path newPath;
        FileStatus fStatus = null;
        MD5Hash md5hash = null;
        try {
          if (shared) {
            md5hash
              = MD5Hash.digest(new FileInputStream(tmp.toUri().getPath()));
            newPath = copyRemoteFiles(fs, filesDir, tmp, job, replication,
                md5hash.toString());

            URI pathURI = new URI(newPath.toUri().toString() + "#" +
                newPath.getName());

            DistributedCache.addSharedCacheFile(pathURI, job);
            fileInfo.put(pathURI, new FileInfo(md5hash.toString(),
                  md5hash.getFileLength(),
                  0));

          } else {
            newPath = copyRemoteFiles(fs, filesDir, tmp, job, replication);
            fStatus = DistributedCache.getFileStatus(job, newPath.toUri());

            URI pathURI = new URI(newPath.toUri().toString() + "#" +
                newPath.getName());

            DistributedCache.addCacheFile(pathURI, job);
            fileInfo.put(pathURI, new FileInfo(null,
                  fStatus.getLen(),
                  fStatus.getModificationTime()));
          }

        } catch(URISyntaxException ue) {
          //should not throw a uri exception
          throw new IOException("Failed to create uri for " + tmpFile);
        }
        DistributedCache.createSymlink(job);
      }
    }

    if (libjars != null) {
      if (!fs.exists(libjarsDir)) {
        FileSystem.mkdirs(fs, libjarsDir, mapredSysPerms);
      }

      String[] libjarsArr = libjars.split(",");
      for (String tmpjars: libjarsArr) {
        Path tmp = new Path(tmpjars);

        Path newPath;
        if (shared) {
          MD5Hash md5hash
            = MD5Hash.digest(new FileInputStream(tmp.toUri().getPath()));
          newPath = copyRemoteFiles(fs, libjarsDir, tmp, job, replication,
              md5hash.toString());
          DistributedCache.addSharedArchiveToClassPath(newPath, job);

          fileInfo.put(newPath.makeQualified(newPath.getFileSystem(job)).toUri(),
                       new FileInfo(md5hash.toString(),
                                    md5hash.getFileLength(),
                                    0));
        } else {
          newPath = copyRemoteFiles(fs, libjarsDir, tmp, job, replication);
          DistributedCache.addArchiveToClassPath(newPath, job);

          FileStatus fStatus = DistributedCache.getFileStatus(job,
              newPath.toUri());
          fileInfo.put(newPath.makeQualified(newPath.getFileSystem(job)).toUri(),
                       new FileInfo(null,
                                    fStatus.getLen(),
                                    fStatus.getModificationTime()));
        }
      }
    }

    if (archives != null) {
      if (!fs.exists(archivesDir)) {
        FileSystem.mkdirs(fs, archivesDir, mapredSysPerms);
      }

     String[] archivesArr = archives.split(",");
     for (String tmpArchives: archivesArr) {
       Path tmp = new Path(tmpArchives);

       Path newPath;
       MD5Hash md5hash = null;
       FileStatus fStatus = null;
       try {
         if (shared) {
           md5hash
             = MD5Hash.digest(new FileInputStream(tmp.toUri().getPath()));
           newPath = copyRemoteFiles(fs, archivesDir, tmp, job, replication,
               md5hash.toString());
           URI pathURI = new URI(newPath.toUri().toString() + "#" +
               newPath.getName());

           DistributedCache.addSharedCacheArchive(pathURI, job);
           fileInfo.put(pathURI, new FileInfo(md5hash.toString(),
                 md5hash.getFileLength(),
                 0));

         } else {
           newPath = copyRemoteFiles(fs, archivesDir, tmp, job, replication);
           fStatus = DistributedCache.getFileStatus(job, newPath.toUri());

           URI pathURI = new URI(newPath.toUri().toString() + "#" +
               newPath.getName());

           DistributedCache.addCacheArchive(pathURI, job);
           fileInfo.put(pathURI, new FileInfo(null,
                 fStatus.getLen(),
                 fStatus.getModificationTime()));
         }
       } catch(URISyntaxException ue) {
         //should not throw an uri excpetion
         throw new IOException("Failed to create uri for " + tmpArchives);
       }
       DistributedCache.createSymlink(job);
     }
    }

    // set the timestamps and md5 of the archives and files
    URI[] tarchives = DistributedCache.getSharedCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveLength = new StringBuffer();
      FileStatus fStatus;
      FileInfo info;
      long fileLength;

      for (int i = 0; i < tarchives.length; i++) {
        if (i != 0) archiveLength.append(',');

        info = fileInfo.get(tarchives[i]);
        if (info == null) {
          fStatus = DistributedCache.getFileStatus(job, tarchives[i]);
          fileLength = fStatus.getLen();
        } else {
          fileLength = info.fileLength;
        }

        archiveLength.append(fileLength);
      }

      DistributedCache.setSharedArchiveLength(job, archiveLength.toString());
    }

    URI[] tfiles = DistributedCache.getSharedCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileLength = new StringBuffer();
      FileStatus fStatus;
      FileInfo info;
      long len;

      for (int i = 0; i < tfiles.length; i++) {
        if (i != 0) fileLength.append(',');

        info = fileInfo.get(tfiles[i]);
        if (info == null) {
          fStatus = DistributedCache.getFileStatus(job, tfiles[i]);
          len = fStatus.getLen();
        } else {
          len = info.fileLength;
        }

        fileLength.append(len);
      }

      DistributedCache.setSharedFileLength(job, fileLength.toString());
    }

    tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveTimestamps = new StringBuffer();
      FileInfo info;
      long timeStamp;

      for (int i = 0; i < tarchives.length; i++) {
        if (i != 0) archiveTimestamps.append(',');

        info = fileInfo.get(tarchives[i]);
        if (info == null) {
          timeStamp = DistributedCache.getTimestamp(job, tarchives[i]);
        } else {
          timeStamp = info.timeStamp;
        }
        archiveTimestamps.append(timeStamp);
      }

      DistributedCache.setArchiveTimestamps(job, archiveTimestamps.toString());
    }

    tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileTimestamps = new StringBuffer();
      FileInfo info;
      long timeStamp;

      for (int i = 0; i < tfiles.length; i++) {
        if (i != 0) fileTimestamps.append(',');

        info = fileInfo.get(tfiles[i]);
        if (info == null) {
          timeStamp = DistributedCache.getTimestamp(job, tfiles[i]);
        } else {
          timeStamp = info.timeStamp;
        }
        fileTimestamps.append(timeStamp);
      }

      DistributedCache.setFileTimestamps(job, fileTimestamps.toString());
    }

    configureUserName(job);

  }

  /**
   * set this user's id in job configuration, so later job files can be
   * accessed using this user's id
   * @param job
   * @throws IOException
   */
  private void configureUserName(JobConf job) throws IOException {
    UnixUserGroupInformation ugi = getUGI(job);
   // Set the user's name, group and working directory
    job.setUser(ugi.getUserName());
    if (ugi.getGroupNames() != null && ugi.getGroupNames().length > 0) {
      job.set("group.name", ugi.getGroupNames()[0]);
    }
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(fs.getWorkingDirectory());
    }
  }

  private static UnixUserGroupInformation getUGI(Configuration job) throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(job, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException(
          "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }

  /**
   * Submit a job to the MR system.
   *
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   *
   * @param jobFile the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws InvalidJobConfException
   * @throws IOException
   */
  public RunningJob submitJob(String jobFile) throws FileNotFoundException,
                                                     InvalidJobConfException,
                                                     IOException {
    // Load in the submitted job details
    JobConf job = new JobConf(jobFile);
    return submitJob(job);
  }

  // job files are world-wide readable and owner writable
  final private static FsPermission JOB_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0644); // rw-r--r--

  // job submission directory is world readable/writable/executable
  final static FsPermission JOB_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0777); // rwx-rwx-rwx

  /**
   * Submit a job to the MR system.
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   *
   * @param job the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public RunningJob submitJob(JobConf job) throws FileNotFoundException,
                                                  IOException {
    try {
      return submitJobInternal(job);
    } catch (InterruptedException ie) {
      throw new IOException("interrupted", ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("class not found", cnfe);
    }
  }

  /**
   * Internal method for submitting jobs to the system.
   * @param job the configuration to submit
   * @return a proxy object for the running job
   * @throws FileNotFoundException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  public
  RunningJob submitJobInternal(JobConf job
                               ) throws FileNotFoundException,
                                        ClassNotFoundException,
                                        InterruptedException,
                                        IOException {
    /*
     * configure the command line options correctly on the submitting dfs
     */
    boolean shared = job.getBoolean("mapred.cache.shared.enabled", false);

    JobID jobId = jobSubmitClient.getNewJobId();
    Path submitJobDir = new Path(getSystemDir(), jobId.toString());
    Path sharedFilesDir =
      new Path(getSystemDir(), jobSubmitClient.CAR);
    Path submitSplitFile = new Path(submitJobDir, "job.split");

    getFs();
    if (jobSubmitClient instanceof LocalJobRunner) {
      symLinkAndConfigureFiles(job);
    } else {
      copyAndConfigureFiles(job,
          (shared) ? sharedFilesDir : submitJobDir, shared);
    }

    Path submitJobFile = new Path(submitJobDir, "job.xml");
    int reduces = job.getNumReduceTasks();
    JobContext context = new JobContext(job, jobId);

    // Check the output specification
    if (reduces == 0 ? job.getUseNewMapper() : job.getUseNewReducer()) {
      org.apache.hadoop.mapreduce.OutputFormat<?,?> output =
        ReflectionUtils.newInstance(context.getOutputFormatClass(), job);
      output.checkOutputSpecs(context);
    } else {
      job.getOutputFormat().checkOutputSpecs(fs, job);
    }

    // Create the splits for the job
    LOG.debug("Creating splits at " + fs.makeQualified(submitSplitFile));
    List<RawSplit> maps;
    if (job.getUseNewMapper()) {
      maps = computeNewSplits(context);
    } else {
      maps = computeOldSplits(job);
    }
    job.setNumMapTasks(maps.size());

    if (!isJobTrackerInProc) {
      JobConf conf = null;
      if (job.getUseNewMapper()) {
        conf = context.getJobConf();
      } else {
        conf = job;
      }
      writeComputedSplits(conf, maps, submitSplitFile);
      job.set("mapred.job.split.file", submitSplitFile.toString());
    } else {
      synchronized(JobClient.jobSplitCache) {
        if (JobClient.jobSplitCache.containsKey(jobId)) {
          throw new IOException("Job split already cached " + jobId);
        }
        JobClient.jobSplitCache.put(jobId, maps);
      }
      synchronized(JobClient.jobConfCache) {
        if (JobClient.jobConfCache.containsKey(jobId)) {
          throw new IOException("Job conf already cached " + jobId);
        }
        jobConfCache.put(jobId, job);
      }
    }
    // Write job file to JobTracker's fs
    FSDataOutputStream out =
      FileSystem.create(fs, submitJobFile,
          new FsPermission(JOB_FILE_PERMISSION));
    try {
      job.writeXml(out);
    } finally {
      out.close();
    }

    //
    // Now, actually submit the job (using the submit name)
    //
    JobStatus status = jobSubmitClient.submitJob(jobId);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      throw new IOException("Could not launch job");
    }
  }

  public static void writeComputedSplits(
    JobConf conf, List<RawSplit> splits, Path submitSplitFile)
      throws IOException {
    DataOutputStream out = writeSplitsFileHeader(
      conf, submitSplitFile, splits.size());
    try {
      for (RawSplit split: splits) {
        split.write(out);
      }
    } finally {
      out.close();
    }
  }

  private List<RawSplit> computeOldSplits(JobConf job) throws IOException {
    InputSplit[] splits =
      job.getInputFormat().getSplits(job, job.getNumMapTasks());

    validateNumberOfTasks(splits.length,job.getNumReduceTasks(),job);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new Comparator<InputSplit>() {
      public int compare(InputSplit a, InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size",
                                     ie);
        }
      }
    });

    List<RawSplit> rawSplits = new ArrayList<RawSplit>();
    DataOutputBuffer buffer = new DataOutputBuffer();
    for(InputSplit split: splits) {
      RawSplit rawSplit = new RawSplit();
      rawSplit.setClassName(split.getClass().getName());
      buffer.reset();
      split.write(buffer);
      rawSplit.setDataLength(split.getLength());
      rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
      rawSplit.setLocations(split.getLocations());
      rawSplits.add(rawSplit);
    }
    return rawSplits;
  }

  private static class NewSplitComparator
    implements Comparator<org.apache.hadoop.mapreduce.InputSplit>{

    @Override
    public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
                       org.apache.hadoop.mapreduce.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);
      }
    }
  }

  // if the number of tasks exceed a configured limit, then display an
  // apppropriate error message to the user. This check is also done by
  // the JobTracker and is the right place to enforce it. But the check is
  // done here too so that we can display an appropriate error message
  // to the user. Here we check only the number of mappers whereas the
  // JobTrcker applies this limit against the sum of mappers and reducers.
  private void validateNumberOfTasks(int splits, int reduceTasks, JobConf conf) 
    throws IOException {
    int maxTasks = conf.getInt("mapred.jobtracker.maxtasks.per.job", -1);
    int totalTasks = splits + reduceTasks;

    if ((maxTasks!= -1) && (totalTasks > maxTasks)) {
      throw new IOException(
                "The number of tasks for this job " +
                totalTasks +
                " exceeds the configured limit " + maxTasks);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends org.apache.hadoop.mapreduce.InputSplit>
  List<RawSplit> computeNewSplits(JobContext job)
      throws IOException, InterruptedException, ClassNotFoundException {
    JobConf conf = job.getJobConf();
    org.apache.hadoop.mapreduce.InputFormat<?,?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), job.getJobConf());

    List<org.apache.hadoop.mapreduce.InputSplit> splits = input.getSplits(job);
    T[] array = (T[])
      splits.toArray(new org.apache.hadoop.mapreduce.InputSplit[splits.size()]);

    validateNumberOfTasks(splits.size(),job.getNumReduceTasks(),conf);
    
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(array, new NewSplitComparator());
    List<RawSplit> rawSplits = new ArrayList<RawSplit>();
    if (array.length != 0) {
      DataOutputBuffer buffer = new DataOutputBuffer();
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer<T> serializer =
        factory.getSerializer((Class<T>) array[0].getClass());
      serializer.open(buffer);
      for(T split: array) {
        RawSplit rawSplit = new RawSplit();
        rawSplit.setClassName(split.getClass().getName());
        buffer.reset();
        serializer.serialize(split);
        rawSplit.setDataLength(split.getLength());
        rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
        rawSplit.setLocations(split.getLocations());
        rawSplits.add(rawSplit);
      }
      serializer.close();
    }
    return rawSplits;
  }

  /**
   * Checks if the job directory is clean and has all the required components
   * for (re) starting the job
   */
  public static boolean isJobDirValid(Path jobDirPath, FileSystem fs)
  throws IOException {
    FileStatus[] contents = fs.listStatus(jobDirPath);
    int matchCount = 0;
    if (contents != null && contents.length >=2) {
      for (FileStatus status : contents) {
        if ("job.xml".equals(status.getPath().getName())) {
          ++matchCount;
        }
        if ("job.split".equals(status.getPath().getName())) {
          ++matchCount;
        }
      }
      if (matchCount == 2) {
        return true;
      }
    }
    return false;
  }

  static class RawSplit implements Writable {
    private String splitClass;
    private BytesWritable bytes = new BytesWritable();
    private String[] locations;
    long dataLength;

    public void setBytes(byte[] data, int offset, int length) {
      bytes.set(data, offset, length);
    }

    public void setClassName(String className) {
      splitClass = className;
    }

    public String getClassName() {
      return splitClass;
    }

    public BytesWritable getBytes() {
      return bytes;
    }

    public void clearBytes() {
      bytes = null;
    }

    public void setLocations(String[] locations) {
      this.locations = locations;
    }

    public String[] getLocations() {
      return locations;
    }

    public void readFields(DataInput in) throws IOException {
      splitClass = Text.readString(in);
      dataLength = in.readLong();
      bytes.readFields(in);
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for(int i=0; i < len; ++i) {
        locations[i] = Text.readString(in).intern();
      }
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitClass);
      out.writeLong(dataLength);
      bytes.write(out);
      WritableUtils.writeVInt(out, locations.length);
      for(int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }
    }

    public long getDataLength() {
      return dataLength;
    }
    public void setDataLength(long l) {
      dataLength = l;
    }

  }

  private static final int CURRENT_SPLIT_FILE_VERSION = 0;
  private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();

  private static DataOutputStream writeSplitsFileHeader(Configuration conf,
                                                 Path filename,
                                                 int length
                                                 ) throws IOException {
    // write the splits to a file for the job tracker
    FileSystem fs = filename.getFileSystem(conf);
    FSDataOutputStream out =
      FileSystem.create(fs, filename, new FsPermission(JOB_FILE_PERMISSION));
    out.write(SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, CURRENT_SPLIT_FILE_VERSION);
    WritableUtils.writeVInt(out, length);
    return out;
  }

  /** Create the list of input splits and write them out in a file for
   *the JobTracker. The format is:
   * <format version>
   * <numSplits>
   * for each split:
   *    <RawSplit>
   * @param splits the input splits to write out
   * @param out the stream to write to
   */
  private void writeOldSplitsFile(InputSplit[] splits,
                                  FSDataOutputStream out) throws IOException {
  }

  /**
   * Read a splits file into a list of raw splits
   * @param in the stream to read from
   * @return the complete list of splits
   * @throws IOException
   */
  static RawSplit[] readSplitFile(DataInput in) throws IOException {
    byte[] header = new byte[SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != CURRENT_SPLIT_FILE_VERSION) {
      throw new IOException("Unsupported split version " + vers);
    }
    int len = WritableUtils.readVInt(in);
    RawSplit[] result = new RawSplit[len];
    for(int i=0; i < len; ++i) {
      result[i] = new RawSplit();
      result[i].readFields(in);
    }
    return result;
  }

  static RawSplit[] getAndRemoveCachedSplits(JobID jobId) {
    synchronized(JobClient.jobSplitCache) {
      List<RawSplit> rawSplits = JobClient.jobSplitCache.remove(jobId);
      if (rawSplits != null) {
        return rawSplits.toArray(new RawSplit[rawSplits.size()]);
      }
      return null;
    }
  }

  static JobConf getAndRemoveCachedJobConf(JobID jobId) {
    synchronized(JobClient.jobConfCache) {
      JobConf jobConf = JobClient.jobConfCache.get(jobId);
      if (jobConf != null) {
        JobClient.jobConfCache.remove(jobId);
      }
      return jobConf;
    }
  }

  /**
   * Get an {@link RunningJob} object to track an ongoing job.  Returns
   * null if the id does not correspond to any known job.
   *
   * @param jobid the jobid of the job.
   * @return the {@link RunningJob} handle to track the job, null if the
   *         <code>jobid</code> doesn't correspond to any known job.
   * @throws IOException
   */
  public RunningJob getJob(JobID jobid) throws IOException {
    JobStatus status = jobSubmitClient.getJobStatus(jobid);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      return null;
    }
  }

  /**@deprecated Applications should rather use {@link #getJob(JobID)}.
   */
  @Deprecated
  public RunningJob getJob(String jobid) throws IOException {
    return getJob(JobID.forName(jobid));
  }

  /**
   * Get the information of the current state of the map tasks of a job.
   *
   * @param jobId the job to query.
   * @return the list of all of the map tips.
   * @throws IOException
   */
  public TaskReport[] getMapTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getMapTaskReports(jobId);
  }

  /**@deprecated Applications should rather use {@link #getMapTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getMapTaskReports(String jobId) throws IOException {
    return getMapTaskReports(JobID.forName(jobId));
  }

  /**
   * Get the information of the current state of the reduce tasks of a job.
   *
   * @param jobId the job to query.
   * @return the list of all of the reduce tips.
   * @throws IOException
   */
  public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getReduceTaskReports(jobId);
  }

  /**
   * Get the information of the current state of the cleanup tasks of a job.
   *
   * @param jobId the job to query.
   * @return the list of all of the cleanup tips.
   * @throws IOException
   */
  public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getCleanupTaskReports(jobId);
  }

  /**
   * Get the information of the current state of the setup tasks of a job.
   *
   * @param jobId the job to query.
   * @return the list of all of the setup tips.
   * @throws IOException
   */
  public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getSetupTaskReports(jobId);
  }

  /**@deprecated Applications should rather use {@link #getReduceTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getReduceTaskReports(String jobId) throws IOException {
    return getReduceTaskReports(JobID.forName(jobId));
  }

  /**
   * Display the information about a job's tasks, of a particular type and
   * in a particular state
   *
   * @param jobId the ID of the job
   * @param type the type of the task (map/reduce/setup/cleanup)
   * @param state the state of the task
   * (pending/running/completed/failed/killed)
   */
  public void displayTasks(JobID jobId, String type, String state)
  throws IOException {
    TaskReport[] reports = new TaskReport[0];
    if (type.equals("map")) {
      reports = getMapTaskReports(jobId);
    } else if (type.equals("reduce")) {
      reports = getReduceTaskReports(jobId);
    } else if (type.equals("setup")) {
      reports = getSetupTaskReports(jobId);
    } else if (type.equals("cleanup")) {
      reports = getCleanupTaskReports(jobId);
    }
    for (TaskReport report : reports) {
      TIPStatus status = report.getCurrentStatus();
      if ((state.equals("pending") && status ==TIPStatus.PENDING) ||
          (state.equals("running") && status ==TIPStatus.RUNNING) ||
          (state.equals("completed") && status == TIPStatus.COMPLETE) ||
          (state.equals("failed") && status == TIPStatus.FAILED) ||
          (state.equals("killed") && status == TIPStatus.KILLED)) {
        printTaskAttempts(report);
      }
    }
  }
  private void printTaskAttempts(TaskReport report) {
    if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
      System.out.println(report.getSuccessfulTaskAttempt());
    } else if (report.getCurrentStatus() == TIPStatus.RUNNING) {
      for (TaskAttemptID t :
        report.getRunningTaskAttempts()) {
        System.out.println(t);
      }
    }
  }
  /**
   * Get status information about the Map-Reduce cluster.
   *
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return getClusterStatus(false);
  }

  /**
   * Get status information about the Map-Reduce cluster.
   *
   * @param  detailed if true then get a detailed status including the
   *         tracker names
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    return jobSubmitClient.getClusterStatus(detailed);
  }


  /**
   * Get the jobs that are not completed and not failed.
   *
   * @return array of {@link JobStatus} for the running/to-be-run jobs.
   * @throws IOException
   */
  public JobStatus[] jobsToComplete() throws IOException {
    return jobSubmitClient.jobsToComplete();
  }

  private static void downloadProfile(TaskCompletionEvent e
                                      ) throws IOException  {
    URLConnection connection =
      new URL(getTaskLogURL(e.getTaskAttemptId(), e.getTaskTrackerHttp()) +
              "&filter=profile").openConnection();
    InputStream in = connection.getInputStream();
    OutputStream out = new FileOutputStream(e.getTaskAttemptId() + ".profile");
    IOUtils.copyBytes(in, out, 64 * 1024, true);
  }

  /**
   * Get the jobs that are submitted.
   *
   * @return array of {@link JobStatus} for the submitted jobs.
   * @throws IOException
   */
  public JobStatus[] getAllJobs() throws IOException {
    return jobSubmitClient.getAllJobs();
  }

  /**
   * Utility that submits a job, then polls for progress until the job is
   * complete.
   *
   * @param job the job configuration.
   * @throws IOException if the job fails
   */
  public static RunningJob runJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    RunningJob rj = jc.submitJob(job);
    try {
      if (!jc.monitorAndPrintJob(job, rj)) {
        throw new IOException("Job failed!");
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    return rj;
  }

  /**
   * Monitor a job and print status in real-time as progress is made and tasks
   * fail.
   * @param conf the job's configuration
   * @param job the job to track
   * @return true if the job succeeded
   * @throws IOException if communication to the JobTracker fails
   */
  public boolean monitorAndPrintJob(JobConf conf,
                                    RunningJob job
  ) throws IOException, InterruptedException {
    String lastReport = null;
    TaskStatusFilter filter;
    filter = getTaskOutputFilter(conf);
    JobID jobId = job.getID();
    LOG.info("Running job: " + jobId);
    int eventCounter = 0;
    boolean profiling = conf.getProfileEnabled();
    Configuration.IntegerRanges mapRanges = conf.getProfileTaskRange(true);
    Configuration.IntegerRanges reduceRanges = conf.getProfileTaskRange(false);

    while (!job.isComplete()) {
      Thread.sleep(MAX_JOBPROFILE_AGE);
      String report =
        (" map " + StringUtils.formatPercent(job.mapProgress(), 0)+
            " reduce " +
            StringUtils.formatPercent(job.reduceProgress(), 0));
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }

      TaskCompletionEvent[] events =
        job.getTaskCompletionEvents(eventCounter);
      eventCounter += events.length;
      for(TaskCompletionEvent event : events){
        TaskCompletionEvent.Status status = event.getTaskStatus();
        if (profiling &&
            (status == TaskCompletionEvent.Status.SUCCEEDED ||
                status == TaskCompletionEvent.Status.FAILED) &&
                (event.isMap ? mapRanges : reduceRanges).
                isIncluded(event.idWithinJob())) {
          downloadProfile(event);
        }
        switch(filter){
        case NONE:
          break;
        case SUCCEEDED:
          if (event.getTaskStatus() ==
            TaskCompletionEvent.Status.SUCCEEDED){
            LOG.info(event.toString());
            displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          }
          break;
        case FAILED:
          if (event.getTaskStatus() ==
            TaskCompletionEvent.Status.FAILED){
            LOG.info(event.toString());
            // Displaying the task diagnostic information
            TaskAttemptID taskId = event.getTaskAttemptId();
            String[] taskDiagnostics =
              jobSubmitClient.getTaskDiagnostics(taskId);
            if (taskDiagnostics != null) {
              for(String diagnostics : taskDiagnostics){
                System.err.println(diagnostics);
              }
            }
            // Displaying the task logs
            displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          }
          break;
        case KILLED:
          if (event.getTaskStatus() == TaskCompletionEvent.Status.KILLED){
            LOG.info(event.toString());
          }
          break;
        case ALL:
          LOG.info(event.toString());
          displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          break;
        }
      }
    }
    LOG.info("Job complete: " + jobId);
    Counters counters = job.getCounters();
    if (counters != null) {
      counters.log(LOG);
    }
    return job.isSuccessful();
  }

  static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
    return (baseUrl + "/tasklog?plaintext=true&taskid=" + taskId);
  }

  private static void displayTaskLogs(TaskAttemptID taskId, String baseUrl)
    throws IOException {
    // The tasktracker for a 'failed/killed' job might not be around...
    if (baseUrl != null) {
      // Construct the url for the tasklogs
      String taskLogUrl = getTaskLogURL(taskId, baseUrl);

      // Copy tasks's stdout of the JobClient
      getTaskLogs(taskId, new URL(taskLogUrl+"&filter=stdout"), System.out);

      // Copy task's stderr to stderr of the JobClient
      getTaskLogs(taskId, new URL(taskLogUrl+"&filter=stderr"), System.err);
    }
  }

  private static void getTaskLogs(TaskAttemptID taskId, URL taskLogUrl,
                                  OutputStream out) {
    try {
      BufferedReader input = getTaskLogReader(taskLogUrl);
      BufferedWriter output = new BufferedWriter(new OutputStreamWriter(out));
      try {
        String logData = null;
        while ((logData = input.readLine()) != null) {
          if (logData.length() > 0) {
            output.write(taskId + ": " + logData + "\n");
            output.flush();
          }
        }
      } finally {
        input.close();
      }
    }catch(IOException ioe){
      LOG.warn("Error reading task output" + ioe.getMessage());
    }
  }

  /**
   * Get a {@link BufferedReader} to read the taskLog by lines.
   * @param taskLogUrl The URL of the taskLog location
   * @return A {@link BufferedReader} to read the taskLog by lines
   * @throws IOException Will throw this if we cannot get the reader for 5 mins
   */
  private static BufferedReader getTaskLogReader(final URL taskLogUrl)
      throws IOException {
    FutureTask<BufferedReader> task = new FutureTask<BufferedReader>(
        new Callable<BufferedReader>() {
          @Override
          public BufferedReader call() throws IOException {
            URLConnection connection = taskLogUrl.openConnection();
            BufferedReader input = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
            return input;
          }
        });
    Thread thread = new Thread(task);
    thread.setDaemon(true);
    thread.start();
    BufferedReader result = null;
    try {
      // Fails if we cannot open the input stream for 5 minutes.
      // This prevents JobClient from hanging
      result = task.get(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (TimeoutException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    if (result == null) {
      throw new IOException("Failed to open input stream for " + taskLogUrl);
    }
    return result;
  }

  static Configuration getConfiguration(String jobTrackerSpec)
  {
    Configuration conf = new Configuration();
    if (jobTrackerSpec != null) {
      if (jobTrackerSpec.indexOf(":") >= 0) {
        conf.set("mapred.job.tracker", jobTrackerSpec);
      } else {
        String classpathFile = "hadoop-" + jobTrackerSpec + ".xml";
        URL validate = conf.getResource(classpathFile);
        if (validate == null) {
          throw new RuntimeException(classpathFile + " not found on CLASSPATH");
        }
        conf.addResource(classpathFile);
      }
    }
    return conf;
  }

  /**
   * Sets the output filter for tasks. only those tasks are printed whose
   * output matches the filter.
   * @param newValue task filter.
   */
  @Deprecated
  public void setTaskOutputFilter(TaskStatusFilter newValue){
    this.taskOutputFilter = newValue;
  }

  /**
   * Get the task output filter out of the JobConf.
   *
   * @param job the JobConf to examine.
   * @return the filter level.
   */
  public static TaskStatusFilter getTaskOutputFilter(JobConf job) {
    return TaskStatusFilter.valueOf(job.get("jobclient.output.filter",
                                            "FAILED"));
  }

  /**
   * Modify the JobConf to set the task output filter.
   *
   * @param job the JobConf to modify.
   * @param newValue the value to set.
   */
  public static void setTaskOutputFilter(JobConf job,
                                         TaskStatusFilter newValue) {
    job.set("jobclient.output.filter", newValue.toString());
  }

  /**
   * Returns task output filter.
   * @return task filter.
   */
  @Deprecated
  public TaskStatusFilter getTaskOutputFilter(){
    return this.taskOutputFilter;
  }

  private String getJobPriorityNames() {
    StringBuffer sb = new StringBuffer();
    for (JobPriority p : JobPriority.values()) {
      sb.append(p.name()).append(" ");
    }
    return sb.substring(0, sb.length()-1);
  }

  /**
   * Display usage of the command-line tool and terminate execution
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: JobClient ";
    String jobPriorityValues = getJobPriorityNames();
    String taskTypes = "map, reduce, setup, cleanup";
    String taskStates = "running, completed";
    if("-submit".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-file>]");
    } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id>]");
    } else if ("-counter".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <group-name> <counter-name>]");
    } else if ("-events".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <from-event-#> <#-of-events>]");
    } else if ("-history".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <jobOutputDir>]");
    } else if ("-list".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all]]");
    } else if ("-kill-task".equals(cmd) || "-fail-task".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <task-id>]");
    } else if ("-set-priority".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <priority>]. " +
          "Valid values for priorities are: "
          + jobPriorityValues);
    } else if ("-list-active-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-attempt-ids".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <task-type> <task-state>]. " +
          "Valid values for <task-type> are " + taskTypes + ". " +
          "Valid values for <task-state> are " + taskStates);
    } else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-submit <job-file>]\n");
      System.err.printf("\t[-status <job-id>]\n");
      System.err.printf("\t[-counter <job-id> <group-name> <counter-name>]\n");
      System.err.printf("\t[-kill <job-id>]\n");
      System.err.printf("\t[-set-priority <job-id> <priority>]. " +
                                      "Valid values for priorities are: " +
                                      jobPriorityValues + "\n");
      System.err.printf("\t[-events <job-id> <from-event-#> <#-of-events>]\n");
      System.err.printf("\t[-history <jobOutputDir>]\n");
      System.err.printf("\t[-list [all]]\n");
      System.err.printf("\t[-list-active-trackers]\n");
      System.err.printf("\t[-list-blacklisted-trackers]\n");
      System.err.printf("\t[-list-trackers]\n");
      System.err.println("\t[-list-attempt-ids <job-id> <task-type> " +
      		"<task-state>]\n");
      System.err.printf("\t[-kill-task <task-id>]\n");
      System.err.printf("\t[-fail-task <task-id>]\n\n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    if (argv.length < 1) {
      displayUsage("");
      return exitCode;
    }
    // process arguments
    String cmd = argv[0];
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    String outputDir = null;
    String counterGroupName = null;
    String counterName = null;
    String newPriority = null;
    String taskType = null;
    String taskState = null;
    int fromEvent = 0;
    int nEvents = 0;
    boolean getStatus = false;
    boolean getCounter = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean viewHistory = false;
    boolean viewAllHistory = false;
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean listActiveTrackers = false;
    boolean listBlacklistedTrackers = false;
    boolean listTrackers = false;
    boolean displayTasks = false;
    boolean killTask = false;
    boolean failTask = false;
    boolean setJobPriority = false;

    if ("-submit".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJobFile = argv[1];
    } else if ("-status".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      getStatus = true;
    } else if("-counter".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      getCounter = true;
      jobid = argv[1];
      counterGroupName = argv[2];
      counterName = argv[3];
    } else if ("-kill".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      killJob = true;
    } else if ("-set-priority".equals(cmd)) {
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      newPriority = argv[2];
      try {
        JobPriority jp = JobPriority.valueOf(newPriority);
      } catch (IllegalArgumentException iae) {
        displayUsage(cmd);
        return exitCode;
      }
      setJobPriority = true;
    } else if ("-events".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      fromEvent = Integer.parseInt(argv[2]);
      nEvents = Integer.parseInt(argv[3]);
      listEvents = true;
    } else if ("-history".equals(cmd)) {
      if (argv.length != 2 && !(argv.length == 3 && "all".equals(argv[1]))) {
         displayUsage(cmd);
         return exitCode;
      }
      viewHistory = true;
      if (argv.length == 3 && "all".equals(argv[1])) {
         viewAllHistory = true;
         outputDir = argv[2];
      } else {
         outputDir = argv[1];
      }
    } else if ("-list".equals(cmd)) {
      if (argv.length != 1 && !(argv.length == 2 && "all".equals(argv[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && "all".equals(argv[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if("-kill-task".equals(cmd)) {
      if(argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killTask = true;
      taskid = argv[1];
    } else if("-fail-task".equals(cmd)) {
      if(argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      failTask = true;
      taskid = argv[1];
    } else if ("-list-active-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listActiveTrackers = true;
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listBlacklistedTrackers = true;
    } else if ("-list-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listTrackers = true;
    } else if ("-list-attempt-ids".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      taskType = argv[2];
      taskState = argv[3];
      displayTasks = true;
    } else {
      displayUsage(cmd);
      return exitCode;
    }

    // initialize JobClient
    JobConf conf = null;
    if (submitJobFile != null) {
      conf = new JobConf(submitJobFile);
    } else {
      conf = new JobConf(getConf());
    }
    init(conf);

    // Submit the request
    try {
      if (submitJobFile != null) {
        RunningJob job = submitJob(conf);
        System.out.println("Created job " + job.getID());
        exitCode = 0;
      } else if (getStatus) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          System.out.println();
          System.out.println(job);
          Counters counters = job.getCounters();
          if (counters != null) {
            System.out.println(counters);
          } else {
            System.out.println("Counters not available. Job is retired.");
          }
          exitCode = 0;
        }
      } else if (getCounter) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          if (counters == null) {
            System.out.println("Counters not available for retired job " +
                jobid);
            exitCode = -1;
          } else {
            Group group = counters.getGroup(counterGroupName);
            Counter counter = group.getCounterForName(counterName);
            System.out.println(counter.getCounter());
            exitCode = 0;
          }
        }
      } else if (killJob) {
        jobSubmitClient.killJob(JobID.forName(jobid));
        System.out.println("Killed job " + jobid);
      } else if (setJobPriority) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          job.setJobPriority(newPriority);
          System.out.println("Changed job priority.");
          exitCode = 0;
        }
      } else if (viewHistory) {
        viewHistory(outputDir, viewAllHistory);
        exitCode = 0;
      } else if (listEvents) {
        listEvents(JobID.forName(jobid), fromEvent, nEvents);
        exitCode = 0;
      } else if (listJobs) {
        listJobs();
        exitCode = 0;
      } else if (listAllJobs) {
        listAllJobs();
        exitCode = 0;
      } else if (listActiveTrackers) {
        listActiveTrackers();
        exitCode = 0;
      } else if (listBlacklistedTrackers) {
        listBlacklistedTrackers();
        exitCode = 0;
      } else if (listTrackers) {
        listTrackers();
        exitCode = 0;
      } else if (displayTasks) {
        displayTasks(JobID.forName(jobid), taskType, taskState);
      } else if(killTask) {
        if(jobSubmitClient.killTask(TaskAttemptID.forName(taskid), false)) {
          System.out.println("Killed task " + taskid);
          exitCode = 0;
        } else {
          System.out.println("Could not kill task " + taskid);
          exitCode = -1;
        }
      } else if(failTask) {
        if(jobSubmitClient.killTask(TaskAttemptID.forName(taskid), true)) {
          System.out.println("Killed task " + taskid + " by failing it");
          exitCode = 0;
        } else {
          System.out.println("Could not fail task " + taskid);
          exitCode = -1;
        }
      }
    } finally {
      close();
    }
    return exitCode;
  }

  private void viewHistory(String outputDir, boolean all)
    throws IOException {
    HistoryViewer historyViewer = new HistoryViewer(outputDir,
                                        getConf(), all);
    historyViewer.print();
  }

  /**
   * List the events for the given job
   * @param jobId the job id for the job's events to list
   * @throws IOException
   */
  private void listEvents(JobID jobId, int fromEventId, int numEvents)
    throws IOException {
    TaskCompletionEvent[] events =
      jobSubmitClient.getTaskCompletionEvents(jobId, fromEventId, numEvents);
    System.out.println("Task completion events for " + jobId);
    System.out.println("Number of events (from " + fromEventId +
                       ") are: " + events.length);
    for(TaskCompletionEvent event: events) {
      System.out.println(event.getTaskStatus() + " " + event.getTaskAttemptId() + " " +
                         getTaskLogURL(event.getTaskAttemptId(),
                                       event.getTaskTrackerHttp()));
    }
  }

  /**
   * Dump a list of currently running jobs
   * @throws IOException
   */
  private void listJobs() throws IOException {
    JobStatus[] jobs = jobsToComplete();
    if (jobs == null)
      jobs = new JobStatus[0];

    System.out.printf("%d jobs currently running\n", jobs.length);
    displayJobList(jobs);
  }

  /**
   * Dump a list of all jobs submitted.
   * @throws IOException
   */
  private void listAllJobs() throws IOException {
    JobStatus[] jobs = getAllJobs();
    if (jobs == null)
      jobs = new JobStatus[0];
    System.out.printf("%d jobs submitted\n", jobs.length);
    System.out.printf("States are:\n\tRunning : 1\tSucceded : 2" +
    "\tFailed : 3\tPrep : 4\n");
    displayJobList(jobs);
  }

  /**
   * Display the list of active trackers
   */
  private void listActiveTrackers() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    Collection<String> trackers = c.getActiveTrackerNames();
    for (String trackerName : trackers) {
      System.out.println(trackerName);
    }
  }

  /**
   * Display the list of blacklisted trackers
   */
  private void listBlacklistedTrackers() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    Collection<String> trackers = c.getBlacklistedTrackerNames();
    for (String trackerName : trackers) {
      System.out.println(trackerName);
    }
  }

  /**
   * Display the stats of the cluster with per tracker details
   * @throws IOException
   */
  private void listTrackers() throws IOException {
    ClusterStatus fullStatus = jobSubmitClient.getClusterStatus(true);
    Collection<TaskTrackerStatus> trackers =
            fullStatus.getTaskTrackersDetails();
    Set<String> activeTrackers =
            new HashSet<String>(fullStatus.getActiveTrackerNames());
    List<Float> mapsProgress = new ArrayList<Float>();
    List<Float> reducesProgress = new ArrayList<Float>();
    int finishedMapsFromRunningJobs = 0;
    int finishedReducesFromRunningJobs = 0;

    System.out.println("Total Map Tasks in Running Jobs: " +
            fullStatus.getTotalMapTasks());
    System.out.println("Total Reduce Tasks in Running Jobs: " +
            fullStatus.getTotalReduceTasks());

    for (TaskTrackerStatus tracker : trackers) {
      System.out.println(tracker.getTrackerName());


      //List<TaskStatus> tasks = tracker.getTaskReports();
      Collection<TaskStatus> tasks =
              fullStatus.getTaskTrackerTasksStatuses(tracker.getTrackerName());
      for (TaskStatus task : tasks) {
        TaskStatus.State state = task.getRunState();
        if (task.getIsMap() &&
                (state == TaskStatus.State.RUNNING ||
                state == TaskStatus.State.UNASSIGNED)) {
          mapsProgress.add(task.getProgress());
        } else if (!task.getIsMap() &&
                (state == TaskStatus.State.RUNNING ||
                state == TaskStatus.State.UNASSIGNED)) {
          reducesProgress.add(task.getProgress());
        } else if (task.getIsMap() && state == TaskStatus.State.SUCCEEDED) {
          finishedMapsFromRunningJobs++;
        } else if (!task.getIsMap() && state == TaskStatus.State.SUCCEEDED) {
          finishedReducesFromRunningJobs++;
        }
      }

      if (activeTrackers.contains(tracker.getTrackerName())) {
        System.out.println("\tActive");
      } else {
        System.out.println("\tBlacklisted");
      }
      System.out.println("\tLast Seen: " + tracker.getLastSeen());

      System.out.println("\tMap Tasks Running: " + tracker.countMapTasks() +
              "/" + tracker.getMaxMapSlots());
      System.out.println("\tMap Tasks Progress: " + mapsProgress.toString());
      System.out.println("\tFinished Map Tasks From Running Jobs: " +
              finishedMapsFromRunningJobs);
      System.out.println("\tReduce Tasks Running: " +
              tracker.countReduceTasks() + "/" + tracker.getMaxReduceSlots());
      System.out.println("\tReduce Tasks Progress: " +
              reducesProgress.toString());

      System.out.println("\tTask Tracker Failures: " + tracker.getFailures());
      mapsProgress.clear();
      reducesProgress.clear();
    }
  }


  void displayJobList(JobStatus[] jobs) {
    System.out.printf("JobId\tState\tStartTime\tUserName\tPriority\tSchedulingInfo\n");
    for (JobStatus job : jobs) {
      System.out.printf("%s\t%d\t%d\t%s\t%s\t%s\n", job.getJobID(), job.getRunState(),
          job.getStartTime(), job.getUsername(),
          job.getJobPriority().name(), job.getSchedulingInfo());
    }
  }

  /**
   * Get status information about the max available Maps in the cluster.
   *
   * @return the max available Maps in the cluster
   * @throws IOException
   */
  public int getDefaultMaps() throws IOException {
    return getClusterStatus().getMaxMapTasks();
  }

  /**
   * Get status information about the max available Reduces in the cluster.
   *
   * @return the max available Reduces in the cluster
   * @throws IOException
   */
  public int getDefaultReduces() throws IOException {
    return getClusterStatus().getMaxReduceTasks();
  }

  /**
   * Grab the jobtracker system directory path where job-specific files are to be placed.
   *
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() {
    if (sysDir == null) {
      sysDir = new Path(jobSubmitClient.getSystemDir());
    }
    return sysDir;
  }


  /**
   * Return an array of queue information objects about all the Job Queues
   * configured.
   *
   * @return Array of JobQueueInfo objects
   * @throws IOException
   */
  public JobQueueInfo[] getQueues() throws IOException {
    return jobSubmitClient.getQueues();
  }

  /**
   * Gets all the jobs which were added to particular Job Queue
   *
   * @param queueName name of the Job Queue
   * @return Array of jobs present in the job queue
   * @throws IOException
   */

  public JobStatus[] getJobsFromQueue(String queueName) throws IOException {
    return jobSubmitClient.getJobsFromQueue(queueName);
  }

  /**
   * Gets the queue information associated to a particular Job Queue
   *
   * @param queueName name of the job queue.
   * @return Queue information associated to particular queue.
   * @throws IOException
   */
  public JobQueueInfo getQueueInfo(String queueName) throws IOException {
    return jobSubmitClient.getQueueInfo(queueName);
  }

  /**
   * Gets the Queue ACLs for current user
   * @return array of QueueAclsInfo object for current user.
   * @throws IOException
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    return jobSubmitClient.getQueueAclsForCurrentUser();
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new JobClient(), argv);
    System.exit(res);
  }

  /**
   * Retrieves information about files that are present in distributed cache
   */
  private static class DistributedCacheIndex {
    /** File system where distributed cache files are stored */
    private FileSystem fs;

    /**
     * Constructs index on provided file system
     * @param fs
     */
    public DistributedCacheIndex(FileSystem fs) {
      this.fs = fs;
    }

    /**
     * Notifies that given path is present in distributed cache
     * @param path path to check
     */
    public void add(Path path) {
    }

    /**
     * Checks whether given file is present in distributed cache
     * @param path path to check
     * @return
     */
    public boolean contains(Path path) {
      try {
        return fs.exists(path);
      } catch (IOException e) {
        // This is not a tragedy, in worst case we reupload file
        return false;
      }
    }

  }

}

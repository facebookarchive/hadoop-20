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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.MD5Hash;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.net.URI;
import java.util.Arrays;

/** Base class that runs a task in a separate process.  Tasks are run in a
 * separate process in order to isolate the map/reduce system code from bugs in
 * user supplied map and reduce functions.
 */
abstract class TaskRunner extends Thread {
  public static final Log LOG =
    LogFactory.getLog(TaskRunner.class);
  /** Should the user classpath be added first? */
  public static final String MAPREDUCE_JOB_USER_CLASSPATH_FIRST =
      "mapreduce.job.user.classpath.first";
  public static final String MAPREDUCE_TASK_SYSTEM_CLASSPATH_PROPERTY =
      "MAPREDUCE_TASK_SYSTEM_CLASSPATH";
  /** Number of milliseconds to delay starting a task (default of 0) */
  public static final String MAPREDUCE_TASK_DELAY_MS =
      "mapreduce.task.delay.ms";
  /** Default is to use no delay */
  public static final long MAPREDUCE_TASK_DELAY_MS_DEFAULT = 0;

  volatile boolean killed = false;
  private TaskTracker.TaskInProgress tip;
  private Task t;
  protected Object lock = new Object();
  protected volatile boolean done = false;
  private int exitCode = -1;
  private boolean exitCodeSet = false;

  protected TaskTracker tracker;

  protected JobConf conf;
  JvmManager jvmManager;

  /**
   * for cleaning up old map outputs
   */
  protected MapOutputFile mapOutputFile;

  public TaskRunner(
      TaskTracker.TaskInProgress tip,
      TaskTracker tracker,
      JobConf conf) {
    this(tip, tip.getTask(), tracker, conf);
  }

  public TaskRunner(
      TaskTracker.TaskInProgress tip,
      Task task,
      TaskTracker tracker,
      JobConf conf) {
    this.tip = tip;
    this.t = task;
    this.tracker = tracker;
    this.conf = conf;
    this.mapOutputFile =
      new MapOutputFile(task.getJobID(), tracker.getAsyncDiskService());
    this.mapOutputFile.setConf(conf);
    this.jvmManager = tracker.getJvmManagerInstance();
  }

  public Task getTask() { return t; }
  public TaskTracker.TaskInProgress getTaskInProgress() { return tip; }
  public TaskTracker getTracker() { return tracker; }

  /** Called to assemble this task's input.  This method is run in the parent
   * process before the child is spawned.  It should not execute user code,
   * only system code. */
  public boolean prepare() throws IOException {
    return true;
  }

  /** Called when this task's output is no longer needed.
   * This method is run in the parent process after the child exits.  It should
   * not execute user code, only system code.
   */
  public void close() throws IOException {}

  private static String stringifyPathArray(Path[] p){
    if (p == null){
      return null;
    }
    StringBuffer str = new StringBuffer(p[0].toString());
    for (int i = 1; i < p.length; i++){
      str.append(",");
      str.append(p[i].toString());
    }
    return str.toString();
  }

  /**
   * Get the java command line options for the child map/reduce tasks.
   * Overriden by specific launchers.
   *
   * @param jobConf job configuration
   * @param defaultValue default value
   * @return the java command line options for child map/reduce tasks
   * @deprecated Use command line options specific to map or reduce tasks set
   *             via {@link JobConf#MAPRED_MAP_TASK_JAVA_OPTS} or
   *             {@link JobConf#MAPRED_REDUCE_TASK_JAVA_OPTS}
   */
  @Deprecated
  public String getChildJavaOpts(JobConf jobConf, String defaultValue) {
    if (getTask().isJobSetupTask()) {
      return jobConf.get(JobConf.MAPRED_JOB_SETUP_TASK_JAVA_OPTS,
          defaultValue);
    } else if (getTask().isJobCleanupTask()) {
      return jobConf.get(JobConf.MAPRED_JOB_CLEANUP_TASK_JAVA_OPTS,
          defaultValue);
    } else if (getTask().isTaskCleanupTask()) {
      return jobConf.get(JobConf.MAPRED_TASK_CLEANUP_TASK_JAVA_OPTS,
          defaultValue);
    } else {
      return jobConf.get(JobConf.MAPRED_TASK_JAVA_OPTS, defaultValue);
    }
  }

  public String getAdminChildJavaOpts(JobConf jobConf) {
    String opts = jobConf.get(JobConf.MAPRED_ADMIN_TASK_JAVA_OPTS);
    return (opts == null) ? "" : opts;
  }

  /**
   * Corona will schedule tasks immediately after a failure, which can cause
   * the new task to potentially coincide with the failed task JVM still
   * running.  Concurrent running JVMs can cause failure if they expect an
   * initial amount of memory available.  As a temporary workaround, there
   * is an option to set a customizable delay to starting every task in
   * milliseconds based on the cluster/job configuration.
   *
   * @return Milliseconds of delay
   */
  private static long delayStartingTask(JobConf conf) {
    long delayMs =
        conf.getLong(MAPREDUCE_TASK_DELAY_MS, MAPREDUCE_TASK_DELAY_MS_DEFAULT);
    try {
      Thread.sleep(delayMs);
    } catch (InterruptedException e) {
      LOG.info("delayStartingTask: Unexpected interruption of " +
          delayMs + " ms");
    }

    return delayMs;
  }

  /**
   * Get the maximum virtual memory of the child map/reduce tasks.
   * @param jobConf job configuration
   * @return the maximum virtual memory of the child task or <code>-1</code> if
   *         none is specified
   * @deprecated Use limits specific to the map or reduce tasks set via
   *             {@link JobConf#MAPRED_MAP_TASK_ULIMIT} or
   *             {@link JobConf#MAPRED_REDUCE_TASK_ULIMIT}
   */
  @Deprecated
  public int getChildUlimit(JobConf jobConf) {
    return jobConf.getInt(JobConf.MAPRED_TASK_ULIMIT, -1);
  }

  /**
   * Get the environment variables for the child map/reduce tasks.
   * @param jobConf job configuration
   * @return the environment variables for the child map/reduce tasks or
   *         <code>null</code> if unspecified
   * @deprecated Use environment variables specific to the map or reduce tasks
   *             set via {@link JobConf#MAPRED_MAP_TASK_ENV} or
   *             {@link JobConf#MAPRED_REDUCE_TASK_ENV}
   */
  public String getChildEnv(JobConf jobConf) {
    return jobConf.get(JobConf.MAPRED_TASK_ENV);
  }

  public static class CacheFile {
    URI uri;
    long timeStamp;
    CacheFile (URI uri, long timeStamp) {
      this.uri = uri;
      this.timeStamp = timeStamp;
    }
    CacheFile(URI uri) {
      this.uri = uri;
      this.timeStamp = 0;
    }
  }

  /**
   * Given the path to the localized job jar file, add it's constituents to
   * the classpath
   */
  private void addJobJarToClassPath(String localJarFile, StringBuffer classPath) {
    File jobCacheDir = new File
      (new Path(localJarFile).getParent().toString());
    File[] libs = new File(jobCacheDir, "lib").listFiles();
    String sep = System.getProperty("path.separator");

    if (libs != null) {
      for (int i = 0; i < libs.length; i++) {
        classPath.append(sep); // add libs from jar to classpath
        classPath.append(libs[i]);
      }
    }
    classPath.append(sep);
    classPath.append(new File(jobCacheDir, "classes"));
    classPath.append(sep);
    classPath.append(jobCacheDir);
  }

  /**
   * Add the system class path to a class path
   * @param conf JobConf
   * @param pathSeparator Path separator
   * @param classPath Where to add the system class path
   */
  private static void appendSystemClasspath(JobConf conf,
                                            String pathSeparator,
                                            StringBuffer classPath) {
    // The alternate runtime can be used to debug tasks by putting a
    // custom version of the mapred libraries. This will get loaded before
    // the TT's jars.
    String debugRuntime = conf.get("mapred.task.debug.runtime.classpath");
    if (debugRuntime != null) {
      classPath.append(pathSeparator);
      classPath.append(debugRuntime);
    }
    // Determine system classpath for tasks. Default to tasktracker's
    // classpath.
    String systemClasspath = System.getenv(
      MAPREDUCE_TASK_SYSTEM_CLASSPATH_PROPERTY);
    if (systemClasspath == null) {
      systemClasspath = System.getProperty("java.class.path");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("System classpath " + systemClasspath);
    }
    classPath.append(pathSeparator);
    classPath.append(systemClasspath);
  }

  @Override
  public void run() {
    String errorInfo = "Child Error";
    List<CacheFile> localizedCacheFiles = new ArrayList<CacheFile>();
    try {
      //before preparing the job localize
      //all the archives
      TaskAttemptID taskid = t.getTaskID();
      LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

      File workDir = new File(lDirAlloc.getLocalPathToRead(
                                TaskTracker.getLocalTaskDir(
                                  t.getJobID().toString(),
                                  t.getTaskID().toString(),
                                  t.isTaskCleanupTask())
                                + Path.SEPARATOR + MRConstants.WORKDIR,
                                conf). toString());

      URI[] archives = DistributedCache.getCacheArchives(conf);
      URI[] files = DistributedCache.getCacheFiles(conf);
      URI[] sharedArchives = DistributedCache.getSharedCacheArchives(conf);
      URI[] sharedFiles = DistributedCache.getSharedCacheFiles(conf);
      FileStatus fileStatus;
      FileSystem fileSystem;
      if ((archives != null) || (files != null) ||
          (sharedArchives != null) || (sharedFiles != null)) {

        if (archives != null) {
          String[] archivesTimestamps =
            DistributedCache.getArchiveTimestamps(conf);
          Path[] p = new Path[archives.length];
          for (int i = 0; i < archives.length;i++){
            fileSystem = FileSystem.get(archives[i], conf);
            fileStatus = fileSystem.getFileStatus(
                new Path(archives[i].getPath()));
            p[i] = DistributedCache.getLocalCacheFromTimestamps(
                archives[i], conf, new Path(TaskTracker.getCacheSubdir()),
                fileStatus, true, Long.parseLong(archivesTimestamps[i]),
                fileStatus.getLen(),
                new Path(workDir.getAbsolutePath()), false,
                tracker.getAsyncDiskService(), lDirAlloc);
            localizedCacheFiles.add(new CacheFile(archives[i], Long
                  .parseLong(archivesTimestamps[i])));

          }
          DistributedCache.setLocalArchives(conf, stringifyPathArray(p));
        }

        if (sharedArchives != null) {
          String[] archiveLength
            = DistributedCache.getSharedArchiveLength(conf);

          Path[] p = new Path[sharedArchives.length];
          for (int i = 0; i < sharedArchives.length;i++){
            p[i] = DistributedCache.getLocalCacheFromURI(
                sharedArchives[i], // cache
                conf, // conf
                new Path(TaskTracker.getCacheSubdir()), // subDir
                true, // isArchive
                Long.parseLong(archiveLength[i]), // fileLength
                new Path(workDir.getAbsolutePath()), // currentWorkDir
                false, // honorSymLinkConf
                tracker.getAsyncDiskService(), // asyncDiskService
                lDirAlloc); // lDirAllocator
            localizedCacheFiles.add(new CacheFile(
                  sharedArchives[i]));

          }
          DistributedCache.setLocalSharedArchives(conf, stringifyPathArray(p));
        }

        if ((files != null)) {
          String[] fileTimestamps = DistributedCache.getFileTimestamps(conf);
          Path[] p = new Path[files.length];
          for (int i = 0; i < files.length;i++){
            fileSystem = FileSystem.get(files[i], conf);
            fileStatus = fileSystem.getFileStatus(
                new Path(files[i].getPath()));
            p[i] = DistributedCache.getLocalCacheFromTimestamps(
                files[i], conf, new Path(TaskTracker.getCacheSubdir()),
                fileStatus, false, Long.parseLong(fileTimestamps[i]),
                fileStatus.getLen(),
                new Path(workDir.getAbsolutePath()), false,
                tracker.getAsyncDiskService(), lDirAlloc);
            localizedCacheFiles.add(new CacheFile(files[i], Long
                  .parseLong(fileTimestamps[i])));
          }
          DistributedCache.setLocalFiles(conf, stringifyPathArray(p));
        }

        if ((sharedFiles != null)) {
          String[] fileLength = DistributedCache.getSharedFileLength(conf);

          Path[] p = new Path[sharedFiles.length];
          for (int i = 0; i < sharedFiles.length;i++){
            p[i] = DistributedCache.getLocalCacheFromURI(
                sharedFiles[i], conf, new Path(TaskTracker.getCacheSubdir()),
                false, Long.parseLong(fileLength[i]),
                new Path(workDir.getAbsolutePath()), false,
                tracker.getAsyncDiskService(), lDirAlloc);
            localizedCacheFiles.add(new CacheFile(sharedFiles[i]));
          }
          DistributedCache.setLocalSharedFiles(conf, stringifyPathArray(p));
        }

        Path localTaskFile = new Path(t.getJobFile());
        FileSystem localFs = FileSystem.getLocal(conf);
        localFs.delete(localTaskFile, true);
        OutputStream out = localFs.create(localTaskFile);
        try {
          conf.writeXml(out);
        } finally {
          out.close();
        }
      }

      if (!prepare()) {
        return;
      }

      final String sep = System.getProperty("path.separator");
      final boolean userClassesTakesPrecedence =
          conf.getBoolean(MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
      StringBuffer classPath = new StringBuffer();

      if (!userClassesTakesPrecedence) {
        // parent process's class paths added first
        appendSystemClasspath(conf, sep, classPath);
      }

      if (!workDir.mkdirs()) {
        if (!workDir.isDirectory()) {
          LOG.fatal("Mkdirs failed to create " + workDir.toString());
        }
      }

      boolean shared = conf.getBoolean("mapred.cache.shared.enabled", false);
      String localJar = conf.getJar();

      // handle job jar file for the non shared case
      if (!shared && (localJar != null)) {
        addJobJarToClassPath(localJar, classPath);
      }

      // include the user specified classpath

      //archive paths
      Path[] archiveClasspaths = DistributedCache.getArchiveClassPaths(conf);
      if (archiveClasspaths != null && archives != null) {
        Path[] localArchives = DistributedCache
          .getLocalCacheArchives(conf);
        if (localArchives != null){
          for (int i=0;i<archives.length;i++){
            for(int j=0;j<archiveClasspaths.length;j++){
              if (archives[i].getPath().equals(
                                               archiveClasspaths[j].toString())){
                classPath.append(sep);
                classPath.append(localArchives[i]
                                 .toString());
                // we found a match in classpath for this archive
                break;

              } // if archives[i] equals classpaths[j]
            }
          }
        }
      }

      archiveClasspaths = DistributedCache.getArchiveClassPaths(conf);
      if (archiveClasspaths != null && sharedArchives != null) {
        Path[] localArchives = DistributedCache
          .getLocalSharedCacheArchives(conf);
        if (localArchives != null){
          for (int i=0;i<sharedArchives.length;i++){
            int j=0;
            for(;j<archiveClasspaths.length;j++){
              if (sharedArchives[i].getPath().equals(
                    archiveClasspaths[j].toString())){
                if ((i==0) && shared && (localJar != null)) {
                  // sharedArchives[0] is the job jar file in shared mode
                  // we are honoring the contract to put job jar file the
                  // first thing in the classpath (i==0)
                  // we need to lock on job localization since we are doing
                  // unJar here
                  TaskTracker.RunningJob job = null;
                  synchronized (tracker.runningJobs) {
                    job = tracker.runningJobs.get(t.getJobID());
                  }
                  synchronized (job.localizationLock) {
                    // the job jar may already have been copied and unjarred.
                    // this can be detected by checking if the localjar file
                    // exists
                    if (! (new File(localJar)).exists()) {
                      Path localJarFile = new Path(localJar);
                      FileSystem localFs = FileSystem.getLocal(conf);

                      Path sourceJarFile = new Path(localArchives[0],
                                                    localArchives[0].getName());
                      localFs.copyToLocalFile(false, sourceJarFile,
                                              localJarFile);
                      RunJar.unJar(new File(localJar),
                                   new File(localJarFile.getParent().toString()));
                    }
                    addJobJarToClassPath(localJar, classPath);
                  }
                } else {
                  classPath.append(sep);
                  classPath.append(localArchives[i]
                      .toString());
                }

                // we found a match in classpath for this shared archive
                break;

              } // if sharedArchives[i] equals classpaths[j]
            }

            if ((i==0) && shared && (localJar != null) && (j==archiveClasspaths.length)) {
              LOG.warn ("Could not find match for job jar: " + sharedArchives[0].toString() +
                        " in classpath: " + Arrays.toString(archiveClasspaths));
              throw new IOException ("Error in localizing shared job jar");
            }

          }
        }
      }

      //file paths
      Path[] fileClasspaths = DistributedCache.getFileClassPaths(conf);
      if (fileClasspaths!=null && files != null) {
        Path[] localFiles = DistributedCache
          .getLocalCacheFiles(conf);
        if (localFiles != null) {
          for (int i = 0; i < files.length; i++) {
            for (int j = 0; j < fileClasspaths.length; j++) {
              if (files[i].getPath().equals(
                                            fileClasspaths[j].toString())) {
                classPath.append(sep);
                classPath.append(localFiles[i].toString());
              }
            }
          }
        }
      }

      fileClasspaths = DistributedCache.getFileClassPaths(conf);
      if (fileClasspaths!=null && sharedFiles != null) {
        Path[] localFiles = DistributedCache
          .getLocalSharedCacheFiles(conf);
        if (localFiles != null) {
          for (int i = 0; i < sharedFiles.length; i++) {
            for (int j = 0; j < fileClasspaths.length; j++) {
              if (sharedFiles[i].getPath().equals(
                                            fileClasspaths[j].toString())) {
                classPath.append(sep);
                classPath.append(localFiles[i].toString());
              }
            }
          }
        }
      }

      classPath.append(sep);
      classPath.append(workDir);

      if (userClassesTakesPrecedence) {
        // parent process's class paths added last
        appendSystemClasspath(conf, sep, classPath);
      }

      //  Build exec child jmv args.
      Vector<String> vargs = new Vector<String>(11);

      // Set a user-specified nice level for the Linux process of the task.
      // The motivation is to enable system processes to run despite
      // CPU-intensive tasks. We set this for both custom and default runner.
      String niceLevel = conf.get("mapred.task.nicelevel", "default");
      if (!niceLevel.equals("default")) {
        String[] niceCmd = Shell.getNiceCommand(niceLevel);
        if (niceCmd != null) {
          LOG.info("Added nice level " + niceLevel + " to task " +
              taskid.toString());
          for (String arg : niceCmd) {
            vargs.add(arg);
          }
        }
      }

      boolean useCustomRunner = ((conf.getCustomRunnerEnabled() &&
          conf.getCustomRunnerTaskRange(t.isMapTask()).isIncluded(t.getPartition())));
      if (useCustomRunner) {
        String javaRunnerCommand = conf.getCustomRunnerCommand();
        String taskLogDir = TaskLog.getBaseDir(taskid.toString()).getAbsolutePath();
        javaRunnerCommand = javaRunnerCommand.replace("@tasklogdir@", taskLogDir);
        LOG.info("Using custom runner " + javaRunnerCommand);
        for (String arg : javaRunnerCommand.split("\\s+")) {
          vargs.add(arg);
        }
      } else {
        File jvm =                                  // use same jvm as parent
          new File(new File(System.getProperty("java.home"), "bin"), "java");
        vargs.add(jvm.toString());
      }

      // Add child (task) java-vm options.
      //
      // The following symbols if present in mapred.{map|reduce}.child.java.opts
      // value are replaced:
      // + @taskid@ is interpolated with value of TaskID.
      // Other occurrences of @ will not be altered.
      //
      // Example with multiple arguments and substitutions, showing
      // jvm GC logging, and start of a passwordless JVM JMX agent so can
      // connect with jconsole and the likes to watch child memory, threads
      // and get thread dumps.
      //
      //  <property>
      //    <name>mapred.map.child.java.opts</name>
      //    <value>-Xmx 512M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
      //           -Dcom.sun.management.jmxremote.authenticate=false \
      //           -Dcom.sun.management.jmxremote.ssl=false \
      //    </value>
      //  </property>
      //
      //  <property>
      //    <name>mapred.reduce.child.java.opts</name>
      //    <value>-Xmx 1024M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
      //           -Dcom.sun.management.jmxremote.authenticate=false \
      //           -Dcom.sun.management.jmxremote.ssl=false \
      //    </value>
      //  </property>
      //
      String javaOpts = getChildJavaOpts(conf,
                                         JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS);
      javaOpts = javaOpts.replace("@taskid@", taskid.toString());

      String adminJavaOpts = getAdminChildJavaOpts(conf);
      adminJavaOpts = adminJavaOpts.replace("@taskid@", taskid.toString());

      javaOpts = javaOpts + " " + adminJavaOpts;
      String [] javaOptsSplit = javaOpts.split(" +");

      // Add java.library.path; necessary for loading native libraries.
      //
      // 1. To support native-hadoop library i.e. libhadoop.so, we add the
      //    parent processes' java.library.path to the child.
      // 2. We also add the 'cwd' of the task to it's java.library.path to help
      //    users distribute native libraries via the DistributedCache.
      // 3. The user can also specify extra paths to be added to the
      //    java.library.path via mapred.{map|reduce}.child.java.opts.
      //
      String libraryPath = System.getProperty("java.library.path");
      if (libraryPath == null) {
        libraryPath = workDir.getAbsolutePath();
      } else {
        libraryPath += sep + workDir;
      }
      boolean hasUserLDPath = false;
      for(int i=0; i<javaOptsSplit.length ;i++) {
        if(javaOptsSplit[i].startsWith("-Djava.library.path=")) {
          javaOptsSplit[i] += sep + libraryPath;
          hasUserLDPath = true;
          break;
        }
      }
      if(!hasUserLDPath) {
        vargs.add("-Djava.library.path=" + libraryPath);
      }
      for (int i = 0; i < javaOptsSplit.length; i++) {
        vargs.add(javaOptsSplit[i]);
      }

      // add java.io.tmpdir given by mapred.child.tmp
      String tmp = conf.get("mapred.child.tmp", "./tmp");
      Path tmpDir = new Path(tmp);

      // if temp directory path is not absolute
      // prepend it with workDir.
      if (!tmpDir.isAbsolute()) {
        tmpDir = new Path(workDir.toString(), tmp);
      }
      FileSystem localFs = FileSystem.getLocal(conf);
      if (!localFs.mkdirs(tmpDir) && !localFs.getFileStatus(tmpDir).isDir()) {
        throw new IOException("Mkdirs failed to create " + tmpDir.toString());
      }
      vargs.add("-Djava.io.tmpdir=" + tmpDir.toString());

      // Add classpath.
      vargs.add("-classpath");
      vargs.add(classPath.toString());

      // Setup the log4j prop
      long logSize = TaskLog.getTaskLogLength(conf);
      vargs.add("-Dhadoop.log.dir=" +
          new File(System.getProperty("hadoop.log.dir")
          ).getAbsolutePath());
      boolean logToScribe = conf.getBoolean("mapred.task.log.scribe", false);
      if (logToScribe) {
        vargs.addAll(conf.getStringCollection("mapred.task.log.scribe.conf"));
      }
      String logger = logToScribe ? "INFO,TLA,scribe" : "INFO,TLA";

      vargs.add("-Dhadoop.root.logger=" + logger);
      vargs.add("-Dhadoop.tasklog.taskid=" + taskid);
      vargs.add("-Dhadoop.tasklog.totalLogFileSize=" + logSize);

      boolean shouldProfile = (tracker.getProfileAllTasks() ||
        (conf.getProfileEnabled() &&
          conf.getProfileTaskRange(t.isMapTask()).isIncluded(t.getPartition())));
      if (shouldProfile) {
        File prof = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.PROFILE);
        vargs.add(String.format(conf.getProfileParams(), prof.toString()));
      }

      // Add main class and its arguments
      vargs.add(conf.getTaskRunnerChildClassName());  // main of Child
      // pass umbilical address
      InetSocketAddress address = tracker.getTaskTrackerReportAddress();
      vargs.add(address.getAddress().getHostAddress());
      vargs.add(Integer.toString(address.getPort()));
      vargs.add(taskid.toString());                      // pass task identifier

      tracker.addToMemoryManager(t.getTaskID(), t.isMapTask(), conf);

      // set memory limit using ulimit if feasible and necessary ...
      String[] ulimitCmd = Shell.getUlimitMemoryCommand(getChildUlimit(conf));
      List<String> setup = null;
      if (ulimitCmd != null) {
        setup = new ArrayList<String>();
        for (String arg : ulimitCmd) {
          setup.add(arg);
        }
      }

      // Set up the redirection of the task's stdout and stderr streams
      File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
      File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);
      File parent = stdout.getParentFile();
      parent.mkdirs();
      LOG.info("Task log directory " + parent);
      tracker.getTaskTrackerInstrumentation().reportTaskLaunch(taskid, stdout, stderr);

      Map<String, String> env = new HashMap<String, String>();
      StringBuffer ldLibraryPath = new StringBuffer();
      ldLibraryPath.append(workDir.toString());
      String oldLdLibraryPath = null;
      oldLdLibraryPath = System.getenv("LD_LIBRARY_PATH");
      if (oldLdLibraryPath != null) {
        ldLibraryPath.append(sep);
        ldLibraryPath.append(oldLdLibraryPath);
      }
      env.put("LD_LIBRARY_PATH", ldLibraryPath.toString());

      // add the env variables passed by the user
      String mapredChildEnv = getChildEnv(conf);
      if (mapredChildEnv != null && mapredChildEnv.length() > 0) {
        String childEnvs[] = mapredChildEnv.split(",");
        for (String cEnv : childEnvs) {
          try {
            String[] parts = cEnv.split("="); // split on '='
            String value = env.get(parts[0]);
            if (value != null) {
              // replace $env with the child's env constructed by tt's
              // example LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp
              value = parts[1].replace("$" + parts[0], value);
            } else {
              // this key is not configured by the tt for the child .. get it
              // from the tt's env
              // example PATH=$PATH:/tmp
              value = System.getenv(parts[0]);
              if (value != null) {
                // the env key is present in the tt's env
                value = parts[1].replace("$" + parts[0], value);
              } else {
                // the env key is note present anywhere .. simply set it
                // example X=$X:/tmp or X=/tmp
                value = parts[1].replace("$" + parts[0], "");
              }
            }
            env.put(parts[0], value);
          } catch (Throwable t) {
            // set the error msg
            errorInfo = "Invalid User environment settings : " + mapredChildEnv
                        + ". Failed to parse user-passed environment param."
                        + " Expecting : env1=value1,env2=value2...";
            LOG.warn(errorInfo);
            throw t;
          }
        }
      }
      JvmManager.JvmEnv jvmEnv = jvmManager.constructJvmEnv(setup,vargs,stdout,stderr,logSize,
          workDir, env, conf);

      long delayedMs = delayStartingTask(conf);
      LOG.info("Running task " + taskid +
          ((delayedMs == 0) ? "" : " after a delay of " + delayedMs + " ms ") +
          " in the jvm " + jvmEnv);
      jvmManager.launchJvm(this, jvmEnv);
      synchronized (lock) {
        while (!done) {
          lock.wait();
        }
      }
      tracker.getTaskTrackerInstrumentation().reportTaskEnd(t.getTaskID());
      if (exitCodeSet) {
        if (!killed && exitCode != 0) {
          if (exitCode == 65) {
            tracker.getTaskTrackerInstrumentation().taskFailedPing(t.getTaskID());
          }
          throw new IOException("Task process exit with nonzero status of " +
              exitCode + ".");
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError", e);
      try {
        tracker.fsError(t.getTaskID(), e.getMessage());
      } catch (IOException ie) {
        LOG.fatal(t.getTaskID()+" reporting FSError", ie);
      }
    } catch (Throwable throwable) {
      if (tracker.isKilledByCGroup(t.getTaskID())) {
          LOG.info(t.getTaskID() + " is killed by CGroup");
      } else { 
        LOG.warn(t.getTaskID() + errorInfo, throwable);
        Throwable causeThrowable = new Throwable(errorInfo, throwable);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        causeThrowable.printStackTrace(new PrintStream(baos));
        try {
          tracker.reportDiagnosticInfo(t.getTaskID(), baos.toString());
        } catch (IOException e) {
          LOG.warn(t.getTaskID()+" Reporting Diagnostics", e);
        }
      }
    } finally {
      try{
        for (CacheFile cf : localizedCacheFiles) {
            DistributedCache.releaseCache(cf.uri, conf, cf.timeStamp);
        }
      }catch(IOException ie){
        LOG.warn("Error releasing caches : " +
            "Cache files might not have been cleaned up");
      }

      // It is safe to call TaskTracker.TaskInProgress.reportTaskFinished with
      // *false* since the task has either
      // a) SUCCEEDED - which means commit has been done
      // b) FAILED - which means we do not need to commit
      tip.reportTaskFinished(false);
    }
  }

  /**
   * Makes dir empty directory(does not delete dir itself).
   */
  static void deleteDirContents(JobConf conf, File dir) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(new Path(dir.getAbsolutePath()))) {
      File contents[] = dir.listFiles();
      if (contents != null) {
        for (int i = 0; i < contents.length; i++) {
          if (!fs.delete(new Path(contents[i].getAbsolutePath()), true)) {
            LOG.warn("Unable to delete "+ contents[i]);
          }
        }
      }
    }
    else {
      LOG.warn(dir + " does not exist.");
    }
  }

  //Mostly for setting up the symlinks. Note that when we setup the distributed
  //cache, we didn't create the symlinks. This is done on a per task basis
  //by the currently executing task.
  public static void setupWorkDir(JobConf conf) throws IOException {
    File workDir = new File(".").getAbsoluteFile();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fully deleting contents of " + workDir);
    }

    /** delete only the contents of workDir leaving the directory empty. We
     * can't delete the workDir as it is the current working directory.
     */
    deleteDirContents(conf, workDir);

    if (DistributedCache.getSymlink(conf)) {
      URI[] archives = DistributedCache.getCacheArchives(conf);
      URI[] files = DistributedCache.getCacheFiles(conf);
      URI[] sharedArchives = DistributedCache.getSharedCacheArchives(conf);
      URI[] sharedFiles = DistributedCache.getSharedCacheFiles(conf);

      Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
      Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
      Path[] localSharedArchives = DistributedCache.getLocalSharedCacheArchives(conf);
      Path[] localSharedFiles = DistributedCache.getLocalSharedCacheFiles(conf);

      if (archives != null) {
        for (int i = 0; i < archives.length; i++) {
          String link = archives[i].getFragment();
          if (link != null) {
            link = workDir.toString() + Path.SEPARATOR + link;
            File flink = new File(link);
            if (!flink.exists()) {
              FileUtil.symLink(localArchives[i].toString(), link);
            }
          }
        }
      }
      if (sharedArchives != null) {
        for (int i = 0; i < sharedArchives.length; i++) {
          String link = sharedArchives[i].getFragment();
          if (link != null) {
            // Remove md5 prefix: 2 chars per byte of MD5, plus 1 underscore
            link = link.substring(MD5Hash.MD5_LEN * 2 + 1);
            link = workDir.toString() + Path.SEPARATOR + link;
            File flink = new File(link);
            if (!flink.exists()) {
              FileUtil.symLink(localSharedArchives[i].toString(), link);
            }
          }
        }
      }
      if (files != null) {
        for (int i = 0; i < files.length; i++) {
          String link = files[i].getFragment();
          if (link != null) {
            link = workDir.toString() + Path.SEPARATOR + link;
            File flink = new File(link);
            if (!flink.exists()) {
              FileUtil.symLink(localFiles[i].toString(), link);
            }
          }
        }
      }
      if (sharedFiles != null) {
        for (int i = 0; i < sharedFiles.length; i++) {
          String link = sharedFiles[i].getFragment();
          if (link != null) {
            // Remove md5 prefix: 2 chars per byte of MD5, plus 1 underscore
            link = link.substring(MD5Hash.MD5_LEN * 2 + 1);
            link = workDir.toString() + Path.SEPARATOR + link;
            File flink = new File(link);
            if (!flink.exists()) {
              FileUtil.symLink(localSharedFiles[i].toString(), link);
            }
          }
        }
      }
    }
    File jobCacheDir = null;
    if (conf.getJar() != null) {
      jobCacheDir = new File(
          new Path(conf.getJar()).getParent().toString());
    }

    // create symlinks for all the files in job cache dir in current
    // workingdir for streaming
    try{
      DistributedCache.createAllSymlink(conf, jobCacheDir,
          workDir);
    } catch(IOException ie){
      // Do not exit even if symlinks have not been created.
      LOG.warn(StringUtils.stringifyException(ie));
    }
    // add java.io.tmpdir given by mapred.child.tmp
    String tmp = conf.get("mapred.child.tmp", "./tmp");
    Path tmpDir = new Path(tmp);

    // if temp directory path is not absolute
    // prepend it with workDir.
    if (!tmpDir.isAbsolute()) {
      tmpDir = new Path(workDir.toString(), tmp);
      FileSystem localFs = FileSystem.getLocal(conf);
      if (!localFs.mkdirs(tmpDir) && !localFs.getFileStatus(tmpDir).isDir()){
        throw new IOException("Mkdirs failed to create " + tmpDir.toString());
      }
    }
  }

  /**
   * Kill the child process
   */
  public void kill() {
    killed = true;
    jvmManager.taskKilled(this);
    signalDone();
  }
  public void signalDone() {
    synchronized (lock) {
      done = true;
      lock.notify();
    }
  }
  public void setExitCode(int exitCode) {
    this.exitCodeSet = true;
    this.exitCode = exitCode;
  }
}

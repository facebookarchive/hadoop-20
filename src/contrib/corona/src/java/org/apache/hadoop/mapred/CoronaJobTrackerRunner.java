package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.CoronaFailureEvent;
import org.apache.hadoop.util.CoronaFailureEventInjector;
import org.apache.hadoop.util.Shell;

public class CoronaJobTrackerRunner extends TaskRunner {
  @SuppressWarnings("unused")
  private final CoronaSessionInfo coronaSessionInfo;
  private final File workDir;
  private final String originalPath;
  private final String releasePath;
  private Path localizedJobFile;
  @SuppressWarnings("deprecation")
  public CoronaJobTrackerRunner(
      TaskTracker.TaskInProgress tip, Task task, TaskTracker tracker,
      JobConf ttConf, CoronaSessionInfo info, String originalPath,
      String releasePath) throws IOException {
    super(tip, task, tracker, ttConf);
    this.coronaSessionInfo = info;
    this.originalPath = originalPath;
    this.releasePath = releasePath;
    LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

    workDir = new File(lDirAlloc.getLocalPathForWrite(
        TaskTracker.getLocalTaskDir(
            task.getJobID().toString(),
            task.getTaskID().toString(),
            task.isTaskCleanupTask())
            + Path.SEPARATOR + MRConstants.WORKDIR,
            conf). toString());
    if (!workDir.mkdirs()) {
      if (!workDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + workDir.toString());
      }
    }
    localizeTaskConfiguration(tracker, ttConf, workDir.toString(), task, task
        .getJobID());
  }

  /**
   * Copies the job file to the working directory of the process that will be
   * started.
   */
  @SuppressWarnings("deprecation")
  private void localizeTaskConfiguration(TaskTracker tracker, JobConf ttConf,
      String workDir, Task t, JobID jobID) throws IOException {
    Path jobFile = new Path(t.getJobFile());
    FileSystem systemFS = tracker.systemFS;
    this.localizedJobFile = new Path(workDir, jobID + ".xml");
    LOG.info("Localizing CJT configuration from " + jobFile + " to " +
        localizedJobFile);
    systemFS.copyToLocalFile(jobFile, localizedJobFile);
    JobConf localJobConf = new JobConf(localizedJobFile);
    boolean modified = Task.saveStaticResolutions(localJobConf);
    if (modified) {
      FSDataOutputStream out = new FSDataOutputStream(
        new FileOutputStream(localizedJobFile.toUri().getPath()));
      try {
        localJobConf.writeXml(out);
      } catch (IOException e) {
        out.close();
        throw e;
      }
    }
    // Add the values from the job conf to the configuration of this runner
    this.conf.addResource(localizedJobFile);
  }

  /** Delete any temporary files from previous failed attempts. */
  @Override
  public boolean prepare() throws IOException {
    if (!super.prepare()) {
      return false;
    }

    mapOutputFile.removeAll(getTask().getTaskID());
    return true;
  }

  /** Delete all of the temporary map output files. */
  @Override
  public void close() throws IOException {
    LOG.info(getTask()+" done; removing files.");
    mapOutputFile.removeAll(getTask().getTaskID());
  }

  private String getCJTJavaOpts(JobConf conf) {
    return conf.get("mapred.corona.standalonecjt.java.opts",
                       JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS);
  }
  
  private static File getJobStdLogFile(TaskAttemptID taskid, TaskLog.LogName filter) {
    return new File(
      CoronaTaskTracker.jobTrackerLogDir() + File.separator + "userlogs" + 
      File.separator+ taskid.toString() + File.separator + filter.toString()
    );
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public void run() {
    Task task = getTask();
    TaskAttemptID taskid = task.getTaskID();
    try {
      if (!prepare()) {
        return;
      }

      String sep = System.getProperty("path.separator");
      StringBuffer classPath = new StringBuffer();
      // The alternate runtime can be used to debug tasks by putting a
      // custom version of the mapred libraries. This will get loaded before
      // the TT's jars.
      String debugRuntime = conf.get("mapred.task.debug.runtime.classpath");
      if (debugRuntime != null) {
        classPath.append(debugRuntime);
        classPath.append(sep);
      }
      // start with same classpath as parent process

      String systemClassPath = System.getProperty("java.class.path");
      if (releasePath != null && !releasePath.isEmpty() &&
        originalPath != null && !releasePath.isEmpty()) {
        systemClassPath = systemClassPath.replaceAll(originalPath, releasePath);
      }
      classPath.append(systemClassPath);
      classPath.append(sep);
      //  Build exec child jmv args.
      Vector<String> vargs = new Vector<String>(8);
      File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");

      vargs.add(jvm.toString());

      // Add child (task) java-vm options.
      String javaOpts = getCJTJavaOpts(conf);
      javaOpts = javaOpts.replace("@taskid@", taskid.toString());
      String [] javaOptsSplit = javaOpts.split(" ");
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
      vargs.add("-Dhadoop.log.dir=" + CoronaTaskTracker.jobTrackerLogDir());
      boolean logToScribe = conf.getBoolean("mapred.task.log.scribe", false);
      if (logToScribe) {
        vargs.addAll(conf.getStringCollection("mapred.task.log.scribe.conf"));
      }
      String logger = logToScribe ? "INFO,TLA,scribe" : "INFO,TLA";

      vargs.add("-Dhadoop.root.logger=" + logger);
      vargs.add("-Dhadoop.tasklog.taskid=" + taskid);
      vargs.add("-Dhadoop.tasklog.totalLogFileSize=" + logSize);

      Path systemDirectory = tracker.systemDirectory;
      if (!systemDirectory.isAbsolute()) {
        systemDirectory = new Path(tracker.systemFS.getWorkingDirectory(),
            systemDirectory);
      }
      systemDirectory = systemDirectory.makeQualified(tracker.systemFS);
      vargs.add("-Dmapred.system.dir=" + systemDirectory);

      // Add main class and its arguments
      vargs.add(CoronaJobTracker.class.getName());  // main of CJT
      vargs.add(task.getJobID().toString());      // Pass job id.
      vargs.add(task.getTaskID().toString()); // Pass attempt id.
      vargs.add(coronaSessionInfo.getJobTrackerAddr().getHostName());
      vargs.add(Integer.toString(coronaSessionInfo.getJobTrackerAddr()
        .getPort()));
      // add the task log http server host and port
      vargs.add(this.tracker.localHostname);
      vargs.add(Integer.toString(this.tracker.httpPort));
      
      // inject the job tracker failure event if needed
      if (this.tracker instanceof CoronaTaskTracker) {
        CoronaFailureEventInjector jtEventInjector = 
            ((CoronaTaskTracker)this.tracker).jtFailureEventInjector;
        if (jtEventInjector != null) {
          LOG.info("Injecting failure event to RJT");
          CoronaFailureEvent failureEvent = jtEventInjector.pollFailureEvent();
          if (failureEvent != null) {
            LOG.info("Injecting failure event " + failureEvent.toString());
            vargs.add(failureEvent.toString());
          }
        }
      }
     //vargs.add("2:0");
     
      tracker.addToMemoryManager(task.getTaskID(), task.isMapTask(), conf,
        false);

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
      File stdout = getJobStdLogFile(taskid, TaskLog.LogName.STDOUT);
      File stderr = getJobStdLogFile(taskid, TaskLog.LogName.STDERR);
      stdout.getParentFile().mkdirs();

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

      LOG.info("Launching CJT " + taskid + " in working directory " + workDir +
        " Command line " + vargs);

      jvmManager.launchJvm(this,
          jvmManager.constructJvmEnv(setup,vargs,stdout,stderr,logSize,
              workDir, env, conf));
      synchronized (lock) {
        while (!done) {
          lock.wait();
        }
      }
    } catch (IOException e) {
      LOG.error("Error while launching CJT ", e);
    } catch (InterruptedException e) {
      LOG.warn("Error while launching CJT ", e);
    }
  }

}

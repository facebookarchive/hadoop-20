package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.CoronaDirectTaskUmbilical.VersionedProtocolPointer;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;

public class CoronaChild extends Child {
  /** List of proxies to close on cleanup */
  static List<VersionedProtocolPointer> proxiesCreated =
      new ArrayList<VersionedProtocolPointer>();

  public static void main(String[] args) throws Throwable {
    LOG.info("Corona Child starting");

    JobConf defaultConf = new JobConf();
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    InetSocketAddress address = new InetSocketAddress(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    final int SLEEP_LONGER_COUNT = 5;
    int jvmIdInt = Integer.parseInt(args[3]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),firstTaskid.isMap(),jvmIdInt);
    UserGroupInformation ticket = UserGroupInformation.login(defaultConf);
    int timeout = defaultConf.getInt("mapred.socket.timeout", 60000);
    TaskUmbilicalProtocol umbilical =
      ((ProtocolProxy<TaskUmbilicalProtocol>) RPC.getProtocolProxy(
          TaskUmbilicalProtocol.class,
          TaskUmbilicalProtocol.versionID,
          address,
          ticket,
          defaultConf,
          NetUtils.getDefaultSocketFactory(defaultConf),
          timeout)).getProxy();
    proxiesCreated.add(new VersionedProtocolPointer(umbilical));
    int numTasksToExecute = -1; //-1 signifies "no limit"
    int numTasksExecuted = 0;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (taskid != null) {
            TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
          }
        } catch (Throwable throwable) {
        }
      }
    });
    Thread t = new Thread() {
      public void run() {
        //every so often wake up and syncLogs so that we can track
        //logs of the currently running task
        while (true) {
          try {
            Thread.sleep(5000);
            if (taskid != null) {
              TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
            }
          } catch (InterruptedException ie) {
          } catch (IOException iee) {
            LOG.error("Error in syncLogs: " + iee);
            System.exit(-1);
          }
        }
      }
    };
    t.setName("Thread for syncLogs");
    t.setDaemon(true);
    t.start();
    
    String pid = "";
    if (!Shell.WINDOWS) {
      pid = System.getenv().get("JVM_PID");
    }
    JvmContext context = new JvmContext(jvmId, pid);
    int idleLoopCount = 0;
    Task task = null;
    try {
      while (true) {
        taskid = null;
        JvmTask myTask = umbilical.getTask(context);
        if (myTask.shouldDie()) {
          break;
        } else {
          if (myTask.getTask() == null) {
            taskid = null;
            if (++idleLoopCount >= SLEEP_LONGER_COUNT) {
              //we sleep for a bigger interval when we don't receive
              //tasks for a while
              Thread.sleep(1500);
            } else {
              Thread.sleep(500);
            }
            continue;
          }
        }
        idleLoopCount = 0;
        task = myTask.getTask();
        taskid = task.getTaskID();
        isCleanup = task.isTaskCleanupTask();
        // reset the statistics for the task
        FileSystem.clearStatistics();

        //create the index file so that the log files 
        //are viewable immediately
        TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
        
        //setupWorkDir actually sets up the symlinks for the distributed
        //cache. After a task exits we wipe the workdir clean, and hence
        //the symlinks have to be rebuilt.
        JobConf job = new JobConf(task.getJobFile());
        
        //read from bigParam if it exists
        String bigParamStr = job.get("mapred.bigparam.path", "");
        if ( bigParamStr != null && bigParamStr.length() > 0) {
          Path bigParamPath = new Path(bigParamStr);
          File file = new File(bigParamPath.toUri().getPath()).getAbsoluteFile();
          BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
          int MAX_BUFFER_SIZE = 1024; 

          StringBuilder result = new StringBuilder(); 
          char[] buffer = new char[MAX_BUFFER_SIZE];
          int readChars = 0, totalChars = 0;
          while ((readChars = in.read(buffer, 0, MAX_BUFFER_SIZE)) > 0) {
            result.append(buffer, 0, readChars);
            totalChars += readChars;
          }
          job.set("mapred.input.dir", result.toString());
          LOG.info("Read mapred.input.dir: " + totalChars);
          in.close();
        }
        
        TaskRunner.setupWorkDir(job);

        {
          // Use conf written by CoronaTaskTracker.reconfigureLocalJobConf()
          // for some reason attempt conf is not updated between tasks
          // and DirectTaskUmbilical c'tor is falling back to local JT
          LocalDirAllocator lDir = new LocalDirAllocator("mapred.local.dir");
          Path umbilicalConfFile = lDir.getLocalPathToRead(
              CoronaTaskTracker.getLocalJobDir(task.getJobID().toString())
                  + Path.SEPARATOR + "job.xml", job);
          JobConf umbilicalConf = new JobConf(umbilicalConfFile);
          umbilical = convertToDirectUmbilicalIfNecessary(umbilical,
              umbilicalConf, task);
        }

        numTasksToExecute = job.getNumTasksToExecutePerJvm();
        assert(numTasksToExecute != 0);

        task.setConf(job);

        defaultConf.addResource(new Path(task.getJobFile()));

        // Initiate Java VM metrics
        JvmMetrics.init(task.getPhase().toString(), job.getSessionId());
        // use job-specified working directory
        FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
        try {
          task.run(job, umbilical);             // run the task
        } finally {
          TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
        }
        if (numTasksToExecute > 0 && ++numTasksExecuted == numTasksToExecute) {
          break;
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskid, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Error running child", exception);
      try {
        if (task != null) {
          // do cleanup for the task
          task.taskCleanup(umbilical);
        }
      } catch (Exception e) {
        LOG.info("Error cleaning up" + e);
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskid != null) {
        umbilical.reportDiagnosticInfo(taskid, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
                + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        Throwable tCause = throwable.getCause();
        String cause = tCause == null 
                       ? throwable.getMessage() 
                       : StringUtils.stringifyException(tCause);
        umbilical.fatalError(taskid, cause);
      }
    } finally {
      for (VersionedProtocolPointer proxy : proxiesCreated) {
        RPC.stopProxy(proxy.getClient());
      }
      MetricsContext metricsContext = MetricsUtil.getContext("mapred");
      metricsContext.close();
      // Shutting down log4j of the child-vm... 
      // This assumes that on return from Task.run() 
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  private static TaskUmbilicalProtocol convertToDirectUmbilicalIfNecessary(
      TaskUmbilicalProtocol umbilical, JobConf job, Task task) throws IOException {
    // We only need a direct umbilical for reducers.
    if (task.isMapTask()) {
      return umbilical;
    }
    InetSocketAddress directAddress = CoronaDirectTaskUmbilical.getAddress(
        job, CoronaDirectTaskUmbilical.DIRECT_UMBILICAL_JT_ADDRESS);
    InetSocketAddress secondaryAddress = CoronaDirectTaskUmbilical
        .getAddress(job, CoronaDirectTaskUmbilical.DIRECT_UMBILICAL_FALLBACK_ADDRESS);
    if (directAddress != null) {
      CoronaDirectTaskUmbilical direct = CoronaDirectTaskUmbilical.createDirectUmbilical(
          umbilical, directAddress, secondaryAddress, job);
      proxiesCreated.addAll(direct.getCreatedProxies());
      return direct;
    }
    return umbilical;
  }
}

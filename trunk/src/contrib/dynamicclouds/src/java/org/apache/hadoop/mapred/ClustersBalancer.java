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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.util.ajax.JSON;

/**
 *
 * @author dms
 */
public class ClustersBalancer extends Configured implements Tool {

  private static int MAX_START_FAILURES = 3;
  private static final String SELECT_IDLE_MACHINES_OPTION =
          "-select-idle-machines";
  private static String START_TASKTRACKER_COMMAND_21 =
          "bin/hadoop-daemon.sh --script bin/mapred start tasktracker";
  private static String START_TASKTRACKER_COMMAND_20 =
          "bin/hadoop-daemon.sh start tasktracker";
  private boolean moveMachines = false;
  private boolean displayReport = false;
  private boolean selectMachines = false;
  private String fromClusterHadoopHome = null;
  private String toClusterHadoopHome = null;


  public void displayUsage(String cmd) {
    String prefix = "Usage: ClustersBalancer ";
    if (cmd.equals("-move")) {
      System.err.println(prefix + "[" + cmd + "<from-job-tracker-url> " +
              "<to-job-tracker-url> <#-of-machines> " +
              "[<jobtracker-bin-location>]]");
    } else if (cmd.equals("-report")) {
      System.err.println(prefix + "[" + cmd + "<job-tracker-url>]");
    } else if (cmd.equals(SELECT_IDLE_MACHINES_OPTION)) {
      System.err.println(prefix + "[" + cmd + "<job-tracker-url> " +
              "<#-of-machines>]");
    } else {
      System.err.println(prefix + "<command> <args>");
      System.err.println("\t[-move <from-job-tracker-url> " +
              "<to-job-tracker-url> <#-of-machines> " +
              "[<jobtracker-bin-location>]]");
      System.err.println("\t[-report <job-tracker-url>]");
      System.err.println("\t[" + SELECT_IDLE_MACHINES_OPTION +
              " <job-tracker-url> <#-of-machines>]");
    }
  }

  private Object getJSONObject(URL jsonURL)
          throws IOException {
    URLConnection conn = jsonURL.openConnection();

    return JSON.parse(
            new BufferedReader(new InputStreamReader(
            conn.getInputStream())));
  }

  Map<String, String> getJobTrackerConf(String jobTrackerUrl)
          throws IOException {

    URL jobTrackerStatusJsp = new URL(jobTrackerUrl +
            "/jobtrackersdetailsjson.jsp?jobTrackerConf=1");

    Map<String, String> confFilesLocation =
            (Map<String, String>) getJSONObject(jobTrackerStatusJsp);

    return confFilesLocation;
  }

  List<TaskTrackerLoadInfo> getJobTrackerStatus(String jobTrackerUrl)
          throws IOException {

    List<TaskTrackerLoadInfo> result = new ArrayList<TaskTrackerLoadInfo>();

    URL jobTrackerStatusJsp = new URL(jobTrackerUrl +
            "/jobtrackersdetailsjson.jsp?status=1");

    Map<String, Object> trackersStatus =
            (Map<String, Object>) getJSONObject(jobTrackerStatusJsp);

    for (String taskTrackerName : trackersStatus.keySet()) {
      Map<String, Object> trackerInfo =
              (Map<String, Object>) trackersStatus.get(taskTrackerName);

      TaskTrackerLoadInfo status = new TaskTrackerLoadInfo(taskTrackerName);
      status.parseMap(trackerInfo);

      result.add(status);
    }

    return result;
  }

  private TaskTrackerLoadInfoIterator selectNMostIdleMachines(
          List<TaskTrackerLoadInfo> trackers) {
    TaskTrackerLoadInfoIterator iterator = new WastedTimeTTLIIterator();
    iterator.setTrackers(trackers);
    return iterator;
  }

  private int selectIdleMachines(String fromCluster, int numMachines)
          throws Exception {

    int exitCode = 0;

    List<TaskTrackerLoadInfo> trackers = getJobTrackerStatus(fromCluster);

    TaskTrackerLoadInfoIterator idleTrackersIterator =
            selectNMostIdleMachines(trackers);
    int machines = 0;
    while(idleTrackersIterator.hasNext() && machines < numMachines) {
      machines++;
      TaskTrackerLoadInfo tracker = idleTrackersIterator.next();
      System.out.println(tracker.toString());
    }

    return exitCode;
  }

  private int moveMachines(String fromCluster, String toCluster,
          int numMachines) throws Exception {
    int exitCode = 0;
    List<TaskTrackerLoadInfo> trackers = getJobTrackerStatus(fromCluster);
    TaskTrackerLoadInfoIterator forDecommission =
            selectNMostIdleMachines(trackers);

    Map<String, String> jobTrackerConf = getJobTrackerConf(fromCluster);



    // Figure out where the hadoop home is
    String hadoopHome;
    if (fromClusterHadoopHome != null) {
      hadoopHome = fromClusterHadoopHome;
    } else {
      File slavesFile = new File(jobTrackerConf.get("slaves.file"));
      // By default slaves should be in HADOOP_HOME/conf/slaves
      hadoopHome = slavesFile.getParentFile().getParent();

    }
    int moved = 0;
    URL fromClusterUrl = new URL(fromCluster);
    URL toClusterUrl = new URL(toCluster);
    
    while (forDecommission.hasNext() && moved < numMachines) {

      moved++;
      TaskTrackerLoadInfo tracker = forDecommission.next();
      String host = tracker.getTaskTrackerHost();

      ShellCommandExecutor removeHostCommand = new ShellCommandExecutor(
              new String[]{"ssh",
                fromClusterUrl.getHost(),
                "cd " + hadoopHome + " && " +
                "bin/hadoop " +
                TTMover.class.getCanonicalName() +
                " -remove " + host
              });

      

      jobTrackerConf = getJobTrackerConf(toCluster);
      if (toClusterHadoopHome != null) {
        hadoopHome = toClusterHadoopHome;
      } else {
        File slavesFile = new File(jobTrackerConf.get("slaves.file"));
        // By default slaves should be in HADOOP_HOME/conf/slaves
        hadoopHome = slavesFile.getParentFile().getParent();
      }


      ShellCommandExecutor addHostCommand = new ShellCommandExecutor(
              new String[]{
                "ssh",
                toClusterUrl.getHost(),
                "cd " + hadoopHome + " && " +
                "bin/hadoop " + TTMover.class.getCanonicalName() +
                " -add " + host
              });
      



      String startCommand = getStartCommand(jobTrackerConf.get("version"));
      ShellCommandExecutor startTTCommand = new ShellCommandExecutor(
              new String[]{"ssh",
                host,
                "cd " + hadoopHome + " && " +
                startCommand});


      System.out.println(removeHostCommand.toString());
      removeHostCommand.execute();

      
      System.out.println(addHostCommand.toString());
      addHostCommand.execute();

      int numFailures = 0;
      int sleepInterval = MRConstants.HEARTBEAT_INTERVAL_MIN * 2;
      while (numFailures < MAX_START_FAILURES) {
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException ex) {
        }

        try {
          System.out.println(startTTCommand.toString());
          startTTCommand.execute();
          break;
        } catch (IOException ex) {
          numFailures++;
          sleepInterval *= 2;
        }
      }
      
      
    }
    System.out.println("Moved " + moved + " hosts from cluster " +
            fromCluster + " to cluster " + toCluster);

    return exitCode;
  }

  private String getStartCommand(String version) {
    if (version.contains("0.20")) {
      return START_TASKTRACKER_COMMAND_20;
    } else if (version.contains("0.21")) {
      return START_TASKTRACKER_COMMAND_21;
    } else {
      return START_TASKTRACKER_COMMAND_20;
    }
  }

  private static UnixUserGroupInformation getUGI(
          Configuration conf)
          throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException(
              "Failed to get the current user's information.").initCause(e));
    }

    return ugi;
  }

  private int displayReport(String jobTrackerUrl) throws IOException {
    int exitCode = 0;

    List<TaskTrackerLoadInfo> trackers = getJobTrackerStatus(jobTrackerUrl);

    int totalMapCapacity = 0;
    int totalReduceCapacity = 0;
    int runningMaps = 0;
    int runningReducers = 0;
    for (TaskTrackerLoadInfo tracker : trackers) {
      runningMaps += tracker.getRunningMapTasks();
      runningReducers += tracker.getRunningReduceTasks();
      totalMapCapacity += tracker.getMaxMapTasks();
      totalReduceCapacity += tracker.getMaxReduceTasks();
    }

    System.err.println("JobTracker is operating at " +
            ((int) (runningMaps * 100.0 / totalMapCapacity)) + "% map load, " +
            ((int) (runningReducers * 100.0 / totalReduceCapacity)) +
            "% reduce load.");

    return exitCode;
  }

  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    if (argv.length < 1) {
      displayUsage("");
      return exitCode;
    }

    String cmd = argv[0];
    String firstCluster = null;
    String toCluster = null;
    int machines = 0;

    if (cmd.equals("-move")) {
      if (argv.length != 4 &&
              argv.length != 5) {
        displayUsage(cmd);
        return exitCode;
      }

      moveMachines = true;
      firstCluster = argv[1];
      toCluster = argv[2];
      machines = Integer.valueOf(argv[3]);
      if (argv.length == 5) {
        fromClusterHadoopHome = argv[4];
      }
    } else if (cmd.equals("-report")) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      firstCluster = argv[1];
      displayReport = true;
    } else if (cmd.equals(SELECT_IDLE_MACHINES_OPTION)) {
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      firstCluster = argv[1];
      machines = Integer.valueOf(argv[2]);
      selectMachines = true;
    } else {
      displayUsage("");
      return exitCode;
    }

    if (moveMachines) {
      exitCode = moveMachines(firstCluster, toCluster, machines);
    } else if (displayReport) {
      exitCode = displayReport(firstCluster);
    } else if (selectMachines) {
      exitCode = selectIdleMachines(firstCluster, machines);
    }
    return exitCode;
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new ClustersBalancer(), argv);
    System.exit(res);
  }
}

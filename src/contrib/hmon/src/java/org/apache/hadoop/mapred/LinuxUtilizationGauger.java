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
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

/**
 * A {@link UtilizationGauger} which runs on Linux system
 */
public class LinuxUtilizationGauger extends UtilizationGauger {

  static private final String[] CMD =
      {"ps", "-eo", "pid,ppid,pcpu,rss,command"};
  //pcpu: cpu utilization percentage of one core in "##.#" format.
  //rss: resident set size, the non-swapped physical memory that a
  //     task has used (in kiloBytes).
  static private final int NUM_FIELDS = 5;
  static private final int PID = 0, PPID = 1, PCPU = 2, RSS = 3, COMMAND = 4;
  static private final Pattern psPattern = Pattern.compile(
          "([0-9]+)[ \t]+([0-9]+)[ \t]+([0-9.]+)[ \t]+([0-9.]+)[ \t]+(.*)");
  static private final String MEM_INFO = "/proc/meminfo";
  static private final String CPU_INFO = "/proc/cpuinfo";

  public static final Log LOG =
          LogFactory.getLog("org.apache.hadoop.mapred.resourceutilization");

  @Override
  public void initialGauge() {
    try {
      parseMemInfo(readFile(MEM_INFO));
      parseCpuInfo(readFile(CPU_INFO));
      ttUtilization.setHostName(InetAddress.getLocalHost().getHostName());
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  /**
   * Read a file line by line
   * @param fileName
   * @return String[] contains lines
   * @throws IOException
   */
  private String[] readFile(String fileName) throws IOException {
    ArrayList<String> result = new ArrayList<String>();
    FileReader fReader = new FileReader(fileName);
    BufferedReader bReader = new BufferedReader(fReader);
    while (true) {
      String line = bReader.readLine();
      if (line == null) {
        break;
      }
      result.add(line);
    }
    bReader.close();
    fReader.close();
    return (String[])result.toArray(new String[result.size()]);
  }

  /**
   * read total memory from /proc directory
   */
  protected void parseMemInfo(String[] memInfoFile) throws IOException {
    double memTotalGB = 0;
    Pattern pattern = Pattern.compile("MemTotal:[ \t]+([0-9]+)[ \t]+kB");
    for (String line : memInfoFile) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        memTotalGB = Double.parseDouble(matcher.group(1)) / 1000000d;
        break;
      }
    }
    ttUtilization.setMemTotalGB(memTotalGB);
  }

  /**
   * read total cpu information from /proc directory
   */
  protected void parseCpuInfo(String[] cpuInfoFile) throws IOException {
    int numCpu = 0;
    double cpuTotalGHz = 0;
    for (String line : cpuInfoFile) {
      if (line.startsWith("processor")) {
        numCpu++;
      }
      if (line.startsWith("cpu MHz")) {
        cpuTotalGHz += Double.parseDouble(line.split(":")[1].trim()) / 1000d;
      }
    }
    ttUtilization.setNumCpu(numCpu);
    ttUtilization.setCpuTotalGHz(cpuTotalGHz);
  }



  /**
   * Execute "ps -eo pid,ppid,pcpu,rss,command"
   * @return String[] which contains the execution result
   */
  protected String[] getPS() {
    ShellCommandExecutor shellExecutor = new ShellCommandExecutor(CMD);
    try {
      shellExecutor.execute();
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      return null;
    }
    return shellExecutor.getOutput().split("\n");
  }

  /**
   * Parse PS results into fields
   * @param psStrings
   * @return fields contains the PS information
   */
  private String[][] parsePS(String[] psStrings) {
    String[][] result = new String[psStrings.length-1][NUM_FIELDS];
    for (int i = 1; i < psStrings.length; i++) {
      Matcher matcher = psPattern.matcher(psStrings[i]);
      if (matcher.find()) {
        for (int j = 0; j < NUM_FIELDS; j++) {
          result[i-1][j] = matcher.group(j+1);
        }
      }
    }
    return result;
  }

  // "ps -eo pcpu" gives per core %. We convert it to GHz
  private double percentageToGHz(double cpuUsage) {
    cpuUsage /= 100;
    cpuUsage /= ttUtilization.getNumCpu();
    cpuUsage *= ttUtilization.getCpuTotalGHz();
    return cpuUsage;
  }

  @Override
  public void gauge() {
    String [][] psResult = parsePS(getPS());

    // Get the overall CPU and memory usage
    double cpuUsage = 0d;
    double memUsage = 0d;
    for (String[] psFields : psResult) {
      try {
        cpuUsage += Double.parseDouble(psFields[PCPU]);
        memUsage += Double.parseDouble(psFields[RSS]);
      } catch (NumberFormatException e) {
        // do nothing
      }
    }
    //"ps -eo pcpu" gives % per core. We convert it to GB.
    cpuUsage = percentageToGHz(cpuUsage);
    memUsage /= 1000000d;              // "ps -eo rss" gives memory in kB
    ttUtilization.setMemUsageGB(memUsage);
    ttUtilization.setCpuUsageGHz(cpuUsage);

    // Index the results of PS by the pid
    Map<String, String[]> pidToContent =
            new HashMap<String, String[]>();
    for (String[] psFields : psResult) {
      pidToContent.put(psFields[PID], psFields);
    }

    // Obtain all child processes of every process
    Map<String, LinkedList<String>> pidToChildPid =
            new HashMap<String, LinkedList<String>>();
    for (String[] psFields : psResult) {
      if (!pidToChildPid.containsKey(psFields[PID])) {
        pidToChildPid.put(psFields[PID], new LinkedList<String>());
      }
      if (!pidToChildPid.containsKey(psFields[PPID])) {
        pidToChildPid.put(psFields[PPID], new LinkedList<String>());
      }
      pidToChildPid.get(psFields[PPID]).add(psFields[PID]);
    }

    // There can be multiple TaskTracker on one machine
    List<String> taskTrackerPidList = new LinkedList<String>();
    double[] taskTrackerUsage = new double[2];
    for (String[] psFields : psResult) {
      if (psFields[PPID].equals("1")) {
        if (psFields[COMMAND].matches(".*TaskTracker.*")) {
          taskTrackerPidList.add(psFields[PID]);
          // "ps -eo pcpu" gives per core %. We convert it to GHz
          taskTrackerUsage[0] +=
                  percentageToGHz(Double.parseDouble(psFields[PCPU]));
          // "ps -eo rss" gives memory in kB. We convert it to GB
          taskTrackerUsage[1] += Double.parseDouble(psFields[RSS]) / 1000000d;
        }
      }
    }
    if (taskTrackerPidList.isEmpty()) {
      localJobUtilization = null;
      return;
    }

    // Obtain all jobID
    String jobIDRegex = "(job_[0-9]+_[0-9]+)";
    Map<String, double[]> jobIdToUsage = new HashMap<String, double[]>();
    Pattern jobIdPattern = Pattern.compile(jobIDRegex);
    for (String[] psFields : psResult) {
      Matcher jobIdMatcher = jobIdPattern.matcher(psFields[COMMAND]);
      if (jobIdMatcher.find()) {
        String jobID = jobIdMatcher.group(1);
        if (!jobIdToUsage.containsKey(jobID)) {
          jobIdToUsage.put(jobID, new double[2]);
        }
      }
    }
    jobIdToUsage.put("TaskTracker", taskTrackerUsage);

    for (String ttPid : taskTrackerPidList) {
      for (String pid : pidToChildPid.get(ttPid)) {
        String[] psFields = pidToContent.get(pid);
        Matcher jobIdMatcher = jobIdPattern.matcher(psFields[COMMAND]);
        double[] jobUsage = getSubProcessUsage(pid, pidToContent, pidToChildPid);
        if (jobIdMatcher.find()) {
          String jobID = jobIdMatcher.group(1);
          jobIdToUsage.get(jobID)[0] += jobUsage[0];
          jobIdToUsage.get(jobID)[1] += jobUsage[1];
        } else {
          jobIdToUsage.get("TaskTracker")[0] += jobUsage[0];
          jobIdToUsage.get("TaskTracker")[1] += jobUsage[1];
        }
      }
    }

    // Write job information to TaskTrackerReport
    localJobUtilization = new LocalJobUtilization[jobIdToUsage.size()];
    for ( int i = 0; i < jobIdToUsage.size(); i++) {
      localJobUtilization[i] = new LocalJobUtilization();
    }

    int jobReportIndex = 0;
    for (String jobID : jobIdToUsage.keySet()) {
      localJobUtilization[jobReportIndex].setJobId(jobID);
      localJobUtilization[jobReportIndex].setCpuUsageGHz(
              jobIdToUsage.get(jobID)[0]);
      localJobUtilization[jobReportIndex].setMemUsageGB(
              jobIdToUsage.get(jobID)[1]);
      jobReportIndex++;
    }
  }

  /**
   * A function computes the Memory and CPU usage of all subprocess
   * @param pid            PID of the process we are interested in
   * @param pidToContent   Map between pid and the PS content
   * @param pidToChildPid  Map between pid and pid of its child process
   * @return A 2-element array which contants CPU and memory usage
   */
  private double[] getSubProcessUsage(String pid,
                            Map<String, String[]> pidToContent,
                            Map<String, LinkedList<String>> pidToChildPid) {
    double cpuMemUsage[] = new double[2];

    Queue<String> pidQueue = new LinkedList<String>();
    pidQueue.add(pid);
    while (!pidQueue.isEmpty()) {
      pid = pidQueue.poll();
      for (String child : pidToChildPid.get(pid)) {
        pidQueue.add(child);
      }
      String[] psContent = pidToContent.get(pid);
      double cpuUsage = Double.parseDouble(psContent[PCPU]);
      cpuUsage = percentageToGHz(cpuUsage);
      double memUsage = Double.parseDouble(psContent[RSS]);
      // "ps -eo rss" gives memory in kB. We convert it in GB
      memUsage /= 1000000d;
      cpuMemUsage[0] += cpuUsage;
      cpuMemUsage[1] += memUsage;
    }
    return cpuMemUsage;
  }
}

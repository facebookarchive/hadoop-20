/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Displays the real-time resource utilization on the cluster.
 */
public class UtilizationShell implements Tool {
  UtilizationCollectorProtocol rpcCollector = null;
  boolean clientRunning;
  Configuration conf = null;
  public UtilizationShell(Configuration conf) throws IOException {
    setConf(conf);
    initializeRpcCollector(conf);
  }

  /**
   * Create the the {@link UtilizationCollectorProtocol} RPC object
   * @param conf Configuration
   * @throws IOException
   */
  protected void initializeRpcCollector(Configuration conf) throws IOException {
    rpcCollector =
          (UtilizationCollectorProtocol) RPC.getProxy(UtilizationCollectorProtocol.class,
                                           UtilizationCollectorProtocol.versionID,
                                           UtilizationCollector.getAddress(conf),
                                           conf);
  }

  /**
   * Obtain the result to print in command line
   * @param argv
   * @return the response to show users
   * @throws IOException
   */
  public String getResponse(String[] argv) throws IOException {
    String result = "";
    if (argv.length < 1) {
      return result;
    }
    if (argv[0].equals("-all")) {
      result += rpcCollector.getClusterUtilization();
      result += JobUtilization.legendString +
              JobUtilization.unitString;
      for (JobUtilization job : rpcCollector.getAllRunningJobUtilization()) {
        result += job;
      }
      result += TaskTrackerUtilization.legendString +
              TaskTrackerUtilization.unitString;
      for (TaskTrackerUtilization tt :
           rpcCollector.getAllTaskTrackerUtilization()) {
        result += tt;
      }
      return result;
    }
    if (argv[0].equals("-cluster")) {
      result += rpcCollector.getClusterUtilization();
      return result;
    }
    if (argv[0].equals("-job")) {
      result += JobUtilization.legendString +
              JobUtilization.unitString;
      if (argv.length == 1) {
        for (JobUtilization job : rpcCollector.getAllRunningJobUtilization()) {
          result += job;
        }
        return result;
      }
      for (int i = 1; i < argv.length; i++) {
        result += rpcCollector.getJobUtilization(argv[i]);
      }
      return result;
    }
    if (argv[0].equals("-tasktracker")) {
      result += TaskTrackerUtilization.legendString +
              TaskTrackerUtilization.unitString;
      if (argv.length == 1) {
        for (TaskTrackerUtilization tt :
             rpcCollector.getAllTaskTrackerUtilization()) {
          result += tt;
        }
        return result;
      }
      for (int i = 1; i < argv.length; i++) {
        result += rpcCollector.getTaskTrackerUtilization(argv[i]);
      }
      return result;
    }
    return result;
  }

  @Override
  public int run(String[] argv) throws Exception {
    String result = getResponse(argv);
    if (result.equals("")) {
      printUsage();
      return -1;
    }
    System.out.println(result);
    return 0;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.conf.addResource("resourceutilization.xml");
    this.conf.addResource("mapred-site.xml");
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage() {
    String prefix = "Usage: hadoop " + UtilizationShell.class.getSimpleName();
    System.err.println(prefix);
    System.err.println("           [-all]");
    System.err.println("           [-cluster]");
    System.err.println("           [-job]");
    System.err.println("           [-job jobID1 jobID2...]");
    System.err.println("           [-tasktracker]");
    System.err.println("           [-tasktracker hostname1 hostname2...]");
    System.err.println("           [-help [cmd]]");
    System.err.println();
    ToolRunner.printGenericCommandUsage(System.err);
  }

  /**
   * Close the connection to the {@link UtilizationCollector}
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcCollector);
    }
  }

  public static void main(String[] argv) throws Exception {
    UtilizationShell shell = null;
    try {
      shell = new UtilizationShell(new Configuration());
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Bad connection to Collector. command aborted.");
      System.exit(-1);
    }

    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}

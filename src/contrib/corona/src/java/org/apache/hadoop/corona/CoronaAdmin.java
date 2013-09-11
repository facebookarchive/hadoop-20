/*
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

package org.apache.hadoop.corona;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.ClusterManagerSafeModeProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

/**
 * A tool to perform administrative actions on a corona cluster.
 */
public class CoronaAdmin extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java CoronaAdmin [-refreshNodes]");
    } else {
      System.err.println("Usage: java CoronaAdmin");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-setSafeMode]");
      System.err.println("           [-unsetSafeMode]");
      System.err.println("           [-forceSetSafeModeOnPJT]");
      System.err.println("           [-forceUnsetSafeModeOnPJT]");
      System.err.println("           [-restartTaskTracker]");
      System.err.println("           [-forceRestartTaskTracker]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * Command to ask the Cluster Manager to reread the hosts and excluded hosts
   * file.
   *
   * @exception IOException
   * @return Returns 0 where no exception is thrown.
   */
  private int refreshNodes() throws IOException {
    // Get the current configuration
    CoronaConf conf = new CoronaConf(getConf());

    InetSocketAddress address = NetUtils.createSocketAddr(conf
        .getClusterManagerAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    ClusterManagerService.Client client = new ClusterManagerService.Client(
        new TBinaryProtocol(transport));

    try {
      transport.open();
      client.refreshNodes();
    } catch (SafeModeException e) {
      System.err.println("ClusterManager is in Safe Mode");
    } catch (TException e) {
      throw new IOException(e);
    }

    return 0;
  }

  /**
   * Command to ask the Cluster Manager to restart all the task tracker
   *
   * @param forceFlag if CM shall ignore all previous restart requests
   * @exception IOException
   * @return Returns 0 where no exception is thrown.
   */
  private int restartTaskTracker(boolean forceFlag, int batchSize) throws IOException {
    // Get the current configuration
    CoronaConf conf = new CoronaConf(getConf());

    InetSocketAddress address = NetUtils.createSocketAddr(conf
        .getClusterManagerAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    ClusterManagerService.Client client = new ClusterManagerService.Client(
        new TBinaryProtocol(transport));
    int restartBatch = (batchSize > 0) ? batchSize :
      conf.getCoronaNodeRestartBatch();

    try {
      transport.open();
      RestartNodesArgs restartNodeArgs = new RestartNodesArgs(
        forceFlag, restartBatch);
      client.restartNodes(restartNodeArgs);
    } catch (SafeModeException e) {
      System.err.println("ClusterManager is in Safe Mode");
    } catch (TException e) {
      throw new IOException(e);
    }

    return 0;
  }

  /**
   * Turns on the Safe Mode if safeMode is true. Turns off the Safe Mode if
   * safeMode is false.
   * @param safeMode Is true if we want the Safe Mode to be on. false
   *                 otherwise.
   * @return 0 if successful.
   * @throws IOException
   */
  private int setSafeMode(boolean safeMode) throws IOException {
    // Get the current configuration
    CoronaConf conf = new CoronaConf(getConf());

    InetSocketAddress address = NetUtils.createSocketAddr(conf
      .getClusterManagerAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    ClusterManagerService.Client client = new ClusterManagerService.Client(
      new TBinaryProtocol(transport));

    try {
      transport.open();
      if (client.setSafeMode(safeMode)) {
        System.out.println("The safeMode is: " +
                            (safeMode ? "ON" : "OFF"));
      } else {
        System.err.println("Could not set the safeMode flag");
      }
    } catch (TException e) {
      throw new IOException(e);
    }

    return 0;
  }

  /**
   * Persists the state of the ClusterManager
   * @return 0 if successful.
   * @throws IOException
   */
  private int persistState() throws IOException {
    // Get the current configuration
    CoronaConf conf = new CoronaConf(getConf());

    InetSocketAddress address = NetUtils.createSocketAddr(conf
      .getClusterManagerAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    ClusterManagerService.Client client = new ClusterManagerService.Client(
      new TBinaryProtocol(transport));

    try {
      transport.open();
      if (!client.persistState())  {
        System.err.println("Persisting Cluster Manager state failed. ");
      }
    } catch (TException e) {
      throw new IOException(e);
    }

    return 0;
  }

  /**
   * Forcefully set the Safe Mode on the PJT
   * @return 0 if successful
   * @throws IOException
   */
  private int forceSetSafeModeOnPJT(boolean safeMode) throws IOException {
    CoronaConf conf = new CoronaConf(getConf());
    try {
      ClusterManagerAvailabilityChecker.getPJTClient(conf).
        setClusterManagerSafeModeFlag(safeMode);
    } catch (IOException e) {
      System.err.println("Could not set the Safe Mode flag on the PJT: " + e);
    } catch (TException e) {
      System.err.println("Could not set the Safe Mode flag on the PJT: " + e);
    }
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int i = 0;
    String cmd = args[i++];

    int exitCode = 0;
    try {
      if ("-refreshNodes".equals(cmd)) {
        exitCode = refreshNodes();
      } else if ("-help".equals(cmd)) {
        printUsage(args[i]);
      } else if ("-setSafeMode".equals(cmd)) {
        exitCode = setSafeMode(true);
      } else if ("-unsetSafeMode".equals(cmd)) {
        exitCode = setSafeMode(false);
      } else if ("-persistState".equals(cmd)) {
        exitCode = persistState();
      } else if ("-forceSetSafeModeOnPJT".equals(cmd)) {
        exitCode = forceSetSafeModeOnPJT(true);
      } else if ("-forceUnsetSafeModeOnPJT".equals(cmd)) {
        exitCode = forceSetSafeModeOnPJT(false);
      } else if ("-restartTaskTracker".equals(cmd)) {
        int batchSize = 0;
        if (args.length > 1) {
          batchSize = Integer.parseInt(args[i++]);
        }
        exitCode = restartTaskTracker(false, batchSize);
      } else if ("-forceRestartTaskTracker".equals(cmd)) {
        int batchSize = 0;
        if (args.length > 1) {
          batchSize = Integer.parseInt(args[i++]);
        }
        exitCode = restartTaskTracker(true, batchSize);
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (NumberFormatException e) {
      exitCode = -1;
      System.err.println(cmd.substring(1));
      e.printStackTrace();

    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge);
      printUsage(cmd);
    } catch (RemoteException e) {
      exitCode = -1;
      String[] content;
      content = e.getLocalizedMessage().split("\n");
      System.err.println(cmd.substring(1) + ": " + content[0]);
    }
    return exitCode;
  }

  /**
   * Entry point for the tool.
   *
   * @param args
   *          The command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new CoronaAdmin(), args);
    System.exit(result);
  }
}

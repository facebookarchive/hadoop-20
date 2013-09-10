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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * The Tool that can talk to the cluster manager to work with the sessions
 * List, Kill, etc
 */
public class CoronaClient extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /**
   * Print the usage instructions for this class.
   * If a specific command is given only prints the usage for that command
   * otherwise lists all the commands and their usages
   * @param cmd the command to list the usage for
   */
  public static void printUsage(String cmd) {
    if ("-kill".equals(cmd)) {
      System.err.println("Usage: CoronaClient -kill <session id>");
    } else {
      System.err.println("Usage: CoronaClient");
      System.err.println("\t\t[-list]");
      System.err.println("\t\t[-kill <session id>]");
    }
  }


  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int i = 0;

    String cmd = args[i++];
    if ("-list".equals(cmd)) {
      if (args.length > 1) {
        printUsage("");
        return -1;
      }
      return listSessions();
    } else if ("-kill".equals(cmd)) {
      if (args.length != 2) {
        printUsage(cmd);
        return -1;
      }
      return killSession(args[i]);
    }

    return 0;
  }


  /**
   * Tells the cluster manager to kill the session with a given id
   * @param sessionId the id of the session to kill
   * @return 0 in case of success, non zero value on error
   * @throws IOException
   */
  private int killSession(String sessionId)throws IOException {
    try {
      System.out.printf("Killing %s", sessionId);
      ClusterManagerService.Client client = getCMSClient();
      try {
        client.killSession(sessionId);
      } catch (SafeModeException e) {
        throw new IOException(
          "Cannot kill session yet, ClusterManager is in Safe Mode");
      }
      System.err.printf("%s killed", sessionId);
    } catch (TException e) {
      throw new IOException(e);
    }
    return 0;
  }

  public static void killSession(String sessionId, Configuration conf)
    throws IOException {
    try {
      ClusterManagerService.Client client = getCMSClient(new CoronaConf(conf));
      client.killSession(sessionId);
    } catch (SafeModeException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets a list of the sessions from the cluster manager
   * and outputs them on the console
   * @return 0 in case of success, non zero value on error
   * @throws IOException
   */
  private int listSessions() throws IOException {
    try {
      ClusterManagerService.Client client = getCMSClient();
      List<RunningSession> sessions;
      try {
        sessions = client.getSessions();
      } catch (SafeModeException e) {
        throw new IOException(
          "Cannot list sessions, ClusterManager is in Safe Mode");
      }
      System.out.printf("%d sessions currently running:\n",
          sessions.size());
      System.out.printf("SessionID\t" +
          "Session Name\t" +
          "Session User\t" +
          "Session Poolgroup\t" +
          "Session Pool\t" +
          "Session Priority\t" +
          "Running Mappers\t" +
          "Running Reducers\t" +
          "Running Jobtrackers\n");
      for (RunningSession session : sessions) {
        SessionPriority priority = session.getPriority();
        if (priority == null) {
          priority = SessionPriority.NORMAL;
        }
        System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
            session.getHandle(),
            session.getName().replace("\t", "\\t").replace("\n", "\\n"),
            session.getUserId(),
            session.getPoolInfo().getPoolGroupName(),
            session.getPoolInfo().getPoolName(),
            priority,
            session.getRunningResources().get(ResourceType.MAP),
            session.getRunningResources().get(ResourceType.REDUCE),
            session.getRunningResources().get(ResourceType.JOBTRACKER));
      }
    } catch (TException e) {
      throw new IOException(e);
    }

    return 0;
  }

  /**
   * Get the thrift client to communicate with the cluster manager
   * @return a thrift client initialized to talk to the cluster manager
   * @throws TTransportException
   */
  private ClusterManagerService.Client getCMSClient()
    throws TTransportException {
    // Get the current configuration
    CoronaConf conf = new CoronaConf(getConf());
    return getCMSClient(conf);
  }

  /**
   * Get the thrift client to communicate with the cluster manager
   * @return a thrift client initialized to talk to the cluster manager
   * @param conf The configuration.
   * @throws TTransportException
   */
  private static ClusterManagerService.Client getCMSClient(CoronaConf conf)
    throws TTransportException {
    InetSocketAddress address = NetUtils.createSocketAddr(conf
        .getClusterManagerAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    ClusterManagerService.Client client = new ClusterManagerService.Client(
        new TBinaryProtocol(transport));
    transport.open();
    return client;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new CoronaClient(), args);
    System.exit(result);
  }
}

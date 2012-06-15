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
import org.apache.hadoop.ipc.RemoteException;
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
    } catch (TException e) {
      throw new IOException(e);
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
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
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

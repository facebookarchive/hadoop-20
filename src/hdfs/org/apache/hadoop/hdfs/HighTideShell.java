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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.protocol.PolicyInfo;
import org.apache.hadoop.hdfs.protocol.HighTideProtocol;
import org.apache.hadoop.hdfs.server.hightidenode.HighTideNode;

/**
 * A {@link HighTideShell} that allows browsing configured HighTide policies.
 */
public class HighTideShell extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.HighTideShell");
  public HighTideProtocol hightidenode;
  HighTideProtocol rpcHighTidenode;
  private UnixUserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private Configuration conf;

  /**
   * Start HighTideShell.
   * <p>
   * The HighTideShell connects to the specified HighTideNode and performs basic
   * configuration options.
   * @throws IOException
   */
  public HighTideShell(Configuration conf) throws IOException {
    super(conf);
    this.conf = conf;
  }

  private void initializeRpc(Configuration conf, InetSocketAddress address) throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }

    this.rpcHighTidenode = createRPCHighTidenode(address, conf, ugi);
    this.hightidenode = createHighTidenode(rpcHighTidenode);
  }

  private void initializeLocal(Configuration conf) throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }

  public static HighTideProtocol createHighTidenode(Configuration conf) throws IOException {
    return createHighTidenode(HighTideNode.getAddress(conf), conf);
  }

  public static HighTideProtocol createHighTidenode(InetSocketAddress htNodeAddr,
      Configuration conf) throws IOException {
    try {
      return createHighTidenode(createRPCHighTidenode(htNodeAddr, conf,
        UnixUserGroupInformation.login(conf, true)));
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }

  private static HighTideProtocol createRPCHighTidenode(InetSocketAddress htNodeAddr,
      Configuration conf, UnixUserGroupInformation ugi)
    throws IOException {
    LOG.info("HighTideShell connecting to " + htNodeAddr);
    return (HighTideProtocol)RPC.getProxy(HighTideProtocol.class,
        HighTideProtocol.versionID, htNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, HighTideProtocol.class));
  }

  private static HighTideProtocol createHighTidenode(HighTideProtocol rpcHighTidenode)
    throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, 5000, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class,
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (HighTideProtocol) RetryProxy.create(HighTideProtocol.class,
        rpcHighTidenode, methodNameToPolicyMap);
  }

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("HighTideNode closed");
      throw result;
    }
  }

  /**
   * Close the connection to the HighTideNode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcHighTidenode);
    }
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + HighTideShell.class.getSimpleName();
    if ("-showConfig".equals(cmd)) {
      System.err.println("Usage: java org.apache.hadoop.hdfs.HighTideShell");
    } else {
      System.err.println("Usage: java HighTideShell");
      System.err.println("           [-showConfig ]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-showConfig".equals(cmd)) {
      if (argv.length < 1) {
        printUsage(cmd);
        return exitCode;
      }
    } 

    try {
      if ("-showConfig".equals(cmd)) {
        initializeRpc(conf, HighTideNode.getAddress(conf));
        exitCode = showConfig(cmd, argv, i);
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hightidenode server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " +
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " +
                           ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      // 
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " +
                         e.getLocalizedMessage());
    } catch (Exception re) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());
    } finally {
    }
    return exitCode;
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int showConfig(String cmd, String argv[], int startindex) throws IOException {
    int exitCode = 0;
    PolicyInfo[] all = hightidenode.getAllPolicies();
    for (int i = 0; i < all.length; i++) {
      System.out.println(all[i]);
    }
    return exitCode;
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    DnsMonitorSecurityManager.setTheManager();
    HighTideShell shell = null;
    try {
      shell = new HighTideShell(new Configuration());
      int res = ToolRunner.run(shell, argv);
      System.exit(res);
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Bad connection to HighTideNode. command aborted.");
      System.exit(-1);
    } finally {
      shell.close();
    }
  }
}

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

package org.apache.hadoop.hdfs.server.namenode;

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
import org.apache.hadoop.hdfs.server.protocol.SnapshotProtocol;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.fs.Path;

/**
 * A SnapshotShell that allows creating new snapshots, deleting existing snapshots
 * and listing all snapshots
 */
public class SnapshotShell extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(SnapshotShell.class);
  public SnapshotProtocol ssNode;
  final SnapshotProtocol rpcSsNode;
  private UnixUserGroupInformation ugi;
  volatile boolean clientRunning = true;

  /**
   * Start SnapshotShell.
   * 
   * The SnapshotShell connects to the specified SnapshotNode and performs basic
   * options.
   * @throws IOException
   */
  public SnapshotShell() throws IOException {
    this(new Configuration());
  }

  public SnapshotShell(Configuration conf) throws IOException {
    this(conf, SnapshotNode.getAddress(conf));
  }

  public SnapshotShell(Configuration conf, InetSocketAddress address) throws IOException {
    super(conf);
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }

    this.rpcSsNode = createRPCSnapshotNode(address, conf, ugi);
    this.ssNode = createSnapshotNode(rpcSsNode);
  }

  public static SnapshotProtocol createSnapshotNode(Configuration conf) throws IOException {
   return createSnapshotNode(SnapshotNode.getAddress(conf), conf);
  }

  public static SnapshotProtocol createSnapshotNode(InetSocketAddress ssNodeAddr,
                                            Configuration conf) throws IOException {
    try {
      return createSnapshotNode(createRPCSnapshotNode(ssNodeAddr, conf,
                                     UnixUserGroupInformation.login(conf, true)));
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }

  private static SnapshotProtocol createSnapshotNode(SnapshotProtocol rpcSnapshotNode) 
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

    return (SnapshotProtocol) RetryProxy.create(SnapshotProtocol.class, 
                                 rpcSnapshotNode, methodNameToPolicyMap);
  }


  private static SnapshotProtocol createRPCSnapshotNode(InetSocketAddress ssNodeAddr,
                    Configuration conf, UnixUserGroupInformation ugi) throws IOException {
    LOG.info("SnapshotShell connecting to " + ssNodeAddr);
    return (SnapshotProtocol)RPC.getProxy(SnapshotProtocol.class, SnapshotProtocol.versionID, 
           ssNodeAddr, ugi, conf, NetUtils.getSocketFactory(conf, SnapshotProtocol.class));
  }


  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("SnapshotNode closed");
      throw result;
    }
  }

  public SnapshotProtocol getSnapshotNode() {
    return ssNode;
  }

  /**
   * Close the connection to the SnapshotNode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcSsNode);
    }
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    if ("-create".equals(cmd)) {
      System.err.println("Usage: java SnapshotShell [-create snapshot_id [-ignoreleases]]");
    } else if ("-list".equals(cmd)) {
      System.err.println("Usage: java SnapshotShell [-list]");
    } else if ("-delete".equals(cmd)) {
      System.err.println("Usage: java SnapshotShell [-delete snapshot_id]");
    } else {
      System.err.println("Usage: java SnapshotShell "
                         + "[-create snapshot_id [-ignoreleases]] "
                         + "[-list] "
                         + "[-delete snapshot_id] ");
    }
  }

  /**
   * run
   */
  @Override
  public int run(String argv[]) throws Exception {
    if (argv.length < 1) {
      printUsage(null);
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    // verify that we have enough command line parameters
    if ("-create".equals(cmd)) {
      boolean error = true;
      if (argv.length == 2) {
        error = false;
      } else if (argv.length == 3) {
        if ("-ignoreleases".equals(argv[2])) {
          error = false;
        }
      }

      if (error) { 
        printUsage(cmd);
        return exitCode;
      }
    }
    else if ("-list".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    else if ("-delete".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    }
    else {
      LOG.error(cmd.substring(1) + ": Unknown command");
      printUsage(null);
      return exitCode;
    }

    try {
      if ("-create".equals(cmd)) {
        exitCode = createSnapshot(argv[1], argv.length == 3);
      } else if ("-list".equals(cmd)) {
        exitCode = listSnapshots();
      } else if ("-delete".equals(cmd)) {
        exitCode = deleteSnapshot(argv[1]);
      }
    } catch (RemoteException e) {
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        LOG.error(cmd.substring(1) + ": " + content[0]);
      } catch (Exception ex) {
        LOG.error(cmd.substring(1) + ": " + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      // IO exception encountered locally.
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": " + e.getLocalizedMessage());
    } catch (Exception e) {
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": " + e.getLocalizedMessage());
    } finally {
      // Does the RPC connection need to be closed?
    }

    return exitCode;
  }

  private int createSnapshot(String id, boolean updateLeases) throws IOException {
    checkOpen();

    ssNode.createSnapshot(id, updateLeases);
    System.out.println("Snapshot created with id: " + id);

    return 0;
  }

  private int listSnapshots() throws IOException {
    checkOpen();

    String[] rtn = ssNode.listSnapshots();

    // Print ids
    for (String ss: rtn) {
      System.out.println(ss);
    }

    return 0;
  }

  private int deleteSnapshot(String id) throws IOException {
    checkOpen();

    boolean success = ssNode.deleteSnapshot(id);
    System.out.println("Snapshot with id " + id + " deleted: " + success);

    return 0;
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    SnapshotShell shell = null;
    try {
      shell = new SnapshotShell();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Bad connection to SnapshotNode... command aborted.");
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

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

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * A SecurityManager that logs DNS resolution call's stack traces.
 * 
 */
public class DnsMonitorSecurityManager extends SecurityManager {
  private static final Log LOG = LogFactory.getLog(DnsMonitorSecurityManager.class);
  private final Set<Integer> cacheOfHashOfSeenStackTraces = new HashSet<Integer>();
  private static final String ACTIVATE_FLAG = "dns_call_stack_logging";
  private static final DnsMonitorSecurityManager theManager = new DnsMonitorSecurityManager();

  /**
   * Update's the system's security manager to an instance of this class if
   * system property dns_call_stack_logging is set to true.
   */
  public static void setTheManager() {
    if ("true".equalsIgnoreCase(System.getProperty(ACTIVATE_FLAG))) {
      if (!(System.getSecurityManager() instanceof DnsMonitorSecurityManager)) {
        System.setSecurityManager(theManager);
      }
    }
  }

  /**
   * Private in order to enforce use of {@link #setTheManager()} and check
   * System's property check of {@value #ACTIVATE_FLAG}
   */
  private DnsMonitorSecurityManager() {
  }

  @Override
  public void checkConnect(String host, int port) {
    final String stackTrace = DFSUtil.getStackTrace();
    if (!stackTrace.contains(InetAddress.class.getName())) {
      return;
    }
    final int hashCodeOfStackTrace = stackTrace.hashCode();
    boolean toLogThisStackTrace = false;
    synchronized (cacheOfHashOfSeenStackTraces) {
      if (!cacheOfHashOfSeenStackTraces.contains(hashCodeOfStackTrace)) {
        cacheOfHashOfSeenStackTraces.add(hashCodeOfStackTrace);
        toLogThisStackTrace = true;
      }
    }
    if (toLogThisStackTrace) {
      LOG.info("checkConnect(host=" + host + ", port=" + port + ")");
      LOG.info(stackTrace);
    }
  }

  @Override
  public void checkConnect(String host, int port, Object context) {
    this.checkConnect(host, port);
  }

  @Override
  public void checkAccept(String host, int port) {
  }

  @Override
  public void checkAccess(Thread t) {
  }

  @Override
  public void checkAccess(ThreadGroup g) {
  }

  @Override
  public void checkAwtEventQueueAccess() {
  }

  @Override
  public void checkCreateClassLoader() {
  }

  @Override
  public void checkDelete(String file) {
  }

  @Override
  public void checkExec(String cmd) {
  }

  @Override
  public void checkExit(int status) {
  }

  @Override
  public void checkLink(String lib) {
  }

  @Override
  public void checkListen(int port) {
  }

  @Override
  public void checkMemberAccess(Class<?> clazz, int which) {
  }

  @Override
  public void checkMulticast(InetAddress maddr) {
  }

  @Override
  public void checkMulticast(InetAddress maddr, byte ttl) {
  }

  @Override
  public void checkPackageAccess(String pkg) {
  }

  @Override
  public void checkPackageDefinition(String pkg) {
  }

  @Override
  public void checkPermission(Permission p) {
  }

  @Override
  public void checkPermission(Permission p, Object o) {
  }

  @Override
  public void checkPropertyAccess(String s) {
  }

  @Override
  public void checkPropertiesAccess() {
  }

  @Override
  public void checkPrintJobAccess() {
  }

  @Override
  public void checkRead(FileDescriptor fd) {
  }

  @Override
  public void checkRead(String file) {
  }

  @Override
  public void checkRead(String file, Object context) {
  }

  @Override
  public void checkSecurityAccess(String target) {
  }

  @Override
  public void checkSetFactory() {
  }

  @Override
  public void checkSystemClipboardAccess() {
  }

  @Override
  public void checkWrite(FileDescriptor fd) {
  }

  @Override
  public void checkWrite(String file) {
  }
}

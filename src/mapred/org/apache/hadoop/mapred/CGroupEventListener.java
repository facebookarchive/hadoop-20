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

import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.syscall.LinuxSystemCall;
import org.apache.hadoop.util.NativeCodeLoader;

public class CGroupEventListener {
  private static final Log LOG =
    LogFactory.getLog(CGroupEventListener.class);
  private static boolean nativeCodeLoaded = false;

  public native static int init(String cfile);
  public native static int waitForNotification(int waitTime);
  public native static void close();

  public static boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  public static void main (String args[]) {
    File exitFile = new File("/tmp/cgroup_exit_flag");
    int ret = -1;
    while (!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.info("Loading NativeCode");
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
      }
    }
    while (!exitFile.exists()) {
      if (ret < 0) {
        ret = init("/cgroup/memory/task_container/memory.oom_control");
      }
      if (ret < 0) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
        }
        continue;
      }
      ret = waitForNotification(60000);
      LOG.info(" Get " + ret);
      if (ret <= 0) {
        continue;
      }
      File containDir = new File("/cgroup/memory/task_container");
      for (String child: containDir.list()) {
        try {
          if (child.startsWith("attempt")) {
            LOG.info(" check " + child);
            BufferedReader reader = new BufferedReader(new FileReader("/cgroup/memory/task_container/" + child + "/tasks"));
            String thread = "";
            boolean killed = false;
            while( ( thread = reader.readLine() ) != null) {
              LOG.info(" kill " + thread);
              if (LinuxSystemCall.killProcessGroup(Integer.parseInt(thread)) >= 0) {
                killed = true;
              }
            }
            reader.close();
            if (killed) {
              break;
            }
          }
        } catch (java.io.IOException e) {
          LOG.info("Exception in killing tasks");
        }
      }
    }
    close();
  }
  static {
    try {
    System.loadLibrary ( "CGroupEventListener" ) ;
      LOG.info("Loaded the native-CGroupEventListener library");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
      LOG.error("Failed to load native-CGroupEventListener with error: " + t);
      LOG.error("java.library.path=" + System.getProperty("java.library.path"));
    }
  }
}

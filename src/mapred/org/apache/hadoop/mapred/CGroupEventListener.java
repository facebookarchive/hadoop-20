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
    int ret = init("/cgroup/memory/tasktrackers/memory.oom_control");
    if (ret >= 0) {
      while (!exitFile.exists()) {
        ret = waitForNotification(300000);
        System.out.println(" Get " + ret);
      }
    } else {
      System.out.println("init failed " + ret);
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

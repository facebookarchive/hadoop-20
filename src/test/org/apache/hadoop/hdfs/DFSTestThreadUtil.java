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

import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

public class DFSTestThreadUtil {

  // A list of threads that might hang around (maybe due to a JVM bug).
  private static final String[] excludedThreads = { "SunPKCS11" };
  private static final String[] excludedThreadGroups = { "system" };

  private static boolean isExcludedThread(Thread th) {
    for (String badThread : excludedThreads) {
      if (th.getName().contains(badThread)) {
        return true;
      }
    }
    return false;
  }
  
  private static boolean isExcludedGroupThread(Thread th) {
    for (String badThread : excludedThreadGroups) {
      if (th.getThreadGroup().getName().contains(badThread)) {
        return true;
      }
    }
    return false;
  }
  
  public static void checkRemainingThreads(Set<Thread> old) throws Exception {
    Thread.sleep(15000);

    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    threads.removeAll(old);
    if (threads.size() != 0) {
      System.out.println("Following threads are not clean up:");
      Iterator<Thread> it = threads.iterator();
      while (it.hasNext()) {
        Thread th = it.next();
        if (isExcludedThread(th) || isExcludedGroupThread(th)) {
          it.remove();
          continue;
        }
        String msg = "";
        for (StackTraceElement stack : th.getStackTrace()) {
          msg += stack + "\n";
        }
        
        System.out.println("Thread: " + th.getName() + " : " + th.toString() + "\n" + msg);
        
      }
    }
    assertTrue("This is not a clean shutdown", threads.size() == 0);
  }
}

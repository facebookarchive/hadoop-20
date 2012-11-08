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

import org.apache.commons.logging.Log;

import java.util.EnumMap;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.StringTokenizer;

/**
 * a collection of utility classes and functions
 */
public class Utilities {
  /** One unit of the compute specs */
  public static final ComputeSpecs UNIT_COMPUTE_SPECS =
    new ComputeSpecs((short) 1);
  /** The pattern of the application address in the appinfo string */
  public static final Pattern INET_ADDRESS_PATTERN =
    Pattern.compile("(.+):(\\d+)");
  /** The pattern of IPAddress */
  public static final Pattern IP_ADDRESS_PATTERN =
    Pattern.compile("^([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)$");
  /** Cache of the ResourceRequests based on the type */
  private static Map<ResourceType, ResourceRequest> unitResourceRequestMap =
    new EnumMap<ResourceType, ResourceRequest>(ResourceType.class);

  /**
   * Do not construct this utility class.
   */
  private Utilities() { }

  /**
   * Get a {@link ResourceRequest} object for a given resource type
   * Will cache the resource request after the first time it is created
   *
   * @param type the type to return the {@link ResourceRequest} for
   * @return the {@link ResourceRequest} object for a given type
   */
  public static ResourceRequest getUnitResourceRequest(ResourceType type) {
    ResourceRequest req = unitResourceRequestMap.get(type);
    if (req == null) {
      req = new ResourceRequest(1, type);
      req.setSpecs(UNIT_COMPUTE_SPECS);

      // instead of using concurrent classes or locking - we clone and replace
      // the map with a new entry. this makes sense because this structure is
      // entirely read-only and will be populated quickly at bootstrap time

      HashMap<ResourceType, ResourceRequest> newMap =
          new HashMap<ResourceType, ResourceRequest>();
      for (Map.Entry<ResourceType, ResourceRequest> entry :
          unitResourceRequestMap.entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }
      newMap.put(type, req);
      unitResourceRequestMap = newMap;
    }
    return req;
  }

  /**
   * Increase the compute specs
   * @param target the compute specs to increase
   * @param incr the increment
   */
  public static void incrComputeSpecs(ComputeSpecs target, ComputeSpecs incr) {
    target.numCpus += incr.numCpus;
    target.memoryMB += incr.memoryMB;
    target.diskGB += incr.diskGB;
  }

  /**
   * Decrease the compute specs by decr
   * @param target the specs to decrease
   * @param decr the decrement
   */
  public static void decrComputeSpecs(ComputeSpecs target, ComputeSpecs decr) {
    target.numCpus -= decr.numCpus;
    target.memoryMB -= decr.memoryMB;
    target.diskGB -= decr.diskGB;
  }

  /**
   * Remove the object o from the list l. Different from l.remove(o)
   * because this method only removes it if it is the same object
   * @param l the list to remove from
   * @param o the object to remove
   * @return removed object if it was found in the list, null otherwise
   */
  public static Object removeReference(List l, Object o) {
    Iterator iter = l.iterator();
    while (iter.hasNext()) {
      Object no = iter.next();
      if (no == o) {
        iter.remove();
        return o;
      }
    }
    return null;
  }

  /**
   * A realiable way to wait for the thread termination
   * @param thread thread to wait for
   */
  public static void waitThreadTermination(Thread thread) {
    while (thread != null && thread.isAlive()) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Convert the appinfo string to the address the application is available on
   * @param info the string of the appinfo
   * @return the address application is available on
   */
  public static InetAddress appInfoToAddress(String info) {
    Matcher m = INET_ADDRESS_PATTERN.matcher(info);
    if (m.find()) {
      int port = Integer.parseInt(m.group(2));
      return new InetAddress(m.group(1), port);
    }
    return null;
  }

  /**
   * Sets an uncaught exception handler. This will make the process exit with
   * exit code 1 if a thread exits due to an uncaught exception.
   */
  public static void makeProcessExitOnUncaughtException(final Log log) {
    Thread.setDefaultUncaughtExceptionHandler(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          log.error("UNCAUGHT: Thread " + t.getName() +
            " got an uncaught exception", e);
          System.exit(1);
        }
      });
  }

  public static byte[] asBytes(String addr) {
    // Convert the TCP/IP address string to an integer value
    int ipInt = parseNumericAddress(addr);
    if ( ipInt == 0) {
      return null;
    }
    byte[] ipByts = new byte[4];
    ipByts[3] = (byte) (ipInt & 0xFF);
    ipByts[2] = (byte) ((ipInt >> 8) & 0xFF);
    ipByts[1] = (byte) ((ipInt >> 16) & 0xFF);
    ipByts[0] = (byte) ((ipInt >> 24) & 0xFF);
    return ipByts;
  }

  /**
   * Check if the specified address is a valid numeric TCP/IP address and return as an integer value
   * 
   * @param ipaddr String
   * @return int
   */
  public static int parseNumericAddress(String ipaddr) {
    Matcher m = IP_ADDRESS_PATTERN.matcher(ipaddr);
    int ipInt = 0;
    if (m.find()) {
      for (int i = 1;i < 5;i++) 
        try {
          int ipVal = Integer.valueOf(m.group(i)).intValue();
          if ( ipVal < 0 || ipVal > 255) {
            return 0;
          }
          //  Add to the integer address
          ipInt = (ipInt << 8) + ipVal;
        }
        catch (NumberFormatException ex) {
          return 0;
        }
    }
    //  Return the integer address
    return ipInt;
  }

}

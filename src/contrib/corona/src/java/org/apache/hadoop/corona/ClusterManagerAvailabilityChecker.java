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

import org.apache.hadoop.net.NetUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Has helper method(s) related to the availability of the ClusterManager
 */
public class ClusterManagerAvailabilityChecker {
  static final Log LOG =
    LogFactory.getLog(ClusterManagerAvailabilityChecker.class);

  /**
   * Used for getting a client to the CoronaProxyJobTracker
   * @param conf
   * @return Returns a client to the CPJT
   * @throws IOException
   */
  public static CoronaProxyJobTrackerService.Client
    getPJTClient(CoronaConf conf) throws IOException {
    InetSocketAddress address =
      NetUtils.createSocketAddr(conf.getProxyJobTrackerThriftAddress());
    TFramedTransport transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    CoronaProxyJobTrackerService.Client client =
      new CoronaProxyJobTrackerService.Client(new TBinaryProtocol(transport));
    try {
      transport.open();
    } catch (TException e) {
      LOG.info("Transport Exception: ", e);
    }
    return client;
  }

  /**
   * This helper method simply polls the ProxyJobTracker if and until the
   * clusterManagerSafeMode flag is set there.
   * @param conf
   * @throws IOException
   */
  public static void waitWhileClusterManagerInSafeMode(CoronaConf conf)
    throws IOException {

    CoronaProxyJobTrackerService.Client pjtClient = getPJTClient(conf);
    while (true) {
      try {
        // If this condition holds true, then two things can happen:
        // 1. The CM was never in Safe Mode
        // 2. CM was in Safe Mode, just before we made this method call, and
        //    came out of Safe Mode before the RPC call.
        if (!pjtClient.getClusterManagerSafeModeFlag()) {
          break;
        }

        // If the safe mode flag is indeed set
        LOG.info("Safe mode flag is set on the ProxyJobTracker");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      } catch (TException e) {
        throw new IOException(
          "Could not check the safe mode flag on the ProxyJobTracker", e);
      }
    }
  }
}

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
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.ipc.Client;

public abstract class LeaseRenewal implements Runnable {
  public volatile boolean running;
  private final String clientName;
  private final Configuration conf;
  private final Log LOG = LogFactory.getLog(LeaseRenewal.class);

  public LeaseRenewal(String clientName, Configuration conf) {
    this.clientName = clientName;
    this.conf = conf;
    running = true;
  }
  
  /**
   * This method is called each time we need to renew the lease.
   * 
   * @throws IOException
   */
  protected abstract void renew() throws IOException;

  /**
   * This method is invoked when our LeaseRenewal thread is aborting.
   */
  protected abstract void abort();
  
  /**
   * Computes the renewal period for the lease.
   * 
   * @return the renewal period in ms
   */
  private long computeRenewalPeriod() {
    long hardLeaseLimit = conf.getLong(
        FSConstants.DFS_HARD_LEASE_KEY, FSConstants.LEASE_HARDLIMIT_PERIOD);
    long softLeaseLimit = conf.getLong(
        FSConstants.DFS_SOFT_LEASE_KEY, FSConstants.LEASE_SOFTLIMIT_PERIOD);
    long renewal = Math.min(hardLeaseLimit, softLeaseLimit) / 2;
    long hdfsTimeout = Client.getTimeout(conf);
    if (hdfsTimeout > 0) {
      renewal = Math.min(renewal, hdfsTimeout/2);
    }
    return renewal;
  }

  private void renewalSleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(this + " is interrupted.", ie);
      }
      // Shutdown lease renewal.
      running = false;
      return;
    }
  }

  public void closeRenewal() {
    running = false;
  }

  @Override
  public void run() {
    long lastRenewed = 0;
    int retries = 0;
    long renewal = computeRenewalPeriod();
    int maxRetries = conf.getInt("dfs.lease.renewal.retries",
        FSConstants.MAX_LEASE_RENEWAL_RETRIES);
    while (running && !Thread.interrupted()) {
      if (System.currentTimeMillis() - lastRenewed > renewal) {
        try {
          renew();
          lastRenewed = System.currentTimeMillis();
          retries = 0;
        } catch (SocketException ie) {
          retries++;
          LOG.warn("Can't renew lease for " + clientName +
              " for a period of " + (renewal/1000) +
              " seconds because NameNode is not reachable. Retried " + retries
              + " out of " + maxRetries, ie);
          if (retries > maxRetries) {
            LOG.warn("Can't renew lease for " + clientName +
                " for a period of " + (renewal/1000) +
                " seconds because NameNode is not reachable. Shutting down HDFS client...", ie);
            abort();
            break;
          }
        } catch (IOException ie) {
          LOG.warn("Problem renewing lease for " + clientName +
              " for a period of " + (renewal/1000) +
              " seconds. Will retry shortly...", ie);
        }
      }
      renewalSleep();
    }
  }
}

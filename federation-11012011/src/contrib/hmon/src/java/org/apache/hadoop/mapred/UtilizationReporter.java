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

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;

/**
 * Reports the resource utilization on the TaskTracker to {@link UtilizationCollector}
 */
public class UtilizationReporter implements Configurable {

  private UtilizationGauger utilizationGauger = new LinuxUtilizationGauger();
  // How often does the tasktracker reports the utilization
  private long transmitPeriod;
  private static final long DEFAULT_TRANSMIT_PERIOD = 3000L; // 3 sec
  protected UtilizationCollectorProtocol rpcCollector;
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    conf.addResource("resourceutilization.xml");
    conf.addResource("mapred-site.xml");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static final Log LOG =
          LogFactory.getLog("org.apache.hadoop.mapred.resourceutilization");

  public UtilizationReporter(Configuration conf) throws IOException {
    utilizationGauger.initialGauge();
    setConf(conf);
    // How often does the tasktracker reports the utilization
    transmitPeriod =
            this.conf.getLong("mapred.resourceutilization.transmitperiod",
                              DEFAULT_TRANSMIT_PERIOD);
    initializeRpcCollector(this.conf);
  }

  /**
   * Create the the {@link UtilizationCollectorProtocol} RPC object
   * @param conf Configuration
   * @throws IOException
   */
  protected void initializeRpcCollector(Configuration conf) throws IOException {
    rpcCollector =
          (UtilizationCollectorProtocol) RPC.getProxy(UtilizationCollectorProtocol.class,
                                           UtilizationCollectorProtocol.versionID,
                                           UtilizationCollector.getAddress(conf),
                                           conf);
  }

  /**
   * Transmit the {@link TaskTrackerUtilization} to {@link UtilizationCollectorProtocol}
   * via RPC
   */
  protected void submitReport() {
    try {
      utilizationGauger.gauge();
      rpcCollector.reportTaskTrackerUtilization(
              utilizationGauger.getTaskTrackerUtilization(),
              utilizationGauger.getLocalJobUtilization());
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  /**
   * Keep submitting information to {@link UtilizationCollector}
   */
  void start() {
    while (true) {
      try {
        submitReport();
        Thread.sleep(transmitPeriod);
      } catch (Exception e) {
        System.err.println(e);
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * main program to run on each TaskTracker
   */
  public static void main(String argv[]) throws Exception {
    try {
      Configuration conf = new Configuration();
      UtilizationReporter utilizationReporter = 
              new UtilizationReporter(conf);
      utilizationReporter.start();
    } catch (Throwable e) {
      System.err.println(e);
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}

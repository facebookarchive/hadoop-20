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
package org.apache.hadoop.hdfs.storageservice.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Launcher class for single storage service node */
public class StorageServiceNode implements Tool {
  /** Populate default configs */
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  private static final Log LOG = LogFactory.getLog(StorageServiceNode.class);
  /** Configuration received from ToolRunner */
  private Configuration conf;
  private ClientProxyService proxyService;
  private String clusterId;

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new StorageServiceNode(), args));
    } catch (Exception e) {
      System.exit(-1);
    }
  }

  private void parseArgs(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing clusterName argument");
    }
    clusterId = args[0];
  }

  private void init() throws IOException {
    // Register cleanup hook
    Runtime.getRuntime().addShutdownHook(new Thread("shutdown thread") {
      public void run() {
        LOG.info("Storage service node shutting down...");
        IOUtils.cleanup(LOG, proxyService);
      }
    });
    // Run proxy service
    proxyService = new ClientProxyService(new ClientProxyCommons(conf, clusterId));
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      parseArgs(args);
      init();
      StringUtils.startupShutdownMessage(StorageServiceNode.class, args, LOG);
      while (true) {
        TimeUnit.SECONDS.sleep(10);
        proxyService.getCommons().metrics.dump();
      }
    } catch (Exception e) {
      LOG.fatal("StorageServiceNode shutting down due to exception", e);
      throw e;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}

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
package org.apache.hadoop.hdfs.storageservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.storageservice.NNLatencyBenchmark.MeanDevAccumulator;
import org.apache.hadoop.hdfs.storageservice.NNLatencyBenchmark.TimedCallable;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestNNLatencyBenchmark {
  private MiniAvatarCluster cluster;
  private ClientProxyService proxyService;
  private NNLatencyBenchmark benchmark;

  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp() throws Exception {
    try {
      Configuration conf = new Configuration();
      // Bind port automatically
      conf.setInt(StorageServiceConfigKeys.PROXY_THRIFT_PORT_KEY, 0);
      conf.setInt(StorageServiceConfigKeys.PROXY_RPC_PORT_KEY, 0);

      cluster = new MiniAvatarCluster(conf, 2, true, null, null, 1, true);

      proxyService = new ClientProxyService(new ClientProxyCommons(conf, conf.get(
          FSConstants.DFS_CLUSTER_NAME)));

      benchmark = new NNLatencyBenchmark();
      benchmark.setConf(conf);
    } catch (IOException e) {
      tearDown();
      throw e;
    }
  }

  public void tearDown() throws IOException {
    if (proxyService != null) {
      proxyService.getCommons().metrics.dump();
    }
    IOUtils.cleanup(null, proxyService);
    try {
      if (cluster != null) {
        cluster.shutDown();
      }
    } catch (Exception e) {
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testMeanDevAccumulator() {
    MeanDevAccumulator acc0 = new MeanDevAccumulator();
    for (int i : new int[]{2, 4, 4, 4, 5, 5, 5, 7, 9}) {
      acc0.add(i);
    }
    assertEquals(5, acc0.getMean(), 1e-6);
    assertEquals(4, acc0.getVariance(), 1e-6);
    assertEquals(2, acc0.getStdDev(), 1e-6);
  }

  @Test
  public void testTimedCallable() throws Exception {
    long start = System.nanoTime();
    double time = new TimedCallable() {
      @Override
      protected void callTimed() throws Exception {
        Thread.sleep(337);
      }
    }.call();
    long end = System.nanoTime();
    assertEquals(337, (end - start) / 1e6, 30);
    assertEquals(337, time, 10);
  }

  @Test
  public void testSimpleRun() throws Exception {
    setUp();
    try {
      NNLatencyBenchmark.WARMUP_SAMPLES = 5;
      NNLatencyBenchmark.MEASURED_SAMPLES = 50;
      assertEquals(0, benchmark.run(new String[]{"localhost", "" + proxyService.getThriftPort(),
          "" + proxyService.getRPCPort()}));
    } finally

    {
      tearDown();
    }
  }
}

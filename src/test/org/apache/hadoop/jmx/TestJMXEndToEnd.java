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

package org.apache.hadoop.jmx;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.metrics.ContextFactory;

public class TestJMXEndToEnd extends JMXJsonServletTestCase {

  private Configuration config;
  private MiniDFSCluster cluster;

  protected void setUp() throws Exception {
    super.setUp();
    config = new Configuration();
    config.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);

    // set dfs metrics context class to JMXContext
    ContextFactory.resetFactory();
    ContextFactory f = ContextFactory.getFactory();
    f.setAttribute("dfs.class", "org.apache.hadoop.metrics.jmx.JMXContext");

  }

  public void tearDown() throws Exception {
    ContextFactory.resetFactory();
    if (cluster.isClusterUp())
      cluster.shutdown();

    File data_dir = new File(cluster.getDataDirectory());
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Could not delete hdfs directory in tearDown '" + data_dir + "'");
    }
    super.tearDown();
  }

  public void testDataNodeMetricsExported() throws Exception {
    cluster = new MiniDFSCluster(config, 1, true, null);
    cluster.waitActive();
    // ensure jmx report shows both NameNode and Datanode metrics
    assertRequestMatches("?qry=hadoop:service=DataNode,name=DataNodeActivity*",
        "replaceBlockOpNumOps", "blocks_read", "bytes_written");
    assertRequestMatches("?qry=hadoop:service=NameNode,name=NameNodeInfo", "IsPrimary",
        "PercentUsed", "LiveNodes");
  }
}

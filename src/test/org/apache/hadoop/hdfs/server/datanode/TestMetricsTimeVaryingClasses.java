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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import junit.framework.TestCase;
import java.io.IOException;

public class TestMetricsTimeVaryingClasses extends TestCase {
  
  private MiniDFSCluster cluster;
  private FileSystem fileSystem;
  private DataNodeMetrics metrics;

  @Before
  protected void setUp() throws Exception {
    super.setUp();
    final Configuration conf = new Configuration();
    init(conf);
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
    cluster = null;
  }

  private void init(Configuration conf) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitClusterUp();
    fileSystem = cluster.getFileSystem();

    List<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(datanodes.size(), 1);
    DataNode datanode = datanodes.get(0);
    metrics = datanode.getMetrics();
  }


  public void testMetricsTimeVaryingClasses() throws Exception {
    metrics.bytesWrittenLatency.resetMinMax();
    metrics.bytesWrittenRate.resetMinMax();

    //writesFromLocalClient uses MetricsTimeVaryingInt
    assertEquals(metrics.writesFromLocalClient.getCurrentIntervalValue(),0);
    final long LONG_FILE_LEN = Integer.MAX_VALUE+1L;
    DFSTestUtil.createFile(fileSystem, new Path("/tmp.txt"),
        LONG_FILE_LEN, (short)1, 1L);

    //bytesWritten uses MetricsTimeVaryingLong
    assertEquals(LONG_FILE_LEN, metrics.bytesWritten.getCurrentIntervalValue());
   
    //writesFromLocalClient uses MetricsTimeVaryingInt
    assertTrue(metrics.writesFromLocalClient.getCurrentIntervalValue()>0);

    metrics.doUpdates(null);
    //bytesWrittenLatency uses MetricsTimeVaryingRate
    assertTrue(metrics.bytesWrittenLatency.getMaxTime()>0);
  }

  public void testMetricsTimeVaryingRateClass() throws Exception {
    metrics.bytesWrittenRate.resetMinMax();

    long LEN = 1024 * 42;
    DFSTestUtil.createFile(fileSystem, new Path("/tmp.txt"), LEN, (short) 1, 1L);

    // check we have 42K written in current interval
    assertEquals(42, metrics.bytesWrittenRate.getCurrentIntervalNumOps());
    assertEquals(0, metrics.bytesWrittenRate.getPreviousIntervalNumOps());

    metrics.doUpdates(null);

    // check we have 42K written in previous interval (and none in current)
    assertEquals(0, metrics.bytesWrittenRate.getCurrentIntervalNumOps());
    assertEquals(42, metrics.bytesWrittenRate.getPreviousIntervalNumOps());

    DFSTestUtil.createFile(fileSystem, new Path("/tmp.txt"), LEN, (short) 1, 1L);

    // check we have 42K written in both previous and current intervals
    assertEquals(42, metrics.bytesWrittenRate.getCurrentIntervalNumOps());
    assertEquals(42, metrics.bytesWrittenRate.getPreviousIntervalNumOps());
  }
}

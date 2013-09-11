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

import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData;
import org.apache.hadoop.io.WriteOptions;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSlowWritePacketLogging {
  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  /**
   * This test just triggers the codepath and doesn't do any other verification.
   */
  public void testSlowWritePacketLogging() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    WriteOptions options = new WriteOptions();
    options.setLogSlowWriteProfileDataThreshold(1);
    DFSWriteProfilingData data = new DFSWriteProfilingData();
    DFSClient.setProfileDataForNextOutputStream(data);
    OutputStream out = fs.create(new Path("/test"), null, true, 4096,
        (short) 3, 1024, 512, null, null, options);
    byte[] buffer = new byte[1024 * 1024];
    out.write(buffer);
    out.close();
  }
}

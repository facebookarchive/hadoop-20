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

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.APITraceFileSystem;

import junit.framework.TestCase;

/**
 * Test the APITraceFileSystem preserves functionality
 */
public class TestAPITraceFileSystem extends TestCase {
  public void testGetPos() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster;
    FileSystem fs;
    FSDataInputStream sin;
    FSDataOutputStream sout;
    Path testPath = new Path("/testAppend");
    byte buf[] = "Hello, World!".getBytes("UTF-16");
    byte buf2[] = new byte[buf.length];
    conf.setClass("fs.hdfs.impl",
                  APITraceFileSystem.class,
                  FileSystem.class);
    conf.setBoolean("dfs.support.append", true);
    conf.setInt("dfs.replication", 1);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();
      // create/write
      sout = fs.create(testPath);
      sout.write(buf, 0, buf.length);
      sout.close();

      // open/getPos()/readFully
      sin = fs.open(testPath);
      assertEquals(sin.getPos(), 0);
      sin.readFully(0, buf2, 0, buf2.length);
      assertTrue(Arrays.equals(buf, buf2));
      sin.close();

      // append/getPos()
      sout = fs.append(testPath);
      assertEquals(sout.getPos(), buf.length);
      sout.close();
      fs.close();
    } finally {
      cluster.shutdown();
    }
  }
}

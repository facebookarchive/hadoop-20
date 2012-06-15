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

import java.io.*;
import junit.framework.TestCase;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.MRSharedCaching.TestResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A JUnit test to test caching with DFS
 * 
 */
public class TestMiniMRDFSSharedCaching extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMiniMRDFSSharedCaching.class);

  public void testWithDFS() throws Throwable {
    MiniMRCluster mr = null;
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
    try {
      JobConf conf = new JobConf();
      conf.set("fs.hdfs.impl",
               "org.apache.hadoop.hdfs.ChecksumDistributedFileSystem");

      dfs = new MiniDFSCluster(conf, 1, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(2, fileSys.getName(), 4);
      TestResult ret = MRSharedCaching.launchMRCache("/testing/wc/input",
                                        "/testing/wc/output",
                                        "/cachedir",
                                        mr.createJobConf(),
                                        "The quick brown fox\nhas many silly\n"
                                        + "red fox sox\n", false);
      LOG.info ("Finished Job1");
      assertTrue("Archives not matching", ret.isOutputOk);

      mr.shutdown();
      mr = new MiniMRCluster(2, fileSys.getName(), 4);

      JobConf jConf = mr.createJobConf();
      JobClient jc = new JobClient(jConf);
      UtilsForTests.waitForJobTracker(jc);

      ret = MRSharedCaching.launchMRCache("/testing/wc/input",
                                          "/testing/wc/output",
                                          "/cachedir",
                                          jConf,
                                          "The quick brown fox\nhas many silly\n"
                                          + "red fox sox\n", true);
      LOG.info ("Finished Job2");
      assertTrue("Symlinks not working", ret.isOutputOk);

      ret = MRSharedCaching.launchMRCache2("/testing/wc/input",
                                       "/testing/wc/output",
                                       "/cachedir",
                                       mr.createJobConf(),
                                       "The quick brown fox\nhas many silly\n"
                                       + "red fox sox\n");
      LOG.info ("Finished Job3");
      assertTrue("Duplicate filenames", ret.isOutputOk);

      ret = MRSharedCaching.launchMRCache3("/testing/wc/input",
                                       "/testing/wc/output",
                                       "/cachedir",
                                       mr.createJobConf(),
                                       "The quick brown fox\nhas many silly\n"
                                       + "red fox sox\n");
      LOG.info ("Finished Job4");
      assertTrue("Same file, different filename", ret.isOutputOk);
    } catch (Throwable t) {

      t.printStackTrace();
      throw t;

    } finally {
      if (fileSys != null) {
        fileSys.close();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  public static void main(String[] argv) throws Throwable {
    TestMiniMRDFSSharedCaching td = new TestMiniMRDFSSharedCaching();
    td.testWithDFS();
  }
}

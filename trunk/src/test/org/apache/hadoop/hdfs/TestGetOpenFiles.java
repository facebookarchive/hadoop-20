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

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;

public class TestGetOpenFiles extends junit.framework.TestCase {

  final Path dir = new Path("/test/getopenfiles/");

  // NOTE: this number should be bigger than rpcBatchSize in
  // LeaseManager for the test to cover the case where not all the
  // files can be returns in a single get
  static final int NUM_FILES = 1200;

  public void testOpenFiles() throws Exception {

    ////////////////////////////////////////
    // startup
    ////////////////////////////////////////

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();

    try {
      ////////////////////////////////////////
      // request list of open files
      ////////////////////////////////////////
      {
        assertTrue(fs.mkdirs(dir));

        // make a bunch of files
        for (int x=0; x<NUM_FILES; x++) {
          Path p = new Path(dir, "foo" + x);
          DataOutputStream out = fs.create(p);
          out.writeBytes("blahblah");
        }
        System.err.println("Created " + NUM_FILES + " files");

        // check that they are all open
        int count = 0;
        String start = "";

        // fetch the first batch
        OpenFileInfo[] infoList =
          cluster.getNameNode().iterativeGetOpenFiles(dir.toString(), 0, start);
        count += infoList.length;

        // fetch subsequent batches
        while (infoList.length > 0) {
          start = infoList[infoList.length-1].filePath;
          infoList =
            cluster.getNameNode().iterativeGetOpenFiles(dir.toString(), 0, start);
          count += infoList.length;
        }

        System.err.println("count == " + count);
        System.err.println("NUM_FILES == " + NUM_FILES);

        // assert that we got back the expected open file count
        assertTrue(NUM_FILES == count);

        fs.delete(dir, true);
      }

      ////////////////////////////////////////
      // restart survival test
      ////////////////////////////////////////
      {
        assertTrue(fs.mkdirs(dir));

        // make a bunch of files
        for (int x=0; x<NUM_FILES; x++) {
          Path p = new Path(dir, "foo" + x);
          DataOutputStream out = fs.create(p);
          out.writeBytes("blahblah");
        }
        System.err.println("Created " + NUM_FILES + " files");

        // now restart
        cluster.restartNameNodes();

        // check that they are all open
        int count = 0;
        String start = "";

        // fetch the first batch
        OpenFileInfo[] infoList =
          cluster.getNameNode().iterativeGetOpenFiles(dir.toString(), 0, start);
        count += infoList.length;

        // fetch subsequent batches
        while (infoList.length > 0) {
          start = infoList[infoList.length-1].filePath;
          infoList =
            cluster.getNameNode().iterativeGetOpenFiles(dir.toString(), 0, start);
          count += infoList.length;
        }

        System.err.println("count == " + count);
        System.err.println("NUM_FILES == " + NUM_FILES);

        // assert that we got back the expected open file count
        assertTrue(NUM_FILES == count);

        fs.delete(dir, true);
      }

      ////////////////////////////////////////
      // request partial list of open files
      ////////////////////////////////////////
      {
        assertTrue(fs.mkdirs(dir));

        // make a bunch of files
        for (int x=0; x<NUM_FILES; x++) {
          Path p = new Path(dir, "foo" + x);
          DataOutputStream out = fs.create(p);
          out.writeBytes("blahblah");
        }
        System.err.println("Created " + NUM_FILES + " files");

        // check that they are all open
        int count = 0;
        String start = "";

        // fetch the first batch
        OpenFileInfo[] infoList =
          cluster.getNameNode().iterativeGetOpenFiles(dir.toString(), 0, start);
        count += infoList.length;

        // don't fetch any more

        System.err.println("count == " + count);
        System.err.println("NUM_FILES == " + NUM_FILES);

        fs.delete(dir, true);
      }

      ////////////////////////////////////////
      // shutdown
      ////////////////////////////////////////
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {cluster.shutdown();}
    }
  }
}

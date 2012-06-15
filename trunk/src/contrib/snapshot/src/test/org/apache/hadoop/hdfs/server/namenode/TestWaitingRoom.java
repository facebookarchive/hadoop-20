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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.SnapshotNode.*;
import org.apache.hadoop.net.NetUtils;

/**
 * Tests the WaitingRoom utility
 */
public class TestWaitingRoom extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestWaitingRoom.class);

  public void testWaitingRoom() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 4, true, null);

    FileSystem fs = FileSystem.get(conf);
    WaitingRoom wr = new WaitingRoom(conf);
    String wrPath = conf.get("fs.snapshot.waitingroom", "/.WR");    

    // Create some files
    Path foo = new Path ("/tmp/hadoop/foo");
    fs.create(foo);
    Path bar = new Path ("/tmp/hadoop/fb/bar");
    Path fb = bar.getParent(); // directory
    fs.create(bar);

    // Move to waiting room
    assertTrue(wr.moveToWaitingRoom(foo)); 
    assertTrue(wr.moveToWaitingRoom(fb)); 

    // Confirm that file/dir has been moved to waiting room
    assertFalse(fs.exists(foo));
    assertFalse(fs.exists(fb));
    assertFalse(fs.exists(bar));

    // Confirm that file/dir in waiting room
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/foo")));
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/fb")));
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/fb/bar")));

    // Create same file and move to waiting room again
    fs.create(bar);

    // Move to waiting room
    assertTrue(wr.moveToWaitingRoom(fb));

    // Confirm that second file with .WR0 appended
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/fb.WR0")));
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/fb.WR0/bar")));

    Path baz = new Path ("/tmp/hadoop/foo/baz");
    fs.create(baz);

    // Move to waiting room
    assertTrue(wr.moveToWaitingRoom(baz));
    assertFalse(fs.exists(baz));

    // Confirm that a second version (dir) of foo created for baz
    assertTrue(fs.exists(new Path(wrPath + "/tmp/hadoop/foo.WR0/baz")));
  }
}

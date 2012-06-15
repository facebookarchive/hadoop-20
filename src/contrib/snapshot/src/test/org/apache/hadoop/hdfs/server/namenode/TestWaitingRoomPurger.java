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
import org.apache.hadoop.hdfs.server.namenode.WaitingRoom.*;
import org.apache.hadoop.net.NetUtils;

/**
 * Tests the Waiting Room Purger utility
 */
public class TestWaitingRoomPurger extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestWaitingRoomPurger.class);

  public void testWaitingRoomPurger() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitClusterUp();
    FileSystem fs = cluster.getFileSystem();

    WaitingRoom wr = new WaitingRoom(conf);
    WaitingRoomPurger purger = wr.getPurger();
    SnapshotNode ssNode = new SnapshotNode(conf);
    ssNode.shutdownWaitingRoomPurger();

    String wrPath = conf.get("fs.snapshot.waitingroom", "/.WR");    
    String ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");

    Path foo = new Path("/foo");
    Path bar = new Path("/hadoop/bar");
    Path mash = new Path("/hadoop/mash");

    FSDataOutputStream stream;

    // Create foo
    stream = fs.create(foo);
    stream.writeByte(0);
    stream.close();
    
    // Move foo to waiting room.
    assertTrue(wr.moveToWaitingRoom(foo));
    
    // Create snapshot
    ssNode.createSnapshot("first", false); // contains nothing

    // Create bar (V1)
    stream = fs.create(bar);
    stream.write(0);
    stream.close();

    // Create mash (empty)
    stream = fs.create(mash);
    stream.close();

    // Create snapshot
    ssNode.createSnapshot("second", false); // contains bar (V1), mash

    // Move mash, bar to waiting room
    assertTrue(wr.moveToWaitingRoom(mash));
    assertTrue(wr.moveToWaitingRoom(bar));

    // Create bar (V2)
    stream = fs.create(bar);
    stream.write(0);
    stream.close();

    ssNode.createSnapshot("third", false); // contains bar (V2)

    // Verify fs state right now
    assertTrue(fs.exists(bar));
    assertFalse(fs.exists(foo));
    assertFalse(fs.exists(mash));
    assertTrue(fs.exists(new Path(wrPath + "/foo")));
    assertTrue(fs.exists(new Path(wrPath + "/hadoop/bar")));
    assertTrue(fs.exists(new Path(wrPath + "/hadoop/mash")));

    // Run purger
    purger.purge();

    // Verify fs state right now
    assertTrue(fs.exists(bar));
    assertFalse(fs.exists(foo));
    assertFalse(fs.exists(mash));
    assertFalse(fs.exists(new Path(wrPath + "/foo"))); // deleted: unreferenced
    assertTrue(fs.exists(new Path(wrPath + "/hadoop/bar")));
    assertFalse(fs.exists(new Path(wrPath + "/hadoop/mash"))); // deleted: empty

    // Delete snapshot 'second'
    boolean success = fs.delete(new Path(ssDir + "/" + SnapshotNode.SSNAME + "second"));
    assertTrue(success);

    // Run purger again
    purger.purge();

    // Verify fs state right now
    assertTrue(fs.exists(bar));
    assertFalse(fs.exists(foo));
    assertFalse(fs.exists(mash));
    assertFalse(fs.exists(new Path(wrPath + "/foo"))); // deleted: last run
    assertFalse(fs.exists(new Path(wrPath + "/hadoop/bar"))); // deleted: unreferenced
    assertFalse(fs.exists(new Path(wrPath + "/hadoop/mash"))); // deleted: last run
  }
}
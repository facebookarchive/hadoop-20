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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.namenode.SnapshotShell;

/**
 * A SnapshotClient which allows opening snapshot files for reading
 * and listing their LocatedBlocks.
 * Note: For all paths, just prepend snapshot id to them. E.g for
 * file at /foo in snapshot with id test, put src as test/foo
 */
public class SnapshotClient {
  public static final Log LOG = LogFactory.getLog(SnapshotClient.class);

  private Configuration conf;
  private DFSClient client;
  private SnapshotShell shell;
  private boolean clientRunning = true;

  public SnapshotClient(Configuration conf) throws IOException {
    this.conf = conf;
    this.client = new DFSClient(conf);
    this.shell = new SnapshotShell(conf);
  }

  public LocatedBlocksWithMetaInfo[] getLocatedBlocks(String snapshotId,
      String src) throws IOException {
    return shell.getSnapshotNode().getLocatedBlocks(snapshotId, src);
  }

  public DFSInputStream open(String snapshotId, String src) throws IOException {
    LocatedBlocksWithMetaInfo blocks[] = getLocatedBlocks(snapshotId, src);

    // Not strictly correct. block.length = 1 could mean directory with
    // one file. Might want to add a file specific API.
    if (blocks == null || blocks.length != 1) {
      throw new IOException("File at " + src + " doesn't exist in snapshot");
    }
 
    return client.open(blocks[0]);
  }

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      throw new IOException("SnapshotClient not running");
    }
  }

  public synchronized void close() throws IOException {
    clientRunning = false;
    client.close();
    shell.close();
  }
}

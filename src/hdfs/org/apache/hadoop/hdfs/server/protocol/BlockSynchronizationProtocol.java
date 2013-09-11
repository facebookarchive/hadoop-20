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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface BlockSynchronizationProtocol {
  /**
   * Get the next GenerationStamp to be associated with the specified
   * block.
   *
   * @param block block
   * @param fromNN if it is for lease recovery initiated by NameNode
   * @return a new generation stamp
   */
  public long nextGenerationStamp(Block block, boolean fromNN) throws IOException;

  /**
   * Commit block synchronization in lease recovery
   */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException;
}

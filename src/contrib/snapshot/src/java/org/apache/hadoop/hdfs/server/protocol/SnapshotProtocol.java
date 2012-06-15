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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;

import java.io.IOException;

public interface SnapshotProtocol extends VersionedProtocol {
  public static final long versionID = 1L;

  /**
   * Creates a snapshot for the current file system state.
   * The atomicity guarantees are currently weak, byut future 
   * developments will aim at making them stronger.
   * @param id The id for the snapshot
   * @param updateLeases Whether files under construction should be updated
   */
  public void createSnapshot(String id, boolean updatedLeases) throws IOException;

  /**
   * Deletes the snapshot with the specified id
   * @param id the snapshot id
   * @return whether the snapshot was deleted successfully
   * @throws IOException
   */
  public boolean deleteSnapshot(String id) throws IOException;

  /**
   * List the ids of all snapshots taken in the system
   * @return array of snapshot ids
   * @throws IOException
   */
  public String[] listSnapshots() throws IOException;

  /**
   * Get the FileStatus for the file of the snapshot with the specified
   * id.
   * @param id the snapshot id
   * @return the FileStatus for the snapshot file
   * @throws IOException
   */
  public FileStatus getSnapshotFileStatus(String id) throws IOException;

  /**
   * Get locations of the blocks of the specified file within the specified range.
   * DataNode locations for each block are sorted by
   * the proximity to the client.
   * <p>
   * Return {@link LocatedBlocks} which contains
   * file length, blocks and their locations.
   * DataNode locations for each block are sorted by
   * the distance to the client's address.
   * <p>
   * The client will then have to contact
   * one of the indicated DataNodes to obtain the actual data.
   *
   * @param snapshotId id of snapshot
   * @param src path of source file/dir
   * @return file length and array of blocks with their locations
   * @throws IOException
   */
  public LocatedBlocksWithMetaInfo[] getLocatedBlocks(String snapshotId,
      String src) 
  throws IOException;
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * This class defines a FileStatus that includes a file's blocks & locations.
 */
public class LocatedBlockFileStatus extends FileStatus {
  private BlockAndLocation[] blockLocations;
  private boolean isUnderConstruction;

  /**
   * Constructor
   * @param stat a file status
   * @param blockLocations a file's blocks and locations
   * @param isUnderConstruction isUnderConstruction
   */
  public LocatedBlockFileStatus(FileStatus stat,
      BlockAndLocation[] blockLocations, boolean isUnderConstruction)
          throws IOException {
    this(stat.getLen(), stat.isDir(), stat.getReplication(),
        stat.getBlockSize(), stat.getModificationTime(),
        stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
        stat.getGroup(), stat.getPath(), blockLocations,
        isUnderConstruction);
  }

  /**
   * Constructor
   *
   * @param length a file's length
   * @param isdir if the path is a directory
   * @param block_replication the file's replication factor
   * @param blocksize a file's block size
   * @param modification_time a file's modification time
   * @param access_time a file's access time
   * @param permission a file's permission
   * @param owner a file's owner
   * @param group a file's group
   * @param path the path's qualified name
   * @param blockLocations a file's blocks and locations
   * @param isUnderConstruction if last block is under construction
   */
  public LocatedBlockFileStatus(long length, boolean isdir,
      int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group,
      Path path,
      BlockAndLocation[] blockLocations,
      boolean isUnderConstruction) {
    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, permission, owner, group, path);
    this.blockLocations = blockLocations;
    this.isUnderConstruction = isUnderConstruction;
  }

  /** Get the blocks and locations belong to this file */
  public BlockAndLocation[] getBlockLocations() {
    return blockLocations;
  }

  /** Check if the file is under construction */
  public boolean isFileUnderConstruction() {
    return isUnderConstruction;
  }
}
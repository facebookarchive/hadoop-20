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

import java.io.IOException;

import org.apache.hadoop.raid.Codec;

/**
 * This BlockPlacementPolicy uses a simple heuristic, random placement of
 * the replicas of a newly-created block for all the files in the system, 
 * for the purpose of spreading out the 
 * group of blocks which used by RAID for recovering each other. 
 * This is important for the availability of the blocks. 
 * 
 * Replication of an existing block continues to use the default placement
 * policy.
 * 
 * This simple block placement policy does not guarantee that
 * blocks on the RAID stripe are on different nodes. However, BlockMonitor
 * will periodically scans the raided files and will fix the placement
 * if it detects violation. 
 */
public class BlockPlacementPolicyRaidRandom extends BlockPlacementPolicyRaid {
  @Override
  protected FileInfo getFileInfo(FSInodeInfo srcINode, String path) throws IOException {
    FileInfo info = super.getFileInfo(srcINode, path);
    if (info.type == FileType.NOT_RAID) {
      return new FileInfo(FileType.SOURCE, Codec.getCodec("rs"));
    }
    return info;
  }
}

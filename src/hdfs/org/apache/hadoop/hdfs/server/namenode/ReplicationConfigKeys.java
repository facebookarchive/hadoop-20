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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;

class ReplicationConfigKeys {
  
  // constants
  static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
  static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;
  static final int OVERREPLICATION_WORK_MULTIPLIER_PER_ITERATION = 10;
  static final int RAID_ENCODING_TASK_MULTIPLIER_PER_ITERATION = 1;
  static final int RAID_ENCODING_TASK_LIMIT_DEFAULT = 1;
  static final int RAID_DECODING_TASK_MULTIPLIER_PER_ITERATION = 1;
  static final int RAID_DECODING_TASK_LIMIT_DEFAULT = 2;

  // underReplicationRecheckInterval is how often 
  // the namenode checks for new under replication work
  public static long replicationRecheckInterval = 3000;
  
  // How many blocks (heartbeats.size() * multiplier) are replicated in a round
  public static float replicationWorkMultiplier 
    = REPLICATION_WORK_MULTIPLIER_PER_ITERATION;
  
  // How many over-replicated blocks (heartbeats.size() * multiplier)
  // are scheduled for invalidation in one round
  public static int overreplicationWorkMultiplier 
    = OVERREPLICATION_WORK_MULTIPLIER_PER_ITERATION;
  
  // At each heartbeat, ask datanode only up to this many blocks to delete.
  public static int blockInvalidateLimit = FSConstants.BLOCK_INVALIDATE_CHUNK;
  
  // How many raid works (heartbeats.size() * multiplier)
  // are scheduled in one round
  public static volatile int raidEncodingTaskMultiplier = 
      RAID_ENCODING_TASK_MULTIPLIER_PER_ITERATION;
  public static volatile int raidDecodingTaskMultiplier = 
      RAID_DECODING_TASK_MULTIPLIER_PER_ITERATION;
  
  // At each heartbeat, ask datanode only up to this many raid tasks to do
  public static volatile int raidEncodingTaskLimit = RAID_ENCODING_TASK_LIMIT_DEFAULT;
  public static volatile int raidDecodingTaskLimit = RAID_DECODING_TASK_LIMIT_DEFAULT;
  
  static void updateConfigKeys(Configuration conf) {
    replicationRecheckInterval =
        conf.getInt("dfs.replication.interval", 3) * 1000L;
    
    replicationWorkMultiplier =
        conf.getFloat("dfs.replication.iteration.multiplier",
            REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
    
    overreplicationWorkMultiplier =
        conf.getInt("dfs.overreplication.iteration.multiplier",
            OVERREPLICATION_WORK_MULTIPLIER_PER_ITERATION);
    
    blockInvalidateLimit = conf.getInt("dfs.block.invalidate.limit",
        FSConstants.BLOCK_INVALIDATE_CHUNK);
    
    raidEncodingTaskMultiplier = 
        conf.getInt("dfs.raid.encoding.task.iteration.multiplier", 
            RAID_ENCODING_TASK_MULTIPLIER_PER_ITERATION);
    raidDecodingTaskMultiplier = 
        conf.getInt("dfs.raid.decoding.task.iteration.multiplier", 
            RAID_DECODING_TASK_MULTIPLIER_PER_ITERATION);
        
    raidEncodingTaskLimit = conf.getInt("dfs.raid.encoding.task.limit", 
        RAID_ENCODING_TASK_LIMIT_DEFAULT);
    raidDecodingTaskLimit = conf.getInt("dfs.raid.decoding.task.limit", 
        RAID_DECODING_TASK_LIMIT_DEFAULT);
  }
  
}

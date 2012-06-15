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

public class BlockFlags {
  
  /**
   * This constant is used to indicate that the block deletion does not need
   * explicit ACK from the datanode. When a block is put into the list of blocks
   * to be deleted, it's size is set to this constant. We assume that no block
   * would actually have this size. Otherwise, we would miss ACKs for blocks
   * with such size. Positive number is used for compatibility reasons.
   */
  public static final long NO_ACK = Long.MAX_VALUE;
  
  /**
   * The flag indicating the the block has been deleted, when sending
   * block reports.
   */
  public static final long DELETED = Long.MAX_VALUE - 1;
  
  /**
   * When sending incremental block reports there are situations when we cannot
   * process a particular block on a standby node, because the node has still 
   * not processed the edits entirely. This flag marks the block to be ignored
   * by the namenode - the block will be re-processed at a later time
   */
  public static final long IGNORE = Long.MAX_VALUE - 2;

}

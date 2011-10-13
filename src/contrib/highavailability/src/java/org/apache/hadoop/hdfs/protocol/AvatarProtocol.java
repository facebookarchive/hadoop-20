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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;

/**********************************************************************
 * AvatarProtocol is a superset of the ClientPotocol. It includes
 * methods to switch avatars from Namenode to StandbyNode and
 * vice versa
 *
 **********************************************************************/
public interface AvatarProtocol extends ClientProtocol {

  // The version of this protocl is the same as the version of 
  // the underlying client protocol.

  /**
   * Get the avatar of this instance
   * @return the current avatar of this instance
   * @throws IOException
   */
  public Avatar getAvatar() throws IOException;

  /**
   * Set the avatar of this instance
   * @throws IOException
   */
  public void setAvatar(Avatar avatar) throws IOException;
  
  /**
   * Cleanly shutdown this instance.
   * @throws IOException
   */
  public void shutdownAvatar() throws IOException;

  /**
   * Override the blockReceived message in the DatanodeProtocol
   * This makes the namenode return the list of blocks that do not
   * belong to any file, the AvatarDataNode then retries this
   * blockreceived message. This trick populates newly created files/block
   * with their correct replica locations on the StandbyNamenode.
   * If a block truly does not belong to any file, then it will be 
   * cleared up in the next block report.
   * @return the list of blocks that do not belong to any file in the
   * namenode.
   */
  public Block[] blockReceivedAndDeletedNew(DatanodeRegistration registration,
                                            Block blocksReceivedAndDeleted[])
                                            throws IOException;
  
  /**
   * Override the blockReceived message in the DatanodeProtocol
   * This makes the namenode return the list of blocks that do not
   * belong to any file, the AvatarDataNode then retries this
   * blockreceived message. This trick populates newly created files/block
   * with their correct replica locations on the StandbyNamenode.
   * If a block truly does not belong to any file, then it will be 
   * cleared up in the next block report.
   * @return the list of blocks that do not belong to any file in the
   * namenode.
   */
  public ReceivedDeletedBlockInfo[] blockReceivedAndDeletedNew(DatanodeRegistration registration,
                                            ReceivedDeletedBlockInfo blocksReceivedAndDeleted[])
                                            throws IOException;
  
  public DatanodeCommand blockReportNew(DatanodeRegistration reg, BlockReport rep) throws IOException;

  public DatanodeCommand[] sendHeartbeatNew(DatanodeRegistration registration,
                                       long capacity,
                                       long dfsUsed, long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException;
}


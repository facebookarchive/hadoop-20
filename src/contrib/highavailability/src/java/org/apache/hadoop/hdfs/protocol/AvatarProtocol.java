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
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;

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
   * Get the avatar of this instance.
   * This function is BLOCKING on failover in progress.
   * @return the current avatar of this instance
   * @throws IOException
   */
  public Avatar getAvatar() throws IOException;
  
  /**
   * Get the avatar of this instance.
   * This function is NON-BLOCKING on failover in progress,
   * and can be used only for showing the current avatar
   * @return the current avatar of this instance
   * @throws IOException
   */
  public Avatar reportAvatar() throws IOException;
  
  /**
   * Check if this avatar instance is initialized
   * @return true if the instance is initialized, false otherwise
   */
  public boolean isInitialized() throws IOException;

  /**
   * Set the avatar of this instance
   * 
   * @throws IOException
   * @deprecated Use {@link #quiesceForFailover()} followed by
   *             {@link #performFailover()} instead
   */
  public void setAvatar(Avatar avatar) throws IOException;

  /**
   * Quiesces the Standby Avatar to make sure the standby has consumed all
   * transactions and also performs certain verifications that the Standby
   * avatar's state is consistent.
   *
   * @param noverification
   *          whether or not to perform verification
   * @throws IOException
   *           if this is the Primary Avatar or the Standby had some error
   */
  public void quiesceForFailover(boolean noverification) throws IOException;

  /**
   * Instructs the Standby to come as the Primary.
   *
   * @throws IOException
   *           if this is the Primary Avatar or the Standby had some error
   */
  public void performFailover() throws IOException;

  /**
   * Set the avatar of this instance.
   * 
   * @param force
   *          whether or not to force the failover
   * 
   * @throws IOException
   * @deprecated Use {@link #quiesceForFailover()} followed by
   *             {@link #performFailover()} instead
   */
  public void setAvatar(Avatar avatar, boolean force) throws IOException;

  /**
   * Cleanly shutdown this instance.
   * 
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
   * 
   * KEPT FOR BACKWARD COMPATIBILITY
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
   * @return a bitmap indicating the blocks that do not belong to any 
   * file in the namenode.
   */
  public long[] blockReceivedAndDeletedNew(DatanodeRegistration registration,
                                            IncrementalBlockReport receivedAndDeletedBlocks)
                                            throws IOException;
  
  public DatanodeCommand blockReportNew(DatanodeRegistration reg, BlockReport rep) throws IOException;

  public DatanodeCommand[] sendHeartbeatNew(DatanodeRegistration registration,
                                       long capacity,
                                       long dfsUsed, long remaining,
                                       long namespaceUsed,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException;

  /**
   * Instructs the Avatar that the datanode has finished processing all
   * outstanding commands from the primary. This is used during a failover, so
   * that the Standby knows whether the datanode has processed all commands from
   * the Primary.
   * 
   * @param reg
   *          the {@link DatanodeRegistration} for the datanode
   * @throws IOException
   */
  public void primaryCleared(DatanodeRegistration reg) throws IOException;

}


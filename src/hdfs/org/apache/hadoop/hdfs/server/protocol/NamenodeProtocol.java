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

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.ipc.VersionedProtocol;

/*****************************************************************************
 * Protocol that a secondary NameNode and the Snapshot Node use to 
 * communicate with the NameNode.
 * It's used to get part of the name node state
 *****************************************************************************/
public interface NamenodeProtocol extends VersionedProtocol {
  /**
   * 3: Added a parameter to rollFSImage() and
   *    changed the definition of CheckpointSignature
   */
  public static final long versionID = 3L;

  /** Get a list of blocks belonged to <code>datanode</code>
    * whose total size is equal to <code>size</code>
   * @param datanode  a data node
   * @param size      requested size
   * @return          a list of blocks & their locations
   * @throws RemoteException if size is less than or equal to 0 or
                                   datanode does not exist
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException;

  /**
   * @return The most recent transaction ID that has been synced to
   * persistent storage.
   * @throws IOException
   */
  public long getTransactionID() throws IOException;

  /**
   * Closes the current edit log and opens a new one. The 
   * call fails if the file system is in SafeMode.
   * @throws IOException
   * @return a unique token to identify this transaction.
   */
  public CheckpointSignature rollEditLog() throws IOException;

  /**
   * Rolls the fsImage log. It removes the old fsImage, copies the
   * new image to fsImage, removes the old edits and renames edits.new 
   * to edits. The call fails if any of the four files are missing.
   * 
   * @param newImageSignature the signature of the new fsimage
   * @throws IOException
   */
  public void rollFsImage(CheckpointSignature newImageSignature)
  throws IOException;


  /**
   * Gets the length of the blocks with ids in blockIds
   * @param blockIds the ids of block for which the lengths are being requested
   * @return the lengths of the blocks. -1 for blocks which couldn't be resolved.
   */
  public long[] getBlockLengths(long[] blockIds);

  /**
   * Gets the CheckpointSignature at the time the call was made
   * @return the CheckpointSignature
   * @throws IOException 
   */
  public CheckpointSignature getCheckpointSignature() throws IOException;

  /***
   * Updates the DatanodeInfo for each LocatedBlock in locatedBlocks. Used
   * by SnapshotShell to read data for a file from a snapshot.
   * @param locatedBlocks the LocatedBlocks stored in the snapshot for the file
   * @return LocatedBlocks for same block set with updated DatanodeInfo
   * @throws IOException
   */
  public LocatedBlocksWithMetaInfo updateDatanodeInfo(
      LocatedBlocks locatedBlocks) throws IOException;
  
  
  /**
   * Return a structure containing details about all edit logs available to be
   * fetched from the NameNode.
   * 
   * @param sinceTxId return only logs that contain transactions >= sinceTxId
   */
  public RemoteEditLogManifest getEditLogManifest(long l) throws IOException; 
  
  /**
   * Issued by standby to validate that it is allowed to talk to the primary.
   * 
   * @return data transfer version supported by NameNode 
   */
  public int register() throws IOException;
}
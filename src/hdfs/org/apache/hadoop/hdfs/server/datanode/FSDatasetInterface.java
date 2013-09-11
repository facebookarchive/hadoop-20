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
package org.apache.hadoop.hdfs.server.datanode;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * This is an interface for the underlying storage that stores blocks for
 * a data node. 
 * Examples are the FSDataset (which stores blocks on dirs)  and 
 * SimulatedFSDataset (which simulates data).
 *
 */
public interface FSDatasetInterface extends FSDatasetMBean {

  /**
   * Returns the specified block's on-disk length (excluding metadata)
   * @param namespaceId - parent namespace id
   * @param b
   * @return   the specified block's on-disk length (excluding metadta)
   * @throws IOException
   */
  public long getFinalizedBlockLength(int namespaceId, Block b) throws IOException;

  /**
   * Get the object which can be used to set visibility for the block
   * 
   * @param namespaceId- parent namespace id
   * @param block for the object
   * @return the object can be used to set visibility
   * @throws IOException
   */
  public ReplicaBeingWritten getReplicaBeingWritten(int namespaceId, Block b) throws IOException;

  /**
   * Returns the specified block's visible length (has metadata for this)
   * @param namespaceId - parent namespace id
   * @param b
   * @return   the specified block's visible length
   * @throws IOException
   */
  public long getOnDiskLength(int namespaceId, Block b) throws IOException;

  /**
   * @param namespaceId - parent namespace id
   * @param blkid - id of the block whose generation stam is to be fetched
   * @return the generation stamp stored with the block.
   */
  public Block getStoredBlock(int namespaceId, long blkid) throws IOException;


  /**
   * Creates the block and returns output streams to write data and CRC
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @param isRecovery True if this is part of error recovery, otherwise false
   * @param isReplicationRequest True if this is part of block replication request
   * @return a BlockWriteStreams object to allow writing the block data
   *  and CRC
   * @throws IOException
   */
  public DatanodeBlockWriter writeToBlock(int namespaceId, Block b, Block oldBlock,
      boolean isRecovery, boolean isReplicationRequest, int checksumType,
      int bytePerChecksum) throws IOException;

  /**
   * Update the block to the new generation stamp and length.  
   */
  public void updateBlock(int namespaceId, Block oldblock, Block newblock) throws IOException;

  /**
   * Finalizes the block previously opened for writing using writeToBlock.
   * The block size is what is in the parameter b and it must match the amount
   *  of data written
   *  
   * @param namespaceId - parent namespace id
   * @param b
   * @throws IOException
   */
  public void finalizeBlock(int namespaceId, Block b) throws IOException;

  /**
   * Finalizes the block previously opened for writing using writeToBlock 
   * if not already finalized
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @throws IOException
   */
  public void finalizeBlockIfNeeded(int namespaceId, Block b) throws IOException;

  /**
   * Unfinalizes the block previously opened for writing using writeToBlock.
   * The temporary file associated with this block is deleted.
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @throws IOException
   */
  public void unfinalizeBlock(int namespaceId, Block b) throws IOException;
  
  /**
   * return blocksBeingWrittenReport
   * 
   * @param namespaceId - parent namespace id
   * @return blocksBeingWrittenReport
   */
  public Block[] getBlocksBeingWrittenReport(int namespaceId) throws IOException;
  
  /**
   * Returns the block report - the full list of blocks stored
   * Returns only finalized blocks
   * 
   * @param namespaceId - parent namespace id
   * @return - the block report - the full list of blocks stored
   */
  public Block[] getBlockReport(int namespaceId) throws IOException;

  /**
   * Return whether the block is finalized.
   * 
   * @param namespaceId
   *          - parent namespace id
   * @param b
   * @return - true if the specified block is valid
   */
  public boolean isBlockFinalized(int namespaceId, Block block) throws IOException;  

  /**
   * Is the block valid?
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @return - true if the specified block is valid
   */
  public boolean isValidBlock(int namespaceId, Block b, boolean checkSize) throws IOException; 

  /**
   * Invalidates the specified blocks
   * 
   * @param namespaceId - parent namespace id
   * @param invalidBlks - the blocks to be invalidated
   * @throws IOException
   */
  public void invalidate(int namespaceId, Block invalidBlks[]) throws IOException;

    /**
     * Check if all the data directories are healthy
     * @throws DiskErrorException
     */
  public void checkDataDir() throws DiskErrorException;
      
    /**
     * Stringifies the name of the storage
     */
  public String toString();
 
  /**
   * Shutdown the FSDataset
   */
  public void shutdown();

  /**
   * Validate that the contents in the Block matches
   * the file on disk. Returns true if everything is fine.
   * 
   * @param namespaceId - parent namespace id
   * @param b The block to be verified.
   * @throws IOException
   */
  public void validateBlockMetadata(int namespaceId, Block b) throws IOException;

  /**
   * checks how many valid storage volumes are there in the DataNode
   * @return true if more then minimum valid volumes left in the FSDataSet
   */
  public boolean hasEnoughResource();

  /**
   * Get File name for a given data block.
   * @param namespaceId - parent namespace id
   * @param b - block
   **/
  public File getBlockFile(int namespaceId, Block b) throws IOException;

  /** 
   * Get replica information for a given data block.  
   * @param namespaceId - parent namespace id 
   * @param b - block 
   **/  
  public DatanodeBlockInfo getDatanodeBlockInfo(int namespaceId, Block b) throws IOException;
  
  /**
   * Return a replicaToRead object that can fetch file location and
   * size information.
   * @param namespaceId
   * @param blockId
   * @return
   */
  public ReplicaToRead getReplicaToRead(int namespaceId, Block block)
      throws IOException;  


  /**
   * Copies over a block from a block file, note that the block file might be a
   * file on another Datanode sharing this machine.
   * 
   * @param srcFileSystem
   *          filesystem to which srcBlockFile belongs to
   * @param srcBlockFile
   *          the file holding the data for the srcBlock
   * @param srcNamespaceId
   *          the namespace id of the source block
   * @param srcBlock
   *          the source block which needs to be copied
   * @param dstBlock
   *          the destination block to which the srcBlock needs to be copied to
   * @throws IOException
   */
  public void copyBlockLocal(String srcFileSystem, File srcBlockFile,
      int srcNamespaceId, Block srcBlock,
      int dstNamespaceId, Block dstBlock) throws IOException;

  public BlockRecoveryInfo startBlockRecovery(int namespaceId, long blockId) throws IOException;

  /**
   * Given a block, determine which file system it belongs to.
   * 
   * @param namespaceId
   *          the namespace id of the block
   * @param block
   *          the block that needs to be mapped to a file system
   * @return the file system that the block belongs to
   * @throws IOException
   *           if the block is invalid
   */
  public String getFileSystemForBlock(int namespaceId, Block block) throws IOException;

  /**
   * add new namespace
   * 
   * @param namespaceId
   * @param namespaceDir - where the namespace disk directory structure is to be placed
   * @param conf Configuration
   */
  public void addNamespace(int namespaceId, String namespaceDir, Configuration conf) throws IOException;
  
  /**
   * remove namespace
   * 
   * @param namespaceId
   */
  public void removeNamespace(int namespaceId);
  
  /**
   * Initializes FSDataset.
   * @param storage
   * @throws IOException
   */

  void initialize(DataStorage storage) throws IOException;
  
  /**
   * Get the number of blocks for a given namespace.
   * @param namespaceId
   * @return the number of blocks in the namespace
   */
  public long size(int namespaceId);

}

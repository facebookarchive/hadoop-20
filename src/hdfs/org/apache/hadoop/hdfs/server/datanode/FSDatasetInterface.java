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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
   * Returns the length of the metadata file of the specified block
   * @param namespaceId - parent namespace id
   * @param b - the block for which the metadata length is desired
   * @return the length of the metadata file for the specified block.
   * @throws IOException
   */
  public long getMetaDataLength(int namespaceId, Block b) throws IOException;

  /**
   * This class provides the input stream and length of the metadata
   * of a block
   *
   */
  static class MetaDataInputStream extends FilterInputStream {
    MetaDataInputStream(InputStream stream, long len) {
      super(stream);
      length = len;
    }
    private long length;
    
    public long getLength() {
      return length;
    }
  }
  
  /**
   * Returns metaData of block b as an input stream (and its length)
   * @param namespaceId - parent namespace id
   * @param b - the block
   * @return the metadata input stream; 
   * @throws IOException
   */
  public MetaDataInputStream getMetaDataInputStream(int namespaceId, Block b)
        throws IOException;
  
  /**
   * Does the meta file exist for this block?
   * @param namespaceId - parent namespace id
   * @param b - the block
   * @return true of the metafile for specified block exits
   * @throws IOException
   */
  public boolean metaFileExists(int namespaceId, Block b) throws IOException;


  /**
   * Returns the specified block's on-disk length (excluding metadata)
   * @param namespaceId - parent namespace id
   * @param b
   * @return   the specified block's on-disk length (excluding metadta)
   * @throws IOException
   */
  public long getFinalizedBlockLength(int namespaceId, Block b) throws IOException;

  /**
   * Returns the specified block's visible length (has metadata for this)
   * @param namespaceId - parent namespace id
   * @param b
   * @return   the specified block's visible length
   * @throws IOException
   */
  public long getVisibleLength(int namespaceId, Block b) throws IOException;

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
   * Returns an input stream to read the contents of the specified block
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @return an input stream to read the contents of the specified block
   * @throws IOException
   */
  public InputStream getBlockInputStream(int namespaceId, Block b) throws IOException;
  
  /**
   * Returns an input stream at specified offset of the specified block
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @param seekOffset
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public InputStream getBlockInputStream(int namespaceId, Block b, long seekOffset)
            throws IOException;

  /**
   * Returns an input stream at specified offset of the specified block
   * The block is still in the tmp directory and is not finalized
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @param blkoff
   * @param ckoff
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public BlockInputStreams getTmpInputStreams(int namespaceId, Block b, long blkoff, long ckoff)
            throws IOException;

     /**
      * 
      * This class contains the output streams for the data and checksum
      * of a block
      *
      */
     static class BlockWriteStreams {
      OutputStream dataOut;
      OutputStream checksumOut;
      BlockWriteStreams(OutputStream dOut, OutputStream cOut) {
        dataOut = dOut;
        checksumOut = cOut;
      }
      
    }

  /**
   * This class contains the input streams for the data and checksum
   * of a block
   */
  static class BlockInputStreams implements Closeable {
    final InputStream dataIn;
    final InputStream checksumIn;

    BlockInputStreams(InputStream dataIn, InputStream checksumIn) {
      this.dataIn = dataIn;
      this.checksumIn = checksumIn;
    }

    /** {@inheritDoc} */
    public void close() {
      IOUtils.closeStream(dataIn);
      IOUtils.closeStream(checksumIn);
    }
  }
    
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
  public BlockWriteStreams writeToBlock(int namespaceId, Block b, boolean isRecovery, 
                                        boolean isReplicationRequest) throws IOException;

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
   * Returns the current offset in the data stream.
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @param stream The stream to the data file and checksum file
   * @return the position of the file pointer in the data stream
   * @throws IOException
   */
  public long getChannelPosition(int namespaceId, Block b, BlockWriteStreams stream) throws IOException;

  /**
   * Sets the file pointer of the data stream and checksum stream to
   * the specified values.
   * 
   * @param namespaceId - parent namespace id
   * @param b
   * @param stream The stream for the data file and checksum file
   * @param dataOffset The position to which the file pointre for the data stream
   *        should be set
   * @param ckOffset The position to which the file pointre for the checksum stream
   *        should be set
   * @throws IOException
   */
  public void setChannelPosition(int namespaceId, Block b, BlockWriteStreams stream, long dataOffset,
                                 long ckOffset) throws IOException;

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

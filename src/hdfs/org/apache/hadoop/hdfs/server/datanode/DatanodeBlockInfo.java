
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

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.io.IOUtils;

/**
 * This class is used by the datanode to maintain the map from a block 
 * to its metadata.
 */
public class DatanodeBlockInfo implements ReplicaToRead {
  public static long UNFINALIZED = -1;

  final private boolean inlineChecksum; // whether the file uses inline checksum.
  final private int checksumType;
  final private int bytesPerChecksum;
  private boolean detached;      // copy-on-write done for block
  private long finalizedSize;             // finalized size of the block
  final private boolean visible;
  volatile private boolean blockCrcValid;
  private int blockCrc;
  private Block block;
  
  final BlockDataFile blockDataFile;
  

  DatanodeBlockInfo(FSVolume vol, File file, long finalizedSize,
      boolean visible, boolean inlineChecksum, int checksumType,
      int bytesPerChecksum, boolean blockCrcValid, int blockCrc) {
    this.finalizedSize = finalizedSize;
    detached = false;
    this.visible = visible;
    this.inlineChecksum = inlineChecksum;
    this.checksumType = checksumType;
    this.bytesPerChecksum = bytesPerChecksum;
    this.blockCrcValid = blockCrcValid;
    this.blockCrc = blockCrc;
    this.block = null;
    this.blockDataFile = new BlockDataFile(file, vol);
  }
  
  void setBlock(Block b) {
    this.block = b;
  }

  Block getBlock() {
    return block;
  }

  @Override
  public File getDataFileToRead() {
    if (!visible) {
      return null;
    } else {
      return blockDataFile.file;
    }
  }

  public int getChecksumType() {
    return checksumType;
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }
  
  public boolean isFinalized() {
    return finalizedSize != UNFINALIZED;
  }

  public boolean isInlineChecksum() {
    return inlineChecksum;
  }

  public long getFinalizedSize() throws IOException {
    if (finalizedSize == UNFINALIZED) {
      throw new IOException("Try to get finalized size for unfinalized block");
    }
    return finalizedSize;
  }

  public void setFinalizedSize(long size) {
    this.finalizedSize = size;
  }

  /**
   * THIS METHOD IS ONLY CALLED BY UNIT TESTS to synchronize
   * in memory size after directly calling truncateBlock()
   *
   * @throws IOException
   */
  public void syncInMemorySize() throws IOException {
    if (finalizedSize == UNFINALIZED) {
      throw new IOException("Block is not finalized");
    }
    if (!inlineChecksum) {
      this.finalizedSize = blockDataFile.getFile().length();
    } else {
      this.finalizedSize = BlockInlineChecksumReader
          .getBlockSizeFromFileLength(blockDataFile.getFile().length(),
              checksumType, bytesPerChecksum);
    }
  }

  public void verifyFinalizedSize() throws IOException {
    if (this.blockDataFile.getFile() == null) {
      throw new IOException("No file for block.");
    }
    if (!this.blockDataFile.getFile().exists()) {
      throw new IOException("File " + this.blockDataFile.getFile()
          + " doesn't exist on disk.");
    }
    if (this.finalizedSize == UNFINALIZED) {
      return;
    }
    long onDiskSize = this.blockDataFile.getFile().length();
    if (!inlineChecksum) {
      if (onDiskSize != this.finalizedSize) {
        throw new IOException("finalized size of file "
            + this.blockDataFile.getFile()
            + " doesn't match size on disk. On disk size: " + onDiskSize
            + " size in memory: " + this.finalizedSize);
      }
    } else {
      if (BlockInlineChecksumReader.getBlockSizeFromFileLength(
          onDiskSize, checksumType, bytesPerChecksum) != this.finalizedSize) {
        throw new IOException("finalized size of file "
            + this.blockDataFile.getFile()
            + " doesn't match block size on disk. On disk size: " + onDiskSize
            + " size in memory: " + this.finalizedSize);
      }      
    }
  }
  
  /**
   * Is this block already detached?
   */
  boolean isDetached() {
    return detached;
  }

  /**
   *  Block has been successfully detached
   */
  void setDetached() {
    detached = true;
  }

  /**
   * Copy specified file into a temporary file. Then rename the
   * temporary file to the original name. This will cause any
   * hardlinks to the original file to be removed. The temporary
   * files are created in the detachDir. The temporary files will
   * be recovered (especially on Windows) on datanode restart.
   */
  private void detachFile(int namespaceId, File file, Block b) throws IOException {
    File tmpFile = blockDataFile.volume.createDetachFile(namespaceId, b, file.getName());
    try {
      IOUtils.copyBytes(new FileInputStream(file),
                        new FileOutputStream(tmpFile),
                        16*1024, true);
      if (file.length() != tmpFile.length()) {
        throw new IOException("Copy of file " + file + " size " + file.length()+
                              " into file " + tmpFile +
                              " resulted in a size of " + tmpFile.length());
      }
      FileUtil.replaceFile(tmpFile, file);
    } catch (IOException e) {
      boolean done = tmpFile.delete();
      if (!done) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
                          tmpFile);
      }
      throw e;
    }
  }

  /**
   * Returns true if this block was copied, otherwise returns false.
   */
  boolean detachBlock(int namespaceId, Block block, int numLinks) throws IOException {
    if (isDetached()) {
      return false;
    }
    if (blockDataFile.getFile() == null || blockDataFile.volume == null) {
      throw new IOException("detachBlock:Block not found. " + block);
    }

    File meta = null;
    if (!inlineChecksum) {
      meta = BlockWithChecksumFileWriter.getMetaFile(blockDataFile.getFile(), block);
      if (meta == null) {
        throw new IOException("Meta file not found for block " + block);
      }
    }

    if (HardLink.getLinkCount(blockDataFile.getFile()) > numLinks) {
      DataNode.LOG.info("CopyOnWrite for block " + block);
      detachFile(namespaceId, blockDataFile.getFile(), block);
    }
    if (!inlineChecksum) {
      if (HardLink.getLinkCount(meta) > numLinks) {
        detachFile(namespaceId, meta, block);
      }
    }
    setDetached();
    return true;
  }

  public String toString() {
    return getClass().getSimpleName() + "(volume=" + blockDataFile.volume
        + ", file=" + blockDataFile.getFile() + ", detached=" + detached + ")";
  }

  @Override
  public long getBytesVisible() throws IOException {
    return getFinalizedSize();
  }

  @Override
  public long getBytesWritten() throws IOException {
    return getFinalizedSize();
  }
  
  @Override
  public boolean hasBlockCrcInfo() {
    return blockCrcValid;
  }

  public void setBlockCrc(int blockCrc) throws IOException {
    if (blockCrcValid) {
      return;
    }
    this.blockCrc = blockCrc;
    this.blockCrcValid = true;
  }

  
  @Override
  public int getBlockCrc() throws IOException {
    if (!blockCrcValid) {
      throw new IOException("Block CRC information not available.");
    }
    return blockCrc;
  }
  
  public void setBlockCrc(long expectedBlockSize, int blockCrc) throws IOException {
    if (expectedBlockSize != this.getBytesWritten()) {
      return;
    }
    this.blockCrc = blockCrc;
    blockCrcValid = true;
  }
    
  @Override
  public InputStream getBlockInputStream(DataNode datanode, long offset) throws IOException {
    File f = getDataFileToRead();
    if(f.exists()) {
      if (offset <= 0) {
        return new FileInputStream(f);
      } else {
        RandomAccessFile blockInFile = new RandomAccessFile(f, "r");
        blockInFile.seek(offset);
        return new FileInputStream(blockInFile.getFD());
      }
    }
 
    // if file is not null, but doesn't exist - possibly disk failed
    if (datanode != null) {
      datanode.checkDiskError();
    }
    
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG
          .debug("DatanodeBlockInfo.getBlockInputStream failure. file: " + f);
    }
    throw new IOException("Cannot open file " + f);
  }

  @Override
  public BlockDataFile getBlockDataFile() {
    return blockDataFile;
  }
}

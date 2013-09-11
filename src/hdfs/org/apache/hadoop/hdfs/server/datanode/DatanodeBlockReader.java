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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSDataNodeReadProfilingData;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender.InputStreamFactory;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

/**
 * Base class for reading data for blocks from local disks and
 * fill in packet buffer.
 * 
 * @author sdong
 *
 */
abstract class DatanodeBlockReader implements java.io.Closeable {
  static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  protected int namespaceId;
  protected Block block;
  protected boolean isFinalized;
  protected FSDataNodeReadProfilingData dnData;

  protected boolean ignoreChecksum;
  protected boolean verifyChecksum; // if true, check is verified while reading
  protected boolean corruptChecksumOk; // if need to verify checksum

  protected DataChecksum checksum; // checksum stream
  protected int bytesPerChecksum; // chunk size
  protected int checksumSize; // checksum size

  DatanodeBlockReader(int namespaceId, Block block, boolean isFinalized,
      boolean ignoreChecksum, boolean verifyChecksum, boolean corruptChecksumOk) {
    this.namespaceId = namespaceId;
    this.block = block;
    this.isFinalized = isFinalized;
    this.ignoreChecksum = ignoreChecksum;
    this.verifyChecksum = verifyChecksum;
    this.corruptChecksumOk = corruptChecksumOk;
  }
  /**   
   * Read metadata (file header) from disk and generate checksum    
   * metadata of the block. 
   *    
   * @param blockLength 
   * @return    
   * @throws IOException    
   */
  public abstract DataChecksum getChecksumToSend(long blockLength) throws IOException;

  public abstract void fadviseStream(int advise, long offset, long len)
      throws IOException;

  /** 
   * Initialize input file stream(s) of local files for the block.  
   *    
   * @param offset  
   * @param blockLength 
   * @throws IOException    
   */
  public abstract void initialize(long offset, long blockLength)
      throws IOException;

  /**
   * Pre-processing to determine whether it is possible to directly stream
   * data from data blocks to output stream. It should only worked for
   * the separate checksum file case. For other cases, a false should be
   * directly returned.
   * @return
   * @throws IOException
   */
  public abstract boolean prepareTransferTo() throws IOException;

  /**
   * From current location, stream from current position to output stream
   * or packet buffer, based on the case.
   * 
   * The expected format for the output packet buffer:
   * 
   * checksumOff-->+---------------------+
   *               |Checksum for Chunk 1 |
   *               +---------------------+
   *               |Checksum for Chunk 2 |
   *               +---------------------+
   *               |         .           |
   *               |         .           |
   *               |         .           |
   *               +---------------------+
   *               |Checksum for Chunk N |
   *               +---------------------+
   *               |                     |
   *               |                     |
   *               |                     |
   *               |        Data         |
   *               |                     |
   *               |                     |
   *               |                     |
   *               +---------------------+
   * 
   * @param out          output stream to clients, only used in direct
   *                     transfer mode. In that case, buf is not used.
   * @param buf          packet buffer for the data
   * @param offset       starting block offset.
   * @param checksumOff  starting checksum offset.
   * @param numChunks    how many chunks to read
   * @param len          how many bytes to read
   * @throws IOException
   */
  public abstract void sendChunks(OutputStream out, byte[] buf, long offset,
      int checksumOff, int numChunks, int len, BlockCrcUpdater crcUpdater,
      int packetVersion) throws IOException;
  
  public abstract int getPreferredPacketVersion();

  /**   
   * @return number of bytes per data chunk for checksum    
   */
  int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  /**
   * @return number of bytes per checksum for a data chunk
   */
  int getChecksumSize() {
    return checksumSize;
  }

  /**
   * Populate bytes per checksum and checksum size information from
   * local checksum object.
   * 
   * @param blockLength
   */
  protected void getChecksumInfo(long blockLength) {
    /*
     * If bytesPerChecksum is very large, then the metadata file is mostly
     * corrupted. For now just truncate bytesPerchecksum to blockLength.
     */
    bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum > 10 * 1024 * 1024 && bytesPerChecksum > blockLength) {
      checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
          Math.max((int) blockLength, 10 * 1024 * 1024));
      bytesPerChecksum = checksum.getBytesPerChecksum();
    }
    checksumSize = checksum.getChecksumSize();
  }

  static class BlockInputStreamFactory implements
      BlockWithChecksumFileReader.InputStreamWithChecksumFactory {
    private final int namespaceId;
    private final Block block;
    private final ReplicaToRead replica;
    private final DataNode datanode;
    private final FSDatasetInterface data;
    private final boolean ignoreChecksum;
    private final boolean verifyChecksum;
    private final boolean corruptChecksumOk;
    protected DataInputStream metadataIn = null;

    BlockInputStreamFactory(int namespaceId, Block block,
        ReplicaToRead replica, DataNode datanode, FSDatasetInterface data,
        boolean ignoreChecksum, boolean verifyChecksum,
        boolean corruptChecksumOk) {
      this.namespaceId = namespaceId;
      this.block = block;
      this.replica = replica;
      this.datanode = datanode;
      this.data = data;
      this.ignoreChecksum = ignoreChecksum;
      this.verifyChecksum = verifyChecksum;
      this.corruptChecksumOk = corruptChecksumOk;
    }

    @Override
    public InputStream createStream(long offset) throws IOException {
      return replica.getBlockInputStream(datanode, offset);
    }

    @Override
    public DataInputStream getChecksumStream() throws IOException {
      if (replica == null || replica.isInlineChecksum()) {
        throw new IOException(
            "Getting checksum stream for inline checksum stream is not supported");
      }
      if (metadataIn != null) {
        return metadataIn;
      }
      if (!ignoreChecksum) {
        try {
          metadataIn = new DataInputStream(new BufferedInputStream(
              BlockWithChecksumFileReader.getMetaDataInputStream(data, namespaceId, block), FSConstants.BUFFER_SIZE));
        } catch (IOException e) {
          if (!corruptChecksumOk
              || BlockWithChecksumFileReader.metaFileExists(data, namespaceId,
                  block)) {
            throw e;
          }
          metadataIn = null;
        }
      }
      return metadataIn;
    }
    
    public DatanodeBlockReader getBlockReader() throws IOException {
      if (replica.isInlineChecksum()) {
        int checksumType = replica.getChecksumType();
        int bytesPerChecksum = replica.getBytesPerChecksum();
        return new BlockInlineChecksumReader(this.namespaceId, this.block, replica.isFinalized(),
            this.ignoreChecksum, this.verifyChecksum, this.corruptChecksumOk,
            this, checksumType, bytesPerChecksum);

      } else {
        return new BlockWithChecksumFileReader(this.namespaceId, this.block, replica.isFinalized(),
            this.ignoreChecksum, this.verifyChecksum, this.corruptChecksumOk,
            this);
      }
    }
    
    public FSDatasetInterface getDataset() {
      return this.data;
    }

    @Override
    public BlockDataFile.Reader getBlockDataFileReader() throws IOException {
      return replica.getBlockDataFile().getReader(datanode);
    }
  }

  public void enableReadProfiling(FSDataNodeReadProfilingData dnData) {
    this.dnData = dnData;
  }
}

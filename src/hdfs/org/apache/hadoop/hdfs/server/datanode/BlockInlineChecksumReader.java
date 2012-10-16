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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.BlockSender.InputStreamFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ChecksumUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
/**
 * The class to read from inline checksum block file and stream it to
 * output packet buffer. The expected block file name is:
 * blk_(blockId)_(generation_id)
 * 
 * The file format is following:
 * +---------------------------+
 * |     Checksum Header       |
 * +---------------------------+
 * |                           |
 * |    Data for Chunk 1       |
 * |        ......             |
 * |                           |
 * +---------------------------+
 * |   Checksum for Chunk 1    |
 * +---------------------------+
 * |                           |
 * |    Data for Chunk 2       |
 * |         ......            |
 * |                           |
 * +---------------------------+
 * |   Checksum for Chunk 2    |
 * +---------------------------+
 * |                           |
 * |    Data for Chunk 3       |
 * |            .              |
 * |            .              |
 * |            .              |
 * |                           |
 * +---------------------------+
 * |    Data for Last Chunk    |
 * |     (Can be Partial)      |
 * +---------------------------+
 * |  Checksum for Last Chunk  |
 * +---------------------------+
 * 
 * After the file header, chunks are saved. For every chunk, first data
 * are saved, and then checksums.
 * 
 */
public class BlockInlineChecksumReader extends DatanodeBlockReader {
  private InputStreamFactory streamFactory;
  private InputStream blockIn; // data stream
  long blockInPosition = -1;
  MemoizedBlock memoizedBlock;
  private int initChecksumType;
  private int initBytesPerChecksum;

  BlockInlineChecksumReader(int namespaceId, Block block,
      FSDatasetInterface data, boolean ignoreChecksum, boolean verifyChecksum,
      boolean corruptChecksumOk, InputStreamFactory streamFactory, int checksumType, int bytesPerChecksum) {
    super(namespaceId, block, data, ignoreChecksum, verifyChecksum,
        corruptChecksumOk);
    this.streamFactory = streamFactory;
    this.initChecksumType = checksumType;
    this.initBytesPerChecksum = bytesPerChecksum;
  }

  @Override
  public DataChecksum getChecksumToSend(long blockLength) throws IOException {
    if (checksum == null) {
      assert initChecksumType != DataChecksum.CHECKSUM_UNKNOWN;
      checksum = DataChecksum.newDataChecksum(initChecksumType,
          initBytesPerChecksum);
      super.getChecksumInfo(blockLength);
    }
    assert checksum != null;
    if (ignoreChecksum) {
      return DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL,
          checksum.getBytesPerChecksum());
    } else {
      return checksum;
    }
  }

  /**
   * get file length for the block size.
   * 
   * @param blockSize
   * @param bytesPerChecksum
   * @param checksumSize
   * @return
   */
  public static long getFileLengthFromBlockSize(long blockSize,
      int bytesPerChecksum, int checksumSize) {
    long numChunks; 
    if (blockSize % bytesPerChecksum == 0) {    
      numChunks = blockSize / bytesPerChecksum; 
    } else {    
      numChunks = blockSize / bytesPerChecksum + 1; 
    }   
    return blockSize + numChunks * checksumSize 
        + BlockInlineChecksumReader.getHeaderSize();  
  } 

  /**
   * Translate from block offset to position in file.
   * 
   * @param offsetInBlock
   * @param bytesPerChecksum
   * @param checksumSize
   * @return
   */
  public static long getPosFromBlockOffset(long offsetInBlock, int bytesPerChecksum,
      int checksumSize) {
    // We only support to read full chunks, so offsetInBlock must be the boundary
    // of the chunks.
    assert offsetInBlock % bytesPerChecksum == 0;
    // The position in the file will be the same as the file size for the block
    // size.
    return getFileLengthFromBlockSize(offsetInBlock, bytesPerChecksum, checksumSize);
  }

  public void initializeStream(long offset, long blockLength)
      throws IOException {
    // seek to the right offsets
    long offsetInFile = BlockInlineChecksumReader
        .getPosFromBlockOffset(offset, bytesPerChecksum, checksumSize);
    // Seek to to start of the chunk. The new position will be the same 
    // as the file size if block size is current offset.
    blockIn = streamFactory.createStream(offsetInFile);
    memoizedBlock = new MemoizedBlock(blockIn, blockLength, streamFactory,
        block);
  }

  @Override
  public boolean prepareTransferTo() throws IOException {
    return false;
  }

  @Override
  public void sendChunks(OutputStream out, byte[] buf, long startOffset,
      int startChecksumOff, int numChunks, int len) throws IOException {
    long offset = startOffset;
    long endOffset = startOffset + len;
    int checksumOff = startChecksumOff;
    int checksumLen =  ignoreChecksum ? 0 : (numChunks * checksumSize);
    int dataOff = checksumOff + checksumLen;
    int remain = len;

    for (int i = 0; i < numChunks; i++) {
      assert remain > 0;

      int lenToRead = (remain > bytesPerChecksum) ? bytesPerChecksum : remain;

      IOUtils.readFully(blockIn, buf, dataOff, lenToRead);
      if (!ignoreChecksum) {
        IOUtils.readFully(blockIn, buf, checksumOff, checksumSize);
      } else {
        IOUtils.skipFully(blockIn, checksumSize);
      }

      if (verifyChecksum && !corruptChecksumOk) {
        checksum.reset();
        checksum.update(buf, dataOff, lenToRead);
        if (!checksum.compare(buf, checksumOff)) {
          throw new ChecksumException("Checksum failed at "
              + (offset + len - remain), len);
        }
      }

      dataOff += lenToRead;
      checksumOff += checksumSize;
      remain -= lenToRead;
    }
    
    // only recompute checksum if we can't trust the meta data due to
    // concurrent writes
    if ((checksumSize != 0 && endOffset % bytesPerChecksum != 0)
        && memoizedBlock.hasBlockChanged(endOffset)) {
      ChecksumUtil.updateChunkChecksum(buf, startChecksumOff, startChecksumOff
          + checksumLen, len, checksum);
    }

    try {
      out.write(buf, 0, dataOff);
    } catch (IOException e) {
      throw BlockSender.ioeToSocketException(e);
    }
  }

  @Override
  public void close() throws IOException {
    IOException ioe = null;

    // close data file
    if (blockIn != null) {
      try {
        blockIn.close();
      } catch (IOException e) {
        ioe = e;
      }
      blockIn = null;
    }
    // throw IOException if there is any
    if (ioe != null) {
      throw ioe;
    }
  }

  /**
   * helper class used to track if a block's meta data is verifiable or not
   */
  class MemoizedBlock {
    // block data stream
    private InputStream inputStream;
    // visible block length
    private long blockLength;
    private final Block block;
    private final InputStreamFactory isf;

    private boolean riInitialized = false;
    private ReplicaToRead ri;

    private MemoizedBlock(InputStream inputStream, long blockLength,
        InputStreamFactory isf, Block block) throws IOException {
      this.inputStream = inputStream;
      this.blockLength = blockLength;
      this.isf = isf;
      this.block = block;
    }

    // logic: if we are starting or ending on a partial chunk and the block
    // has more data than we were told at construction, the block has 'changed'
    // in a way that we care about (ie, we can't trust crc data)
    boolean hasBlockChanged(long endOffset) throws IOException {
      if (!riInitialized) {
        FSDatasetInterface ds = null;
        if (isf instanceof BlockInputStreamFactory) {
          ds = ((BlockInputStreamFactory) isf).getDataset();
          ri = ds.getReplicaToRead(namespaceId, block);
          riInitialized = true;
        }
      }
      
      if (ri != null && ri.getBytesWritten() > blockLength) {
        return true;
      }
      
      // check if we are using transferTo since we tell if the file has changed
      // (blockInPosition >= 0 => we are using transferTo and File Channels
      long currentLength = ((FileInputStream) inputStream).getChannel().size();

      return currentLength != BlockInlineChecksumReader
          .getFileLengthFromBlockSize(blockLength, bytesPerChecksum,
              checksumSize);
    }
  }

  static public long getBlockSizeFromFileLength(long fileSize, int checksumType,
      int bytesPerChecksum) {
    assert checksumType != DataChecksum.CHECKSUM_UNKNOWN;

    long headerSize = BlockInlineChecksumReader.getHeaderSize();
    if (fileSize <= headerSize) {
      return 0;
    }
    long checksumSize = DataChecksum.getChecksumSizeByType(checksumType);
    long numChunks = (fileSize - headerSize - 1)
        / (bytesPerChecksum + checksumSize) + 1;
    if (fileSize <= headerSize + checksumSize * numChunks + bytesPerChecksum
        * (numChunks - 1)) {
      DataNode.LOG.warn("Block File has wrong size: size " + fileSize
          + " checksumType: " + checksumType + " bytesPerChecksum"
          + bytesPerChecksum);
    }
    return fileSize - headerSize - checksumSize * numChunks;
  }

  static class GenStampAndChecksum {
    public GenStampAndChecksum(long generationStamp, int checksumType,
            int bytesPerChecksum) {
      super();
      this.generationStamp = generationStamp;
      this.checksumType = checksumType;
      this.bytesPerChecksum = bytesPerChecksum;
    }
    long generationStamp;
    int checksumType;
    int bytesPerChecksum;
  }

  /** Return the generation stamp from the name of the block file.
   */
  static GenStampAndChecksum getGenStampAndChecksumFromInlineChecksumFile(
      String fileName) throws IOException {
    String[] vals = StringUtils.split(fileName, '_');
    if (vals.length != 6) {
      // blk, blkid, genstamp, version, checksumtype, byte per checksum
      throw new IOException("unidentified block name format: " + fileName);
    }
    if (Integer.parseInt(vals[3]) != FSDataset.FORMAT_VERSION_INLINECHECKSUM) {
      // We only support one version of meta version now.
      throw new IOException("Unsupported format version for file "
          + fileName);

    }
    return new GenStampAndChecksum(Long.parseLong(vals[2]),
            Integer.parseInt(vals[4]), Integer.parseInt(vals[5]));
  }
  /** Return the generation stamp from the name of the block file.
   */
  static long getGenerationStampFromInlineChecksumFile(String blockName)
      throws IOException {
    String[] vals = StringUtils.split(blockName, '_');
    if (vals.length != 6) {
      // blk, blkid, genstamp, version, checksumtype, byte per checksum
      throw new IOException("unidentified block name format: " + blockName);
    }
    return Long.parseLong(vals[2]);
  }
  
  /**
   * Returns the size of the header for data file
   */
  public static int getHeaderSize() {
    return 0;
  }

}

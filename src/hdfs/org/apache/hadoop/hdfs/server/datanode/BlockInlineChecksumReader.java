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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender.InputStreamFactory;
import org.apache.hadoop.hdfs.server.datanode.BlockWithChecksumFileReader.MemoizedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.ChecksumUtil;
import org.apache.hadoop.util.CrcConcat;
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
  private BlockInputStreamFactory streamFactory;
  private BlockDataFile.Reader blockDataFileReader;
  long blockInPosition = -1;
  MemoizedBlock memoizedBlock;
  private int initChecksumType;
  private int initBytesPerChecksum;
  private byte[] tempBuffer = null;

  BlockInlineChecksumReader(int namespaceId, Block block,
      boolean isFinalized, boolean ignoreChecksum, boolean verifyChecksum,
      boolean corruptChecksumOk, BlockInputStreamFactory streamFactory,
      int checksumType, int bytesPerChecksum) {
    super(namespaceId, block, isFinalized, ignoreChecksum, verifyChecksum,
        corruptChecksumOk);
    this.streamFactory = streamFactory;
    this.initChecksumType = checksumType;
    this.initBytesPerChecksum = bytesPerChecksum;
  }

  @Override
  public void fadviseStream(int advise, long offset, long len)
      throws IOException {
    long fileOffset = BlockInlineChecksumReader.getPosFromBlockOffset(offset,
        bytesPerChecksum, checksumSize);
    long fileLen = BlockInlineChecksumReader.getFileLengthFromBlockSize(len
        + offset, bytesPerChecksum, checksumSize)
        - fileOffset;
    blockDataFileReader.posixFadviseIfPossible(fileOffset, fileLen, advise);
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

  public void initialize(long offset, long blockLength)
      throws IOException {
    blockDataFileReader = streamFactory.getBlockDataFileReader();
    memoizedBlock = new MemoizedBlock(blockLength);
  }

  @Override
  public boolean prepareTransferTo() throws IOException {
    return false;
  }

  @Override
  public void sendChunks(OutputStream out, byte[] buf, long startOffset,
      int bufStartOff, int numChunks, int len, BlockCrcUpdater crcUpdater, int packetVersion)
      throws IOException {
    long offset = startOffset;
    long endOffset = startOffset + len;
    int checksumOff = bufStartOff;
    int checksumLen =  ignoreChecksum ? 0 : (numChunks * checksumSize);
    
    int bytesToRead = len + checksumSize * numChunks;
    
    long offsetInFile = BlockInlineChecksumReader
        .getPosFromBlockOffset(offset, bytesPerChecksum, checksumSize);

    if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
      if (tempBuffer == null || tempBuffer.length < bytesToRead) {
        tempBuffer = new byte[bytesToRead];
      }
      blockDataFileReader.readFully(tempBuffer, 0, bytesToRead, offsetInFile,
          true);

      if (dnData != null) {
        dnData.recordReadChunkInlineTime();
      }

      int tempBufferPos = 0;
      int dataOff = checksumOff + checksumLen;

      int remain = len;
      for (int i = 0; i < numChunks; i++) {
        assert remain > 0;

        int lenToRead = (remain > bytesPerChecksum) ? bytesPerChecksum : remain;

        System.arraycopy(tempBuffer, tempBufferPos, buf, dataOff, lenToRead);
        if (dnData != null) {
          dnData.recordCopyChunkDataTime();
        }
        tempBufferPos += lenToRead;
        if (!ignoreChecksum) {
          System.arraycopy(tempBuffer, tempBufferPos, buf, checksumOff,
              checksumSize);
          if (dnData != null) {
            dnData.recordCopyChunkChecksumTime();
          }
          if (crcUpdater != null) {
            crcUpdater.updateBlockCrc(offset + dataOff - bufStartOff
                - checksumLen, lenToRead,
                DataChecksum.getIntFromBytes(buf, checksumOff));
          }
        } else {
          if (crcUpdater != null) {
            crcUpdater.disable();
          }
        }
        tempBufferPos += checksumSize;

        if (verifyChecksum && !corruptChecksumOk) {
          checksum.reset();
          checksum.update(buf, dataOff, lenToRead);
          if (!checksum.compare(buf, checksumOff)) {
            throw new ChecksumException("Checksum failed at "
                + (offset + len - remain), len);
          }
          if (dnData != null) {
            dnData.recordVerifyCheckSumTime();
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
        ChecksumUtil.updateChunkChecksum(buf, bufStartOff, bufStartOff
            + checksumLen, len, checksum);
      }
    } else if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_INLINE){

      blockDataFileReader.readFully(buf, bufStartOff, bytesToRead,
          offsetInFile, true);

      if (dnData != null) {
        dnData.recordReadChunkInlineTime();
      }

      if (verifyChecksum && !corruptChecksumOk) {
        int dataOff = bufStartOff;
        int remain = len;

        for (int i = 0; i < numChunks; i++) {
          assert remain > 0;

          int lenToRead = (remain > bytesPerChecksum) ? bytesPerChecksum : remain;

          checksum.reset();
          checksum.update(buf, dataOff, lenToRead);
          dataOff += lenToRead;
          if (!checksum.compare(buf, dataOff)) {
            throw new ChecksumException("Checksum failed at "
                + (offset + len - remain), len);
          }

          dataOff += checksumSize;
          remain -= lenToRead;
        }
        if (dnData != null) {
          dnData.recordVerifyCheckSumTime();
        }
      }

      // only recompute checksum if we can't trust the meta data due to
      // concurrent writes
      if ((checksumSize != 0 && endOffset % bytesPerChecksum != 0)
          && memoizedBlock.hasBlockChanged(endOffset)) {
        ChecksumUtil.updateChunkChecksum(buf, bufStartOff + len, bufStartOff,
            len, checksum);
        if (dnData != null) {
          dnData.recordUpdateChunkCheckSumTime();
        }
      }
    } else {
      throw new IOException("Unidentified packet version.");
    }

    try {
      out.write(buf, 0, bufStartOff + bytesToRead);
      if (dnData != null) {
        dnData.recordSendChunkToClientTime();
      }
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("IOException when reading block " + block + " offset "
            + offset, e);
      }
      throw BlockSender.ioeToSocketException(e);
    }
  }
  
  @Override
  public int getPreferredPacketVersion() {
    return DataTransferProtocol.PACKET_VERSION_CHECKSUM_INLINE;
  }

  @Override
  public void close() throws IOException {
    IOException ioe = null;

    // throw IOException if there is any
    if (ioe != null) {
      throw ioe;
    }
  }

  /**
   * helper class used to track if a block's meta data is verifiable or not
   */
  class MemoizedBlock {
    // visible block length
    private long blockLength;

    private MemoizedBlock(long blockLength)
        throws IOException {
      this.blockLength = blockLength;
    }
    
    boolean isChannelSizeMatchBlockLength() throws IOException {
      long currentLength = blockDataFileReader.size(); 
      return (currentLength == BlockInlineChecksumReader
          .getFileLengthFromBlockSize(blockLength, bytesPerChecksum,
              checksumSize));
      
    }

    // logic: if we are starting or ending on a partial chunk and the block
    // has more data than we were told at construction, the block has 'changed'
    // in a way that we care about (ie, we can't trust crc data)
    boolean hasBlockChanged(long endOffset) throws IOException {
      if (isFinalized) {
        // We would treat it an error case for a finalized block at open time
        // has an unmatched size when closing. There might be false positive
        // for append() case. We made the trade-off to avoid false negative.
        // always return true so it data integrity is guaranteed by checksum
        // checking.
        return false;
      }

      return !isChannelSizeMatchBlockLength();
    }
  }
  
  /**
   * Implement Scatter Gather read. Since checksum and data are saved separately,
   * we go over the data file twice, the first time for checksums and the second
   * time for data. The speed of it then is not necessarily to be faster than
   * normal read() and is likely to be slower. We have this method here just
   * for backward compatible.
   * @param s
   * @param replica
   * @param dataFile
   * @param block
   * @param startOffset
   * @param length
   * @param datanode
   * @return
   * @throws IOException
   */
  static long readBlockAccelerator(Socket s, ReplicaToRead replica,
      File dataFile, Block block, long startOffset, long length,
      DataNode datanode) throws IOException {
    FileInputStream datain = new FileInputStream(dataFile);
    FileChannel dch = datain.getChannel();

    
    int type = replica.getChecksumType();
    int bytesPerChecksum = replica.getBytesPerChecksum();
    long checksumSize = DataChecksum.getChecksumSizeByType(type);
    DataChecksum checksum = DataChecksum.newDataChecksum(type, bytesPerChecksum);

    // align the startOffset with the previous bytesPerChecksum boundary.
    long delta = startOffset % bytesPerChecksum; 
    startOffset -= delta;
    length += delta;

    // align the length to encompass the entire last checksum chunk
    delta = length % bytesPerChecksum;
    if (delta != 0) {
      delta = bytesPerChecksum - delta;
      length += delta;
    }
    
    // find the offset in the metafile
    long startChunkNumber = startOffset / bytesPerChecksum;
    long numChunks = length / bytesPerChecksum;

    // get a connection back to the client
    SocketOutputStream out = new SocketOutputStream(s, datanode.socketWriteTimeout);

    try {
      // Write checksum information
      checksum.writeHeader(new DataOutputStream(out));
      
      // Transfer checksums
      int remain  = (int) length;
      long pos = startChunkNumber * (bytesPerChecksum + checksumSize);
      for (int i = 0; i < numChunks; i++) {
        assert remain > 0;
        
        int lenToRead = (remain > bytesPerChecksum) ? bytesPerChecksum : remain;

        pos += lenToRead;
        dch.position(pos);
        
        long val = dch.transferTo(pos, checksumSize, out);
        
        if (val != checksumSize) {
          String msg = "readBlockAccelerator for block  " + block +
                       " at offset " + pos + 
                       " Cannot read the full checksum.";
          LOG.warn(msg);
          throw new IOException(msg);
        }
        pos += checksumSize;
        remain -= lenToRead;
      }      
      
      // Transfer data
      remain  = (int) length;
      pos = startChunkNumber * (bytesPerChecksum + checksumSize);
      for (int i = 0; i < numChunks; i++) {
        assert remain > 0;
        dch.position(pos);

        int lenToRead = (remain > bytesPerChecksum) ? bytesPerChecksum : remain;

        long val = dch.transferTo(pos, lenToRead, out);
        
        if (val != lenToRead) {
          String msg = "readBlockAccelerator for block  " + block +
                       " at offset " + pos + 
                       " Cannot read a full chunk.";
          LOG.warn(msg);
          throw new IOException(msg);
        }
        pos += lenToRead + checksumSize;
        remain -= lenToRead;
      }

      return length;
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.blocksRead.inc();
      return -1;
    } catch ( IOException ioe ) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(datanode.getDatanodeInfo() +  
          ":readBlockAccelerator:Got exception while serving " + 
          block + " to " +
                s.getInetAddress() + ":\n" + 
                StringUtils.stringifyException(ioe) );
      throw ioe;
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(datain);
    }
  }
  
  /**
   * Calculate CRC Checksum of the whole block. Implemented by concatenating
   * checksums of all the chunks.
   * 
   * @param datanode
   * @param ri
   * @param namespaceId
   * @param block
   * @return
   * @throws IOException
   */
  static public int getBlockCrc(DataNode datanode, ReplicaToRead ri,
      int namespaceId, Block block) throws IOException {

    InputStream rawStreamIn = null;
    DataInputStream streamIn = null;
    int blockCrc = 0;

    try {
      int bytesPerCRC;
      int checksumSize;

      bytesPerCRC = ri.getBytesPerChecksum();
      int checksumType = ri.getChecksumType();
      if (checksumType != DataChecksum.CHECKSUM_CRC32) {
        throw new IOException("File Checksum now is only supported for CRC32");
      }
      DataChecksum dataChecksum = DataChecksum.newDataChecksum(checksumType,
          bytesPerCRC);
      checksumSize = dataChecksum.getChecksumSize();
      
      rawStreamIn = ri.getBlockInputStream(datanode, 0);
      streamIn = new DataInputStream(new BufferedInputStream(rawStreamIn,
          FSConstants.BUFFER_SIZE));
      IOUtils.skipFully(streamIn, BlockInlineChecksumReader.getHeaderSize());

      long lengthLeft = ((FileInputStream) rawStreamIn).getChannel().size()
          - BlockInlineChecksumReader.getHeaderSize();
      if (lengthLeft == 0) {
        blockCrc = (int) dataChecksum.getValue();
      } else {
        byte[] buffer = new byte[checksumSize];
        boolean firstChecksum = true;

        while (lengthLeft > 0) {
          long dataByteLengh;

          if (lengthLeft >= bytesPerCRC + checksumSize) {
            lengthLeft -= bytesPerCRC + checksumSize;
            dataByteLengh = bytesPerCRC;
          } else if (lengthLeft > checksumSize) {
            dataByteLengh = lengthLeft - checksumSize;
            lengthLeft = 0;
          } else {
            // report to name node the corruption.
            DataBlockScanner.reportBadBlocks(block, namespaceId, datanode);
            throw new IOException("File for namespace " + namespaceId
                + " block " + block + " seems to be corrupted");
          }

          IOUtils.skipFully(streamIn, dataByteLengh);
          IOUtils.readFully(streamIn, buffer, 0, buffer.length);
          int intChecksum = DataChecksum.getIntFromBytes(buffer, 0);
          if (firstChecksum) {
            blockCrc = intChecksum;
            firstChecksum = false;
          } else {
            blockCrc = CrcConcat.concatCrc(blockCrc, intChecksum,
                (int) dataByteLengh);
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
            + ", crc=" + blockCrc);
      }
      return blockCrc;
    } finally {
      IOUtils.closeStream(streamIn);
      IOUtils.closeStream(rawStreamIn);
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

  public static class GenStampAndChecksum {
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

    public int getChecksumType() {
      return checksumType;
    }
    public int getBytesPerChecksum() {
      return bytesPerChecksum;
    }
  }

  /** Return the generation stamp from the name of the block file.
   */
  public static GenStampAndChecksum getGenStampAndChecksumFromInlineChecksumFile(
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

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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumReader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Read a local block for inline checksum format.
 * 
 */
public class BlockReaderLocalInlineChecksum extends BlockReaderLocalBase {

  private boolean needVerifyChecksum = false;

  private final FileChannel dataFileChannel; // reader for the data file
  private final FileDescriptor dataFileDescriptor;

  /**
   * Constructor for the cast when checksum is not to be verified
   * 
   * @param conf
   * @param hdfsfile
   *          file name of the HDFS file
   * @param block
   * @param startOffset
   *          start offset to read (block offset)
   * @param length
   *          length of the block
   * @param pathinfo
   *          local path information of the block
   * @param metrics
   * @param checksum
   * @param dataFileChannel
   *          file channel of data file
   * @param dataFileDescriptor
   *          file descriptor of the data file
   * @param clearOsBuffer
   *          whether clear OS buffer after read
   * @param positionalReadMode
   *          whether the object is for positional read
   * @throws IOException
   */
  BlockReaderLocalInlineChecksum(Configuration conf, String hdfsfile,
      Block block, long startOffset, long length, BlockPathInfo pathinfo,
      DFSClientMetrics metrics, DataChecksum checksum,
      FileChannel dataFileChannel, FileDescriptor dataFileDescriptor,
      boolean clearOsBuffer, boolean positionalReadMode) throws IOException {
    super(conf, hdfsfile, block, startOffset, length, pathinfo, metrics,
        clearOsBuffer, positionalReadMode);

    this.bytesPerChecksum = checksum.getBytesPerChecksum();
    this.checksumSize = checksum.getChecksumSize();

    this.checksum = checksum;
    this.dataFileChannel = dataFileChannel;
    this.dataFileDescriptor = dataFileDescriptor;
    if (!positionalReadMode) {
      long chunkOffset = startOffset % bytesPerChecksum;
      long startPosInFile = BlockInlineChecksumReader.getPosFromBlockOffset(
          startOffset - chunkOffset, bytesPerChecksum, checksumSize)
          + chunkOffset;
      this.dataFileChannel.position(startPosInFile);
    }
  }

  /**
   * Constructor for the case when checksum needs to be verified.
   * 
   * @param conf
   * @param hdfsfile
   *          file name of the HDFS file
   * @param block
   * @param startOffset
   *          start offset of the block to read
   * @param length
   *          length of the block
   * @param pathinfo
   *          object for local path of the block
   * @param metrics
   * @param checksum
   * @param verifyChecksum
   *          whether need to verify checksum when read
   * @param dataFileChannel
   *          file channel of local file for the block
   * @param dataFileDescriptor
   *          file descriptor of the opened local block file
   * @param clearOsBuffer
   *          whether clear OS cache after reading
   * @param positionalReadMode
   *          whether it is for positional read
   * @throws IOException
   */
  BlockReaderLocalInlineChecksum(Configuration conf, String hdfsfile,
      Block block, long startOffset, long length, BlockPathInfo pathinfo,
      DFSClientMetrics metrics, DataChecksum checksum, boolean verifyChecksum,
      FileChannel dataFileChannel, FileDescriptor dataFileDescriptor,
      boolean clearOsBuffer, boolean positionalReadMode) throws IOException {
    super(conf, hdfsfile, block, startOffset, length, pathinfo, metrics,
        checksum, verifyChecksum, clearOsBuffer, positionalReadMode);

    sum = null;
    this.dataFileChannel = dataFileChannel;
    this.dataFileDescriptor = dataFileDescriptor;
    this.needVerifyChecksum = verifyChecksum;

    long blockLength = pathinfo.getNumBytes();

    if (positionalReadMode) {
      // We don't need to set initial offsets of the file if
      // the reader is for positional reads.
      return;
    }

    // if the requested size exceeds the currently known length of the file
    // then check the blockFile to see if its length has grown. This can
    // occur if the file is being concurrently written to while it is being
    // read too. If the blockFile has grown in size, then update the new
    // size in our cache.
    if (startOffset > blockLength || (length + startOffset) > blockLength) {
      File blkFile = new File(pathinfo.getBlockPath());
      long diskFileLength = blkFile.length();
      long newlength = BlockInlineChecksumReader.getBlockSizeFromFileLength(
          diskFileLength, checksum.getChecksumType(),
          checksum.getChecksumSize());
      LOG.warn("BlockReaderLocal found short block " + blkFile
          + " requested offset " + startOffset + " length " + length
          + " but known size of block is " + blockLength + ", size on disk is "
          + newlength);
      if (newlength > blockLength) {
        blockLength = newlength;
        pathinfo.setNumBytes(newlength);
      }
    }
    long endOffset = blockLength;
    if (startOffset < 0 || startOffset > endOffset
        || (length + startOffset) > endOffset) {
      String msg = " Offset " + startOffset + " and length " + length
          + " don't match block " + block + " ( blockLen " + endOffset + " )";
      LOG.warn("BlockReaderLocal requested with incorrect offset: " + msg);
      throw new IOException(msg);
    }

    firstChunkOffset = (startOffset - (startOffset % bytesPerChecksum));

    // seek to the right offsets
    if (firstChunkOffset > 0) {
      long offsetInFile = BlockInlineChecksumReader.getPosFromBlockOffset(
          firstChunkOffset, bytesPerChecksum, checksumSize);
      dataFileChannel.position(offsetInFile);
    }

    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;
  }

  public long getBlockLength() throws IOException {
    return BlockInlineChecksumReader.getBlockSizeFromFileLength(
        dataFileChannel.size(), checksum.getChecksumType(), bytesPerChecksum);

  }
  
  /**
   * Read from the current position of the file.
   * 
   * @param fileChannel
   *          file channel to read from
   * @param len
   *          number of bytes are requested
   * @param skipBytes
   *          first number of bytes shouldn't go to user buffer. They need to be
   *          read only for checksum verification purpose.
   * @param outBuffer
   *          output buffer
   * @param outStartPos
   *          starting position to fill the output buffer
   * @return how many bytes are actually read.
   * @throws IOException
   */
  private int read(int len, int skipBytes, byte[] outBuffer, int outStartPos)
      throws IOException {
    if (outBuffer.length - outStartPos < len) {
      len = outBuffer.length - outStartPos;
    }

    // Calculate current block size from current file position
    long filePos = dataFileChannel.position();
    long blockPos = BlockInlineChecksumReader.getBlockSizeFromFileLength(
        filePos - filePos % (checksumSize + bytesPerChecksum), checksum.getChecksumType(),
        bytesPerChecksum)
        + filePos % (checksumSize + bytesPerChecksum);

    long startChunkOffset; // start offset in the current chunk
    if (blockPos % bytesPerChecksum != 0) {
      // Current position is not the beginning of a chunk. It is only
      // allowed if checksum is not verified.
      if (needVerifyChecksum) {
        throw new IOException(
            "Cannot read from middle of a chunk and verify checksum.");
      }
      startChunkOffset = blockPos % bytesPerChecksum;
    } else {
      startChunkOffset = 0;
    }

    // Get current block size
    long blockSize = getBlockLength();

    int totalLen = len + skipBytes; // total size to
                                    // read including
                                    // the bytes to
                                    // skip
    if (totalLen + blockPos > blockSize) {
      // If the number of bytes requests is more than bytes available,
      // shrink the length to read up to the end of the file.
      totalLen = (int) (blockSize - blockPos);
      len = totalLen - skipBytes;
    }

    long lastChunkSize; // size of the last chunk to read
    long endBlockPos = blockPos + totalLen;
    if (endBlockPos % bytesPerChecksum != 0
        && blockSize % bytesPerChecksum != 0
        && endBlockPos > blockSize - (blockSize % bytesPerChecksum)) {
      // This is the case that the last chunk of the file is partial and the
      // read request needs to read to the last chunk (not necessarily the full
      // chunk is requested).
      lastChunkSize = blockSize % bytesPerChecksum;
    } else {
      lastChunkSize = bytesPerChecksum;
    }

    int numChunks = (totalLen + (int) startChunkOffset - 1) / bytesPerChecksum + 1;
    
    int totalBytesToRead = (int) ((bytesPerChecksum + checksumSize)
        * (numChunks - 1) + lastChunkSize - startChunkOffset);
    if (needVerifyChecksum || totalLen % bytesPerChecksum == 0) {
      totalBytesToRead += checksumSize;
    }
    // We first copy to a temp buffer and then to
    // final user buffer to reduce number of file system calls.
    // We pay a new byte array allocation and another mem copy for it.
    ByteBuffer tempBuffer = ByteBuffer.allocate(totalBytesToRead);

    IOUtils.readFileChannelFully(dataFileChannel, tempBuffer, 0,
        totalBytesToRead, true);
    tempBuffer.flip();
    
    
    // Up to here, we calculated what we need to read and how:
    // numChunks needs to be read. Obviously, all chunks other than the first
    // chunk
    // and the last chunk need to be read fully directly to user buffer.
    //
    // For the first chunk:
    // data needs to read from startChunkOffset to the end of the chunk. If
    // there
    // is only one chunk, the lastChunkSize - startChunkOffset needs to be read.
    // Otherwise, it is bytesPerChecksum - startChunkOffset.
    // The first skipBytes bytes will be skipped and from the next bytes will be
    // filled to user buffer, until the expected number of bytes are filled, or
    // the end of the chunk.
    //
    // For the last chunk (for more than one chunk):
    // there are lastChunkSize bytes in the last chunk. If checksum checking is
    // required, all the bytes need to be read. But not necessarily all the
    // bytes
    // are needed to user buffer. User buffer is only filled up to len.

    int remain = totalLen;
    int bytesFilledBuffer = 0; // how many bytes are filled into user's buffer
    for (int i = 0; i < numChunks; i++) {
      assert remain > 0;

      long chunkActualRead; 
      int endChunkOffsetToRead; // how many bytes should be returned to user's
                                // buffer
      int lenToRead; // How many bytes to read from the file
      int headBytesToSkip = 0; // extra bytes not to copied to user's buffer.
      
      if (needVerifyChecksum) {
        endChunkOffsetToRead = (remain > bytesPerChecksum) ? bytesPerChecksum
            : remain;
        // The case that checksum needs to be verified.
        //
        if (i == numChunks - 1) {
          // It's the last chunk and checksum needs to be verified. (could be the
          // first one in the same time)
          // We read to read lastChunkSize;
          lenToRead = (int) lastChunkSize;
        } else {
          lenToRead = bytesPerChecksum;          
        }
        if (i == 0 && skipBytes > 0) {
          // checksum needs to be verified and there is extra bytes need to be
          // skipped. We need to use a separate buffer to verify checksum and
          // then copy bytes needed to user buffer.
          headBytesToSkip = skipBytes;
        }
      } else {
        // Case that checksum doesn't need to be verified.
        //
        if (i == numChunks - 1) {
          endChunkOffsetToRead = remain;
        } else if (i == 0) {
          // It's the first chunk. Bytes need to be read are the chunk size -
          // startChunkOffset.
          endChunkOffsetToRead = bytesPerChecksum - (int) startChunkOffset;
        } else {
          endChunkOffsetToRead = bytesPerChecksum;
        }
        lenToRead = endChunkOffsetToRead;
      }
      
      if (needVerifyChecksum) {
        // verify checksum.
        checksum.reset();
        checksum.update(tempBuffer.array(), tempBuffer.position(), lenToRead);
        if (!checksum.compare(tempBuffer.array(), tempBuffer.position()
            + lenToRead)) {
          throw new ChecksumException("Checksum failed at "
              + (blockPos + len - remain), len);
        }
      }

      if (headBytesToSkip > 0) {
        tempBuffer.position(tempBuffer.position() + headBytesToSkip);
      }
      chunkActualRead = endChunkOffsetToRead - headBytesToSkip;
      tempBuffer.get(outBuffer, outStartPos + bytesFilledBuffer,
          (int) chunkActualRead);
      if (i != numChunks - 1) {
        tempBuffer.position(tempBuffer.position() + checksumSize);
      }
      bytesFilledBuffer += chunkActualRead;
      remain -= endChunkOffsetToRead;
    }

    return bytesFilledBuffer;
  }

  private int readNonPositional(byte[] buf, int off, int len)
      throws IOException {
    if (positionalReadMode) {
      throw new IOException(
          "Try to do sequential read using a block reader for positional read.");
    }

    return read(len, 0, buf, off);
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem read off " + off + " len " + len);
    }
    metrics.readsFromLocalFile.inc();
    int byteRead;
    if (!needVerifyChecksum) {
      byteRead = readNonPositional(buf, off, len);
      updateStatsAfterRead(byteRead);
    } else {
      byteRead = super.read(buf, off, len);
    }
    if (clearOsBuffer) {
      // drop all pages from the OS buffer cache
      NativeIO.posixFadviseIfPossible(dataFileDescriptor, off, len,
          NativeIO.POSIX_FADV_DONTNEED);
    }
    return byteRead;
  }

  public synchronized int read(long pos, byte[] buf, int off, int len)
      throws IOException {
    if (!positionalReadMode) {
      throw new IOException(
          "Try to do positional read using a block reader forsequantial read.");
    }

    int skipBytes = (int) (pos % bytesPerChecksum);
    long filePos = BlockInlineChecksumReader.getPosFromBlockOffset(pos
        - skipBytes, bytesPerChecksum, checksumSize);
    if (needVerifyChecksum) {
      dataFileChannel.position(filePos);
      return read(len, skipBytes, buf, off);
    } else {
      dataFileChannel.position(filePos + skipBytes);
      return read(len, 0, buf, off);
    }
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem skip " + n);
    }
    if (!needVerifyChecksum) {
      long currentPos = dataFileChannel.position();
      long fileSize = dataFileChannel.size();
      long newPos;
      long currentBlockPos = BlockInlineChecksumReader.getPosFromBlockOffset(
          fileSize, bytesPerChecksum, checksumSize);
      long newBlockPos = currentBlockPos + n;
      long partialChunk = newBlockPos % bytesPerChecksum;
      newPos = BlockInlineChecksumReader.getPosFromBlockOffset(newBlockPos
          - partialChunk, bytesPerChecksum, checksumSize);
      if (partialChunk != 0) {
        newPos += checksumSize;
      }
      long skipped = n;
      if (newPos > fileSize) {
        skipped = fileSize - currentPos;
        newPos = fileSize;
      }
      dataFileChannel.position(newPos);
      return skipped;
    } else {
      return super.skip(n);
    }
  }

  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset,
      int len, byte[] checksumBuf) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading chunk from position " + pos + " at offset " + offset
          + " with length " + len);
    }

    if (eos) {
      if (startOffset < 0) {
        // This is mainly for debugging. can be removed.
        throw new IOException("BlockRead: already got EOS or an error");
      }
      startOffset = -1;
      return -1;
    }

    if (checksumBuf.length != checksumSize) {
      throw new IOException("Cannot read checksum into provided buffer. "
          + "The buffer must be exactly '" + checksumSize
          + "' bytes long to hold the checksum bytes.");
    }

    if ((pos + firstChunkOffset) != lastChunkOffset) {
      throw new IOException("Mismatch in pos : " + pos + " + "
          + firstChunkOffset + " != " + lastChunkOffset);
    }

    int nRead = readNonPositional(buf, offset, bytesPerChecksum);
    if (nRead < bytesPerChecksum) {
      eos = true;

    }

    lastChunkOffset += nRead;
    lastChunkLen = nRead;

    return nRead;
  }

  /**
   * Channel Transfer is not possible for inline checksum. To keep backward
   * compatible, we call normal read() to return the same data.
   */
  public ByteBuffer readAll() throws IOException {
    ByteBuffer bb = ByteBuffer.allocate((int) length);
    if (positionalReadMode) {
      read(0, bb.array(), 0, (int) length);
      return bb;
    } else {
      read(bb.array(), 0, (int) length);
      return bb;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem close");
    }
    dataFileChannel.close();
  }
}

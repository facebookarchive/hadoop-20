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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile.RandomAccessor;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.CrcConcat;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

/** 
 * The class to write into local inline checksum block file.
 * The expected block file name is:   
 *   blk_(blockId)_(generation_id)_(checksum_type)_(bytes_per_checksum)    
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
 * |    Checksum for Chunk 3   |    
 * +---------------------------+  
 *  
 */
public class BlockInlineChecksumWriter extends DatanodeBlockWriter {  
  final protected BlockDataFile blockDataFile;
  protected BlockDataFile.Writer blockDataWriter = null;
  private int checksumType = DataChecksum.CHECKSUM_UNKNOWN;
  private final int writePacketSize;

  public BlockInlineChecksumWriter(BlockDataFile blockDataFile, int checksumType,
      int bytesPerChecksum, int writePacketSize) {
    this.blockDataFile = blockDataFile;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;
    this.writePacketSize = writePacketSize;
  }

  public void initializeStreams(int bytesPerChecksum, int checksumSize,
      Block block, String inAddr, int namespaceId, DataNode datanode)
      throws FileNotFoundException, IOException {
    if (this.blockDataWriter == null) {
      this.blockDataWriter = blockDataFile.getWriter(datanode.writePacketSize * 2);
      firstChunkOffset = 0;
    }

    setParameters(bytesPerChecksum, checksumSize, block, inAddr, namespaceId,
        datanode);
  }

  @Override
  public void writeHeader(DataChecksum checksum) throws IOException {
    // In current version, no header is written.
  }

  @Override
  public void fadviseStream(int advise, long offset, long len)
      throws IOException {
      fadviseStream(advise, offset, len, false);
  }

  @Override
  public void fadviseStream(int advise, long offset, long len, boolean sync)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("posix_fadvise with advise : " + advise + " for : " + blockDataFile.getFile());
    }
    long fileOffset = BlockInlineChecksumReader.getPosFromBlockOffset(
        offset, bytesPerChecksum, checksumSize);
    long fileLen = BlockInlineChecksumReader.getFileLengthFromBlockSize(
        len + offset, bytesPerChecksum, checksumSize) - fileOffset;
    blockDataWriter.posixFadviseIfPossible(fileOffset, fileLen, advise, sync);
  }

  @Override
  public void writePacket(byte pktBuf[], int len, int startDataOff,
      int pktBufStartOff, int numChunks, int packetVersion) throws IOException {
    
    if (len == 0) {
      return;
    }

    int chunkOffset = firstChunkOffset;
    int remain = len;

    if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
      int dataOff = startDataOff;
      int checksumOff = pktBufStartOff;
      
      for (int i = 0; i < numChunks; i++) {
        assert remain > 0;

        int lenToWrite = (remain > bytesPerChecksum - chunkOffset) ? bytesPerChecksum
            - chunkOffset
            : remain;

        // finally write to the disk :
        blockDataWriter.write(pktBuf, dataOff, lenToWrite);
        if (chunkOffset > 0) {
          // Partial chunk
          int crcPart2 = DataChecksum.getIntFromBytes(pktBuf, pktBufStartOff);

          partialCrcInt = CrcConcat.concatCrc(partialCrcInt, crcPart2,
              lenToWrite);
          byte[] tempBuf = new byte[4];
          DataChecksum.writeIntToBuf(partialCrcInt, tempBuf, 0);
          blockDataWriter.write(tempBuf);
          LOG.debug("Writing out partial crc for data len " + lenToWrite);
        } else {
          blockDataWriter.write(pktBuf, checksumOff, checksumSize);
          if (lenToWrite < bytesPerChecksum) {
            // partial chunk, need to remember the partial CRC
            partialCrcInt = DataChecksum.getIntFromBytes(pktBuf, checksumOff);
          }
        }
        chunkOffset = (chunkOffset + lenToWrite) % bytesPerChecksum;

        dataOff += lenToWrite;
        remain -= lenToWrite;
        checksumOff += checksumSize;
      }
    } else if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_INLINE) {
      int firstChunkSize = 0;
      int dataOff = startDataOff;
      if (chunkOffset > 0) {
        // Figure out size of first chunk
        firstChunkSize = (len > bytesPerChecksum - chunkOffset) ? bytesPerChecksum
            - chunkOffset : len;
        
        // Partial chunk
        int crcPart2 = DataChecksum.getIntFromBytes(pktBuf, pktBufStartOff
            + firstChunkSize);
        partialCrcInt = CrcConcat.concatCrc(partialCrcInt, crcPart2,
            firstChunkSize);
        byte[] tempBuf = new byte[4];
        DataChecksum.writeIntToBuf(partialCrcInt, tempBuf, 0);

        blockDataWriter.write(pktBuf, dataOff, firstChunkSize);
        blockDataWriter.write(tempBuf);
        dataOff += firstChunkSize + checksumSize;
        LOG.debug("Writing out partial crc for data len " + firstChunkSize);
        
        remain -= firstChunkSize;
        chunkOffset = (chunkOffset + firstChunkSize) % bytesPerChecksum;
      }
      if (remain > 0) {
        int numFullChunks = remain / bytesPerChecksum;
        chunkOffset = remain % bytesPerChecksum;
        int bytesLeftInBuf = remain + checksumSize * numFullChunks;
        if (chunkOffset > 0) {
          // last chunk is partial
          partialCrcInt = DataChecksum.getIntFromBytes(pktBuf, dataOff
              + bytesLeftInBuf);
          
          bytesLeftInBuf += checksumSize;
        }
        
        blockDataWriter.write(pktBuf, dataOff, bytesLeftInBuf);

      }
    } else {
      throw new IOException("inline checksum doesn't support packet version "
          + packetVersion);
    }
    blockDataWriter.flush();

    firstChunkOffset = chunkOffset;
  }

  /**
   * Retrieves the offset in the block to which the the next write will write
   * data to.
   */
  public long getChannelPosition() throws IOException {
    return blockDataWriter.getChannelPosition();
  }
  
  @Override
  public void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock, DataChecksum checksum) throws IOException {
    long expectedFileLength = BlockInlineChecksumReader.getFileLengthFromBlockSize(
        offsetInBlock, bytesPerChecksum, checksumSize);

    if (getChannelPosition() == expectedFileLength) {
      if (offsetInBlock % bytesPerChecksum != firstChunkOffset) {
        throw new IOException("chunk Offset " + firstChunkOffset + " doesn't match offset in block " +
            offsetInBlock+ " which should never happen.");
      }
      
      if (offsetInBlock % bytesPerChecksum != 0) {
        // Previous packet is a partial chunk.
        // If the position is the expected file length, we assume
        // everything is fine and we just set to the correct position.
        setChannelPosition(expectedFileLength - checksumSize, true);
      }
      return;
    }
    
    
    if (blockDataWriter != null) {
      blockDataWriter.flush();
    }

    long positionToSeekTo = expectedFileLength;
    // If this is a partial chunk, then read in pre-existing checksum
    if (offsetInBlock % bytesPerChecksum != 0) {
      // Previous packet is a partial chunk.
      positionToSeekTo -= checksumSize;
      LOG.info("setBlockPosition trying to set position to " + offsetInBlock
          + " for block " + block
          + " which is not a multiple of bytesPerChecksum " + bytesPerChecksum);
      computePartialChunkCrc(offsetInBlock, bytesPerChecksum, checksum);
    }
    firstChunkOffset = (int) (offsetInBlock % bytesPerChecksum);

    // set the position of the block file
    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing block file offset of block " + block + " from "
          + getChannelPosition() + " to " + positionToSeekTo);
    }

    setChannelPosition(positionToSeekTo, firstChunkOffset > 0);
  }

  /**
   * Sets the offset in the block to which the the next write will write data
   * to.
   */
  public void setChannelPosition(long dataOffset, boolean startWithPartialChunk)
      throws IOException {
    long channelSize = blockDataWriter.getChannelSize();
    if (channelSize < dataOffset) {
      String msg = "Trying to change block file offset of block "
          + block
          + "file "
          + ((blockDataFile.getFile() != null) ? blockDataFile.getFile()
              : "unknown") + " to " + dataOffset
          + " but actual size of file is " + blockDataWriter.getChannelSize();
      throw new IOException(msg);
    }
    if (dataOffset > channelSize) {
      throw new IOException("Set position over the end of the data file.");
    }
    
    if (startWithPartialChunk && channelSize != dataOffset + checksumSize) {
      DFSClient.LOG.warn("Inline Checksum Block " + block + " channel size "
          + channelSize + " but packet needs to start from " + dataOffset);
    }
    
    // This flush should be a no-op since we always flush at the end of
    // writePacket() and hence the buffer should be empty.
    // However we do this just to be extra careful so that the
    // channel.position() doesn't mess up things with respect to the
    // buffered dataOut stream.
    blockDataWriter.flush();
    blockDataWriter.position(dataOffset);
  }

  /**
   * reads in the partial crc chunk and computes checksum of pre-existing data
   * in partial chunk.
   */
  private void computePartialChunkCrc(long blkoff, int bytesPerChecksum,
      DataChecksum checksum) throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    int checksumSize = checksum.getChecksumSize();
    long fileOff = BlockInlineChecksumReader.getPosFromBlockOffset(blkoff
        - sizePartialChunk, bytesPerChecksum, checksumSize);
    LOG.info("computePartialChunkCrc sizePartialChunk " + sizePartialChunk
        + " block " + block + " offset in block " + blkoff);

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    // FileInputStream dataIn = null;
    
      /*
      RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
      dataIn = new FileInputStream(blockInFile.getFD());

      if (fileOff > 0) {
        blockInFile.seek(fileOff);
      }
      IOUtils.readFully(dataIn, buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(dataIn, crcbuf, 0, crcbuf.length);
      */
    BlockDataFile.Reader blockReader = blockDataFile.getReader(datanode);
    blockReader.readFully(buf, 0, sizePartialChunk, fileOff, true);
    blockReader.readFully(crcbuf, 0, crcbuf.length, fileOff + sizePartialChunk,
        true);


    // compute crc of partial chunk from data read in the block file.
    Checksum partialCrc = new CRC32();
    partialCrc.update(buf, 0, sizePartialChunk);
    LOG.info("Read in partial CRC chunk from disk for block " + block);

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != FSInputChecker.checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue()
          + " does not match value computed the "
          + " last time file was closed "
          + FSInputChecker.checksum2long(crcbuf);
      throw new IOException(msg);
    }
    partialCrcInt = (int) partialCrc.getValue();
  }

  /**
   * Flush the data and checksum data out to the stream. Please call sync to
   * make sure to write the data in to disk
   * 
   * @throws IOException
   */
  @Override
  public void flush(boolean forceSync)
      throws IOException {
    if (blockDataWriter != null) {
      blockDataWriter.flush();
      if (forceSync) {
        blockDataWriter.force(true);
      }
    }
  }
  
  @Override
  public void fileRangeSync(long lastBytesToSync, int flags) throws IOException {
    if (blockDataWriter.hasChannel() && lastBytesToSync > 0) {
      long channelPos = blockDataWriter.getChannelPosition();
      long blockPos = BlockInlineChecksumReader.getBlockSizeFromFileLength(
          channelPos, this.checksumType, this.bytesPerChecksum);
      long startOffsetInBlock = blockPos - lastBytesToSync;
      if (startOffsetInBlock < 0) {
        startOffsetInBlock = 0;
      }
      long lastChunkSizeForStartOffset = startOffsetInBlock % bytesPerChecksum;
      long startOffsetInChannel = BlockInlineChecksumReader
          .getFileLengthFromBlockSize(startOffsetInBlock
              - lastChunkSizeForStartOffset, bytesPerChecksum, checksumSize)
          + lastChunkSizeForStartOffset;

      if (LOG.isDebugEnabled()) {
        LOG.debug("file_range_sync " + block + " channel position "
            + blockDataWriter.getChannelPosition() + " offset "
            + startOffsetInChannel);
      }
      blockDataWriter.syncFileRangeIfPossible(startOffsetInChannel, channelPos
          - startOffsetInChannel, flags);
    }
  }

  public void truncateBlock(long newBlockLen)
      throws IOException {
    if (newBlockLen == 0) {
      // Special case for truncating to 0 length, since there's no previous
      // chunk.
      RandomAccessor ra = blockDataFile.getRandomAccessor();
      try {
        ra.setLength(BlockInlineChecksumReader.getHeaderSize());
      } finally {
        ra.close();
      }
      return;
    }

    DataChecksum dcs = DataChecksum.newDataChecksum(this.checksumType, this.bytesPerChecksum);
    this.checksumSize = dcs.getChecksumSize();

    long newBlockFileSize = BlockInlineChecksumReader
        .getFileLengthFromBlockSize(newBlockLen, bytesPerChecksum, checksumSize);

    int lastchunksize = (int) (newBlockLen % bytesPerChecksum);

    RandomAccessor ra = blockDataFile.getRandomAccessor();
    try {
      // truncate blockFile
      ra.setLength(newBlockFileSize);

      if (lastchunksize != 0) {
        // Calculate last partial checksum.
        long lastchunkoffset = BlockInlineChecksumReader.getPosFromBlockOffset(
            newBlockLen - lastchunksize, bytesPerChecksum, checksumSize);

        byte[] b = new byte[Math.max(lastchunksize, checksumSize)];

        // read last chunk
        ra.seek(lastchunkoffset);
        ra.readFully(b, 0, lastchunksize);

        // compute checksum
        dcs.update(b, 0, lastchunksize);
        dcs.writeValue(b, 0, false);

        ra.seek(newBlockFileSize - checksumSize);
        ra.write(b, 0, checksumSize);
      }
    } finally {
      ra.close();
    }
  }

  @Override
  public void close() throws IOException {
    close(0);
  }

  public void close(int fadvise) throws IOException {
    IOException ioe = null;
    // close block file
    try {
      try {
        flush(datanode.syncOnClose);
        if (fadvise != 0) {
          fadviseStream(fadvise, 0, 0, true);
        }
      } finally {
        if (blockDataWriter != null) {
          blockDataWriter.close();
          blockDataWriter = null;
        }
      }
    } catch (IOException e) {
      ioe = e;
    }

    // disk check
    // We don't check disk for ClosedChannelException as close() can be
    // called twice and it is possible that out.close() throws.
    // No need to check or recheck disk then.
    //
    if (ioe != null) {
      if (!(ioe instanceof ClosedChannelException)) {
        datanode.checkDiskError(ioe);
      }
      throw ioe;
    }
  }
  
  static public String getInlineChecksumFileName(Block block, int checksumType,
      int bytesPerChecksum) {
    assert checksumType != DataChecksum.CHECKSUM_UNKNOWN;
    return block.getBlockName() + "_" + block.getGenerationStamp() + "_"
        + FSDataset.FORMAT_VERSION_INLINECHECKSUM + "_" + checksumType + "_"
        + bytesPerChecksum;
  }
  
  /**
   * Only used for testing
   */
  public BlockDataFile getBlockDataFile() {
    return blockDataFile;
  }
}

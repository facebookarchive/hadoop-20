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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

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
  private File blockFile;

  protected OutputStream dataOut = null; // to block file at local disk
  private Checksum partialCrc = null;
  private int checksumType = DataChecksum.CHECKSUM_UNKNOWN;

  public BlockInlineChecksumWriter(File blockFile, int checksumType, int bytesPerChecksum) {
    this.blockFile = blockFile;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;
  }

  public void initializeStreams(int bytesPerChecksum, int checksumSize,
      Block block, String inAddr, int namespaceId, DataNode datanode)
      throws FileNotFoundException, IOException {
    if (this.dataOut == null) {
      this.dataOut = new FileOutputStream(
          new RandomAccessFile(blockFile, "rw").getFD());
    }

    setParameters(bytesPerChecksum, checksumSize, block, inAddr, namespaceId,
        datanode);
  }

  @Override
  public void writeHeader(DataChecksum checksum) throws IOException {
    // In current version, no header is written.
  }

  @Override
  public void writePacket(byte pktBuf[], int len, int startDataOff, int startChecksumOff,
      int numChunks) throws IOException {
    int dataOff = startDataOff;
    int checksumOff = startChecksumOff;
    
    if (partialCrc != null) {
      // If this is a partial chunk, then verify that this is the only
      // chunk in the packet. Calculate new crc for this chunk.
      if (len > bytesPerChecksum) {
        throw new IOException("Got wrong length during writeBlock(" + block
            + ") from " + inAddr + " "
            + "A packet can have only one partial chunk." + " len = " + len
            + " bytesPerChecksum " + bytesPerChecksum);
      }
      dataOut.write(pktBuf, dataOff, len);
      partialCrc.update(pktBuf, dataOff, len);
      byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
      dataOut.write(buf);
      LOG.debug("Writing out partial crc for data len " + len);
      partialCrc = null;
    } else {
      int remain = len;
      for (int i = 0; i < numChunks; i++) {
        assert remain > 0;

        int lenToWrite = (remain > bytesPerChecksum) ? bytesPerChecksum
            : remain;

        // finally write to the disk :
        dataOut.write(pktBuf, dataOff, lenToWrite);


        dataOut.write(pktBuf, checksumOff, checksumSize);
        dataOff += lenToWrite;
        remain -= lenToWrite;
        checksumOff += checksumSize;
      }
    }
  }

  /**
   * Retrieves the offset in the block to which the the next write will write
   * data to.
   */
  public long getChannelPosition() throws IOException {
    return ((FileOutputStream) dataOut).getChannel().position();
  }
  
  @Override
  public void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock, DataChecksum checksum) throws IOException {
    long expectedFileLength = BlockInlineChecksumReader.getFileLengthFromBlockSize(
        offsetInBlock, bytesPerChecksum, checksumSize);

    if (getChannelPosition() == expectedFileLength) {
      if (offsetInBlock % bytesPerChecksum != 0) {
        // Previous packet is a partial chunk.
        // If the position is the expected file length, we assume
        // everything is fine and we just set to the correct position.
        setChannelPosition(expectedFileLength - checksumSize);
      }
      return;
    }
    
    if (dataOut != null) {
      dataOut.flush();
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

    // set the position of the block file
    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing block file offset of block " + block + " from "
          + getChannelPosition() + " to " + positionToSeekTo);
    }

    setChannelPosition(positionToSeekTo);
  }

  /**
   * Sets the offset in the block to which the the next write will write data
   * to.
   */
  public void setChannelPosition(long dataOffset)
      throws IOException {
    if (((FileOutputStream) dataOut).getChannel().size() < dataOffset) {
      String msg = "Trying to change block file offset of block " + block
          + "file " + ((blockFile != null) ? blockFile.getPath() : "unknown")
          + " to " + dataOffset + " but actual size of file is "
          + ((FileOutputStream) dataOut).getChannel().size();
      throw new IOException(msg);
    }
    if (dataOffset > ((FileOutputStream) dataOut).getChannel().size()) {
      throw new IOException("Set position over the end of the data file.");
    }
    ((FileOutputStream) dataOut).getChannel().position(dataOffset);
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
    FileInputStream dataIn = null;
    
    try {
      RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
      dataIn = new FileInputStream(blockInFile.getFD());

      if (fileOff > 0) {
        blockInFile.seek(fileOff);
      }
      IOUtils.readFully(dataIn, buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(dataIn, crcbuf, 0, crcbuf.length);
    } finally {
      if (dataIn != null) {
        dataIn.close();
      }
    }

    // compute crc of partial chunk from data read in the block file.
    partialCrc = new CRC32();
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
  }

  /**
   * Flush the data and checksum data out to the stream. Please call sync to
   * make sure to write the data in to disk
   * 
   * @throws IOException
   */
  @Override
  public void flush(boolean forceSync) throws IOException {
    if (dataOut != null) {
      dataOut.flush();
      if (forceSync && (dataOut instanceof FileOutputStream)) {
        ((FileOutputStream) dataOut).getChannel().force(true);
      }
    }
  }

  public void truncateBlock(long newBlockLen)
      throws IOException {
    if (newBlockLen == 0) {
      // Special case for truncating to 0 length, since there's no previous
      // chunk.
      RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
      try {
        // truncate blockFile
        blockRAF.setLength(BlockInlineChecksumReader.getHeaderSize());
      } finally {
        blockRAF.close();
      }
      return;
    }

    DataChecksum dcs = DataChecksum.newDataChecksum(this.checksumType, this.bytesPerChecksum);
    this.checksumSize = dcs.getChecksumSize();

    long newBlockFileSize = BlockInlineChecksumReader
        .getFileLengthFromBlockSize(newBlockLen, bytesPerChecksum, checksumSize);

    int lastchunksize = (int) (newBlockLen % bytesPerChecksum);
    long lastchunkoffset = BlockInlineChecksumReader
        .getPosFromBlockOffset(newBlockLen - lastchunksize,
            bytesPerChecksum, checksumSize);
    
    byte[] b = new byte[Math.max(lastchunksize, checksumSize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      // truncate blockFile
      blockRAF.setLength(newBlockFileSize);

      // read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);

      // compute checksum
      dcs.update(b, 0, lastchunksize);
      dcs.writeValue(b, 0, false);

      blockRAF.seek(newBlockFileSize - checksumSize);
      blockRAF.write(b, 0, checksumSize);   
    } finally {
      blockRAF.close();
    }
  }

  @Override
  public void close() throws IOException {
    IOException ioe = null;
    // close block file
    try {
      try {
        flush(datanode.syncOnClose);
      } finally {
        if (dataOut != null) {
          dataOut.close();
          dataOut = null;
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
}

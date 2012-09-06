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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender.InputStreamFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.ChecksumUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/**
 * Read from blocks with separate checksum files.
 * Block file name:
 * blk_(blockId)
 * 
 * Checksum file name:
 * blk_(blockId)_(generation_stamp).meta
 * 
 * The on disk file format is:
 * Data file keeps just data in the block:
 * 
 *  +---------------+
 *  |               |
 *  |     Data      |
 *  |      .        |
 *  |      .        |
 *  |      .        |
 *  |      .        |
 *  |      .        |
 *  |      .        |
 *  |               |
 *  +---------------+
 *  
 *  Checksum file:
 *  +----------------------+
 *  |   Checksum Header    |
 *  +----------------------+
 *  | Checksum for Chunk 1 |
 *  +----------------------+
 *  | Checksum for Chunk 2 |
 *  +----------------------+
 *  |          .           |
 *  |          .           |
 *  |          .           |
 *  +----------------------+
 *  |  Checksum for last   |
 *  |   Chunk (Partial)    |
 *  +----------------------+
 * 
 */
public class BlockWithChecksumFileReader extends DatanodeBlockReader {
  private InputStreamWithChecksumFactory streamFactory;
  private DataInputStream checksumIn; // checksum datastream
  private InputStream blockIn; // data stream
  long blockInPosition = -1;
  MemoizedBlock memoizedBlock;

  BlockWithChecksumFileReader(int namespaceId, Block block,
      FSDatasetInterface data, boolean ignoreChecksum, boolean verifyChecksum,
      boolean corruptChecksumOk, InputStreamWithChecksumFactory streamFactory) throws IOException {
    super(namespaceId, block, data, ignoreChecksum, verifyChecksum,
        corruptChecksumOk);
    this.streamFactory = streamFactory;
    this.checksumIn = streamFactory.getChecksumStream();
    this.block = block;
  }
  
  private void initializeNullChecksum() {
    checksumIn = null;
    // This only decides the buffer size. Use BUFFER_SIZE?
    checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL,
        16 * 1024);
  }

  public DataChecksum getChecksum(long blockLength) throws IOException {
    if (!corruptChecksumOk || checksumIn != null) {

      // read and handle the common header here. For now just a version
      try {
      BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      short version = header.getVersion();

      if (version != FSDataset.METADATA_VERSION) {
        LOG.warn("Wrong version (" + version + ") for metadata file for "
            + block + " ignoring ...");
      }
      checksum = header.getChecksum();
      } catch (IOException ioe) {
        if (blockLength == 0) {
          initializeNullChecksum();
        } else {
          throw ioe;
        }
      }
    } else {
      LOG.warn("Could not find metadata file for " + block);
      initializeNullChecksum();
    }
    super.getChecksumInfo(blockLength);
    return checksum;

  }

  public void initializeStream(long offset, long blockLength)
      throws IOException {
    // seek to the right offsets
    if (offset > 0) {
      long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
      // note blockInStream is seeked when created below
      if (checksumSkip > 0) {
        // Should we use seek() for checksum file as well?
        IOUtils.skipFully(checksumIn, checksumSkip);
      }
    }

    blockIn = streamFactory.createStream(offset);
    memoizedBlock = new MemoizedBlock(blockIn, blockLength, streamFactory,
        block);
  }

  public boolean prepareTransferTo() throws IOException {
    if (blockIn instanceof FileInputStream) {
      FileChannel fileChannel = ((FileInputStream) blockIn).getChannel();

      // blockInPosition also indicates sendChunks() uses transferTo.
      blockInPosition = fileChannel.position();
      return true;
    }
    return false;
  }

  @Override
  public void sendChunks(OutputStream out, byte[] buf, long offset,
      int checksumOff, int numChunks, int len) throws IOException {
    int checksumLen = numChunks * checksumSize;

    if (checksumSize > 0 && checksumIn != null) {
      try {
        checksumIn.readFully(buf, checksumOff, checksumLen);
      } catch (IOException e) {
        LOG.warn(" Could not read or failed to veirfy checksum for data"
            + " at offset " + offset + " for block " + block + " got : "
            + StringUtils.stringifyException(e));
        IOUtils.closeStream(checksumIn);
        checksumIn = null;
        if (corruptChecksumOk) {
          if (checksumOff < checksumLen) {
            // Just fill the array with zeros.
            Arrays.fill(buf, checksumOff, checksumLen, (byte) 0);
          }
        } else {
          throw e;
        }
      }
    }

    int dataOff = checksumOff + checksumLen;
    
    if (blockInPosition < 0) {
      // normal transfer
      IOUtils.readFully(blockIn, buf, dataOff, len);

      if (verifyChecksum) {
        int dOff = dataOff;
        int cOff = checksumOff;
        int dLeft = len;

        for (int i = 0; i < numChunks; i++) {
          checksum.reset();
          int dLen = Math.min(dLeft, bytesPerChecksum);
          checksum.update(buf, dOff, dLen);
          if (!checksum.compare(buf, cOff)) {
            throw new ChecksumException("Checksum failed at "
                + (offset + len - dLeft), len);
          }
          dLeft -= dLen;
          dOff += dLen;
          cOff += checksumSize;
        }
      }

      // only recompute checksum if we can't trust the meta data due to
      // concurrent writes
      if (memoizedBlock.hasBlockChanged(len, offset)) {
        ChecksumUtil.updateChunkChecksum(buf, checksumOff, dataOff, len,
            checksum);
      }

      try {
        out.write(buf, 0, dataOff + len);
      } catch (IOException e) {
        throw BlockSender.ioeToSocketException(e);
      }
    } else {
      try {
        // use transferTo(). Checks on out and blockIn are already done.
        SocketOutputStream sockOut = (SocketOutputStream) out;
        FileChannel fileChannel = ((FileInputStream) blockIn).getChannel();

        if (memoizedBlock.hasBlockChanged(len, offset)) {
          fileChannel.position(blockInPosition);
          IOUtils.readFileChannelFully(fileChannel, buf, dataOff, len);

          ChecksumUtil.updateChunkChecksum(buf, checksumOff, dataOff, len,
              checksum);
          sockOut.write(buf, 0, dataOff + len);
        } else {
          // first write the packet
          sockOut.write(buf, 0, dataOff);
          // no need to flush. since we know out is not a buffered stream.
          sockOut.transferToFully(fileChannel, blockInPosition, len);
        }

        blockInPosition += len;

      } catch (IOException e) {
        /*
         * exception while writing to the client (well, with transferTo(), it
         * could also be while reading from the local file).
         */
        throw BlockSender.ioeToSocketException(e);
      }
    }

  }

  public void close() throws IOException {
    IOException ioe = null;

    // close checksum file
    if (checksumIn != null) {
      try {
        checksumIn.close();
      } catch (IOException e) {
        ioe = e;
      }
      checksumIn = null;
    }
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
    private final InputStreamWithChecksumFactory isf;

    private MemoizedBlock(InputStream inputStream, long blockLength,
        InputStreamWithChecksumFactory isf, Block block) {
      this.inputStream = inputStream;
      this.blockLength = blockLength;
      this.isf = isf;
      this.block = block;
    }

    // logic: if we are starting or ending on a partial chunk and the block
    // has more data than we were told at construction, the block has 'changed'
    // in a way that we care about (ie, we can't trust crc data)
    boolean hasBlockChanged(long dataLen, long offset) throws IOException {
      // check if we are using transferTo since we tell if the file has changed
      // (blockInPosition >= 0 => we are using transferTo and File Channels
      if (blockInPosition >= 0) {
        long currentLength = ((FileInputStream) inputStream).getChannel()
            .size();

        return (blockInPosition % bytesPerChecksum != 0 || dataLen
            % bytesPerChecksum != 0)
            && currentLength > blockLength;

      } else {
        FSDatasetInterface ds = null;
        if (isf instanceof DatanodeBlockReader.BlockInputStreamFactory) {
          ds = ((DatanodeBlockReader.BlockInputStreamFactory) isf).getDataset();
        }

        // offset is the offset into the block
        return (offset % bytesPerChecksum != 0 || dataLen % bytesPerChecksum != 0)
            && ds != null
            && ds.getOnDiskLength(namespaceId, block) > blockLength;
      }
    }
  }
  
  public static interface InputStreamWithChecksumFactory extends
      BlockSender.InputStreamFactory {
    public DataInputStream getChecksumStream() throws IOException; 
  }
}

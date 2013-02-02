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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender.InputStreamFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.ChecksumUtil;
import org.apache.hadoop.util.CrcConcat;
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
      boolean isFinalized,  boolean ignoreChecksum,
      boolean verifyChecksum, boolean corruptChecksumOk,
      InputStreamWithChecksumFactory streamFactory) throws IOException {
    super(namespaceId, block, isFinalized, ignoreChecksum, verifyChecksum,
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

  public DataChecksum getChecksumToSend(long blockLength) throws IOException {
    if (!corruptChecksumOk || checksumIn != null) {

      // read and handle the common header here. For now just a version
      try {
      BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      short version = header.getVersion();

      if (version != FSDataset.FORMAT_VERSION_NON_INLINECHECKSUM) {
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
      int checksumOff, int numChunks, int len, BlockCrcUpdater crcUpdater)
      throws IOException {
    int checksumLen = numChunks * checksumSize;

    if (checksumSize > 0 && checksumIn != null) {
      try {
        checksumIn.readFully(buf, checksumOff, checksumLen);
        if (crcUpdater != null) {
          long tempOffset = offset;
          long remain = len;
          for (int i = 0; i < checksumLen; i += checksumSize) {
            long chunkSize = (remain > bytesPerChecksum) ? bytesPerChecksum
                : remain;
            crcUpdater.updateBlockCrc(tempOffset, true, (int) chunkSize,
                DataChecksum.getIntFromBytes(buf, checksumOff + i));
            remain -= chunkSize;
          }
        }
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("IOException when reading block " + block + " offset " + offset, e);
        }
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("IOException when reading block " + block + " offset "
              + blockInPosition, e);
        }
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
      if (isFinalized) {
        // We would treat it an error case for a finalized block at open time
        // has an unmatched size when closing. There might be false positive
        // for append() case. We made the trade-off to avoid false negative.
        // always return true so it data integrity is guaranteed by checksum
        // checking.
        return false;
      }

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

  /** Find the metadata file for the specified block file.
   * Return the generation stamp from the name of the metafile.
   */
  static long getGenerationStampFromSeperateChecksumFile(String[] listdir, String blockName) {
    for (int j = 0; j < listdir.length; j++) {
      String path = listdir[j];
      if (!path.startsWith(blockName)) {
        continue;
      }
      String[] vals = StringUtils.split(path, '_');
      if (vals.length != 3) {     // blk, blkid, genstamp.meta
        continue;
      }
      String[] str = StringUtils.split(vals[2], '.');
      if (str.length != 2) {
        continue;
      }
      return Long.parseLong(str[0]);
    }
    DataNode.LOG.warn("Block " + blockName +
                      " does not have a metafile!");
    return Block.GRANDFATHER_GENERATION_STAMP;
  }
  

  /**
   * Find generation stamp from block file and meta file.
   * @param blockFile
   * @param metaFile
   * @return
   * @throws IOException
   */
  static long parseGenerationStampInMetaFile(File blockFile, File metaFile
      ) throws IOException {
    String metaname = metaFile.getName();
    String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - FSDataset.METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw (IOException)new IOException("blockFile=" + blockFile
          + ", metaFile=" + metaFile).initCause(nfe);
    }
  }

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
  
  static protected File getMetaFile(FSDatasetInterface dataset, int namespaceId,
      Block b) throws IOException {
    return BlockWithChecksumFileWriter.getMetaFile(dataset.getBlockFile(namespaceId, b), b);
  }
  
  /**
   * Does the meta file exist for this block?
   * @param namespaceId - parent namespace id
   * @param b - the block
   * @return true of the metafile for specified block exits
   * @throws IOException
   */
  static public boolean metaFileExists(FSDatasetInterface dataset, int namespaceId, Block b) throws IOException {
    return getMetaFile(dataset, namespaceId, b).exists();
  }

  /**
   * Returns metaData of block b as an input stream (and its length)
   * @param namespaceId - parent namespace id
   * @param b - the block
   * @return the metadata input stream; 
   * @throws IOException
   */
  static public MetaDataInputStream getMetaDataInputStream(
      FSDatasetInterface dataset, int namespace, Block b) throws IOException {
    File checksumFile = getMetaFile(dataset, namespace, b);
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }
  
  static byte[] getMetaData(FSDatasetInterface dataset, int namespaceId,
      Block block) throws IOException {
    MetaDataInputStream checksumIn = null;
    try {
      checksumIn = getMetaDataInputStream(dataset, namespaceId, block);

      long fileSize = checksumIn.getLength();

      if (fileSize >= 1L << 31 || fileSize <= 0) {
        throw new IOException("Unexpected size for checksumFile of block"
            + block);
      }

      byte[] buf = new byte[(int) fileSize];
      IOUtils.readFully(checksumIn, buf, 0, buf.length);
      return buf;
    } finally {
      IOUtils.closeStream(checksumIn);
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
    try {
      int bytesPerCRC;
      int checksumSize;

      long crcPerBlock;

      rawStreamIn = BlockWithChecksumFileReader.getMetaDataInputStream(
          datanode.data, namespaceId, block);
      streamIn = new DataInputStream(new BufferedInputStream(rawStreamIn,
          FSConstants.BUFFER_SIZE));

      final BlockMetadataHeader header = BlockMetadataHeader
          .readHeader(streamIn);
      final DataChecksum checksum = header.getChecksum();
      if (checksum.getChecksumType() != DataChecksum.CHECKSUM_CRC32) {
        throw new IOException("File Checksum now is only supported for CRC32");
      }

      bytesPerCRC = checksum.getBytesPerChecksum();
      checksumSize = checksum.getChecksumSize();
      crcPerBlock = (((BlockWithChecksumFileReader.MetaDataInputStream) rawStreamIn)
          .getLength() - BlockMetadataHeader.getHeaderSize()) / checksumSize;

      int blockCrc = 0;
      byte[] buffer = new byte[checksumSize];
      for (int i = 0; i < crcPerBlock; i++) {
        IOUtils.readFully(streamIn, buffer, 0, buffer.length);
        int intChecksum = ((buffer[0] & 0xff) << 24)
            | ((buffer[1] & 0xff) << 16) | ((buffer[2] & 0xff) << 8)
            | ((buffer[3] & 0xff));

        if (i == 0) {
          blockCrc = intChecksum;
        } else {
          int chunkLength;
          if (i != crcPerBlock - 1 || ri.getBytesVisible() % bytesPerCRC == 0) {
            chunkLength = bytesPerCRC;
          } else {
            chunkLength = (int) ri.getBytesVisible() % bytesPerCRC;
          }
          blockCrc = CrcConcat.concatCrc(blockCrc, intChecksum, chunkLength);
        }
      }
      return blockCrc;
    } finally {
      if (streamIn != null) {
        IOUtils.closeStream(streamIn);
      }
      if (rawStreamIn != null) {
        IOUtils.closeStream(rawStreamIn);
      }
    }
  }
  
  static long readBlockAccelerator(Socket s, File dataFile, Block block,
      long startOffset, long length, DataNode datanode) throws IOException {
    File checksumFile = BlockWithChecksumFileWriter.getMetaFile(dataFile, block);
    FileInputStream datain = new FileInputStream(dataFile);
    FileInputStream metain = new FileInputStream(checksumFile);
    FileChannel dch = datain.getChannel();
    FileChannel mch = metain.getChannel();

    // read in type of crc and bytes-per-checksum from metadata file
    int versionSize = 2;  // the first two bytes in meta file is the version
    byte[] cksumHeader = new byte[versionSize + DataChecksum.HEADER_LEN]; 
    int numread = metain.read(cksumHeader);
    if (numread != versionSize + DataChecksum.HEADER_LEN) {
      String msg = "readBlockAccelerator: metafile header should be atleast " + 
                   (versionSize + DataChecksum.HEADER_LEN) + " bytes " +
                   " but could read only " + numread + " bytes.";
      LOG.warn(msg);
      throw new IOException(msg);
    }
    DataChecksum ckHdr = DataChecksum.newDataChecksum(cksumHeader, versionSize);

    int type = ckHdr.getChecksumType();
    int bytesPerChecksum =  ckHdr.getBytesPerChecksum();
    long cheaderSize = DataChecksum.getChecksumHeaderSize();

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
    long checksumSize = ckHdr.getChecksumSize();
    long startMetaOffset = versionSize + cheaderSize + startChunkNumber * checksumSize;
    long metaLength = numChunks * checksumSize;

    // get a connection back to the client
    SocketOutputStream out = new SocketOutputStream(s, datanode.socketWriteTimeout);

    try {

      // write out the checksum type and bytesperchecksum to client
      // skip the first two bytes that describe the version
      long val = mch.transferTo(versionSize, cheaderSize, out);
      if (val != cheaderSize) {
        String msg = "readBlockAccelerator for block  " + block +
                     " at offset " + 0 + 
                     " but could not transfer checksum header.";
        LOG.warn(msg);
        throw new IOException(msg);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("readBlockAccelerator metaOffset "  +  startMetaOffset + 
                  " mlength " +  metaLength);
      }
      // write out the checksums back to the client
      val = mch.transferTo(startMetaOffset, metaLength, out);
      if (val != metaLength) {
        String msg = "readBlockAccelerator for block  " + block +
                     " at offset " + startMetaOffset +
                     " but could not transfer checksums of size " +
                     metaLength + ". Transferred only " + val;
        LOG.warn(msg);
        throw new IOException(msg);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("readBlockAccelerator dataOffset " + startOffset + 
                  " length  "  + length);
      }
      // send data block back to client
      long read = dch.transferTo(startOffset, length, out);
      if (read != length) {
        String msg = "readBlockAccelerator for block  " + block +
                     " at offset " + startOffset + 
                     " but block size is only " + length +
                     " and could transfer only " + read;
        LOG.warn(msg);
        throw new IOException(msg);
      }
      return read;
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
      IOUtils.closeStream(metain);
    }
    
  }
}

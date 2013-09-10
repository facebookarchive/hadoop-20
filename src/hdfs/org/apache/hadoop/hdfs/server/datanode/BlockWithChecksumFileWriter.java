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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile.RandomAccessor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.CrcConcat;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.NativeCrc32;


/** 
 * Write data into block file and checksum into a separate checksum files.   
 * 
 * The on disk file format is:  
 * Data file:   
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
public class BlockWithChecksumFileWriter extends DatanodeBlockWriter {
  final private BlockDataFile blockDataFile;
  protected BlockDataFile.Writer blockDataWriter = null;
  
  File metafile;

  protected DataOutputStream checksumOut = null; // to crc file at local disk
  protected OutputStream cout = null; // output stream for checksum file

  public BlockWithChecksumFileWriter(BlockDataFile blockDataFile, File metafile) {
    this.blockDataFile = blockDataFile;
    this.metafile = metafile;
  }

  public void initializeStreams(int bytesPerChecksum, int checksumSize,
      Block block, String inAddr, int namespaceId, DataNode datanode)
      throws FileNotFoundException, IOException {
    if (this.blockDataWriter == null) {
      blockDataWriter = blockDataFile.getWriter(-1);
    }
    if (this.cout == null) {
      this.cout = new FileOutputStream(
          new RandomAccessFile(metafile, "rw").getFD());
    }
    checksumOut = new DataOutputStream(new BufferedOutputStream(cout,
        FSConstants.SMALL_BUFFER_SIZE));

    setParameters(bytesPerChecksum, checksumSize, block, inAddr, namespaceId,
        datanode);
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
      LOG.debug("posix_fadvise with advise : " + advise + " for : "
          + blockDataFile.getFile());
    }
    blockDataWriter.posixFadviseIfPossible(offset, len, advise, sync);
  }

  @Override
  public void writeHeader(DataChecksum checksum) throws IOException {
    BlockMetadataHeader.writeHeader(checksumOut, checksum);
  }

  @Override
  public void writePacket(byte pktBuf[], int len, int dataOff,
      int pktBufStartOff, int numChunks, int packetVersion) throws IOException {
    if (packetVersion != DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
      throw new IOException(
          "non-inline checksum doesn't support packet version " + packetVersion);
    }
    if (len == 0) {
      return;
    }
    
    // finally write to the disk :
    blockDataWriter.write(pktBuf, dataOff, len);

    boolean lastChunkStartsFromChunkStart = false;
    if (firstChunkOffset > 0) {
      // packet doesn't start as beginning of the chunk, need to concatenate
      // checksums of two pieces.
      int crcPart2 = DataChecksum.getIntFromBytes(pktBuf, pktBufStartOff);
      partialCrcInt = CrcConcat.concatCrc(partialCrcInt, crcPart2,
          Math.min(len, bytesPerChecksum - firstChunkOffset));
      byte[] tempBuf = new byte[4];
      DataChecksum.writeIntToBuf(partialCrcInt, tempBuf, 0);
      checksumOut.write(tempBuf);
      if (numChunks > 1) {
        // write the other chunk's checksums.
        checksumOut.write(pktBuf, pktBufStartOff + checksumSize, (numChunks - 1)
            * checksumSize);
        lastChunkStartsFromChunkStart = true;
     }
    } else {
      checksumOut.write(pktBuf, pktBufStartOff, numChunks * checksumSize);
      lastChunkStartsFromChunkStart = true;
    }
    firstChunkOffset = (firstChunkOffset + len) % bytesPerChecksum;
    if (firstChunkOffset > 0 && lastChunkStartsFromChunkStart) {
      // The last chunk is partial and starts from the chunk boundary,
      // need to remember its checksum for the next chunk.
      partialCrcInt = DataChecksum.getIntFromBytes(pktBuf, pktBufStartOff
          + (numChunks - 1) * checksumSize);
    }
  }

  /**
   * Retrieves the offset in the block to which the the next write will write
   * data to.
   */
  public long getChannelPosition() throws IOException {
    return blockDataWriter.getChannelPosition();
  }
  
  private long getChecksumOffset(long offsetInBlock) {
    return BlockMetadataHeader.getHeaderSize() + offsetInBlock
        / bytesPerChecksum * checksumSize;
  }

  @Override
  public void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock, DataChecksum checksum) throws IOException {
    firstChunkOffset = (int) (offsetInBlock % bytesPerChecksum);

    if (getChannelPosition() == offsetInBlock) {
      if (firstChunkOffset > 0) {
        // Partial block, need to seek checksum stream back.
        setChecksumOffset(getChecksumOffset(offsetInBlock));
      }
      return; // nothing to do
    }
    long offsetInChecksum = getChecksumOffset(offsetInBlock);

    if (blockDataWriter != null) {
      blockDataWriter.flush();
    }
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // If this is a partial chunk, then read in pre-existing checksum
    if (offsetInBlock % bytesPerChecksum != 0) {
      LOG.info("setBlockPosition trying to set position to " + offsetInBlock
          + " for block " + block
          + " which is not a multiple of bytesPerChecksum " + bytesPerChecksum);
      computePartialChunkCrc(offsetInBlock, offsetInChecksum, bytesPerChecksum, checksum);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing block file offset of block " + block + " from "
          + getChannelPosition() + " to " + offsetInBlock
          + " meta file offset to " + offsetInChecksum);
    }

    // set the position of the block file
    setChannelPosition(offsetInBlock, offsetInChecksum);
  }

  /**
   * Sets the offset in the block to which the the next write will write data
   * to.
   */
  public void setChannelPosition(long dataOffset, long ckOffset)
      throws IOException {
    long channelSize = blockDataWriter.getChannelSize();
    if (channelSize < dataOffset) {
      String fileName;
      if (datanode.data instanceof FSDataset) {
        FSDataset fsDataset = (FSDataset) datanode.data;
        fileName = fsDataset.getDatanodeBlockInfo(namespaceId, block)
            .getBlockDataFile().getTmpFile(namespaceId, block).toString();
      } else {
        fileName = "unknown";
      }
      String msg = "Trying to change block file offset of block " + block
          + " file " + fileName + " to " + dataOffset
          + " but actual size of file is " + blockDataWriter.getChannelSize();
      throw new IOException(msg);
    }
    if (dataOffset > channelSize) {
      throw new IOException("Set position over the end of the data file.");
    }
    if (dataOffset % bytesPerChecksum != 0 && channelSize != dataOffset) {
      DFSClient.LOG.warn("Non-inline Checksum Block " + block
          + " channel size " + channelSize + " but data starts from "
          + dataOffset);
    }
    blockDataWriter.position(dataOffset);

    setChecksumOffset(ckOffset);
  }
  
  private void setChecksumOffset(long ckOffset) throws IOException {
    FileOutputStream file = (FileOutputStream) cout;
    if (ckOffset > file.getChannel().size()) {
      throw new IOException("Set position over the end of the checksum file.");
    }
    file.getChannel().position(ckOffset);
  }

  /**
   * reads in the partial crc chunk and computes checksum of pre-existing data
   * in partial chunk.
   */
  private void computePartialChunkCrc(long blkoff, long ckoff,
      int bytesPerChecksum, DataChecksum checksum) throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    int checksumSize = checksum.getChecksumSize();
    blkoff = blkoff - sizePartialChunk;
    LOG.info("computePartialChunkCrc sizePartialChunk " + sizePartialChunk
        + " block " + block + " offset in block " + blkoff
        + " offset in metafile " + ckoff);

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    FileInputStream dataIn = null, metaIn = null;
    
    try {

      DatanodeBlockInfo info = datanode.data.getDatanodeBlockInfo(namespaceId,
          block);
      if (info == null) {
        throw new IOException("Block " + block
            + " does not exist in volumeMap.");
      }
      File blockFile = info.getDataFileToRead();
      if (blockFile == null) {
        blockFile = info.getBlockDataFile().getTmpFile(namespaceId, block);
      }
      RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
      
      if (blkoff > 0) {
        blockInFile.seek(blkoff);
      }
      File metaFile = getMetaFile(blockFile, block);
      RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
      if (ckoff > 0) {
        metaInFile.seek(ckoff);
      }
      dataIn = new FileInputStream(blockInFile.getFD());
      metaIn = new FileInputStream(metaInFile.getFD());

      IOUtils.readFully(dataIn, buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(metaIn, crcbuf, 0, crcbuf.length);
    } finally {
      if (dataIn != null) {
        dataIn.close();
      }
      if (metaIn != null) {
        metaIn.close();
      }
    }

    // compute crc of partial chunk from data read in the block file.
    Checksum partialCrc = new NativeCrc32();
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
    // LOG.debug("Partial CRC matches 0x" +
    // Long.toHexString(partialCrc.getValue()));
    
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
    if (checksumOut != null) {
      checksumOut.flush();
      if (forceSync && (cout instanceof FileOutputStream)) {
        ((FileOutputStream) cout).getChannel().force(true);
      }
    }
    if (blockDataWriter != null) {
      blockDataWriter.flush();
      if (forceSync) {
        blockDataWriter.force(true);
      }
    }
  }

  @Override
  public void fileRangeSync(long lastBytesToSync, int flags) throws IOException {
    if (cout instanceof FileOutputStream && lastBytesToSync > 0) {
      FileChannel fc = ((FileOutputStream) cout).getChannel();
      long pos = fc.position();
      long startOffset = pos - lastBytesToSync;
      if (startOffset < 0) {
        startOffset = 0;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("file_range_sync " + block + " channel position " + pos
            + " offset " + startOffset);
      }
      blockDataWriter.syncFileRangeIfPossible(startOffset, pos
          - startOffset, flags);
    }
  }
  
  public void truncateBlock(long oldBlockFileLen, long newlen)
      throws IOException {
    if (newlen == 0) {
      // Special case for truncating to 0 length, since there's no previous
      // chunk.
      RandomAccessor ra = blockDataFile.getRandomAccessor();
      try {
        // truncate blockFile
        ra.setLength(newlen);
      } finally {
        ra.close();
      }
      // update metaFile
      RandomAccessFile metaRAF = new RandomAccessFile(metafile, "rw");
      try {
        metaRAF.setLength(BlockMetadataHeader.getHeaderSize());
      } finally {
        metaRAF.close();
      }
      return;
    }
    DataChecksum dcs = BlockMetadataHeader.readHeader(metafile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long newChunkCount = (newlen - 1) / bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + newChunkCount
        * checksumsize;
    long lastchunkoffset = (newChunkCount - 1) * bpc;
    int lastchunksize = (int) (newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessor ra = blockDataFile.getRandomAccessor();
    try {
      // truncate blockFile
      ra.setLength(newlen);

      // read last chunk
      ra.seek(lastchunkoffset);
      ra.readFully(b, 0, lastchunksize);
    } finally {
      ra.close();
    }

    // compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    // update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metafile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  @Override
  public void close() throws IOException {
    close(0);
  }

  public void close(int fadvise) throws IOException {
    IOException ioe = null;

    // close checksum file
    try {
      if (checksumOut != null) {
        try {
          checksumOut.flush();
          if (datanode.syncOnClose && (cout instanceof FileOutputStream)) {
            ((FileOutputStream) cout).getChannel().force(true);
          }
        } finally {
          checksumOut.close();          
          checksumOut = null;
        }
      }
    } catch (IOException e) {
      ioe = e;
    }
    // close block file
    try {
      if (blockDataWriter != null) {
        try {
          blockDataWriter.flush();
          if (datanode.syncOnClose) {
            blockDataWriter.force(true);
          }
          if (fadvise != 0) {
            fadviseStream(fadvise, 0, 0, true);
          }
        } finally {
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

  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + FSDataset.METADATA_EXTENSION;
  }

  public static File getMetaFile(File f , Block b) {
    return new File(getMetaFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }

  /** Find the corresponding meta data file from a given block file */
  public static File findMetaFile(final File blockFile) throws IOException {
    return findMetaFile(blockFile, false);
  }

  static File findMetaFile(final File blockFile, boolean missingOk)
    throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(FSDataset.METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      if (missingOk) {
        return null;
      } else {
        throw new IOException("Meta file not found, blockFile=" + blockFile);
      }
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: "
          + Arrays.asList(matches));
    }
    return matches[0];
  }

}

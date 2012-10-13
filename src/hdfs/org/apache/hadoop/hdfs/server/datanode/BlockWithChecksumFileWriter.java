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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;

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
  File blockFile, metafile;

  protected OutputStream dataOut = null; // to block file at local disk
  protected DataOutputStream checksumOut = null; // to crc file at local disk
  protected OutputStream cout = null; // output stream for checksum file
  protected Checksum partialCrc = null;

  public BlockWithChecksumFileWriter(File blockFile, File metafile) {
    this.blockFile = blockFile;
    this.metafile = metafile;
  }

  public void initializeStreams(int bytesPerChecksum, int checksumSize,
      Block block, String inAddr, int namespaceId, DataNode datanode)
      throws FileNotFoundException, IOException {
    if (this.dataOut == null) {
      this.dataOut = new FileOutputStream(
          new RandomAccessFile(blockFile, "rw").getFD());
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
  public void writeHeader(DataChecksum checksum) throws IOException {
    BlockMetadataHeader.writeHeader(checksumOut, checksum);
  }

  @Override
  public void writePacket(byte pktBuf[], int len, int dataOff, int checksumOff,
      int numChunks) throws IOException {
    // finally write to the disk :
    dataOut.write(pktBuf, dataOff, len);

    // If this is a partial chunk, then verify that this is the only
    // chunk in the packet. Calculate new crc for this chunk.
    if (partialCrc != null) {
      if (len > bytesPerChecksum) {
        throw new IOException("Got wrong length during writeBlock(" + block
            + ") from " + inAddr + " "
            + "A packet can have only one partial chunk." + " len = " + len
            + " bytesPerChecksum " + bytesPerChecksum);
      }
      partialCrc.update(pktBuf, dataOff, len);
      byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
      checksumOut.write(buf);
      LOG.debug("Writing out partial crc for data len " + len);
      partialCrc = null;
    } else {
      checksumOut.write(pktBuf, checksumOff, numChunks * checksumSize);
    }
  }

  /**
   * Retrieves the offset in the block to which the the next write will write
   * data to.
   */
  public long getChannelPosition() throws IOException {
    FileOutputStream file = (FileOutputStream) dataOut;
    return file.getChannel().position();
  }

  @Override
  public void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock, DataChecksum checksum) throws IOException {
    if (getChannelPosition() == offsetInBlock) {
      return; // nothing to do
    }
    long offsetInChecksum = BlockMetadataHeader.getHeaderSize() + offsetInBlock
        / bytesPerChecksum * checksumSize;

    if (dataOut != null) {
      dataOut.flush();
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
    FileOutputStream file = (FileOutputStream) dataOut;
    if (file.getChannel().size() < dataOffset) {
      String fileName;
      if (datanode.data instanceof FSDataset) {
        FSDataset fsDataset = (FSDataset) datanode.data;
        fileName = fsDataset.getDatanodeBlockInfo(namespaceId, block)
            .getTmpFile(namespaceId, block).toString();
      } else {
        fileName = "unknown";
      }
      String msg = "Trying to change block file offset of block " + block
          + " file " + fileName + " to " + dataOffset
          + " but actual size of file is " + file.getChannel().size();
      throw new IOException(msg);
    }
    if (dataOffset > file.getChannel().size()) {
      throw new IOException("Set position over the end of the data file.");
    }
    file.getChannel().position(dataOffset);
    file = (FileOutputStream) cout;
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
        blockFile = info.getTmpFile(namespaceId, block);
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
    partialCrc = new PureJavaCrc32();
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
  }

  /**
   * Flush the data and checksum data out to the stream. Please call sync to
   * make sure to write the data in to disk
   * 
   * @throws IOException
   */
  @Override
  public void flush(boolean forceSync) throws IOException {
    if (checksumOut != null) {
      checksumOut.flush();
      if (forceSync && (cout instanceof FileOutputStream)) {
        ((FileOutputStream) cout).getChannel().force(true);
      }
    }
    if (dataOut != null) {
      dataOut.flush();
      if (forceSync && (dataOut instanceof FileOutputStream)) {
        ((FileOutputStream) dataOut).getChannel().force(true);
      }
    }
  }

  public void truncateBlock(long oldBlockFileLen, long newlen)
      throws IOException {
    if (newlen == 0) {
      // Special case for truncating to 0 length, since there's no previous
      // chunk.
      RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
      try {
        // truncate blockFile
        blockRAF.setLength(newlen);
      } finally {
        blockRAF.close();
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

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      // truncate blockFile
      blockRAF.setLength(newlen);

      // read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
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
      if (dataOut != null) {
        try {
          dataOut.flush();
          if (datanode.syncOnClose && (dataOut instanceof FileOutputStream)) {
            ((FileOutputStream) dataOut).getChannel().force(true);
          }
        } finally {
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

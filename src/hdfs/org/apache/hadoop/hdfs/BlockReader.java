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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.ReadBlockHeader;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.hdfs.DFSClient.DataNodeSlowException;

/** This is a wrapper around connection to datadone
 * and understands checksum, offset etc
 */
public class BlockReader extends FSInputChecker {

  private Socket dnSock; //for now just sending checksumOk.
  private DataInputStream in;
  protected DataChecksum checksum;
  protected long lastChunkOffset = -1;
  protected long lastChunkLen = -1;
  private long lastSeqNo = -1;
  private boolean transferBlockSize;

  protected long startOffset;
  protected long firstChunkOffset;
  protected int bytesPerChecksum;
  protected int checksumSize;
  protected boolean gotEOS = false;
  
  protected boolean blkLenInfoUpdated = false;
  protected boolean isBlockFinalized;
  protected long updatedBlockLength;

  byte[] skipBuf = null;
  ByteBuffer checksumBytes = null;
  int packetLen = 0;
  int dataLeft = 0;
  boolean isLastPacket = false;
  protected long minSpeedBps;
  protected long bytesRead;
  protected long timeRead;
  protected boolean slownessLoged;
  
  protected boolean isReadLocal = false;
  protected boolean isReadRackLocal = false;
  protected FileSystem.Statistics fsStats = null;
  
  private long artificialSlowdown = 0;
  
  // It's a temporary flag used for tests
  public boolean ENABLE_THROW_FOR_SLOW = false;


  void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }


  /* FSInputChecker interface */

  /* same interface as inputStream java.io.InputStream#read()
   * used by DFSInputStream#read()
   * This violates one rule when there is a checksum error:
   * "Read should not modify user buffer before successful read"
   * because it first reads the data to user buffer and then checks
   * the checksum.
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len)
                               throws IOException {

    //for the first read, skip the extra bytes at the front.
    if (lastChunkLen < 0 && startOffset > firstChunkOffset) {
      // Skip these bytes. But don't call this.skip()!
      int toSkip = (int)(startOffset - firstChunkOffset);
      if ( skipBuf == null ) {
        skipBuf = new byte[bytesPerChecksum];
      }
      if ( super.read(skipBuf, 0, toSkip) != toSkip ) {
        // should never happen
        throw new IOException("Could not skip required number of bytes");
      }
      updateStatsAfterRead(toSkip);
    }

    boolean eosBefore = gotEOS;
    int nRead = super.read(buf, off, len);
    
    // if gotEOS was set in the previous read and checksum is enabled :
    if (dnSock != null && gotEOS && !eosBefore && nRead >= 0 && needChecksum()) {
      //checksum is verified and there are no errors.
      checksumOk(dnSock);
    }
    updateStatsAfterRead(nRead);
    return nRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    /* How can we make sure we don't throw a ChecksumException, at least
     * in majority of the cases?. This one throws. */
    if ( skipBuf == null ) {
      skipBuf = new byte[bytesPerChecksum];
    }

    long nSkipped = 0;
    while ( nSkipped < n ) {
      int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
      int ret = read(skipBuf, 0, toSkip);
      if ( ret <= 0 ) {
        return nSkipped;
      }
      nSkipped += ret;
    }
    return nSkipped;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("read() is not expected to be invoked. " +
                          "Use read(buf, off, len) instead.");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    /* Checksum errors are handled outside the BlockReader.
     * DFSInputStream does not always call 'seekToNewSource'. In the
     * case of pread(), it just tries a different replica without seeking.
     */
    return false;
  }

  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("Seek() is not supported in BlockInputChecker");
  }

  @Override
  protected long getChunkPosition(long pos) {
    throw new RuntimeException("getChunkPosition() is not supported, " +
                               "since seek is not required");
  }

  public void setReadLocal(boolean isReadLocal) {
    this.isReadLocal = isReadLocal;
    if (isReadLocal) {
      this.isReadRackLocal = true;
    }
  }

  public void setReadRackLocal(boolean isReadSwitchLocal) {
    this.isReadRackLocal = isReadSwitchLocal;
  }

  public void setFsStats(FileSystem.Statistics fsStats) {
    this.fsStats = fsStats;
  }

  public boolean isBlkLenInfoUpdated() {
    return blkLenInfoUpdated;
  }

  public boolean isBlockFinalized() {
    return isBlockFinalized;
  }

  public long getUpdatedBlockLength() {
    return updatedBlockLength;
  }

  public void resetBlockLenInfo() {
    blkLenInfoUpdated = false;
  }

  /**
   * Makes sure that checksumBytes has enough capacity
   * and limit is set to the number of checksum bytes needed
   * to be read.
   */
  private void adjustChecksumBytes(int dataLen) {
    int requiredSize =
      ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize;
    if (checksumBytes == null || requiredSize > checksumBytes.capacity()) {
      checksumBytes =  ByteBuffer.wrap(new byte[requiredSize]);
    } else {
      checksumBytes.clear();
    }
    checksumBytes.limit(requiredSize);
  }
  
  /**
   * Read the block length information from data stream
   * 
   * @throws IOException
   */
  private synchronized void readBlockSizeInfo() throws IOException {
    if (!transferBlockSize) {
      return;
    }
    blkLenInfoUpdated = true;
    isBlockFinalized = in.readBoolean();
    updatedBlockLength = in.readLong();
    if (LOG.isDebugEnabled()) {
      LOG.debug("ifBlockComplete? " + isBlockFinalized + " block size: "
          + updatedBlockLength);
    }      
  }

  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset,
                                       int len, byte[] checksumBuf)
                                       throws IOException {
    // Read one chunk.

    if ( gotEOS ) {
      if ( startOffset < 0 ) {
        //This is mainly for debugging. can be removed.
        throw new IOException( "BlockRead: already got EOS or an error" );
      }
      startOffset = -1;
      return -1;
    }

    // Read one DATA_CHUNK.
    long chunkOffset = lastChunkOffset;
    if ( lastChunkLen > 0 ) {
      chunkOffset += lastChunkLen;
    }
    if ( (pos + firstChunkOffset) != chunkOffset ) {
      throw new IOException("Mismatch in pos : " + pos + " + " +
                            firstChunkOffset + " != " + chunkOffset);
    }

    long startTime = System.currentTimeMillis();
    // Read next packet if the previous packet has been read completely.
    if (dataLeft <= 0) {
      // check read speed
      // Time only is counted in readChunk() not outside. It is to distinguish
      // the cases between application to consume data slow or reading from
      // data-nodes is slow. We don't want to throw exception in the former
      // case. So the speed measurement here, actually is how much slower
      // DFSClient reads data from datanodes than application to consume the
      // data. That's the real slowness case users care about.
      //
      if (minSpeedBps > 0) {
        bytesRead += packetLen;
        if (bytesRead > DFSClient.NUM_BYTES_CHECK_READ_SPEED) {
          if (timeRead > 0 && bytesRead * 1000 / timeRead < minSpeedBps) {
            if (!slownessLoged) {
              FileSystem.LogForCollect
                  .info("Too slow when reading block. bytes: " + bytesRead
                      + " time: " + timeRead + " msec. Path: "
                      + super.file.getName());
            }
            if (this.isReadLocal) {
              if (!slownessLoged) {
                LOG.info("Not switch from a local datanode.");
                slownessLoged = true;
              }
            } else if (this.isReadRackLocal) {
              if (!slownessLoged) {
                LOG.info("Not switch from a datanode from the same rack.");
                slownessLoged = true;
              }
            } else {
              if (!ENABLE_THROW_FOR_SLOW) {
                if (!slownessLoged) {
                  LOG.info("Won't swtich to another datanode for not disabled.");
                  slownessLoged = true;
                                   }
              } else {
                throw new DataNodeSlowException(
                    "Block Reading Speed is too slow");
              }
            }
          }
          timeRead = 0;
          bytesRead = 0;
        }
      }
      
      //Read packet headers.
      packetLen = in.readInt();

      if (packetLen == 0) {
        // the end of the stream
        gotEOS = true;
        readBlockSizeInfo();
        return 0;
      }

      long offsetInBlock = in.readLong();
      long seqno = in.readLong();
      boolean lastPacketInBlock = in.readBoolean();

      if (LOG.isDebugEnabled()) {
        LOG.debug("DFSClient readChunk got seqno " + seqno +
                  " offsetInBlock " + offsetInBlock +
                  " lastPacketInBlock " + lastPacketInBlock +
                  " packetLen " + packetLen);
      }

      int dataLen = in.readInt();

      // Sanity check the lengths
      if ( dataLen < 0 ||
           ( (dataLen % bytesPerChecksum) != 0 && !lastPacketInBlock ) ||
           (seqno != (lastSeqNo + 1)) ) {
           throw new IOException("BlockReader: error in packet header" +
                                 "(chunkOffset : " + chunkOffset +
                                 ", dataLen : " + dataLen +
                                 ", seqno : " + seqno +
                                 " (last: " + lastSeqNo + "))");
      }

      lastSeqNo = seqno;
      isLastPacket = lastPacketInBlock;
      dataLeft = dataLen;
      adjustChecksumBytes(dataLen);
      if (dataLen > 0) {
        IOUtils.readFully(in, checksumBytes.array(), 0,
                          checksumBytes.limit());
      }
    }

    int chunkLen = Math.min(dataLeft, bytesPerChecksum);

    if ( chunkLen > 0 ) {
      // len should be >= chunkLen
      IOUtils.readFully(in, buf, offset, chunkLen);
      checksumBytes.get(checksumBuf, 0, checksumSize);

      // This is used by unit test to trigger race conditions.
      if (artificialSlowdown != 0) {
        DFSClient.sleepForUnitTest(artificialSlowdown);
      }
    }

    dataLeft -= chunkLen;
    lastChunkOffset = chunkOffset;
    lastChunkLen = chunkLen;
    
    if (minSpeedBps > 0) {
      this.timeRead += System.currentTimeMillis() - startTime;
    }

    if ((dataLeft == 0 && isLastPacket) || chunkLen == 0) {
      gotEOS = true;
      int expectZero = in.readInt();
      assert expectZero == 0;
      readBlockSizeInfo();
    }
    if ( chunkLen == 0 ) {
      return -1;
    }

    return chunkLen;
  }
  
  protected void updateStatsAfterRead(int bytesRead) {
    if (fsStats == null) {
      return;
    }
    if (isReadLocal) {
      fsStats.incrementLocalBytesRead(bytesRead);
    }
    if (isReadRackLocal) {
      fsStats.incrementRackLocalBytesRead(bytesRead);
    }
  }

  private BlockReader( String file, long blockId, DataInputStream in,
                       DataChecksum checksum, boolean verifyChecksum,
                       long startOffset, long firstChunkOffset,
                       Socket dnSock, long minSpeedBps,
                       long dataTransferVersion ) {
    super(new Path("/blk_" + blockId + ":of:" + file)/*too non path-like?*/,
          1, verifyChecksum,
          checksum.getChecksumSize() > 0? checksum : null,
          checksum.getBytesPerChecksum(),
          checksum.getChecksumSize());

    this.dnSock = dnSock;
    this.in = in;
    this.checksum = checksum;
    this.startOffset = Math.max( startOffset, 0 );
    this.transferBlockSize =
        (dataTransferVersion >= DataTransferProtocol.SEND_DATA_LEN_VERSION);      
    this.firstChunkOffset = firstChunkOffset;
    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;

    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();

    this.bytesRead = 0;
    this.timeRead = 0;
    this.minSpeedBps = minSpeedBps;
    this.slownessLoged = false;
  }

  /**
   * Public constructor
   */
  BlockReader(Path file, int numRetries) {
    super(file, numRetries);
  }
   
   protected BlockReader(Path file, int numRetries, DataChecksum checksum, boolean verifyChecksum) {
     super(file,
         numRetries,
         verifyChecksum,
         checksum.getChecksumSize() > 0? checksum : null, 
         checksum.getBytesPerChecksum(),
         checksum.getChecksumSize());       
   }
   

  public static BlockReader newBlockReader(int dataTransferVersion,
      int namespaceId,
      Socket sock, String file, long blockId,
      long genStamp, long startOffset, long len, int bufferSize) throws IOException {
    return newBlockReader(dataTransferVersion, namespaceId,
        sock, file, blockId, genStamp, startOffset, len, bufferSize,
        true);
  }

  /** Java Doc required */
  public static BlockReader newBlockReader( int dataTransferVersion,
                                     int namespaceId,
                                     Socket sock, String file, long blockId,
                                     long genStamp,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum)
                                     throws IOException {
    return newBlockReader(dataTransferVersion, namespaceId,
                          sock, file, blockId, genStamp,
                          startOffset,
                          len, bufferSize, verifyChecksum, "",
                          -1);
  }
  
  public static BlockReader newBlockReader( int dataTransferVersion,
                                     int namespaceId,
                                     Socket sock, String file,
                                     long blockId,
                                     long genStamp,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum,
                                     String clientName, long minSpeedBps)
                                     throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    DataOutputStream out = new DataOutputStream(
      new BufferedOutputStream(NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT)));

    //write the header.
    ReadBlockHeader readBlockHeader = new ReadBlockHeader(
        dataTransferVersion, namespaceId, blockId, genStamp, startOffset, len,
        clientName);
    readBlockHeader.writeVersionAndOpCode(out);
    readBlockHeader.write(out);
    out.flush();

    //
    // Get bytes in block, set streams
    //

    DataInputStream in = new DataInputStream(
        new BufferedInputStream(NetUtils.getInputStream(sock),
                                bufferSize));

    if ( in.readShort() != DataTransferProtocol.OP_STATUS_SUCCESS ) {
      throw new IOException("Got error in response to OP_READ_BLOCK " +
                            "self=" + sock.getLocalSocketAddress() +
                            ", remote=" + sock.getRemoteSocketAddress() +
                            " for file " + file +
                            " for block " + blockId);
    }
    DataChecksum checksum = DataChecksum.newDataChecksum( in , new PureJavaCrc32());
    //Warning when we get CHECKSUM_NULL?

    // Read the first chunk offset.
    long firstChunkOffset = in.readLong();

    if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
        firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
      throw new IOException("BlockReader: error in first chunk offset (" +
                            firstChunkOffset + ") startOffset is " +
                            startOffset + " for file " + file);
    }

    return new BlockReader(file, blockId, in, checksum, verifyChecksum,
        startOffset, firstChunkOffset, sock, minSpeedBps, dataTransferVersion);
  }

  @Override
  public synchronized void close() throws IOException {
    startOffset = -1;
    checksum = null;
    // in will be closed when its Socket is closed.
  }

  /** kind of like readFully(). Only reads as much as possible.
   * And allows use of protected readFully().
   */
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return readFully(this, buf, offset, len);
  }

  /* When the reader reaches end of a block and there are no checksum
   * errors, we send OP_STATUS_CHECKSUM_OK to datanode to inform that
   * checksum was verified and there was no error.
   */
  private void checksumOk(Socket sock) {
    try {
      OutputStream out = NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT);
      byte buf[] = { (DataTransferProtocol.OP_STATUS_CHECKSUM_OK >>> 8) & 0xff,
                     (DataTransferProtocol.OP_STATUS_CHECKSUM_OK) & 0xff };
      out.write(buf);
      out.flush();
    } catch (IOException e) {
      // its ok not to be able to send this.
      LOG.debug("Could not write to datanode " + sock.getInetAddress() +
                ": " + e.getMessage());
    }
  }
}

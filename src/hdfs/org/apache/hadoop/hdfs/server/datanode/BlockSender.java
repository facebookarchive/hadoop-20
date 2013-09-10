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
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSDataNodeReadProfilingData;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.Progressable;

/**
 * Reads a block from the disk and sends it to a recipient.
 */
public class BlockSender implements java.io.Closeable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private DataChecksum checksum; // checksum stream
  private long offset; // starting position to read
  private long endOffset; // ending position
  private long blockLength;
  private int bytesPerChecksum; // chunk size
  private int checksumSize; // checksum size
  private boolean chunkOffsetOK; // if need to send chunk offset
  private long seqno; // sequence number of packet

  private boolean transferToAllowed = true;
  private boolean pktIncludeVersion = false;
  private int packetVersion;
  // set once entire requested byte range has been sent to the client
  private boolean sentEntireByteRange;
  private boolean verifyChecksum; //if true, check is verified while reading
  private DataTransferThrottler throttler;
  private String clientTraceFmt; // format of client trace log message
  private DatanodeBlockReader blockReader;

  private BlockCrcUpdater crcUpdater = null;
  
  final ReplicaToRead replicaToRead;

  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;

  
  BlockSender(int namespaceId, Block block, long startOffset, long length,
              boolean corruptChecksumOk, boolean chunkOffsetOK,
              boolean verifyChecksum, DataNode datanode) throws IOException {
    this(namespaceId, block, startOffset, length, false, corruptChecksumOk,
        chunkOffsetOK, verifyChecksum, false, true, datanode, null);
  }

  BlockSender(int namespaceId, Block block, long startOffset, long length,
      boolean ignoreChecksum, boolean corruptChecksumOk, boolean chunkOffsetOK,
      boolean verifyChecksum, boolean pktIncludeVersion, boolean forceOldPktVersion, DataNode datanode,
      String clientTraceFmt) throws IOException {
    
    replicaToRead = datanode.data.getReplicaToRead(namespaceId, block);
    if (replicaToRead == null) {
      throw new IOException("Can't find block " + block + " in volumeMap");
    }

    long blockLength = replicaToRead.getBytesVisible();
    boolean transferToAllowed = datanode.transferToAllowed;
    
    DatanodeBlockReader.BlockInputStreamFactory streamFactory =
      new DatanodeBlockReader.BlockInputStreamFactory(
        namespaceId, block, replicaToRead, datanode, datanode.data, ignoreChecksum,
        verifyChecksum, corruptChecksumOk);
    blockReader = streamFactory.getBlockReader();

    initialize(namespaceId, block, blockLength, startOffset, length,
        corruptChecksumOk, chunkOffsetOK, verifyChecksum, transferToAllowed,
        datanode.updateBlockCrcWhenRead, pktIncludeVersion, forceOldPktVersion,
        streamFactory, clientTraceFmt);
  }
  
  public BlockSender(int namespaceId, Block block, long blockLength, long startOffset, long length,
              boolean corruptChecksumOk, boolean chunkOffsetOK,
              boolean verifyChecksum, boolean transferToAllowed, boolean pktIncludeVersion,
              BlockWithChecksumFileReader.InputStreamWithChecksumFactory streamFactory
              ) throws IOException {

    replicaToRead = null;
    blockReader = new BlockWithChecksumFileReader(namespaceId, block, true,
        false, verifyChecksum, corruptChecksumOk, streamFactory);

    initialize(namespaceId, block, blockLength, startOffset, length,
        corruptChecksumOk, chunkOffsetOK, verifyChecksum, transferToAllowed,
        false, pktIncludeVersion, true, streamFactory, null);
  }

  private void initialize(int namespaceId, Block block, long blockLength,
      long startOffset, long length, boolean corruptChecksumOk,
      boolean chunkOffsetOK, boolean verifyChecksum, boolean transferToAllowed,
      boolean allowUpdateBlocrCrc, boolean pktIncludeVersion, boolean forceOldPktVersion,
      BlockWithChecksumFileReader.InputStreamWithChecksumFactory streamFactory,
      String clientTraceFmt) throws IOException {
    try {
      this.chunkOffsetOK = chunkOffsetOK;
      this.verifyChecksum = verifyChecksum;
      this.blockLength = blockLength;
      this.transferToAllowed = transferToAllowed;
      this.clientTraceFmt = clientTraceFmt;
      this.pktIncludeVersion = pktIncludeVersion;
      
      if (this.pktIncludeVersion && ! forceOldPktVersion) {
        this.packetVersion = blockReader.getPreferredPacketVersion();
      } else {
        this.packetVersion = DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST;
      }
      
      checksum = blockReader.getChecksumToSend(blockLength);
      
      bytesPerChecksum = blockReader.getBytesPerChecksum();
      checksumSize = blockReader.getChecksumSize();
      
      if (length < 0) {
        length = blockLength;
      }

      endOffset = blockLength;
      if (startOffset < 0 || startOffset > endOffset
          || (length + startOffset) > endOffset) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + endOffset + " )";
        LOG.warn("sendBlock() : " + msg);
        throw new IOException(msg);
      }

      
      offset = (startOffset - (startOffset % bytesPerChecksum));
      if (length >= 0) {
        // Make sure endOffset points to end of a checksumed chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % bytesPerChecksum != 0) {
          tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
        }
        if (tmpLen < endOffset) {
          endOffset = tmpLen;
        }
      }
      
      // Recalculate block CRC if:
      // 1. it is configured to be allowed;
      // 2. the block is finalized
      // 3. the full block is to be read
      // 4. there is no Block CRC already cached
      // 5. the block format is CRC32 and checksum size is 4
      if (allowUpdateBlocrCrc &&
          (!transferToAllowed || verifyChecksum)
          && startOffset == 0
          && length >= blockLength
          && replicaToRead != null
          && !replicaToRead.hasBlockCrcInfo()
          && replicaToRead.isFinalized()
          && replicaToRead instanceof DatanodeBlockInfo
          && checksumSize == DataChecksum.DEFAULT_CHECKSUM_SIZE
          && checksum != null
          && (checksum.getChecksumType() == DataChecksum.CHECKSUM_CRC32 || checksum
              .getChecksumType() == DataChecksum.CHECKSUM_CRC32C)) {
        // Needs to recalculate block CRC
        crcUpdater = new BlockCrcUpdater(bytesPerChecksum, true);
      }
      
      seqno = 0;
      
      blockReader.initialize(offset, blockLength);
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      throw ioe;
    }
  }

  public void fadviseStream(int advise, long offset, long len)
      throws IOException {
    blockReader.fadviseStream(advise, offset, len);
  }

  public ReplicaToRead getReplicaToRead() {
    return replicaToRead;
  }

  /**
   * close opened files.
   */
  public void close() throws IOException {
    if (blockReader != null) {
      blockReader.close();
    }
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException.
   * This is typically done to indicate to upper layers that the error 
   * was a socket error rather than often more serious exceptions like 
   * disk errors.
   */
  static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /* Change the stacktrace so that original trace is not truncated
       * when printed.*/ 
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }
  
  public void enableReadProfiling(FSDataNodeReadProfilingData dnData) {
    blockReader.enableReadProfiling(dnData);
  }

  /**
   * Sends upto maxChunks chunks of data.
   * 
   * When blockInPosition is >= 0, assumes 'out' is a 
   * {@link SocketOutputStream} and tries 
   * {@link SocketOutputStream#transferToFully(FileChannel, long, int)} to
   * send data (and updates blockInPosition).
   */
  private int sendChunks(ByteBuffer pkt, int maxChunks, OutputStream out) 
                         throws IOException {
    // Sends multiple chunks in one packet with a single write().

    int len = (int) Math.min(endOffset - offset,
                            (((long) bytesPerChecksum) * ((long) maxChunks)));

    // truncate len so that any partial chunks will be sent as a final packet.
    // this is not necessary for correctness, but partial chunks are 
    // ones that may be recomputed and sent via buffer copy, so try to minimize
    // those bytes
    if (len > bytesPerChecksum && len % bytesPerChecksum != 0) {
      len -= len % bytesPerChecksum;
    }

    if (len == 0) {
      return 0;
    }

    int numChunks = (len + bytesPerChecksum - 1)/bytesPerChecksum;
    int packetLen = len + numChunks*checksumSize + 4;
    pkt.clear();

    // The packet format is documented in DFSOuputStream.Packet.getBuffer().
    // Here we need to use the exact packet format since it can be received
    // by both of DFSClient, or BlockReceiver in the case of replication, which
    // uses the same piece of codes as receiving data from DFSOutputStream.
    //

    // write packet header
    pkt.putInt(packetLen);
    if (pktIncludeVersion) {
      pkt.putInt(packetVersion);
    }
    pkt.putLong(offset);
    pkt.putLong(seqno);
    pkt.put((byte)((offset + len >= endOffset) ? 1 : 0));
               //why no ByteBuf.putBoolean()?
    pkt.putInt(len);
    
    int checksumOff = pkt.position();
    byte[] buf = pkt.array();
    
    blockReader.sendChunks(out, buf, offset, checksumOff,
        numChunks, len, crcUpdater, packetVersion);
    
    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }
    
    return len;
  }
  
  private int getPacketHeaderLen() {
    return DataNode.getPacketHeaderLen(pktIncludeVersion);
  }

  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @return total bytes reads, including crc.
   */
  public long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
    return sendBlock(out, baseStream, throttler, null);
  }

  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @param progress for signalling progress.
   * @return total bytes reads, including crc.
   */
  public long sendBlock(DataOutputStream out, OutputStream baseStream, 
       DataTransferThrottler throttler, Progressable progress) throws IOException {
    if( out == null ) {
      throw new IOException( "out stream is null" );
    }
    this.throttler = throttler;

    long initialOffset = offset;
    long totalRead = 0;
    OutputStream streamForSendChunks = out;
    
    final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0; 
    try {
      try {
        checksum.writeHeader(out);
        if ( chunkOffsetOK ) {
          out.writeLong( offset );
        }
        out.flush();
      } catch (IOException e) { //socket error
        throw ioeToSocketException(e);
      }
      
      int maxChunksPerPacket;
      int pktSize = SIZE_OF_INTEGER + getPacketHeaderLen();
      
      if (transferToAllowed && !verifyChecksum && 
          baseStream instanceof SocketOutputStream && 
          blockReader.prepareTransferTo()) {
        streamForSendChunks = baseStream;
        
        // assure a mininum buffer size.
        maxChunksPerPacket = (Math.max(BUFFER_SIZE,
                                       MIN_BUFFER_WITH_TRANSFERTO)
                              + bytesPerChecksum - 1)/bytesPerChecksum;
        
        // packet buffer has to be able to do a normal transfer in the case
        // of recomputing checksum
        pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1,
                 (BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
        pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
      }

      ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

      while (endOffset > offset) {
        long len = sendChunks(pktBuf, maxChunksPerPacket, 
                              streamForSendChunks);
        if (progress != null) {
          progress.progress();
        }
        offset += len;
        totalRead += len + ((len + bytesPerChecksum - 1)/bytesPerChecksum*
                            checksumSize);
        seqno++;
      }
      try {
        out.writeInt(0); // mark the end of block        
        out.flush();
      } catch (IOException e) { //socket error
        throw ioeToSocketException(e);
      }
      
      sentEntireByteRange = true;
    }
    catch (RuntimeException e) {
      LOG.error("unexpected exception sending block", e);
      
      throw new IOException("unexpected runtime exception", e);
    } 
    finally {
      if (clientTraceFmt != null) {
        final long endTime = System.nanoTime();
        ClientTraceLog.info(String.format(clientTraceFmt, totalRead, initialOffset, endTime - startTime));
      }
      close();
    }
    
    if (crcUpdater != null && crcUpdater.isCrcValid(offset)
        && !replicaToRead.hasBlockCrcInfo()) {
      int blockCrcOffset = crcUpdater.getBlockCrcOffset();
      int blockCrc = crcUpdater.getBlockCrc();
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("Setting block CRC " + replicaToRead + " offset "
            + blockCrcOffset + " CRC " + blockCrc);
      }
      ((DatanodeBlockInfo) replicaToRead).setBlockCrc(blockCrcOffset, blockCrc);
    }

    return totalRead;
  }
  
  boolean didSendEntireByteRange() {
    return sentEntireByteRange;
  }

  public static interface InputStreamFactory {
    public BlockDataFile.Reader getBlockDataFileReader() throws IOException;
  }
}

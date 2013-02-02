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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements java.io.Closeable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private Block block; // the block to receive
  protected boolean finalized;
  private DataInputStream in = null; // from where data are read
  private DataChecksum checksum; // from where chunks of a block can be read
  private int bytesPerChecksum;
  private int checksumSize;
  private ByteBuffer buf; // contains one full packet.
  private int bufRead; //amount of valid data in the buf
  private int maxPacketReadLen;
  private int flushKb; // fsync per x number of KB of received data 
  private long bufSizeSinceLastSync; // the number of bytes received since last fsync
  protected long offsetInBlock;
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaBeingWritten replicaBeingWritten;
  private boolean isRecovery = false;
  private String clientName;
  DatanodeInfo srcDataNode = null;
  private DataNode datanode = null;
  volatile private boolean mirrorError;
  private int namespaceId;
  private DatanodeBlockWriter blockWriter;
  

  BlockReceiver(int namespaceId, Block block, DataInputStream in, String inAddr,
                String myAddr, boolean isRecovery, String clientName, 
                DatanodeInfo srcDataNode, DataNode datanode) throws IOException {
    try{
      this.namespaceId = namespaceId;
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.isRecovery = isRecovery;
      this.clientName = clientName;
      this.offsetInBlock = 0;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;
      this.checksum = DataChecksum.newDataChecksum(in);
      this.bytesPerChecksum = checksum.getBytesPerChecksum();
      this.checksumSize = checksum.getChecksumSize();
      this.flushKb = datanode.conf.getInt("dfs.datanode.flush_kb", 1024);
      this.bufSizeSinceLastSync = 0;

      //
      // Open local disk out
      //
      blockWriter = datanode.data.writeToBlock(namespaceId, this.block,
          isRecovery, clientName == null || clientName.length() == 0,
          checksum.getChecksumType(), checksum.getBytesPerChecksum());
      replicaBeingWritten = datanode.data.getReplicaBeingWritten(namespaceId, this.block);
      this.finalized = false;
      if (blockWriter != null) {
        blockWriter.initializeStreams(bytesPerChecksum, checksumSize, block,
            inAddr, namespaceId, datanode);
        // If this block is for appends, then remove it from periodic
        // validation.
        if (datanode.blockScanner != null && isRecovery) {
          datanode.blockScanner.deleteBlock(namespaceId, block);
        }
      }
    } catch (BlockAlreadyExistsException bae) {
      throw bae;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = FSDataset.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
          cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;
        datanode.checkDiskError(ioe); // may throw an exception here
      }
      
      throw ioe;
    }
  }

  /**
   * close files.
   */
  public void close() throws IOException {
    if (blockWriter != null) {
      blockWriter.close();
    }
  }

  /**
   * Flush the data and checksum data out to the stream.
   * Please call sync to make sure to write the data in to disk
   * @throws IOException
   */
  void flush(boolean forceSync) throws IOException {
    blockWriter.flush(forceSync);
  }

  /**
   * Issue a file range sync of last lastBytesToSync bytes
   * @throws IOException
   */
  void fileRangeSync(long lastBytesToSync) throws IOException {
    blockWriter.fileRangeSync(lastBytesToSync);
  }

  
  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    LOG.info(datanode.getDatanodeInfo() + ": Exception writing block " +
             block + " namespaceId: " + namespaceId + " to mirror " + mirrorAddr + "\n" +
             StringUtils.stringifyException(ioe));
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks( byte[] dataBuf, int dataOff, int len, 
                             byte[] checksumBuf, int checksumOff ) 
                             throws IOException {
    while (len > 0) {
      int chunkLen = Math.min(len, bytesPerChecksum);
      
      checksum.update(dataBuf, dataOff, chunkLen);

      if (!checksum.compare(checksumBuf, checksumOff)) {
        if (srcDataNode != null) {
          try {
            LOG.info("report corrupt block " + block + " from datanode " +
                      srcDataNode + " to namenode");
            LocatedBlock lb = new LocatedBlock(block, 
                                            new DatanodeInfo[] {srcDataNode});
            datanode.reportBadBlocks(namespaceId, new LocatedBlock[] {lb});
          } catch (IOException e) {
            LOG.warn("Failed to report bad block " + block + 
                      " from datanode " + srcDataNode + " to namenode");
          }
        }
        throw new IOException("Unexpected checksum mismatch " + 
                              "while writing " + block + " from " + inAddr);
      }

      checksum.reset();
      dataOff += chunkLen;
      checksumOff += checksumSize;
      len -= chunkLen;
    }
  }

  /**
   * Makes sure buf.position() is zero without modifying buf.remaining().
   * It moves the data if position needs to be changed.
   */
  private void shiftBufData() {
    if (bufRead != buf.limit()) {
      throw new IllegalStateException("bufRead should be same as " +
                                      "buf.limit()");
    }
    
    //shift the remaining data on buf to the front
    if (buf.position() > 0) {
      int dataLeft = buf.remaining();
      if (dataLeft > 0) {
        byte[] b = buf.array();
        System.arraycopy(b, buf.position(), b, 0, dataLeft);
      }
      buf.position(0);
      bufRead = dataLeft;
      buf.limit(bufRead);
    }
  }
  
  /**
   * reads upto toRead byte to buf at buf.limit() and increments the limit.
   * throws an IOException if read does not succeed.
   */
  private int readToBuf(int toRead) throws IOException {
    long startTime = System.currentTimeMillis();
    if (toRead < 0) {
      toRead = (maxPacketReadLen > 0 ? maxPacketReadLen : buf.capacity())
               - buf.limit();
    }
    
    int nRead = in.read(buf.array(), buf.limit(), toRead);
    
    if (nRead < 0) {
      throw new EOFException("while trying to read " + toRead + " bytes");
    }
    bufRead = buf.limit() + nRead;
    buf.limit(bufRead);
    long readToBufDuration = System.currentTimeMillis() - startTime;
    // We keep track of the time required to read 'nRead' bytes of data.
    // We track small reads and large reads separately.
    if (nRead > KB_RIGHT_SHIFT_MIN) {
      datanode.myMetrics.largeReadsToBufRate.inc(
          (int) (nRead >> KB_RIGHT_SHIFT_BITS),
          readToBufDuration);
    } else {
      datanode.myMetrics.smallReadsToBufRate.inc(nRead, readToBufDuration);
    }
    datanode.myMetrics.readToBufBytesRead.inc(nRead);
    return nRead;
  }
  
  
  /**
   * Reads (at least) one packet and returns the packet length.
   * buf.position() points to the start of the packet and 
   * buf.limit() point to the end of the packet. There could 
   * be more data from next packet in buf.<br><br>
   * 
   * It tries to read a full packet with single read call.
   * Consecutive packets are usually of the same length.
   */
  private int readNextPacket() throws IOException {
    long startTime = System.currentTimeMillis();;
    /* This dances around buf a little bit, mainly to read 
     * full packet with single read and to accept arbitarary size  
     * for next packet at the same time.
     */
    if (buf == null) {
      /* initialize buffer to the best guess size:
       * 'chunksPerPacket' calculation here should match the same 
       * calculation in DFSClient to make the guess accurate.
       */
      int chunkSize = bytesPerChecksum + checksumSize;
      int chunksPerPacket = (datanode.writePacketSize - DataNode.PKT_HEADER_LEN - 
    SIZE_OF_INTEGER + chunkSize - 1)/chunkSize;
      buf = ByteBuffer.allocate(DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER +
                                Math.max(chunksPerPacket, 1) * chunkSize);
      buf.limit(0);
    }
    
    // See if there is data left in the buffer :
    if (bufRead > buf.limit()) {
      buf.limit(bufRead);
    }
    
    while (buf.remaining() < SIZE_OF_INTEGER) {
      if (buf.position() > 0) {
        shiftBufData();
      }
      readToBuf(-1);
    }
    
    /* We mostly have the full packet or at least enough for an int
     */
    buf.mark();
    int payloadLen = buf.getInt();
    buf.reset();
    
    if (payloadLen == 0) {
      //end of stream!
      buf.limit(buf.position() + SIZE_OF_INTEGER);
      return 0;
    }
    
    // check corrupt values for pktLen, 100MB upper limit should be ok?
    if (payloadLen < 0 || payloadLen > (100*1024*1024)) {
      throw new IOException("Incorrect value for packet payload : " +
                            payloadLen);
    }
    
    int pktSize = payloadLen + DataNode.PKT_HEADER_LEN;
    
    if (buf.remaining() < pktSize) {
      //we need to read more data
      int toRead = pktSize - buf.remaining();
      
      // first make sure buf has enough space.        
      int spaceLeft = buf.capacity() - buf.limit();
      if (toRead > spaceLeft && buf.position() > 0) {
        shiftBufData();
        spaceLeft = buf.capacity() - buf.limit();
      }
      if (toRead > spaceLeft) {
        byte oldBuf[] = buf.array();
        int toCopy = buf.limit();
        buf = ByteBuffer.allocate(toCopy + toRead);
        System.arraycopy(oldBuf, 0, buf.array(), 0, toCopy);
        buf.limit(toCopy);
      }
      
      //now read:
      while (toRead > 0) {
        toRead -= readToBuf(toRead);
      }
    }
    
    if (buf.remaining() > pktSize) {
      buf.limit(buf.position() + pktSize);
    }
    
    if (pktSize > maxPacketReadLen) {
      maxPacketReadLen = pktSize;
    }
    
    long readPacketDuration = System.currentTimeMillis() - startTime;
    datanode.myMetrics.readPacketLatency.inc(readPacketDuration);
    return payloadLen;
  }
  
  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns size of the packet.
   */
  private int receivePacket() throws IOException {
    
    long startTime = System.currentTimeMillis();;
    int payloadLen = readNextPacket();
    
    if (payloadLen <= 0) {
      return payloadLen;
    }
    
    buf.mark();
    //read the header
    buf.getInt(); // packet length
    offsetInBlock = buf.getLong(); // get offset of packet in block
    long seqno = buf.getLong();    // get seqno
    
    byte booleanFieldValue = buf.get();
    boolean lastPacketInBlock = ((booleanFieldValue &
    DataNode.isLastPacketInBlockMask) == 0 ) ?
    false : true;

    boolean forceSync = ((booleanFieldValue & DataNode.forceSyncMask) == 0 ) ?
    false : true;

    int endOfHeader = buf.position();
    buf.reset();
    
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                " of length " + payloadLen +
                " seqno " + seqno +
                " offsetInBlock " + offsetInBlock +
                " lastPacketInBlock " + lastPacketInBlock);
    }
    
    // First write the packet to the mirror:
    if (mirrorOut != null && !mirrorError) {
      try {
        long mirrorWriteStartTime = System.currentTimeMillis();
        mirrorOut.write(buf.array(), buf.position(), buf.remaining());
        mirrorOut.flush();
        long mirrorWritePacketDuration = System.currentTimeMillis() - mirrorWriteStartTime;
        datanode.myMetrics.mirrorWritePacketLatency.inc(mirrorWritePacketDuration);
      } catch (IOException e) {
        handleMirrorOutError(e);
      }
    }

    buf.position(endOfHeader);        
    int len = buf.getInt();
    
    if (len < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            offsetInBlock + ": " + len); 
    }

    Packet ackPacket = null;
    if (responder != null) {
      ackPacket = ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock + len);
    }

    if (len == 0) {
      LOG.debug("Receiving empty packet for block " + block);
    } else {
      setBlockPosition(offsetInBlock);  // adjust file position
      long expectedBlockCrcOffset = offsetInBlock;
      offsetInBlock += len;
      this.replicaBeingWritten.setBytesReceived(offsetInBlock);

      int numChunks = ((len + bytesPerChecksum - 1)/bytesPerChecksum);
      
      int checksumLen = numChunks * checksumSize;

      if ( buf.remaining() != (checksumLen + len)) {
        throw new IOException("Data remaining in packet does not match " +
                              "sum of checksumLen and dataLen");
      }
      int checksumOff = buf.position();
      int dataOff = checksumOff + checksumLen;
      byte pktBuf[] = buf.array();
      
      buf.position(buf.limit()); // move to the end of the data.

      /* skip verifying checksum iff this is not the last one in the 
       * pipeline and clientName is non-null. i.e. Checksum is verified
       * on all the datanodes when the data is being written by a 
       * datanode rather than a client. Whe client is writing the data, 
       * protocol includes acks and only the last datanode needs to verify 
       * checksum.
       */
      if (mirrorOut == null || clientName.length() == 0) {
        verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff);
      }
      
      // update Block CRC.
      // It introduces extra costs in critical code path. But compared to the
      // total checksum calculating costs (the previous operation),
      // this costs are negligible. If we want to optimize, we can optimize it
      // together with the previous operation to hide the latency from disk
      // I/O latency.
      int crcCalculateOffset = checksumOff;
      for (int i = 0; i < numChunks; i++) {
        int bytesInChunk;
        if (i != numChunks -1) {
          bytesInChunk = bytesPerChecksum;
        } else {
          bytesInChunk = len % bytesPerChecksum;
          if (bytesInChunk == 0) {
            bytesInChunk = bytesPerChecksum;
          }
        }
        replicaBeingWritten.updateBlockCrc(expectedBlockCrcOffset,
            lastPacketInBlock && i == numChunks - 1, bytesInChunk,
            DataChecksum.getIntFromBytes(pktBuf, crcCalculateOffset));
        crcCalculateOffset += checksum.getChecksumSize();
        expectedBlockCrcOffset += bytesInChunk;
      }

      try {
        if (!finalized) {
          long writeStartTime = System.currentTimeMillis();
          
          blockWriter.writePacket(pktBuf, len, dataOff,
              checksumOff, numChunks);
          
          datanode.myMetrics.bytesWritten.inc(len);

          /// flush entire packet before sending ack
          this.bufSizeSinceLastSync += len;  
          flush(forceSync); 
          if (forceSync) {
            this.bufSizeSinceLastSync = 0;  
          } else if (this.flushKb > 0 && this.bufSizeSinceLastSync >= this.flushKb * 1024) {
            long syncStartOffset = offsetInBlock - this.bufSizeSinceLastSync;
            if (syncStartOffset < 0) {
              syncStartOffset = 0;
            }
            long bytesToSync = offsetInBlock - syncStartOffset;
            fileRangeSync(bytesToSync);
            this.bufSizeSinceLastSync = 0;
          }
          this.replicaBeingWritten.setBytesOnDisk(offsetInBlock);

          // Record time taken to write packet
          long writePacketDuration = System.currentTimeMillis() - writeStartTime;
          datanode.myMetrics.writePacketLatency.inc(writePacketDuration);
        }
      } catch (ClosedByInterruptException cix) {
        LOG.warn(
            "Thread interrupted when flushing bytes to disk. Might cause inconsistent sates",
            cix);
        throw cix;
      } catch (InterruptedIOException iix) {
        LOG.warn(
            "InterruptedIOException when flushing bytes to disk. Might cause inconsistent sates",
            iix);
        throw iix;
      } catch (IOException iex) {
        datanode.checkDiskError(iex);
        throw iex;
      }
    }
    if (ackPacket != null) {
      ackPacket.setPersistent();
    }
    
    if (throttler != null) { // throttle I/O
      throttler.throttle(payloadLen);
    }

    long receiveAndWritePacketDuration = System.currentTimeMillis() - startTime;
    datanode.myMetrics.receiveAndWritePacketLatency.inc(receiveAndWritePacketDuration);
    return payloadLen;
  }

  void writeChecksumHeader(DataOutputStream mirrorOut) throws IOException {
    checksum.writeHeader(mirrorOut);
  }


  long receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      int numTargets) throws IOException {

      long totalReceiveSize = 0;
      int tempReceiveSize = 0;
      mirrorOut = mirrOut;
      mirrorAddr = mirrAddr;
      throttler = throttlerArg;

      long startTime = System.currentTimeMillis();

    try {
      // write data chunk header
      if (!finalized) {
        blockWriter.writeHeader(checksum);
      }
      if (clientName.length() > 0) {
        responder = new Daemon(datanode.threadGroup, 
                               new PacketResponder(this, block, mirrIn, 
                                                   replyOut, numTargets,
                                                   Thread.currentThread()));
        responder.start(); // start thread to processes reponses
      }

      /* 
       * Receive until packet length is zero.
       */
      while ((tempReceiveSize = receivePacket()) > 0) {
        totalReceiveSize += tempReceiveSize;
      }
      long receiveBlockDuration = System.currentTimeMillis() - startTime;
      // Entire block received, record latency.
      datanode.myMetrics.receiveBlockLatency.inc(receiveBlockDuration);

      // flush the mirror out
      if (mirrorOut != null) {
        try {
          mirrorOut.writeInt(0); // mark the end of the block
          mirrorOut.flush();
        } catch (IOException e) {
          handleMirrorOutError(e);
        }
      }

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
      }

      // if this write is for a replication request (and not
      // from a client), then finalize block. For client-writes, 
      // the block is finalized in the PacketResponder.
      if (clientName.length() == 0) {
        // close the block/crc files
        close();

        // Finalize the block. Does this fsync()?
        block.setNumBytes(offsetInBlock);
        datanode.data.finalizeBlock(namespaceId, block);
        datanode.myMetrics.blocksWritten.inc();
      }

    } catch (IOException ioe) {
      LOG.info("Exception in receiveBlock for block " + block + 
               " " + ioe);
      IOUtils.closeStream(this);
      if (responder != null) {
        responder.interrupt();
      }
      cleanupBlock();
      throw ioe;
    } finally {
      if (responder != null) {
        try {
          responder.join();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted receiveBlock");
        }
        responder = null;
      }
    }

    return totalReceiveSize;
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (clientName.length() == 0) { // not client write
      datanode.data.unfinalizeBlock(namespaceId, block);
    }
  }

  /**
   * Sets the file pointer in the local block file to the specified value.
   */
  private void setBlockPosition(long offsetInBlock) throws IOException {
    if (finalized) {
      if (!isRecovery) {
        throw new IOException("Write to offset " + offsetInBlock +
                              " of block " + block +
                              " that is already finalized.");
      }
      if (offsetInBlock > datanode.data.getFinalizedBlockLength(namespaceId, block)) {
        throw new IOException("Write to offset " + offsetInBlock +
                              " of block " + block +
                              " that is already finalized and is of size " +
                              datanode.data.getFinalizedBlockLength(namespaceId, block));
      }
      return;
    }
    blockWriter.setPosAndRecomputeChecksumIfNeeded(offsetInBlock, checksum);
  }


  
  
  /**
   * Processed responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, FSConstants {   

    //packet waiting for ack
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    private volatile boolean running = true;
    private Block block;
    DataInputStream mirrorIn;   // input from downstream datanode
    DataOutputStream replyOut;  // output to upstream datanode
    private int numTargets;     // number of downstream datanodes including myself
    private BlockReceiver receiver; // The owner of this responder.
    private Thread receiverThread; // the thread that spawns this responder

    public String toString() {
      return "PacketResponder " + numTargets + " for Block " + this.block;
    }

    PacketResponder(BlockReceiver receiver, Block b, DataInputStream in, 
                    DataOutputStream out, int numTargets,
                    Thread receiverThread) {
      this.receiver = receiver;
      this.block = b;
      mirrorIn = in;
      replyOut = out;
      this.numTargets = numTargets;
      this.receiverThread = receiverThread;
    }

    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno
     * @param lastPacketInBlock
     */
    synchronized Packet enqueue(long seqno, boolean lastPacketInBlock, long offsetInBlock) {
      if (running) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("PacketResponder " + numTargets + " adding seqno " + seqno
              + " with new offset in Block " + offsetInBlock + " to ack queue.");
        }
        Packet newPacket = new Packet(seqno, lastPacketInBlock, offsetInBlock);
        ackQueue.addLast(newPacket);
        notifyAll();
        return newPacket;
      }
      return null;
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    synchronized void close() {
      while (running && ackQueue.size() != 0 && datanode.shouldRun) {
        try {
          wait();
        } catch (InterruptedException e) {
          running = false;
        }
      }
      LOG.debug("PacketResponder " + numTargets +
               " for block " + block + " Closing down.");
      running = false;
      notifyAll();
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    public void run() {
      boolean lastPacketInBlock = false;
      boolean isInterrupted = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (running && datanode.shouldRun && !lastPacketInBlock) {
        Packet pkt = null;
          try {
            long expected = PipelineAck.UNKOWN_SEQNO;
            PipelineAck ack = new PipelineAck();
            long seqno = PipelineAck.UNKOWN_SEQNO;
            boolean localMirrorError = mirrorError;
            try { 
              synchronized (this) {
                // wait for a packet to arrive
                while (running && datanode.shouldRun && ackQueue.size() == 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("PacketResponder " + numTargets + 
                              " seqno = " + seqno +
                              " for block " + block +
                              " waiting for local datanode to finish write.");
                  }
                  wait();
                }
                if (!running || !datanode.shouldRun) {
                  break;
                }
                pkt = ackQueue.removeFirst();
                expected = pkt.seqno;
                notifyAll();
              }
              // receive an ack if DN is not the last one in the pipeline
              if (numTargets > 0 && !localMirrorError) {
                // read an ack from downstream datanode
                ack.readFields(mirrorIn, numTargets);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("PacketResponder " + numTargets + 
                      " for block " + block + " got " + ack);
                }
                seqno = ack.getSeqno();
                // verify seqno
                if (seqno != expected) {
                  throw new IOException("PacketResponder " + numTargets +
                      " for block " + block +
                      " expected seqno:" + expected +
                      " received:" + seqno);
                }
              }

              assert pkt != null;
              try {
                pkt.waitForPersistent();
              } catch (InterruptedException ine) {
                isInterrupted = true;
                LOG.info("PacketResponder " + block +  " " + numTargets +
                    " : Thread is interrupted when waiting for data persistent.");
                break;
              }
              
              lastPacketInBlock = pkt.lastPacketInBlock;
              if (pkt.seqno >= 0) {
                replicaBeingWritten.setBytesAcked(pkt.offsetInBlock);
              }
            } catch (InterruptedException ine) {
              isInterrupted = true;
            } catch (IOException ioe) {
              if (Thread.interrupted()) {
                isInterrupted = true;
              } else {
                // continue to run even if can not read from mirror
                // notify client of the error
                // and wait for the client to shut down the pipeline
                mirrorError = true;
                LOG.info("PacketResponder " + block + " " + numTargets +
                    " Exception " + StringUtils.stringifyException(ioe));
              }
            }

            if (Thread.interrupted() || isInterrupted) {
              /* The receiver thread cancelled this thread. 
               * We could also check any other status updates from the 
               * receiver thread (e.g. if it is ok to write to replyOut). 
               * It is prudent to not send any more status back to the client
               * because this datanode has a problem. The upstream datanode
               * will detect that this datanode is bad, and rightly so.
               */
              LOG.info("PacketResponder " + block +  " " + numTargets +
                       " : Thread is interrupted.");
              break;
            }
            
            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (lastPacketInBlock && !receiver.finalized) {
              receiver.close();
              final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
              block.setNumBytes(receiver.offsetInBlock);
              datanode.data.finalizeBlock(namespaceId, block);
              datanode.myMetrics.blocksWritten.inc();
              datanode.notifyNamenodeReceivedBlock(namespaceId, block, null);
              if (ClientTraceLog.isInfoEnabled() &&
                  receiver.clientName.length() > 0) {
                long offset = 0;
                ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT,
                      receiver.inAddr, receiver.myAddr, block.getNumBytes(),
                      "HDFS_WRITE", receiver.clientName, offset, 
                      datanode.getDNRegistrationForNS(namespaceId).getStorageID(), block, endTime-startTime));
              } else {
                LOG.info("Received block " + block + 
                         " of size " + block.getNumBytes() + 
                         " from " + receiver.inAddr);
              }
            }

            // construct my ack message
            short[] replies = null;
            if (mirrorError) { // no ack is read
              replies = new short[2];
              replies[0] = DataTransferProtocol.OP_STATUS_SUCCESS;
              replies[1] = DataTransferProtocol.OP_STATUS_ERROR;
            } else {
              short ackLen = numTargets == 0 ? 0 : ack.getNumOfReplies();
              replies = new short[1+ackLen];
              replies[0] = DataTransferProtocol.OP_STATUS_SUCCESS;
              for (int i=0; i<ackLen; i++) {
                replies[i+1] = ack.getReply(i);
              }
            }
            PipelineAck replyAck = new PipelineAck(expected, replies);
 
            // send my ack back to upstream datanode
            replyAck.write(replyOut);
            replyOut.flush();
            if (LOG.isDebugEnabled()) {
              LOG.debug("PacketResponder " + numTargets +
                        " for block " + block +
                        " responded an ack: " + replyAck);
            }
        } catch (Throwable e) {
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
          if (!Thread.interrupted()) { // error not caused by interruption
            receiverThread.interrupt();
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }
  }
  
  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  static private class Packet {
    long seqno;
    boolean lastPacketInBlock;
    long offsetInBlock;
    boolean persistent;
    static final long TIMEOUT_WAIT_PERSISTENT_MSEC = 30 * 1000; 

    Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.offsetInBlock = offsetInBlock;
      this.persistent = false;
    }
    
    public synchronized void waitForPersistent() throws InterruptedException {
      long msec_waited = 0;
      while (!persistent) {
        wait(TIMEOUT_WAIT_PERSISTENT_MSEC);
        if (!persistent) {
          msec_waited += TIMEOUT_WAIT_PERSISTENT_MSEC;
          LOG.warn("Waiting seqno " + seqno + " for " + msec_waited + " msec.");           
        }
      }
      assert persistent;
    }

    public synchronized void setPersistent() {
      this.persistent = true;
      notifyAll();
    }
  }
}

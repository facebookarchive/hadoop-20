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
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import org.apache.hadoop.util.Daemon;

/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  Socket s;
  String remoteAddress; // address of remote side
  String localAddress;  // local address of this daemon
  DataNode datanode;
  DataXceiverServer dataXceiverServer;
  
  public DataXceiver(Socket s, DataNode datanode, 
      DataXceiverServer dataXceiverServer) {
    
    this.s = s;
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    LOG.debug("Number of active connections is: " + datanode.getXceiverCount());
  }

  /**
   * Update the thread name to contain the current status.
   * Use this only after this receiver has started on its thread, i.e.,
   * outside the constructor.
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DataXceiver for client ").append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /**
   * Read/write data from/to the DataXceiveServer.
   */
  public void run() {
    this.dataXceiverServer.childSockets.put(s, s);
    this.remoteAddress = s.getRemoteSocketAddress().toString();
    this.localAddress = s.getLocalSocketAddress().toString();
    boolean cleanup = true;

    DataInputStream in=null; 
    byte op = -1;
    try {
      in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(s), 
                                  SMALL_BUFFER_SIZE));
      short version = in.readShort();
      if ( version != DataTransferProtocol.DATA_TRANSFER_VERSION ) {
        throw new IOException( "Version Mismatch. Expected" +
            DataTransferProtocol.DATA_TRANSFER_VERSION +
            ", Received " + version);
      }
      boolean local = s.getInetAddress().equals(s.getLocalAddress());
      updateCurrentThreadName("waiting for operation");
      op = in.readByte();
      // Make sure the xciver count is not exceeded
      int curXceiverCount = datanode.getXceiverCount();
      if (curXceiverCount > dataXceiverServer.maxXceiverCount) {
        throw new IOException("xceiverCount " + curXceiverCount
                              + " exceeds the limit of concurrent xcievers "
                              + dataXceiverServer.maxXceiverCount);
      }
      long startTime = DataNode.now();
      switch ( op ) {
      case DataTransferProtocol.OP_READ_BLOCK:
        readBlock( in );
        datanode.myMetrics.readBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.readsFromLocalClient.inc();
        else
          datanode.myMetrics.readsFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_READ_BLOCK_ACCELERATOR:
        readBlockAccelerator( in );
        datanode.myMetrics.readBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.readsFromLocalClient.inc();
        else
          datanode.myMetrics.readsFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_WRITE_BLOCK:
        cleanup = false;  // the forked thread will do necessary cleanup
        // Fork a new thread for every write. This is required because
        // pipeline recovery insists on waiting for writer threads to
        // exit before recovering a block.
        new Daemon(datanode.threadGroup, 
                   new DataWriter(s, datanode, in, dataXceiverServer,
                                  remoteAddress, localAddress)).start();
        datanode.myMetrics.writeBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.writesFromLocalClient.inc();
        else
          datanode.myMetrics.writesFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_READ_METADATA:
        readMetadata( in );
        datanode.myMetrics.readMetadataOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
        replaceBlock(in);
        datanode.myMetrics.replaceBlockOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_COPY_BLOCK:
            // for balancing purpose; send to a proxy source
        copyBlock(in);
        datanode.myMetrics.copyBlockOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_BLOCK_CHECKSUM: //get the checksum of a block
        getBlockChecksum(in);
        datanode.myMetrics.blockChecksumOp.inc(DataNode.now() - startTime);
        break;
      default:
        throw new IOException("Unknown opcode " + op + " in data stream");
      }
    } catch (Throwable t) {
      if (op == DataTransferProtocol.OP_READ_BLOCK && t instanceof SocketTimeoutException) {
        // Ignore SocketTimeoutException for reading.
        // This usually means that the client who's reading data from the DataNode has exited.
        LOG.info(datanode.getDatanodeInfo() + ":DataXceiver" + " (IGNORED) "
            + StringUtils.stringifyException(t));
      } else {
        LOG.error(datanode.getDatanodeInfo() + ":DataXceiver",t);
      }
    } finally {
      LOG.debug(datanode.getDatanodeInfo() + ":Number of active connections is: "
                               + datanode.getXceiverCount());
      updateCurrentThreadName("Idle state");
      if (cleanup) {
        IOUtils.closeStream(in);
        IOUtils.closeSocket(s);
        dataXceiverServer.childSockets.remove(s);
      }
    }
  }

  /**
   * Read a block from the disk.
   * @param in The stream to read from
   * @throws IOException
   */
  private void readBlock(DataInputStream in) throws IOException {
    //
    // Read in the header
    //
    long startTime = System.currentTimeMillis();
    int namespaceId = in.readInt();
    long blockId = in.readLong();          
    Block block = new Block( blockId, 0 , in.readLong());

    long startOffset = in.readLong();
    long length = in.readLong();
    String clientName = Text.readString(in);
    // send the block
    OutputStream baseStream = NetUtils.getOutputStream(s, 
        datanode.socketWriteTimeout);
    DataOutputStream out = new DataOutputStream(
                 new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
    
    BlockSender blockSender = null;
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName, "%d", 
            datanode.getDNRegistrationForNS(namespaceId).getStorageID(), block, "%d")
        : datanode.getDNRegistrationForNS(namespaceId) + " Served block " + block + " to " +
            s.getInetAddress();
    updateCurrentThreadName("sending block " + block);
    try {
      try {
        blockSender = new BlockSender(namespaceId, block, startOffset, length,
            true, true, false, datanode, clientTraceFmt);
      } catch(IOException e) {
        out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
        throw e;
      }

      out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS); // send op status
      long read = blockSender.sendBlock(out, baseStream, null); // send data

      if (blockSender.isBlockReadFully()) {
        // See if client verification succeeded. 
        // This is an optional response from client.
        try {
          if (in.readShort() == DataTransferProtocol.OP_STATUS_CHECKSUM_OK  && 
              datanode.blockScanner != null) {
            datanode.blockScanner.verifiedByClient(namespaceId, block);
          }
        } catch (IOException ignored) {}
      }
      
      long readDuration = System.currentTimeMillis() - startTime;
      datanode.myMetrics.bytesReadLatency.inc(readDuration);
      datanode.myMetrics.bytesRead.inc((int) read);
      if (read > KB_RIGHT_SHIFT_MIN) {
        datanode.myMetrics.bytesReadRate.inc((int) (read >> KB_RIGHT_SHIFT_BITS),
                              readDuration);
      }
      datanode.myMetrics.blocksRead.inc();
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.blocksRead.inc();
    } catch ( IOException ioe ) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(datanode.getDatanodeInfo() +  ":Got exception while serving " + 
          "namespaceId: " + namespaceId + " block: " + block + " to " +
                s.getInetAddress() + ":\n" + 
                StringUtils.stringifyException(ioe) );
      throw ioe;
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(blockSender);
    }
  }

  /**
   * Reads the metadata and sends the data in one 'DATA_CHUNK'.
   * @param in
   */
  void readMetadata(DataInputStream in) throws IOException {
    final int namespaceId = in.readInt();
    Block block = new Block( in.readLong(), 0 , in.readLong());
    MetaDataInputStream checksumIn = null;
    DataOutputStream out = null;
    updateCurrentThreadName("reading metadata for block " + block);
    try {
      checksumIn = datanode.data.getMetaDataInputStream(namespaceId, block);
      
      long fileSize = checksumIn.getLength();

      if (fileSize >= 1L<<31 || fileSize <= 0) {
          throw new IOException("Unexpected size for checksumFile of block" +
                  block);
      }

      byte [] buf = new byte[(int)fileSize];
      IOUtils.readFully(checksumIn, buf, 0, buf.length);
      
      out = new DataOutputStream(
                NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
      
      out.writeByte(DataTransferProtocol.OP_STATUS_SUCCESS);
      out.writeInt(buf.length);
      out.write(buf);
      
      //last DATA_CHUNK
      out.writeInt(0);
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
    }
  }
  
  /**
   * Get block checksum (MD5 of CRC32).
   * @param in
   */
  void getBlockChecksum(DataInputStream in) throws IOException {
    final int namespaceId = in.readInt();
    final Block block = new Block(in.readLong(), 0 , in.readLong());

    DataOutputStream out = null;
    final MetaDataInputStream metadataIn = datanode.data.getMetaDataInputStream(namespaceId, block);
    final DataInputStream checksumIn = new DataInputStream(new BufferedInputStream(
        metadataIn, BUFFER_SIZE));

    updateCurrentThreadName("getting checksum for block " + block);
    try {
      //read metadata file
      final BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      final DataChecksum checksum = header.getChecksum(); 
      final int bytesPerCRC = checksum.getBytesPerChecksum();
      final long crcPerBlock = (metadataIn.getLength()
          - BlockMetadataHeader.getHeaderSize())/checksum.getChecksumSize();
      
      //compute block checksum
      final MD5Hash md5 = MD5Hash.digest(checksumIn);

      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
            + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
      }

      //write reply
      out = new DataOutputStream(
          NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
      out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
      out.writeInt(bytesPerCRC);
      out.writeLong(crcPerBlock);
      md5.write(out);
      out.flush();
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(metadataIn);
    }
  }

  /**
   * Read a block from the disk and then sends it to a destination.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void copyBlock(DataInputStream in) throws IOException {
    // Read in the header
    long startTime = System.currentTimeMillis();
    int namespaceId = in.readInt();
    long blockId = in.readLong(); // read block id
    Block block = new Block(blockId, 0, in.readLong());

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.info("Not able to copy block " + blockId + " to " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      return;
    }

    BlockSender blockSender = null;
    DataOutputStream reply = null;
    boolean isOpSuccess = true;
    updateCurrentThreadName("Copying block " + block);
    try {
      // check if the block exists or not
      blockSender = new BlockSender(namespaceId, block, 0, -1,
          false, false, false, datanode);

      // set up response stream
      OutputStream baseStream = NetUtils.getOutputStream(
          s, datanode.socketWriteTimeout);
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, SMALL_BUFFER_SIZE));

      // send block content to the target
      long read = blockSender.sendBlock(reply, baseStream, 
                                        dataXceiverServer.balanceThrottler);

      long readDuration = System.currentTimeMillis() - startTime;
      datanode.myMetrics.bytesReadLatency.inc(readDuration);
      datanode.myMetrics.bytesRead.inc((int) read);
      if (read > KB_RIGHT_SHIFT_MIN) {
        datanode.myMetrics.bytesReadRate.inc((int) (read >> KB_RIGHT_SHIFT_BITS),
                          readDuration);
      }
      datanode.myMetrics.blocksRead.inc();
      
      LOG.info("Copied block " + block + " to " + s.getRemoteSocketAddress());
    } catch (IOException ioe) {
      isOpSuccess = false;
      throw ioe;
    } finally {
      dataXceiverServer.balanceThrottler.release();
      if (isOpSuccess) {
        try {
          // send one last byte to indicate that the resource is cleaned.
          reply.writeChar('d');
        } catch (IOException ignored) {
        }
      }
      IOUtils.closeStream(reply);
      IOUtils.closeStream(blockSender);
    }
  }

  /**
   * Receive a block and write it to disk, it then notifies the namenode to
   * remove the copy from the source.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void replaceBlock(DataInputStream in) throws IOException {
    long startTime = System.currentTimeMillis();
    /* read header */
    int namespaceId = in.readInt();
    long blockId = in.readLong();
    Block block = new Block(blockId, dataXceiverServer.estimateBlockSize,
        in.readLong()); // block id & generation stamp
    String sourceID = Text.readString(in); // read del hint
    DatanodeInfo proxySource = new DatanodeInfo(); // read proxy source
    proxySource.readFields(in);

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.warn("Not able to receive block " + blockId + " from " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      sendResponse(s, (short)DataTransferProtocol.OP_STATUS_ERROR, 
          datanode.socketWriteTimeout);
      return;
    }

    Socket proxySock = null;
    DataOutputStream proxyOut = null;
    short opStatus = DataTransferProtocol.OP_STATUS_SUCCESS;
    BlockReceiver blockReceiver = null;
    DataInputStream proxyReply = null;
    long totalReceiveSize = 0;
    long writeDuration;
    
    updateCurrentThreadName("replacing block " + block + " from " + sourceID);
    try {
      // get the output stream to the proxy
      InetSocketAddress proxyAddr = NetUtils.createSocketAddr(
          proxySource.getName());
      proxySock = datanode.newSocket();
      NetUtils.connect(proxySock, proxyAddr, datanode.socketTimeout);
      proxySock.setSoTimeout(datanode.socketTimeout);

      OutputStream baseStream = NetUtils.getOutputStream(proxySock, 
          datanode.socketWriteTimeout);
      proxyOut = new DataOutputStream(
                     new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

      /* send request to the proxy */
      proxyOut.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION); // transfer version
      proxyOut.writeByte(DataTransferProtocol.OP_COPY_BLOCK); // op code
      proxyOut.writeInt(namespaceId);
      proxyOut.writeLong(block.getBlockId()); // block id
      proxyOut.writeLong(block.getGenerationStamp()); // block id
      proxyOut.flush();

      // receive the response from the proxy
      proxyReply = new DataInputStream(new BufferedInputStream(
          NetUtils.getInputStream(proxySock), BUFFER_SIZE));

      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(namespaceId,
          block, proxyReply, proxySock.getRemoteSocketAddress().toString(),
          proxySock.getLocalSocketAddress().toString(),
          false, "", null, datanode);

      // receive a block
      totalReceiveSize = blockReceiver.receiveBlock(null, null, null, null,
          dataXceiverServer.balanceThrottler, -1);

      // notify name node
      datanode.notifyNamenodeReceivedBlock(namespaceId, block, sourceID);

      writeDuration = System.currentTimeMillis() - startTime;
      datanode.myMetrics.bytesWrittenLatency.inc(writeDuration);
      if (totalReceiveSize > KB_RIGHT_SHIFT_MIN) {
        datanode.myMetrics.bytesWrittenRate.inc((int) (totalReceiveSize >> KB_RIGHT_SHIFT_BITS),
                                  writeDuration);
      }

      LOG.info("Moved block " + block + 
          " from " + s.getRemoteSocketAddress());
      
    } catch (IOException ioe) {
      opStatus = DataTransferProtocol.OP_STATUS_ERROR;
      throw ioe;
    } finally {
      // receive the last byte that indicates the proxy released its thread resource
      if (opStatus == DataTransferProtocol.OP_STATUS_SUCCESS) {
        try {
          proxyReply.readChar();
        } catch (IOException ignored) {
        }
      }
      
      // now release the thread resource
      dataXceiverServer.balanceThrottler.release();
      
      // send response back
      try {
        sendResponse(s, opStatus, datanode.socketWriteTimeout);
      } catch (IOException ioe) {
        LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
      }
      IOUtils.closeStream(proxyOut);
      IOUtils.closeStream(blockReceiver);
      IOUtils.closeStream(proxyReply);
    }
  }
  
  /**
   * Utility function for sending a response.
   * @param s socket to write to
   * @param opStatus status message to write
   * @param timeout send timeout
   **/
  private void sendResponse(Socket s, short opStatus, long timeout) 
                                                       throws IOException {
    DataOutputStream reply = 
      new DataOutputStream(NetUtils.getOutputStream(s, timeout));
    try {
      reply.writeShort(opStatus);
      reply.flush();
    } finally {
      IOUtils.closeStream(reply);
    }
  }

  /**
   * Read a block from the disk, the emphasis in on speed baby, speed!
   * The focus is to decrease the number of system calls issued to satisfy
   * this read request.
   * @param in The stream to read from
   *
   * Input  4 bytes: namespace id
   *        8 bytes: block id
   *        8 bytes: genstamp
   *        8 bytes: startOffset
   *        8 bytes: length of data to read
   *        n bytes: clientName as a string
   * Output 1 bytes: checksum type
   *         4 bytes: bytes per checksum
   *        -stream of checksum values for all data
   *        -stream of data starting from the previous alignment of startOffset
   *         with bytesPerChecksum
   * @throws IOException
   */
  private void readBlockAccelerator(DataInputStream in) throws IOException {
    //
    // Read in the header
    //
    int namespaceId = in.readInt();
    long blockId = in.readLong();          
    long generationStamp = in.readLong();          
    long startOffset = in.readLong();
    long length = in.readLong();
    String clientName = Text.readString(in);
    if (LOG.isDebugEnabled()) {
      LOG.debug("readBlockAccelerator blkid = " + blockId + 
                " offset " + startOffset + " length " + length);
    }

    long startTime = System.currentTimeMillis();
    Block block = new Block( blockId, 0 , generationStamp);
    long blockLength = datanode.data.getVisibleLength(namespaceId, block);
    File dataFile = datanode.data.getBlockFile(namespaceId, block);
    File checksumFile = FSDataset.getMetaFile(dataFile, block);
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

      long readDuration = System.currentTimeMillis() - startTime;
      datanode.myMetrics.bytesReadLatency.inc(readDuration);
      datanode.myMetrics.bytesRead.inc((int) read);
      if (read > KB_RIGHT_SHIFT_MIN) {
        datanode.myMetrics.bytesReadRate.inc((int) (read >> KB_RIGHT_SHIFT_BITS),
                              readDuration);
      }
      datanode.myMetrics.blocksRead.inc();
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.blocksRead.inc();
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

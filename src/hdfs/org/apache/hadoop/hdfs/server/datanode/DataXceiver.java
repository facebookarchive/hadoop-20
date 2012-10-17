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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockChecksumHeader;
import org.apache.hadoop.hdfs.protocol.CopyBlockHeader;
import org.apache.hadoop.hdfs.protocol.ReadMetadataHeader;
import org.apache.hadoop.hdfs.protocol.ReplaceBlockHeader;
import org.apache.hadoop.hdfs.protocol.VersionAndOpcode;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.ReadBlockHeader;
import org.apache.hadoop.hdfs.protocol.WriteBlockHeader;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  Socket s;
  String remoteAddress = null; // address of remote side
  String localAddress = null;  // local address of this daemon
  DataNode datanode;
  DataXceiverServer dataXceiverServer;
  
  public DataXceiver(Socket s, DataNode datanode, 
      DataXceiverServer dataXceiverServer) {
    this.s = s;
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;

    if (ClientTraceLog.isInfoEnabled()) {
      getAddresses();
      ClientTraceLog.info("Accepted DataXceiver connection: src "
          + remoteAddress + " dest " + localAddress + " XceiverCount: "
          + datanode.getXceiverCount());
    }
    
    dataXceiverServer.childSockets.put(s, s);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of active connections is: " + datanode.getXceiverCount());
    }
  }
  
  private void getAddresses() {
    remoteAddress = s.getRemoteSocketAddress().toString();
    localAddress = s.getLocalSocketAddress().toString();    
  }

  /**
   * Update the thread name to contain the current status.
   * Use this only after this receiver has started on its thread, i.e.,
   * outside the constructor.
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DataXceiver for client ");
    InetAddress ia;
    if (s != null && (ia = s.getInetAddress()) != null) {
      sb.append(ia.toString());
    } else {
      sb.append("unknown");
    }
      
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /**
   * Read/write data from/to the DataXceiveServer.
   */
  public void run() {
    DataInputStream in=null; 
    byte op = -1;
    try {
      s.setTcpNoDelay(true);
      s.setSoTimeout(datanode.socketTimeout*5);

      in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(s), 
                                  SMALL_BUFFER_SIZE));
      VersionAndOpcode versionAndOpcode = new VersionAndOpcode();
      versionAndOpcode.readFields(in);
      op = versionAndOpcode.getOpCode();
      
      boolean local = s.getInetAddress().equals(s.getLocalAddress());
      updateCurrentThreadName("waiting for operation");
      
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
        readBlock( in, versionAndOpcode );
        datanode.myMetrics.readBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.readsFromLocalClient.inc();
        else
          datanode.myMetrics.readsFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_READ_BLOCK_ACCELERATOR:
        readBlockAccelerator(in, versionAndOpcode);
        datanode.myMetrics.readBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.readsFromLocalClient.inc();
        else
          datanode.myMetrics.readsFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_WRITE_BLOCK:
        writeBlock( in, versionAndOpcode );
        datanode.myMetrics.writeBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.writesFromLocalClient.inc();
        else
          datanode.myMetrics.writesFromRemoteClient.inc();
        break;
      case DataTransferProtocol.OP_READ_METADATA:
        readMetadata(in, versionAndOpcode);
        datanode.myMetrics.readMetadataOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
        replaceBlock(in, versionAndOpcode);
        datanode.myMetrics.replaceBlockOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_COPY_BLOCK:
            // for balancing purpose; send to a proxy source
        copyBlock(in, versionAndOpcode);
        datanode.myMetrics.copyBlockOp.inc(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_BLOCK_CHECKSUM: //get the checksum of a block
        getBlockChecksum(in, versionAndOpcode);
        datanode.myMetrics.blockChecksumOp.inc(DataNode.now() - startTime);
        break;
      default:
        throw new IOException("Unknown opcode " + op + " in data stream");
      }
    } catch (Throwable t) {
      if (op == DataTransferProtocol.OP_READ_BLOCK && t instanceof SocketTimeoutException) {
        // Ignore SocketTimeoutException for reading.
        // This usually means that the client who's reading data from the DataNode has exited.
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(datanode.getDatanodeInfo() + ":DataXceiver" + " (IGNORED) "
              + StringUtils.stringifyException(t));
        }
      } else {
        LOG.error(datanode.getDatanodeInfo() + ":DataXceiver",t);
      }
    } finally {
      LOG.debug(datanode.getDatanodeInfo() + ":Number of active connections is: "
                               + datanode.getXceiverCount());
      updateCurrentThreadName("Cleaning up");
      IOUtils.closeStream(in);
      IOUtils.closeSocket(s);
      dataXceiverServer.childSockets.remove(s);
    }
  }

  /**
   * Read a block from the disk.
   * @param in The stream to read from
   * @throws IOException
   */
  private void readBlock(DataInputStream in,
      VersionAndOpcode versionAndOpcode) throws IOException {
    //
    // Read in the header
    //
    long startTime = System.currentTimeMillis();
    
    ReadBlockHeader header = new ReadBlockHeader(versionAndOpcode);
    header.readFields(in);
    
    int namespaceId = header.getNamespaceId();
    long blockId = header.getBlockId();
    Block block = new Block( blockId, 0 , header.getGenStamp());
    long startOffset = header.getStartOffset();
    long length = header.getLen();
    String clientName = header.getClientName();

    // send the block
    OutputStream baseStream = NetUtils.getOutputStream(s, 
        datanode.socketWriteTimeout);
    DataOutputStream out = new DataOutputStream(
                 new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
    
    BlockSender blockSender = null;
    String clientTraceFmt = null;
    if (ClientTraceLog.isInfoEnabled()) {    
      if (remoteAddress == null) {
        getAddresses();
      }
      clientTraceFmt = clientName.length() > 0 ? String.format(
          DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress, "%d",
          "HDFS_READ", clientName, "%d",
          datanode.getDNRegistrationForNS(namespaceId).getStorageID(), block,
          "%d")
          :
          datanode.getDNRegistrationForNS(namespaceId)
          + " Served block " + block + " to " + s.getInetAddress();
    }
    updateCurrentThreadName("sending block " + block);
        
    try {
      try {
        blockSender = new BlockSender(namespaceId, block, startOffset, length,
            datanode.ignoreChecksumWhenRead, true, true, false, datanode,
            clientTraceFmt);
     } catch(IOException e) {
        out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
        throw e;
      }
      if (ClientTraceLog.isInfoEnabled()) {
        ClientTraceLog.info("Sending blocks. namespaceId: "
            + namespaceId + " block: " + block + " to " + remoteAddress);
      }

      out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS); // send op status
      long read = blockSender.sendBlock(out, baseStream, null); // send data

      boolean isBlockFinalized = datanode.data.isBlockFinalized(namespaceId, block);
      long blockLength = datanode.data.getVisibleLength(namespaceId, block);
      out.writeBoolean(isBlockFinalized);
      out.writeLong(blockLength);
      out.flush();

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
      
      // log exception to debug exceptions like socket timeout exceptions
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignore exception while sending blocks. namespaceId: "
          + namespaceId + " block: " + block + " to " + remoteAddress + ": "
          + ignored.getMessage());
      }
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
   * Write a block to disk.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void writeBlock(DataInputStream in,
      VersionAndOpcode versionAndOpcode) throws IOException {
    DatanodeInfo srcDataNode = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
                " tcp no delay " + s.getTcpNoDelay());
    }
    //
    // Read in the header
    //
    long startTime = System.currentTimeMillis();
    
    WriteBlockHeader headerToReceive = new WriteBlockHeader(
        versionAndOpcode);
    headerToReceive.readFields(in);
    int namespaceid = headerToReceive.getNamespaceId();
    Block block = new Block(headerToReceive.getBlockId(), 
        dataXceiverServer.estimateBlockSize, headerToReceive.getGenStamp());
    if (LOG.isInfoEnabled()) {
      if (remoteAddress == null) {
        getAddresses();
      }
      LOG.info("Receiving block " + block + 
               " src: " + remoteAddress +
               " dest: " + localAddress);
    }
    int pipelineSize = headerToReceive.getPipelineDepth(); // num of datanodes in entire pipeline
    boolean isRecovery = headerToReceive.isRecoveryFlag(); // is this part of recovery?
    String client = headerToReceive.getClientName(); // working on behalf of this client
    boolean hasSrcDataNode = headerToReceive.isHasSrcDataNode(); // is src node info present
    if (hasSrcDataNode) {
      srcDataNode = headerToReceive.getSrcDataNode();
    }
    int numTargets = headerToReceive.getNumTargets();
    DatanodeInfo targets[] = headerToReceive.getNodes();

    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
    DataOutputStream replyOut = null;   // stream to prev target
    Socket mirrorSock = null;           // socket to next target
    BlockReceiver blockReceiver = null; // responsible for data handling
    String mirrorNode = null;           // the name:port of next target
    String firstBadLink = "";           // first datanode that failed in connection setup

    updateCurrentThreadName("receiving block " + block + " client=" + client);
    try {
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(namespaceid, block, in, 
          s.getRemoteSocketAddress().toString(),
          s.getLocalSocketAddress().toString(),
          isRecovery, client, srcDataNode, datanode);

      // get a connection back to the previous target
      replyOut = new DataOutputStream(new BufferedOutputStream(
                     NetUtils.getOutputStream(s, datanode.socketWriteTimeout),
                     SMALL_BUFFER_SIZE));

      //
      // Open network conn to backup machine, if 
      // appropriate
      //
      if (targets.length > 0) {
        InetSocketAddress mirrorTarget = null;
        // Connect to backup machine
        mirrorNode = targets[0].getName();
        mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
        mirrorSock = datanode.newSocket();
        try {
          int timeoutValue = datanode.socketTimeout +
                             (datanode.socketReadExtentionTimeout * numTargets);
          int writeTimeout = datanode.socketWriteTimeout + 
                             (datanode.socketWriteExtentionTimeout * numTargets);
          NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
          mirrorOut = new DataOutputStream(
             new BufferedOutputStream(
                         NetUtils.getOutputStream(mirrorSock, writeTimeout),
                         SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));

          // Write header: Copied from DFSClient.java!
          WriteBlockHeader headerToSend = new WriteBlockHeader(
              DataTransferProtocol.DATA_TRANSFER_VERSION, namespaceid,
              block.getBlockId(), block.getGenerationStamp(), pipelineSize,
              isRecovery, hasSrcDataNode, srcDataNode, targets.length - 1, targets,
              client);
          headerToSend.writeVersionAndOpCode(mirrorOut);
          headerToSend.write(mirrorOut);
          blockReceiver.writeChecksumHeader(mirrorOut);
          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (client.length() != 0) {
            firstBadLink = Text.readString(mirrorIn);
            if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (client.length() != 0) {
            Text.writeString(replyOut, mirrorNode);
            replyOut.flush();
          }
          IOUtils.closeStream(mirrorOut);
          mirrorOut = null;
          IOUtils.closeStream(mirrorIn);
          mirrorIn = null;
          IOUtils.closeSocket(mirrorSock);
          mirrorSock = null;
          if (client.length() > 0) {
            throw e;
          } else {
            LOG.info(datanode.getDatanodeInfo() + ":Exception transfering block " +
                     block + " to mirror " + mirrorNode +
                     ". continuing without the mirror.\n" +
                     StringUtils.stringifyException(e));
          }
        }
      }

      // send connect ack back to source (only for clients)
      if (client.length() != 0) {
        if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
        Text.writeString(replyOut, firstBadLink);
        replyOut.flush();
      }

      // receive the block and mirror to the next target
      String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
      long totalReceiveSize = blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
                                 mirrorAddr, null, targets.length);

      // if this write is for a replication request (and not
      // from a client), then confirm block. For client-writes,
      // the block is finalized in the PacketResponder.
      if (client.length() == 0) {
        datanode.notifyNamenodeReceivedBlock(namespaceid, block, null);
        LOG.info("Received block " + block + 
                 " src: " + remoteAddress +
                 " dest: " + localAddress +
                 " of size " + block.getNumBytes());
      } else {
        // Log the fact that the block has been received by this datanode and
        // has been written to the local disk on this datanode.
        LOG.info("Received Block " + block +
            " src: " + remoteAddress +
            " dest: " + localAddress +
            " of size " + block.getNumBytes() +
            " and written to local disk");
      }

      if (datanode.blockScanner != null) {
        datanode.blockScanner.addBlock(namespaceid, block);
      }
      
      long writeDuration = System.currentTimeMillis() - startTime;
      datanode.myMetrics.bytesWrittenLatency.inc(writeDuration);
      if (totalReceiveSize > KB_RIGHT_SHIFT_MIN) {
        datanode.myMetrics.bytesWrittenRate.inc((int) (totalReceiveSize >> KB_RIGHT_SHIFT_BITS),
                              writeDuration);
      }

    } catch (IOException ioe) {
      LOG.info("writeBlock " + block + " received exception " + ioe);
      throw ioe;
    } finally {
      // close all opened streams
      IOUtils.closeStream(mirrorOut);
      IOUtils.closeStream(mirrorIn);
      IOUtils.closeStream(replyOut);
      IOUtils.closeSocket(mirrorSock);
      IOUtils.closeStream(blockReceiver);
    }
  }

  /**
   * Reads the metadata and sends the data in one 'DATA_CHUNK'.
   * @param in
   */
  void readMetadata(DataInputStream in, VersionAndOpcode versionAndOpcode)
      throws IOException {
    ReadMetadataHeader readMetadataHeader = 
        new ReadMetadataHeader(versionAndOpcode);
    readMetadataHeader.readFields(in);
    final int namespaceId = readMetadataHeader.getNamespaceId();
    Block block = new Block(readMetadataHeader.getBlockId(), 0,
        readMetadataHeader.getGenStamp());
    
    ReplicaToRead rtr;
    if ((rtr = datanode.data.getReplicaToRead(namespaceId, block)) == null
        || rtr.isInlineChecksum()) {
      throw new IOException(
          "Read metadata from inline checksum file is not supported");
    }
    DataOutputStream out = null;
    try {
      updateCurrentThreadName("reading metadata for block " + block);
      
      out = new DataOutputStream(
                NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
      
      byte[] buf = BlockWithChecksumFileReader.getMetaData(datanode.data,
          namespaceId, block);
      out.writeByte(DataTransferProtocol.OP_STATUS_SUCCESS);
      out.writeInt(buf.length);
      out.write(buf);
      
      //last DATA_CHUNK
      out.writeInt(0);
    } finally {
      IOUtils.closeStream(out);
    }
  }
  
  /**
   * Get block checksum (MD5 of CRC32).
   * @param in
   */
  void getBlockChecksum(DataInputStream in,
      VersionAndOpcode versionAndOpcode) throws IOException {
    // header
    BlockChecksumHeader blockChecksumHeader =
        new BlockChecksumHeader(versionAndOpcode);
    blockChecksumHeader.readFields(in);
    final int namespaceId = blockChecksumHeader.getNamespaceId();
    final Block block = new Block(blockChecksumHeader.getBlockId(), 0,
            blockChecksumHeader.getGenStamp());

    DataOutputStream out = null;
    InputStream rawStreamIn = null;
    DataInputStream streamIn = null;

    ReplicaToRead ri = datanode.data.getReplicaToRead(namespaceId, block);
    if (ri == null) {
      throw new IOException("Unknown block");
    }

    updateCurrentThreadName("getting checksum for block " + block);
    try {
      int bytesPerCRC;
      int checksumSize;

      long crcPerBlock;
      MD5Hash md5;
      if (!ri.isInlineChecksum()) {
        rawStreamIn = BlockWithChecksumFileReader.getMetaDataInputStream(
            datanode.data, namespaceId, block);
        streamIn = new DataInputStream(new BufferedInputStream(rawStreamIn,
            BUFFER_SIZE));

        final BlockMetadataHeader header = BlockMetadataHeader
            .readHeader(streamIn);
        final DataChecksum checksum = header.getChecksum();
        bytesPerCRC = checksum.getBytesPerChecksum();
        checksumSize = checksum.getChecksumSize();
        crcPerBlock = (((BlockWithChecksumFileReader.MetaDataInputStream) rawStreamIn)
            .getLength() - BlockMetadataHeader.getHeaderSize()) / checksumSize;

       //compute block checksum
       md5 = MD5Hash.digest(streamIn);
      } else {
        bytesPerCRC = ri.getBytesPerChecksum();
        checksumSize = DataChecksum.getChecksumSizeByType(ri.getChecksumType());
        rawStreamIn = datanode.data.getBlockInputStream(namespaceId, block, 0);
        streamIn = new DataInputStream(new BufferedInputStream(rawStreamIn,
            BUFFER_SIZE));

        long lengthLeft = ((FileInputStream) rawStreamIn).getChannel().size()
            - BlockInlineChecksumReader.getHeaderSize();
        if (lengthLeft == 0) {
          crcPerBlock = 0;
          md5 = MD5Hash.digest(new byte[0]);
        } else {
          crcPerBlock = (lengthLeft - 1) / (checksumSize + bytesPerCRC) + 1;
          MessageDigest digester = MD5Hash.getDigester();
          byte[] buffer = new byte[checksumSize];
          while (lengthLeft > 0) {
            if (lengthLeft >= bytesPerCRC + checksumSize) {
              streamIn.skip(bytesPerCRC);
              IOUtils.readFully(streamIn, buffer, 0, buffer.length);
              lengthLeft -= bytesPerCRC
                  + checksumSize;
            } else if (lengthLeft > checksumSize) {
              streamIn.skip(lengthLeft - checksumSize);
              IOUtils.readFully(streamIn, buffer, 0, buffer.length);
              lengthLeft = 0;
            } else {
              out = new DataOutputStream(
                  NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
              out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
              out.flush();
              // report to name node the corruption.
              DataBlockScanner.reportBadBlocks(block, namespaceId, datanode);
              throw new IOException("File for namespace " + namespaceId
                  + " block " + block + " seems to be corrupted");
            }
            digester.update(buffer);
          }
          md5 = new MD5Hash(digester.digest(), checksumSize * crcPerBlock);
        }
      }

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
      if (streamIn != null) {
        IOUtils.closeStream(streamIn);
      }
      if (rawStreamIn != null) {
        IOUtils.closeStream(rawStreamIn);
      }
    }
  }

  /**
   * Read a block from the disk and then sends it to a destination.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void copyBlock(DataInputStream in,
      VersionAndOpcode versionAndOpcode) throws IOException {
    
    // Read in the header
    CopyBlockHeader copyBlockHeader = new CopyBlockHeader(versionAndOpcode);
    copyBlockHeader.readFields(in);
    long startTime = System.currentTimeMillis();
    int namespaceId = copyBlockHeader.getNamespaceId();
    long blockId = copyBlockHeader.getBlockId();
    long genStamp = copyBlockHeader.getGenStamp();
    Block block = new Block(blockId, 0, genStamp);

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
  private void replaceBlock(DataInputStream in,
      VersionAndOpcode versionAndOpcode) throws IOException {
    long startTime = System.currentTimeMillis();
    ReplaceBlockHeader replaceBlockHeader = 
        new ReplaceBlockHeader(versionAndOpcode);
    
    /* read header */
    replaceBlockHeader.readFields(in);
    int namespaceId = replaceBlockHeader.getNamespaceId();
    long blockId = replaceBlockHeader.getBlockId();
    long genStamp = replaceBlockHeader.getGenStamp();
    Block block = new Block(blockId, dataXceiverServer.estimateBlockSize,
        genStamp);
    String sourceID = replaceBlockHeader.getSourceID();
    DatanodeInfo proxySource = replaceBlockHeader.getProxySource();

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
      CopyBlockHeader copyBlockHeader = new CopyBlockHeader(
          DataTransferProtocol.DATA_TRANSFER_VERSION, namespaceId,
          block.getBlockId(), block.getGenerationStamp());
      copyBlockHeader.writeVersionAndOpCode(proxyOut);
      copyBlockHeader.write(proxyOut);
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
  private void readBlockAccelerator(DataInputStream in, 
      VersionAndOpcode versionAndOpcode) throws IOException {
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
    // TODO: support inline checksum
    ReplicaToRead ri = datanode.data.getReplicaToRead(namespaceId, block);
    if (ri != null && ri.isInlineChecksum()) {
      throw new IOException(
          "Block read accelerator is not supported for inline checksum");
    }
    File dataFile = datanode.data.getBlockFile(namespaceId, block);
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

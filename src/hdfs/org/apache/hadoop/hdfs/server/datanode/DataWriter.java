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
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

/**
 * Thread for processing incoming data stream.
 */
class DataWriter implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  Socket s;
  DataNode datanode;
  DataInputStream in;
  DataXceiverServer dataXceiverServer;
  String remoteAddress;
  String localAddress;

  DataWriter(Socket s, DataNode datanode, DataInputStream in, 
            DataXceiverServer dataXceiverServer, 
            String remoteAddress, String localAddress) {
    this.s = s;
    this.datanode = datanode;
    this.in = in;
    this.dataXceiverServer = dataXceiverServer;
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  public void run() {
    dataXceiverServer.incWriter(); /// one more writer
    try {
      writeBlock();
    } catch (IOException e) {
      updateCurrentThreadName("Idle state");
      IOUtils.closeStream(in);
      IOUtils.closeSocket(s);
      dataXceiverServer.childSockets.remove(s);
    } finally {
      dataXceiverServer.decWriter(); // one less writer
    }
  }

  /**
   * Write a block to disk.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void writeBlock() throws IOException {
    DatanodeInfo srcDataNode = null;
    LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
              " tcp no delay " + s.getTcpNoDelay());
    //
    // Read in the header
    //
    long startTime = System.currentTimeMillis();
    int namespaceid = in.readInt();
    Block block = new Block(in.readLong(), 
        dataXceiverServer.estimateBlockSize, in.readLong());
    LOG.info("Receiving block " + block + 
             " src: " + remoteAddress +
             " dest: " + localAddress);
    int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
    boolean isRecovery = in.readBoolean(); // is this part of recovery?
    String client = Text.readString(in); // working on behalf of this client
    boolean hasSrcDataNode = in.readBoolean(); // is src node info present
    if (hasSrcDataNode) {
      srcDataNode = new DatanodeInfo();
      srcDataNode.readFields(in);
    }
    int numTargets = in.readInt();
    if (numTargets < 0) {
      throw new IOException("Mislabelled incoming datastream.");
    }
    DatanodeInfo targets[] = new DatanodeInfo[numTargets];
    for (int i = 0; i < targets.length; i++) {
      DatanodeInfo tmp = new DatanodeInfo();
      tmp.readFields(in);
      targets[i] = tmp;
    }

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
          mirrorOut.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
          mirrorOut.write( DataTransferProtocol.OP_WRITE_BLOCK );
          mirrorOut.writeInt(namespaceid);
          mirrorOut.writeLong( block.getBlockId() );
          mirrorOut.writeLong( block.getGenerationStamp() );
          mirrorOut.writeInt( pipelineSize );
          mirrorOut.writeBoolean( isRecovery );
          Text.writeString( mirrorOut, client );
          mirrorOut.writeBoolean(hasSrcDataNode);
          if (hasSrcDataNode) { // pass src node information
            srcDataNode.write(mirrorOut);
          }
          mirrorOut.writeInt( targets.length - 1 );
          for ( int i = 1; i < targets.length; i++ ) {
            targets[i].write( mirrorOut );
          }

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
   * Update the thread name to contain the current status.
   * Use this only after this receiver has started on its thread, i.e.,
   * outside the constructor.
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DataWriter for client ").append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }
}

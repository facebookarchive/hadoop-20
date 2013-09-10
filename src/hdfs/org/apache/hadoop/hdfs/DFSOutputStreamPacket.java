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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData.WritePacketClientProfile;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNode;


/****************************************************************
 * Packet for DFSOutputStream to use to construct the packet to send to data
 * node.
 ****************************************************************/
abstract class DFSOutputStreamPacket {
  protected final DFSOutputStream dfsOutputStream;

  long seqno; // sequencenumber of buffer in block
  long offsetInBlock; // offset in block
  boolean lastPacketInBlock; // is this the last packet in block?
  int dataLength;
  int numChunks; // number of chunks currently in packet
  int maxChunks; // max chunks in packet

  WritePacketClientProfile profile;
  
  static final long HEART_BEAT_SEQNO = -1L;

  /**
   * create a heartbeat packet
   */
  protected DFSOutputStreamPacket(DFSOutputStream dfsOutputStream) {
    this.dfsOutputStream = dfsOutputStream;
    this.lastPacketInBlock = false;
    this.numChunks = 0;
    this.offsetInBlock = 0;
    this.seqno = HEART_BEAT_SEQNO;

    dataLength = 0;
    maxChunks = 0;
  }

  // create a new packet
  protected DFSOutputStreamPacket(DFSOutputStream dfsOutputStream,
      int chunksPerPkt, long offsetInBlock, WritePacketClientProfile profile)
      throws IOException {
    this.dfsOutputStream = dfsOutputStream;
    this.lastPacketInBlock = false;
    this.numChunks = 0;
    this.offsetInBlock = offsetInBlock;
    this.seqno = this.dfsOutputStream.incAndGetCurrentSeqno();
    this.profile = profile;
    
    maxChunks = chunksPerPkt;
    
    if (this.profile != null) {
      this.profile.packetCreated();
    }
  }
  
  void writeDataAndChecksum(byte[] dataInarray, int dataOff, int dataLen,
      byte[] checksumInarray, int checksumOff, int checksumLen)
      throws IOException {
    writeData(dataInarray, dataOff, dataLen);
    writeChecksum(checksumInarray, checksumOff, checksumLen);
  }
  
  abstract void writeData(byte[] inarray, int off, int len) throws IOException;

  abstract void writeChecksum(byte[] inarray, int off, int len)
      throws IOException;
  
  abstract void cleanup();

  /**
   * Returns ByteBuffer that contains one full packet, including header.
   * 
   * Packet format:
   * 
   * +----------------------------+
   * |   size of payload          |
   * +----------------------------+
   * |   packetVersion*           |
   * +----------------------------+
   * |   offset of data in block  |
   * +----------------------------+
   * |   packet's seq ID          |
   * +----------------------------+
   * |                            |
   * |   payload**                |
   * |                            |
   * +----------------------------+
   * 
   * * Only for data protocol version not ealier than
   *   PACKET_INCLUDE_VERSION_VERSION
   * ** payload format is per packet version and will be described below.
   * 
   * If packet size = 0, it indicates the end of the stream and there will be
   * no packetVersion or data after it sent.
   * 
   * == payload format ==
   * 
   * packetVersion = PACKET_VERSION_CHECKSUM_FIRST:
   * +----------------------------+
   * |   data size to be sent     |
   * +----------------------------+
   * |   checksum for chunk 1     |
   * +----------------------------+
   * |   checksum for chunk 2     |
   * +----------------------------+
   * |   ......                   |
   * +----------------------------+
   * |                            |
   * |   data chunk 1             |
   * |                            |
   * +----------------------------+
   * |                            |
   * |   data chunk 2             |
   * |                            |
   * +----------------------------+
   * |                            |
   * |  ...                       |
   * |                            |
   * +----------------------------+
   * |                            |
   * |   last data chunk          |
   * |   (can be partial)         |
   * +----------------------------+
   * 
   * packetVersion = PACKET_VERSION_CHECKSUM_INLINE:
   * +----------------------------+
   * |   data size to be sent     |
   * +----------------------------+
   * |                            |
   * |   data chunk 1             |
   * |                            |
   * +----------------------------+
   * |   checksum for chunk 1     |
   * +----------------------------+
   * |                            |
   * |   data chunk 2             |
   * |                            |
   * +----------------------------+
   * |   checksum for chunk 2     |
   * +----------------------------+
   * |                            |
   * |  ...                       |
   * |                            |
   * +----------------------------+
   * |                            |
   * |   last data chunk          |
   * |   (can be partial)         |
   * +----------------------------+
   * |   checksum for last chunk  |
   * +----------------------------+
   * 
   * The way data and checksums are inlined is the same as inline checksum
   * on disk files. 
   * 
   * @throws IOException
   */
  abstract ByteBuffer getBuffer() throws IOException;
  
  long getEndPosInCurrBlk() {
    return offsetInBlock + dataLength;
  }

  /**
   * Check if this packet is a heart beat packet
   * 
   * @return true if the sequence number is HEART_BEAT_SEQNO
   */
  boolean isHeartbeatPacket() {
    return seqno == HEART_BEAT_SEQNO;
  }
  
  
  void eventAddToDataQueue() {
    if (profile != null) {
      profile.addToDataQueue();
    }
  }

  void eventPopFromDataQueue() {
    if (profile != null) {
      profile.popFromDataQueue();
    }
  }

  void eventAddToAckQueue() {
    if (profile != null) {
      profile.addToAckQueue();
    }
  }
  
  void eventAckReceived() {
    if (profile != null) {
      profile.ackReceived();
    }
  }
}

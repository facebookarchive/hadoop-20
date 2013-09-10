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
 * node. Checksums are inlined.
 ****************************************************************/
class DFSOutputStreamPacketInlineChecksum extends DFSOutputStreamPacket {
  ByteBuffer buffer; // only one of buf and buffer is non-null
  byte[] buf;

  int dataStart;
  int pos;

  static final long HEART_BEAT_SEQNO = -1L;

  /**
   * create a heartbeat packet
   */
  DFSOutputStreamPacketInlineChecksum(DFSOutputStream dfsOutputStream) {
    super(dfsOutputStream);
    buffer = null;
    int packetSize = this.dfsOutputStream.getPacketHeaderLen()
        + DFSClient.SIZE_OF_INTEGER;
    buf = new byte[packetSize];

    pos = dataStart;
  }

  // create a new packet
  DFSOutputStreamPacketInlineChecksum(DFSOutputStream dfsOutputStream,
      int pktSize, int chunksPerPkt, long offsetInBlock,
      WritePacketClientProfile profile) throws IOException {
    super(dfsOutputStream, chunksPerPkt, offsetInBlock, profile);
    buffer = null;
    buf = new byte[pktSize];

    dataStart = dfsOutputStream.getPacketHeaderLen()
        + DFSClient.SIZE_OF_INTEGER;
    pos = dataStart;
  }

  @Override
  void writeData(byte[] inarray, int off, int len) throws IOException {
    if (len > this.dfsOutputStream.getBytesPerChecksum()) {
      throw new IOException("Chunk size is larger than limit.");
    }

    if (pos + len > buf.length) {
      throw new BufferOverflowException();
    }
    System.arraycopy(inarray, off, buf, pos, len);
    dataLength += len;
    pos += len;
  }

  @Override
  void writeChecksum(byte[] inarray, int off, int len) throws IOException {
    if (len != this.dfsOutputStream.getChecksumSize()) {
      throw new IOException("Checksum size doesn't match.");
    }
    if (pos + len > buf.length) {
      throw new BufferOverflowException();
    }
    System.arraycopy(inarray, off, buf, pos, len);
    pos += len;
  }

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
   * +----------------------------+
   * |   data size to be sent     |
   * +----------------------------+
   * |                            |
   * |   data chunk 1             |
   * |  (can be partial)          |
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
  ByteBuffer getBuffer() throws IOException {
    /*
     * Once this is called, no more data can be added to the packet. setting
     * 'buf' to null ensures that. This is called only when the packet is ready
     * to be sent.
     */
    if (buffer != null) {
      return buffer;
    }

    int pktLen = DFSClient.SIZE_OF_INTEGER + (pos - dataStart);

    // normally dataStart == checksumPos, i.e., offset is zero.
    buffer = ByteBuffer.wrap(buf, 0, dfsOutputStream.getPacketHeaderLen()
        + pktLen);
    buf = null;
    buffer.mark();

    /*
     * write the header and data length. The format is described in comment
     * before DataNode.BlockSender
     */
    buffer.putInt(pktLen); // pktSize
    buffer.putInt(dfsOutputStream.getPacketVersion());
    buffer.putLong(offsetInBlock);
    buffer.putLong(seqno);

    byte booleanFieldValue = 0x00;

    if (lastPacketInBlock) {
      booleanFieldValue |= DataNode.isLastPacketInBlockMask;
    }
    if (dfsOutputStream.ifForceSync()) {
      booleanFieldValue |= DataNode.forceSyncMask;
    }
    buffer.put(booleanFieldValue);

    // end of pkt header
    buffer.putInt(dataLength); // actual data length, excluding checksum.
    buffer.reset();
    return buffer;
  }

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

  @Override
  void cleanup() {
    pos = dataStart;
    dataLength = 0;
    buffer = null;
    int packetSize = this.dfsOutputStream.getPacketHeaderLen()
        + DFSClient.SIZE_OF_INTEGER;
    buf = new byte[packetSize];
  }
}

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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Writable class that data node sends to DFS client or upstream data node
 * for the profile of the life cycle of the packet in this datanode.
 */
public class PacketBlockReceiverProfileData {
  private enum FieldCode {
    DURATION_READ((byte) 1),
    DURATION_FORWARD((byte) 2),
    DURATION_ENQUEUE((byte) 3),
    DURATION_SET_POSITION((byte) 4),
    VERIFY_CHECKSUM((byte) 5),
    UPDATE_BLOCK_CRC((byte) 6),
    WRITE_PACKET((byte) 7),
    FLUSH((byte) 8),
    TIME_IN_ACK_QUEUE((byte) 9),
    DURATION_RECEIVE_ACK((byte) 10),
    DURATION_WAIT_PERSISTENT((byte) 11),

    PREV_PKT_AFTER_FLUSH((byte) 81),
    PREV_PKT_DURATION_SEND_ACK((byte) 82);

    private byte code;

    private FieldCode(byte code) {
      this.code = code;
    }

    private byte getCode() {
      return code;
    }
      
    private static final Map<Byte, FieldCode> byteToEnum = new HashMap<Byte, FieldCode>();

    static {
      // initialize byte to enum map
      for (FieldCode opCode : values())
        byteToEnum.put(opCode.getCode(), opCode);
    }

    private static FieldCode fromByte(byte code) {
      return byteToEnum.get(code);
    }
  }

  public long durationRead;
  public long durationForward;
  public long durationEnqueue;
  public long durationSetPosition;
  public long durationVerifyChecksum;
  public long durationUpdateBlockCrc;
  public long durationWritePacket;
  public long durationFlush;
  public long prevPktDurationAfterFlush;
  public long timeInAckQueue;
  public long durationReceiveAck;
  public long durationWaitPersistent;
  public long prevPktDurationSendAck;

  public PacketBlockReceiverProfileData() {
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(13);

    out.writeByte(FieldCode.DURATION_READ.getCode());
    out.writeLong(durationRead);

    out.writeByte(FieldCode.DURATION_FORWARD.getCode());
    out.writeLong(durationForward);

    out.writeByte(FieldCode.DURATION_ENQUEUE.getCode());
    out.writeLong(durationEnqueue);

    out.writeByte(FieldCode.DURATION_SET_POSITION.getCode());
    out.writeLong(durationSetPosition);

    out.writeByte(FieldCode.VERIFY_CHECKSUM.getCode());
    out.writeLong(durationVerifyChecksum);

    out.writeByte(FieldCode.UPDATE_BLOCK_CRC.getCode());
    out.writeLong(durationUpdateBlockCrc);

    out.writeByte(FieldCode.WRITE_PACKET.getCode());
    out.writeLong(durationWritePacket);

    out.writeByte(FieldCode.FLUSH.getCode());
    out.writeLong(durationFlush);

    out.writeByte(FieldCode.PREV_PKT_AFTER_FLUSH.getCode());
    out.writeLong(prevPktDurationAfterFlush);

    out.writeByte(FieldCode.TIME_IN_ACK_QUEUE.getCode());
    out.writeLong(timeInAckQueue);

    out.writeByte(FieldCode.DURATION_RECEIVE_ACK.getCode());
    out.writeLong(durationReceiveAck);

    out.writeByte(FieldCode.DURATION_WAIT_PERSISTENT.getCode());
    out.writeLong(durationWaitPersistent);

    out.writeByte(FieldCode.PREV_PKT_DURATION_SEND_ACK.getCode());
    out.writeLong(prevPktDurationSendAck);

  }

  public void readFields(DataInput in) throws IOException {
    int numCounters = in.readInt();
    for (int i = 0; i < numCounters; i++) {
      byte codeId = in.readByte();
      long value = in.readLong();
      FieldCode code = FieldCode.fromByte(codeId);
      switch (code) {
      case DURATION_READ:
        durationRead = value;
        break;
      case DURATION_FORWARD:
        durationForward = value;
        break;
      case DURATION_ENQUEUE:
        durationEnqueue = value;
        break;
      case DURATION_SET_POSITION:
        durationSetPosition = value;
        break;
      case VERIFY_CHECKSUM:
        durationVerifyChecksum = value;
        break;
      case UPDATE_BLOCK_CRC:
        durationUpdateBlockCrc = value;
        break;
      case WRITE_PACKET:
        durationWritePacket = value;
        break;
      case FLUSH:
        durationFlush = value;
        break;
      case PREV_PKT_AFTER_FLUSH:
        prevPktDurationAfterFlush = value;
        break;
      case TIME_IN_ACK_QUEUE:
        timeInAckQueue = value;
        break;
      case DURATION_RECEIVE_ACK:
        durationReceiveAck = value;
        break;
      case DURATION_WAIT_PERSISTENT:
        durationWaitPersistent = value;
        break;
      case PREV_PKT_DURATION_SEND_ACK:
        prevPktDurationSendAck = value;
        break;
      }
    }
  }
  
  public String toString() {
    return "    durationRead: " + durationRead + "\n" +
        "    durationForward: " + durationForward + "\n" +
        "    durationEnqueue: " + durationEnqueue + "\n" +
        "    durationSetPosition: " + durationSetPosition + "\n" +
        "    durationVerifyChecksum: " + durationVerifyChecksum + "\n" +
        "    durationUpdateBlockCrc: " + durationUpdateBlockCrc + "\n" +
        "    durationWritePacket: " + durationWritePacket + "\n" +
        "    durationFlush: " + durationFlush + "\n" +
        "    prevPktDurationAfterFlush: " + prevPktDurationAfterFlush + "\n" +
        "    timeInAckQueue: " + timeInAckQueue + "\n" +
        "    durationReceiveAck: " + durationReceiveAck + "\n" +
        "    durationWaitPersistent: " + durationWaitPersistent + "\n" +
        "    prevPktDurationSendAck: " + prevPktDurationSendAck;  
  }
}

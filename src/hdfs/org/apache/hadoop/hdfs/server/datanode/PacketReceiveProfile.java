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

import org.apache.hadoop.hdfs.protocol.PacketBlockReceiverProfileData;


/** 
 * The class is used in BlockReceiver to kepe track a packet's life
 * cycle in a data node.
 **/
class PacketReceiveProfile {
  
  long receiveEventStartTime;
  long ackEventStartTime;
  
  public long durationRead;
  public long durationForward;
  public long durationEnqueue;
  public long durationSetPosition;
  public long durationVerifyChecksum;
  public long durationUpdateBlockCrc;
  public long durationWritePacket;
  public long durationFlush;
  public long durationAfterFlush;
  public long timeInAckQueue;
  public long durationReceiveAck;
  public long durationWaitPersistent;
  public long durationSendAck;

  public long prevPktDurationAfterFlush;
  public long prevPktDurationSendAck;

  
  /**
   * @return a writable object ready to send to clients.
   */
  PacketBlockReceiverProfileData getPacketBlockReceiverProfileData() {
    PacketBlockReceiverProfileData ret = new PacketBlockReceiverProfileData();
    ret.durationRead = this.durationRead;
    ret.durationForward = this.durationForward;
    ret.durationEnqueue = this.durationEnqueue;
    ret.durationSetPosition = this.durationSetPosition;
    ret.durationVerifyChecksum = this.durationVerifyChecksum;
    ret.durationUpdateBlockCrc = this.durationUpdateBlockCrc;
    ret.durationWritePacket = this.durationWritePacket;
    ret.durationFlush = this.durationFlush;
    ret.prevPktDurationAfterFlush = this.prevPktDurationAfterFlush;
    ret.timeInAckQueue = this.timeInAckQueue;
    ret.durationReceiveAck = this.durationReceiveAck;
    ret.durationWaitPersistent = this.durationWaitPersistent;
    ret.prevPktDurationSendAck = this.prevPktDurationSendAck;
    return ret;
  }
  
  public String toString() {
    return "durationRead: " + durationRead + "\n" +
        "durationForward: " + durationForward + "\n" +
        "durationEnqueue: " + durationEnqueue + "\n" +
        "durationSetPosition: " + durationSetPosition + "\n" +
        "durationVerifyChecksum: " + durationVerifyChecksum + "\n" +
        "durationUpdateBlockCrc: " + durationUpdateBlockCrc + "\n" +
        "durationWritePacket: " + durationWritePacket + "\n" +
        "durationFlush: " + durationFlush + "\n" +
        "durationAfterFlush: " + durationAfterFlush + "\n" +
        "timeInAckQueue: " + timeInAckQueue + "\n" +
        "durationReceiveAck: " + durationReceiveAck + "\n" +
        "durationWaitPersistent: " + durationWaitPersistent + "\n" +
        "durationSendAck: " + durationSendAck;  
  }
  
  void startReceivePacket() {
    receiveEventStartTime = System.nanoTime();
  }
  
  void endReceivePacketStartForward() {
    long nowTime = System.nanoTime();
    durationRead = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }

  void endForwardStartEnqueue() {
    long nowTime = System.nanoTime();
    durationForward = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endEnqueueStartSetPostion() {
    ackEventStartTime = System.nanoTime();
    durationEnqueue = ackEventStartTime - receiveEventStartTime;
    receiveEventStartTime = ackEventStartTime;
  }

  void endSetPositionStartVerifyChecksum() {
    long nowTime = System.nanoTime();
    durationSetPosition = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endVerifyChecksumStartUpdateBlockCrc() {
    long nowTime = System.nanoTime();
    durationVerifyChecksum = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endUpdateBlockCrcStartWritePacket() {
    long nowTime = System.nanoTime();
    durationUpdateBlockCrc = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endWritePacketStartFlush() {
    long nowTime = System.nanoTime();
    durationWritePacket = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endFlush() {
    long nowTime = System.nanoTime();
    durationFlush = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void endCurrentPacket() {
    long nowTime = System.nanoTime();
    durationAfterFlush = nowTime - receiveEventStartTime;
    receiveEventStartTime = nowTime;
  }
  
  void ackDequeued() {
    long nowTime = System.nanoTime();
    timeInAckQueue = nowTime - ackEventStartTime;
    ackEventStartTime = nowTime;
  }

  void ackReceived() {
    long nowTime = System.nanoTime();
    durationReceiveAck = nowTime - ackEventStartTime;
    ackEventStartTime = nowTime;
  }

  void responderKnewDataPersistent() {
    long nowTime = System.nanoTime();
    durationWaitPersistent = nowTime - ackEventStartTime;
    ackEventStartTime = nowTime;
  }
  
  void ackForwarded() {
    long nowTime = System.nanoTime();
    durationSendAck = nowTime - ackEventStartTime;
    ackEventStartTime = nowTime;
  }
  
}

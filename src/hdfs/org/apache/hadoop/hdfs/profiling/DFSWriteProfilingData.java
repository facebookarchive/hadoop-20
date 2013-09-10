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

package org.apache.hadoop.hdfs.profiling;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FsWriteProfile;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.protocol.PacketBlockReceiverProfileData;

/**
 * Class contain detailed profile information for a DFSOutpuStream
 */
public class DFSWriteProfilingData extends FsWriteProfile {
  public long recentSyncTimeUpdateChecksum;
  public long recentSyncEnqueuePkt;
  public long recentSyncWaitPktAcked;
  public long recentSyncPostAck;

  public long timeCloseWaitDatanode;
  public long timeCloseWaitNamenode;

  public long timeUpdateChecksum;
  public long timeEnqueuePkt;

  public long timePreparePkt;
  public long timeSendPkt;
  public long timeReceiveAck;

  public WritePacketClientProfile recentPacketProfile;

  public List<WriteBlockProfile> blockProfiles;

  public long getAvgTimeInDataQueue() {
    if (numPkts == 0) {
      return 0;
    }
    return totalTimeInDataQueue / numPkts;
  }

  public long getAvgTimeInAckQueue() {
    if (numPkts == 0) {
      return 0;
    }
    return totalTimeInAckQueue / numPkts;
  }

  private int numPkts;
  private long totalTimeInDataQueue;
  private long totalTimeInAckQueue;

  private WriteBlockProfile currentBlockProfile;

  public void nextBlock() {
    if (blockProfiles == null) {
      blockProfiles = new ArrayList<WriteBlockProfile>();
    }
    currentBlockProfile = new WriteBlockProfile();
    blockProfiles.add(currentBlockProfile);
  }

  private enum State {
    UPDATING_CHECKSUM, ENQUEUE_PACKET, DEFAULT, NOT_IN_WRITE
  }

  private enum SyncState {
    SYNC_START, SYNC_ENQUEUE, SYNC_WAIT_ACK, SYNC_POST_ACK, NOT_IN_SYNC,
  }

  private State state;
  private SyncState syncState;
  private long startTime;
  private long startTimeForSync;
  private long timeStartReceiveAck;

  public synchronized void startWrite() {
    startTime = System.nanoTime();
    state = State.UPDATING_CHECKSUM;
  }

  public synchronized void endWrite() {
    startTime = System.nanoTime();
    state = State.NOT_IN_WRITE;
  }

  public synchronized void startEnqueuePacket() {
    long timeNow = System.nanoTime();
    long duration = timeNow - startTime;
    if (syncState == SyncState.SYNC_START) {
      recentSyncTimeUpdateChecksum = duration;
      syncState = SyncState.SYNC_ENQUEUE;
    }
    if (state == State.UPDATING_CHECKSUM) {
      timeUpdateChecksum += duration;
    }
    startTime = timeNow;
    state = State.ENQUEUE_PACKET;
  }

  public synchronized void endEnquePacket() {
    assert state == State.ENQUEUE_PACKET;
    long timeNow = System.nanoTime();
    long duration = timeNow - startTime;
    if (syncState == SyncState.SYNC_ENQUEUE) {
      recentSyncEnqueuePkt = duration;
      syncState = SyncState.NOT_IN_SYNC;
    }
    timeEnqueuePkt += duration;
    startTime = timeNow;
    state = State.DEFAULT;
  }

  public synchronized void startSync() {
    startWrite();
    startTimeForSync = System.nanoTime();
    syncState = SyncState.SYNC_START;
  }

  public synchronized void syncStartWaitAck() {
    assert syncState == SyncState.NOT_IN_SYNC;
    syncState = SyncState.SYNC_WAIT_ACK;
    startTimeForSync = System.nanoTime();
  }

  public synchronized void syncPktAcked() {
    assert syncState == SyncState.SYNC_WAIT_ACK;
    long timeNow = System.nanoTime();
    recentSyncWaitPktAcked = timeNow - startTimeForSync;
    startTimeForSync = timeNow;
    syncState = SyncState.SYNC_POST_ACK;
  }

  public synchronized void endSync() {
    assert syncState == SyncState.SYNC_POST_ACK;
    recentSyncPostAck = System.nanoTime() - startTimeForSync;
    syncState = SyncState.NOT_IN_SYNC;
  }

  public synchronized void closeAfterFlushBuffer() {
    startTime = System.nanoTime();
  }

  public synchronized void closeReceivedAck() {
    long timeNow = System.nanoTime();
    timeCloseWaitDatanode = timeNow - startTime;
    startTime = timeNow;
  }

  public synchronized void endClose() {
    timeCloseWaitNamenode = System.nanoTime() - startTime;
  }

  public void startReceiveAck() {
    timeStartReceiveAck = System.nanoTime();
  }

  public void finishPacket(WritePacketClientProfile pktProfile,
      PipelineAck pipelineAck) {
    if (pktProfile == null) {
      return;
    }

    if (pipelineAck != null) {
      pktProfile.profilesFromDataNodes = pipelineAck.getProfiles();
    }

    pktProfile.timeInAckQueue = timeStartReceiveAck - pktProfile.startTime;
    pktProfile.timeReceiveAck = System.nanoTime() - timeStartReceiveAck;

    recentPacketProfile = pktProfile;
    timePreparePkt += pktProfile.timeBeforeAddToDataQueue;
    timeSendPkt += pktProfile.timeSendPacket;
    timeReceiveAck += pktProfile.timeReceiveAck;

    numPkts++;
    totalTimeInDataQueue += pktProfile.timeInDataQueue;
    totalTimeInAckQueue += pktProfile.timeInAckQueue;
    
    if (currentBlockProfile != null) {
      currentBlockProfile.updateDnProfiles(pipelineAck.getProfiles());
    }
  }

  public WritePacketClientProfile getWritePacketClientProfile() {
    return new WritePacketClientProfile();
  }

  /**
   * Profile for writing to one data block
   */
  class WriteBlockProfile {
    public DataNodeAccumulatedProfile[] datanodeProfiles;

    public void updateDnProfiles(PacketBlockReceiverProfileData[] pktDnProfiles) {
      if (pktDnProfiles == null) {
        return;
      }
      if (datanodeProfiles == null) {
        datanodeProfiles = new DataNodeAccumulatedProfile[pktDnProfiles.length];
      }
      for (int i = 0; i < Math.min(pktDnProfiles.length,
          datanodeProfiles.length); i++) {
        if (datanodeProfiles[i] == null) {
          datanodeProfiles[i] = new DataNodeAccumulatedProfile();
        }
        datanodeProfiles[i].addPacketProfile(pktDnProfiles[i]);
      }
    }
    
    public String toString() {
      if (datanodeProfiles == null || datanodeProfiles.length == 0) {
        return "Empty";
      }
      StringBuilder sb = new StringBuilder();
      sb.append(" number of data nodes: " + datanodeProfiles.length);
      for (int i = 0; i < datanodeProfiles.length; i++) {
        DataNodeAccumulatedProfile profile = datanodeProfiles[i];
        if (profile != null) {
          sb.append("\n  [datanode #" + (i+1)+ "]\n");
          sb.append(profile.toString());
        }
      }
      return sb.toString();
    }
  }

  /**
   * Profile for one packet's life cycle.
   */
  public class WritePacketClientProfile {
    /**
     * This packet's life cycle information in all the data nodes in the
     * pipeline
     */
    PacketBlockReceiverProfileData[] profilesFromDataNodes;

    private long startTime;

    long timeBeforeAddToDataQueue;
    long timeInDataQueue;
    long timeSendPacket;
    long timeInAckQueue;
    long timeReceiveAck;

    public void packetCreated() {
      startTime = System.nanoTime();
    }

    public void addToDataQueue() {
      long timeNow = System.nanoTime();
      timeBeforeAddToDataQueue = timeNow - startTime;
      startTime = timeNow;
    }

    public void popFromDataQueue() {
      long timeNow = System.nanoTime();
      timeInDataQueue = timeNow - startTime;
      startTime = timeNow;
    }

    public void addToAckQueue() {
      long timeNow = System.nanoTime();
      timeSendPacket = timeNow - startTime;
      startTime = timeNow;
    }

    public void ackReceived() {
      long timeNow = System.nanoTime();
      timeReceiveAck = timeNow - startTime;
    }
    
    public long getTotalTime() {
      return timeReceiveAck + timeInAckQueue + timeSendPacket + timeInDataQueue
          + timeBeforeAddToDataQueue;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(
          "    timeBeforeAddToDataQueue: " + timeBeforeAddToDataQueue + "\n" +
          "    timeInDataQueue: " + timeInDataQueue + "\n" +
          "    timeSendPacket: " + timeSendPacket + "\n" +
          "    timeInAckQueue: " + timeInAckQueue + "\n" +
          "    timeReceiveAck: " + timeReceiveAck + "\n\n");
      if (profilesFromDataNodes != null && profilesFromDataNodes.length > 0) {
        sb.append("  " + profilesFromDataNodes.length + " data nodes involved:\n");
        for (int i = 0; i < profilesFromDataNodes.length; i++) {
          PacketBlockReceiverProfileData profile = profilesFromDataNodes[i];
          if (profile != null) {
            sb.append("  DataNode #" + (i+1) + ":\n");
            sb.append(profile.toString());
            sb.append("\n");
          }
        }
      }
      return sb.toString();
    }
  }

  /**
   * Accumulated profile for one data node accepting data for a block
   * The counters here are accumulated from all the packets.
   */
  public class DataNodeAccumulatedProfile {
    public String toString() {
      return "  number of packets: " + numPackets +
          "\n  durationRead: " + durationRead+
          "\n  durationForward: " + durationForward+
          "\n  durationEnqueue: " + durationEnqueue+
          "\n  durationSetPosition: " + durationSetPosition+
          "\n  durationVerifyChecksum: " + durationVerifyChecksum+
          "\n  durationUpdateBlockCrc: " + durationUpdateBlockCrc+
          "\n  durationWritePacket: " + durationWritePacket+
          "\n  durationFlush: " + durationFlush+
          "\n  durationAfterFlush: " + durationAfterFlush+
          "\n  durationReceiveAck: " + durationReceiveAck+
          "\n  durationWaitPersistent: " + durationWaitPersistent+
          "\n  durationSendAck: " + durationSendAck +
          "\n  pktAvtTimeInAckQueue: " + pktAvtTimeInAckQueue();
    }
    
    public long durationRead = 0;
    public long durationForward = 0;
    public long durationEnqueue = 0;
    public long durationSetPosition = 0;
    public long durationVerifyChecksum = 0;
    public long durationUpdateBlockCrc = 0;
    public long durationWritePacket = 0;
    public long durationFlush = 0;
    public long durationAfterFlush = 0;

    public long durationReceiveAck = 0;
    public long durationWaitPersistent = 0;
    public long durationSendAck = 0;

    public long numPackets = 0;

    private long totalTimeInAckQueue = 0;

    public void addPacketProfile(PacketBlockReceiverProfileData profile) {
      if (profile == null) {
        return;
      }

      durationRead += profile.durationRead;
      durationForward += profile.durationForward;
      durationEnqueue += profile.durationEnqueue;
      durationSetPosition += profile.durationSetPosition;
      durationVerifyChecksum += profile.durationVerifyChecksum;
      durationUpdateBlockCrc += profile.durationUpdateBlockCrc;
      durationWritePacket += profile.durationWritePacket;
      durationFlush += profile.durationFlush;
      durationAfterFlush += profile.prevPktDurationAfterFlush;

      durationReceiveAck += profile.durationReceiveAck;
      durationWaitPersistent += profile.durationWaitPersistent;
      durationSendAck += profile.prevPktDurationSendAck;

      totalTimeInAckQueue += profile.timeInAckQueue;

      numPackets++;
    }

    public long pktAvtTimeInAckQueue() {
      if (numPackets > 0) {
        return totalTimeInAckQueue / numPackets;
      } else {
        return 0;
      }
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntimeUpdateChecksum: " + timeUpdateChecksum + "\ntimeEnqueuePkt: "
        + timeEnqueuePkt + "\ntimePreparePkt: " + timePreparePkt
        + "\ntimeSendPkt: " + timeSendPkt
        + "\ntimeReceiveAck: " + timeReceiveAck
        + "\nAvgTimeInDataQueue: " + getAvgTimeInDataQueue()
        + "\nAvgTimeInAckQueue: " + getAvgTimeInAckQueue()
        + "\n\n");
    sb.append("recentSyncTimeUpdateChecksum: " + recentSyncTimeUpdateChecksum
        + "\nrecentSyncEnqueuePkt: " + recentSyncEnqueuePkt
        + "\nrecentSyncWaitPktAcked: " + recentSyncWaitPktAcked
        + "\nrecentSyncPostAck: " + recentSyncPostAck
        + "\n\ntimeCloseWaitDatanode: " + timeCloseWaitDatanode
        + "\ntimeCloseWaitNamenode: " + timeCloseWaitNamenode
        + "\n\n");
    
    if (recentPacketProfile != null) {
      sb.append("Most Recent Packet: \n");
      sb.append(recentPacketProfile.toString());
      sb.append("\n\n");
    }
    
    if (blockProfiles != null) {
      for (int i = 0; i < blockProfiles.size(); i++) {
        WriteBlockProfile blkProfile = blockProfiles.get(i);
        if (blkProfile != null) {
          sb.append("Block #" + (i+1) + ":\n");
          sb.append(blkProfile);
          sb.append("\n");
        }
      }
    }
    
    return sb.toString();
  }
}
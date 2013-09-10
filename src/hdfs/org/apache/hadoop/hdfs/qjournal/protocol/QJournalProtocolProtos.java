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
package org.apache.hadoop.hdfs.qjournal.protocol;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteImage;
import org.apache.hadoop.io.*;

/**
 * This class defines messages that are used in QJournalProtocol., for
 * communication between the QJM client and the journal nodes. Each message
 * implements Writable interface to enable sending it over the wire.
 */
public class QJournalProtocolProtos {

  public static class JournalIdProto implements Writable {
    String identifier;

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeStringOpt(out, identifier);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      identifier = Text.readStringOpt(in);
    }

    public static JournalIdProto read(DataInput in) throws IOException {
      JournalIdProto jid = new JournalIdProto();
      jid.readFields(in);
      return jid;
    }
  }

  public static class RequestInfoProto implements Writable {
    JournalIdProto journalId;
    long epoch;
    long ipcSerialNumber;
    
    boolean hasCommittedTxId;
    long committedTxId;

    public RequestInfoProto() {
      committedTxId = -1; // optional
    }

    @Override
    public void write(DataOutput out) throws IOException {
      journalId.write(out);
      out.writeLong(epoch);
      out.writeLong(ipcSerialNumber);
      
      out.writeBoolean(hasCommittedTxId);
      if (hasCommittedTxId)
        out.writeLong(committedTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      journalId = JournalIdProto.read(in);
      epoch = in.readLong();
      ipcSerialNumber = in.readLong();
      
      hasCommittedTxId = in.readBoolean();
      if (hasCommittedTxId)
        committedTxId = in.readLong();
    }

    public static RequestInfoProto read(DataInput in) throws IOException {
      RequestInfoProto reqInfo = new RequestInfoProto();
      reqInfo.readFields(in);
      return reqInfo;
    }
  }

  public static class SegmentStateProto implements Writable {
    long startTxId;
    long endTxId;
    boolean isInProgress;
    
    public SegmentStateProto() {  }
    
    public SegmentStateProto(long startTxId, long endTxId, boolean isInProgress) {
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.isInProgress = isInProgress;
    }
    
    public long getStartTxId() {
      return startTxId;
    }
    
    public long getEndTxId() {
      return endTxId;
    }
    
    public boolean getIsInProgress() {
      return isInProgress;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(startTxId);
      out.writeLong(endTxId);
      out.writeBoolean(isInProgress);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      startTxId = in.readLong();
      endTxId = in.readLong();
      isInProgress = in.readBoolean();
    }
    
    @Override
    public String toString() {
      return "(" + startTxId + ", " + endTxId + ", " + isInProgress + ")";
    }
  }

  public static class PersistedRecoveryPaxosData implements Writable {
    SegmentStateProto segmentState;
    long acceptedInEpoch;
    
    public SegmentStateProto getSegmentState() {
      return segmentState;
    }
    
    public void setSegmentState(SegmentStateProto segmentState) {
      this.segmentState = segmentState;
    }
    
    public long getAcceptedInEpoch() {
      return acceptedInEpoch;
    }
    
    public void setAcceptedInEpoch(long acceptedInEpoch) {
      this.acceptedInEpoch = acceptedInEpoch;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      segmentState.write(out);
      out.writeLong(acceptedInEpoch);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      segmentState = new SegmentStateProto();
      segmentState.readFields(in);
      acceptedInEpoch = in.readLong();
    }
    
    @Override
    public String toString() {
      return segmentState + ", epoch: " + acceptedInEpoch;
    }

    public static PersistedRecoveryPaxosData parseDelimitedFrom(InputStream in)
        throws IOException {
      PersistedRecoveryPaxosData ret = new PersistedRecoveryPaxosData();
      ret.readFields(new DataInputStream(in));
      return ret;
    }

    public void writeDelimitedTo(OutputStream fos) throws IOException {
      this.write(new DataOutputStream(fos));
    }
  }

  public static class JournalRequestProto implements Writable {
    RequestInfoProto reqInfo;
    long firstTxnId;
    int numTxns;
    byte[] records;
    long segmentTxnId;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      out.writeLong(firstTxnId);
      out.writeInt(numTxns);
      out.writeInt(records.length);
      out.write(records);
      out.writeLong(segmentTxnId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      reqInfo.readFields(in);
      firstTxnId = in.readLong();
      numTxns = in.readInt();
      records = new byte[in.readInt()];
      in.readFully(records);
      segmentTxnId = in.readLong();
    }
  }

  public static class JournalResponseProto implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class HeartbeatRequestProto implements Writable {
    RequestInfoProto reqInfo;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
    }
  }

  public static class HeartbeatResponseProto implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class StartLogSegmentRequestProto implements Writable {
    RequestInfoProto reqInfo;
    long txid;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      out.writeLong(txid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      txid = in.readLong();
    }
  }

  public static class FinalizeLogSegmentRequestProto implements Writable {
    RequestInfoProto reqInfo;
    long startTxId;
    long endTxId;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      out.writeLong(startTxId);
      out.writeLong(endTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      startTxId = in.readLong();
      endTxId = in.readLong();
    }
  }

  public static class PurgeLogsRequestProto implements Writable {
    RequestInfoProto reqInfo;
    long minTxIdToKeep;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      out.writeLong(minTxIdToKeep);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      minTxIdToKeep = in.readLong();
    }
  }

  public static class PurgeLogsResponseProto implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class IsFormattedRequestProto implements Writable {
    JournalIdProto jid;

    @Override
    public void write(DataOutput out) throws IOException {
      jid.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jid = JournalIdProto.read(in);
    }
  }

  public static class IsFormattedResponseProto implements Writable {
    boolean isFormatted;

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(isFormatted);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      isFormatted = in.readBoolean();
    }
  }

  public static class GetJournalStateRequestProto implements Writable {
    JournalIdProto jid;

    @Override
    public void write(DataOutput out) throws IOException {
      jid.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jid = JournalIdProto.read(in);
    }
  }

  public static class GetJournalStateResponseProto implements Writable {
    long lastPromisedEpoch;
    int httpPort;
    
    public long getLastPromisedEpoch() {
      return lastPromisedEpoch;
    }
    
    public void setLastPromisedEpoch(long lastPromisedEpoch) {
      this.lastPromisedEpoch = lastPromisedEpoch;
    }
    
    public void setHttpPort(int httpPort) {
      this.httpPort = httpPort;
    }
    
    public int getHttpPort() {
      return httpPort;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(lastPromisedEpoch);
      out.writeInt(httpPort);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      lastPromisedEpoch = in.readLong();
      httpPort = in.readInt();
    }
  }

  public static class FormatRequestProto implements Writable {
    JournalIdProto jid;
    String nsInfo;

    @Override
    public void write(DataOutput out) throws IOException {
      jid.write(out);
      Text.writeStringOpt(out, nsInfo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jid = JournalIdProto.read(in);
      nsInfo = Text.readStringOpt(in);
    }
  }

  public static class FormatResponseProto implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class NewEpochRequestProto implements Writable {
    JournalIdProto jid;
    String nsInfo;
    long epoch;

    @Override
    public void write(DataOutput out) throws IOException {
      jid.write(out);
      Text.writeStringOpt(out, nsInfo);
      out.writeLong(epoch);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jid = JournalIdProto.read(in);
      nsInfo = Text.readStringOpt(in);
      epoch = in.readLong();
    }
  }

  public static class NewEpochResponseProto implements Writable {
    boolean hasLastSegmentTxId;
    long lastSegmentTxId;
    
    public long getLastSegmentTxId() {
      return lastSegmentTxId;
    }
    
    public void setLastSegmentTxId(long lastSegmentTxId) {
      this.hasLastSegmentTxId = true;
      this.lastSegmentTxId = lastSegmentTxId;
    }
    
    public boolean hasLastSegmentTxId() {
      return hasLastSegmentTxId;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(hasLastSegmentTxId);
      if (hasLastSegmentTxId)
        out.writeLong(lastSegmentTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      hasLastSegmentTxId = in.readBoolean();
      if (hasLastSegmentTxId) 
        lastSegmentTxId = in.readLong();
    }
  }

  public static class GetEditLogManifestRequestProto implements Writable {
    JournalIdProto jid;
    long sinceTxId;

    @Override
    public void write(DataOutput out) throws IOException {
      jid.write(out);
      out.writeLong(sinceTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jid = JournalIdProto.read(in);
      sinceTxId = in.readLong();
    }
  }

  public static class GetEditLogManifestResponseProto implements Writable {
    List<RemoteEditLog> logs;
    int httpPort;
    
    public List<RemoteEditLog> getLogs() {
      return logs;
    }
    
    public void setLogs(List<RemoteEditLog> logs) {
      this.logs = logs;
    }
    
    public int getHttpPort() {
      return httpPort;
    }
    
    public void setHttpPort(int httpPort) {
      this.httpPort = httpPort;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(logs == null ? 0 : logs.size());
      if (logs != null) {
        for (RemoteEditLog rel : logs) {
          rel.write(out);
        }
      }
      out.writeInt(httpPort);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      logs = new ArrayList<RemoteEditLog>(len);
      for (int i = 0; i < len; i++) {
        RemoteEditLog rel = new RemoteEditLog();
        rel.readFields(in);
        logs.add(rel);
      }
      httpPort = in.readInt();
    }
  }
  
  public static class GetImageManifestResponseProto implements Writable {
    private List<RemoteImage> images;
    
    public List<RemoteImage> getImages() {
      return images;
    }
    
    public void setImages(List<RemoteImage> images) {
      this.images = images;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(images == null ? 0 : images.size());
      if (images != null) {
        for (RemoteImage ri : images) {
          ri.write(out);
        }
      }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      images = new ArrayList<RemoteImage>(len);
      for (int i = 0; i < len; i++) {
        RemoteImage rel = new RemoteImage();
        rel.readFields(in);
        images.add(rel);
      }
    }
  }
  
  public static class GetStorageStateProto implements Writable {
    private StorageState state;
    private StorageInfo storageInfo;
    
    public GetStorageStateProto() {}
    
    public GetStorageStateProto(StorageState state, StorageInfo storageInfo) {
      this.state = state;
      this.storageInfo = storageInfo;
    }
    
    public StorageInfo getStorageInfo() {
      return storageInfo;
    }
    
    public StorageState getStorageState() {
      return state;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeEnum(out, state);
      storageInfo.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      state = WritableUtils.readEnum(in, StorageState.class);
      storageInfo = new StorageInfo();
      storageInfo.readFields(in);
    }
    
    @Override
    public String toString() {
      return "State: " + state + ", storageInfo: ("
          + storageInfo.toColonSeparatedString() + ")";
    }
  }

  public static class PrepareRecoveryRequestProto implements Writable {
    RequestInfoProto reqInfo;
    long segmentTxId;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      out.writeLong(segmentTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      segmentTxId = in.readLong();
    }
  }

  public static class PrepareRecoveryResponseProto implements Writable {
    // optional
    boolean hasSegmentState;
    SegmentStateProto segmentState;
    
    // optional
    boolean hasAcceptedInEpoch;
    long acceptedInEpoch;
    
    long lastWriterEpoch;
    
    // optional
    boolean hasLastCommitedTxId;
    long lastCommittedTxId;
    
    public boolean hasSegmentState() {
      return hasSegmentState;
    }
    
    public SegmentStateProto getSegmentState() {
      return segmentState;
    }
    
    public void setSegmentState(SegmentStateProto segmentState) {
      this.hasSegmentState = true;
      this.segmentState = segmentState;
    }
    
    public boolean hasAcceptedInEpoch() {
      return hasAcceptedInEpoch;
    }
    
    public long getAcceptedInEpoch() {
      return acceptedInEpoch;
    }
    
    public void setAcceptedInEpoch(long acceptedInEpoch) {
      this.hasAcceptedInEpoch = true;
      this.acceptedInEpoch = acceptedInEpoch;
    }

    public boolean hasLastCommittedTxId() {
      return hasLastCommitedTxId;
    }
    
    public long getLastCommittedTxId() {
      return lastCommittedTxId;
    }
    
    public void setLastCommittedTxId(long lastCommittedTxId) {
      this.hasLastCommitedTxId = true;
      this.lastCommittedTxId = lastCommittedTxId;
    }
    
    public long getLastWriterEpoch() {
      return lastWriterEpoch;
    }
    
    public void setLastWriterEpoch(long lastWriterEpoch) {
      this.lastWriterEpoch = lastWriterEpoch;
    }

    public PrepareRecoveryResponseProto() {
      lastWriterEpoch = lastCommittedTxId = acceptedInEpoch = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(hasSegmentState);
      if (hasSegmentState)
        segmentState.write(out);
      
      out.writeBoolean(hasAcceptedInEpoch);
      if (hasAcceptedInEpoch) 
        out.writeLong(acceptedInEpoch);
      
      out.writeLong(lastWriterEpoch);
      
      out.writeBoolean(hasLastCommitedTxId);
      if (hasLastCommitedTxId)
        out.writeLong(lastCommittedTxId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      hasSegmentState = in.readBoolean();
      if (hasSegmentState) {
        segmentState = new SegmentStateProto();
        segmentState.readFields(in);
      }
      
      hasAcceptedInEpoch = in.readBoolean();
      if (hasAcceptedInEpoch)
        acceptedInEpoch = in.readLong();
      
      lastWriterEpoch = in.readLong();
      
      hasLastCommitedTxId = in.readBoolean();
      if (hasLastCommitedTxId)
        lastCommittedTxId = in.readLong();
    }
    
    @Override
    public String toString() {
      String ret = "Segment state: " + (hasSegmentState ? segmentState : " null ") +",";
      ret += "AC: " + acceptedInEpoch + ", LV: " + lastWriterEpoch + ", CM: " + lastCommittedTxId;
      return ret;      
    }
  }

  public static class AcceptRecoveryRequestProto implements Writable {
    RequestInfoProto reqInfo;
    SegmentStateProto stateToAccept;
    String fromURL;

    @Override
    public void write(DataOutput out) throws IOException {
      reqInfo.write(out);
      stateToAccept.write(out);
      Text.writeStringOpt(out, fromURL);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reqInfo = RequestInfoProto.read(in);
      stateToAccept = new SegmentStateProto();
      stateToAccept.readFields(in);
      fromURL = Text.readStringOpt(in);
    }
  }

  public static class AcceptRecoveryResponseProto implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }
}

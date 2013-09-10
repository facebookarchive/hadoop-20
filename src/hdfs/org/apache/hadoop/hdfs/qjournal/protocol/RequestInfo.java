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
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.Writable;

@InterfaceAudience.Private
public class RequestInfo implements Writable {
  private byte[] jid;
  private long epoch;
  private long ipcSerialNumber;
  private long committedTxId;
  
  public RequestInfo() {
    
  }
  
  public RequestInfo(byte[] jid, long epoch, long ipcSerialNumber,
      long committedTxId) {
    this.jid = jid;
    this.epoch = epoch;
    this.ipcSerialNumber = ipcSerialNumber;
    this.committedTxId = committedTxId;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  public byte[] getJournalId() {
    return jid;
  }

  public long getIpcSerialNumber() {
    return ipcSerialNumber;
  }

  public void setIpcSerialNumber(long ipcSerialNumber) {
    this.ipcSerialNumber = ipcSerialNumber;
  }

  public long getCommittedTxId() {
    return committedTxId;
  }

  public boolean hasCommittedTxId() {
    return (committedTxId != HdfsConstants.INVALID_TXID);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(jid.length);
    out.write(jid, 0, jid.length);
    out.writeLong(epoch);
    out.writeLong(ipcSerialNumber);
    out.writeLong(committedTxId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    jid = new byte[in.readShort()];
    in.readFully(jid, 0, jid.length);
    epoch = in.readLong();
    ipcSerialNumber = in.readLong();
    committedTxId = in.readLong();
  }
}

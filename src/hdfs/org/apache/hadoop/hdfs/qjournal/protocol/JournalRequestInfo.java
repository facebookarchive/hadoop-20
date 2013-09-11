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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FastWritableRegister.FastWritable;
import org.apache.hadoop.io.FastWritableRegister;
import org.apache.hadoop.io.ObjectWritable;

/**
 * Used for journaling edits. We send only a single object, to avoid overhead
 * for hadoop rpc.
 */
public final class JournalRequestInfo extends RequestInfo implements
    FastWritable {
  // current segment txid
  long segmentTxId;
  // id of the first transaction
  long firstTxnId;
  // number of transactions
  int numTxns;
  // transactions in serialized form
  byte[] records;

  public JournalRequestInfo() {
    super();
  }

  public JournalRequestInfo(byte[] jid, long epoch, long ipcSerialNumber,
      long committedTxId, long segmentTxId, long firstTxdId, int numTxns,
      byte[] records) {
    super(jid, epoch, ipcSerialNumber, committedTxId);
    this.segmentTxId = segmentTxId;
    this.firstTxnId = firstTxdId;
    this.numTxns = numTxns;
    this.records = records;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(segmentTxId);
    out.writeLong(firstTxnId);
    out.writeInt(numTxns);
    out.writeInt(records.length);
    out.write(records);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    segmentTxId = in.readLong();
    firstTxnId = in.readLong();
    numTxns = in.readInt();
    records = new byte[in.readInt()];
    in.readFully(records, 0, records.length);
  }
  
  @Override
  public String toString() {
    return "segmentTxId: " + segmentTxId + ", firstTxnId:" + firstTxnId + 
        ", numTxns: " + numTxns + ", length of data: " + records.length;
  }

  public long getSegmentTxId() {
    return segmentTxId;
  }

  public long getFirstTxId() {
    return firstTxnId;
  }

  public int getNumTxns() {
    return numTxns;
  }

  public byte[] getRecords() {
    return records;
  }

  // FastWritable
  @Override
  public FastWritable getFastWritableInstance(Configuration conf) {
    return new JournalRequestInfo();
  }

  @Override
  public byte[] getSerializedName() {
    return nameSerialized;
  }

  // singleton instance used in the FastWritable registry
  private static final JournalRequestInfo instance = new JournalRequestInfo();
  // unique id for this class
  private static final FastWritableRegister.FastWritableId classId
    = FastWritableRegister.FastWritableId.SERIAL_VERSION_ID_1;
  // serialized id
  private static final byte[] nameSerialized = ObjectWritable
      .prepareCachedNameBytes(classId.toString());

  // register this class to FastWritable registry
  public static void init() {
    FastWritableRegister.register(classId, instance);
  }

  // for clients which might not explicitly call init()
  // we initialize here
  static {
    init();
  }
}

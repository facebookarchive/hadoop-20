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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto;

import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.EditLogLedgerMetadata;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Compact binary representation for {@link EditLogLedgerMetadata} for
 * e.g., storage in a ZNode.
 * @see Writable
 */
public class EditLogLedgerMetadataWritable implements Writable {

  private int logVersion;
  private long ledgerId;
  private long firstTxId;
  private long lastTxId;

  /**
   * Update internal state with a specified EditLogLedgerMetadata before
   * serializing with {@link #write(DataOutput)}
   * @param metadata The metadata to serialize to a byte array
   */
  public void set(EditLogLedgerMetadata metadata) {
    logVersion = metadata.getLogVersion();
    ledgerId = metadata.getLedgerId();
    firstTxId = metadata.getFirstTxId();
    lastTxId = metadata.getLastTxId();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(logVersion);
    out.writeLong(ledgerId);
    out.writeLong(firstTxId);
    out.writeLong(lastTxId);
  }

  /**
   * Build and return a EditLogLedgerMetadata object. Note: make sure to call
   * {@link #readFields(DataInput)} prior to calling this method
   * @return The metadata object constructed from internal state serialized
   *         from a byte array
   */
  public EditLogLedgerMetadata get() {
    return new EditLogLedgerMetadata(logVersion, ledgerId, firstTxId, lastTxId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    logVersion = in.readInt();
    ledgerId = in.readLong();
    firstTxId = in.readLong();
    lastTxId = in.readLong();
  }
}

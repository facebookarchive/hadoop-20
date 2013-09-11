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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

/**
 * Class representing a single remote image, together with its MD5 hash
 */
public class RemoteImage implements Writable, Comparable<RemoteImage> {

  private long txId = HdfsConstants.INVALID_TXID;
  private MD5Hash digest = null;

  public RemoteImage() {
  }

  public RemoteImage(long startTxId, MD5Hash digest) {
    Preconditions.checkArgument(startTxId >= -1);
    this.txId = startTxId;
    this.digest = digest;
  }

  public long getTxId() {
    return txId;
  }

  public MD5Hash getDigest() {
    return digest;
  }

  public boolean isValid() {
    return digest != null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(txId);
    digest.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    txId = in.readLong();
    digest = new MD5Hash();
    digest.readFields(in);
  }

  @Override
  public int compareTo(RemoteImage im) {
    return ComparisonChain.start().compare(txId, im.txId).result();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RemoteImage))
      return false;
    return this.compareTo((RemoteImage) o) == 0;
  }

  @Override
  public int hashCode() {
    return new Long(txId).hashCode();
  }

  @Override
  public String toString() {
    return "[txid: " + txId + ", md5: " + digest + "]";
  }
}

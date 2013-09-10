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

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Serializable data for {@link StorageInfo} for storage in ZooKeeper.
 * Adds a field to represent the version of ZooKeeper metadata layout.
 * @see {@link StorageInfo}
 */
public class FormatInfoWritable extends StorageInfo implements Writable {

  // Version of the protocol for serializing de-serializing data in znodes
  int protoVersion;

  /**
   * Sets the StorageInfo and associated protocol to be serialized and
   * written to ZooKeeper.
   * @param protoVersion Protocol version to be written to ZooKeeper
   * @param si The storage information object that represents the namespace
   *           information
   * @see {@link #getProtoVersion()}, {@link StorageInfo}
   */
  public void set(int protoVersion, StorageInfo si) {
    super.setStorageInfo(si);
    this.protoVersion = protoVersion;
  }

  /**
   * Return the version of the ZooKeeper metadata layout and serialization
   * protocol. Used to verify that that the metadata in ZooKeeper is compatible
   * with the metadata we expect to read.
   * @return The version of the metadata in ZooKeeper
   */
  public int getProtoVersion() {
    return protoVersion;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(protoVersion);
    out.writeInt(layoutVersion);
    out.writeInt(namespaceID);
    out.writeLong(cTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    protoVersion = in.readInt();
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    cTime = in.readLong();
  }
}

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

import java.io.*;

import org.apache.hadoop.io.*;

public class VersionAndOpcode implements Writable {
  protected int dataTransferVersion;
  protected byte opCode;

  public VersionAndOpcode() {    
  }
  
  public VersionAndOpcode(final int dataTransferVersion, final byte opCode) {
    this.dataTransferVersion = dataTransferVersion;
    this.opCode = opCode;
  }
  
  public int getDataTransferVersion() {
    return dataTransferVersion;
  }

  public byte getOpCode() {
    return opCode;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(dataTransferVersion);
    out.write(opCode);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dataTransferVersion = in.readShort();
    if ( dataTransferVersion > DataTransferProtocol.DATA_TRANSFER_VERSION ||
        dataTransferVersion < DataTransferProtocol.BACKWARD_COMPATIBLE_VERSION) {
      throw new IOException( "Version Mismatch. Expected" +
          DataTransferProtocol.DATA_TRANSFER_VERSION +
          " or earlier, Received " + dataTransferVersion);
    }
    opCode = in.readByte();
  }
}

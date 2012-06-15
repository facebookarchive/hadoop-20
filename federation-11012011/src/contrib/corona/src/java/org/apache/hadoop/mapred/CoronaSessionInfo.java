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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class CoronaSessionInfo implements Writable {
  String sessionHandle;
  InetSocketAddress jobTrackerAddr;

  public CoronaSessionInfo() { }
  
  public CoronaSessionInfo(String sessionHandle,
      InetSocketAddress jobTrackerAddr) {
    this.sessionHandle = sessionHandle;
    this.jobTrackerAddr = jobTrackerAddr;
  }

  public String getSessionHandle() {
    return sessionHandle;
  }

  public InetSocketAddress getJobTrackerAddr() {
    return jobTrackerAddr;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, sessionHandle);
    WritableUtils.writeString(out, jobTrackerAddr.getHostName());
    WritableUtils.writeVInt(out, jobTrackerAddr.getPort());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.sessionHandle = WritableUtils.readString(in);
    String hostName = WritableUtils.readString(in);
    int port = WritableUtils.readVInt(in);
    this.jobTrackerAddr = new InetSocketAddress(hostName, port);
  }
}

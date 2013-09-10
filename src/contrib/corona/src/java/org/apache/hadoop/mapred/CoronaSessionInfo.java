/*
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

/**
 * Descriptor of the corona session
 */
public class CoronaSessionInfo implements Writable {
  /** Number of tcp ports */
  private final static int TCP_PORT_NUM = 1<<16;
  /** The string handle of the session */
  private String sessionHandle;
  /** The address of the job tracker RPC server */
  private InetSocketAddress jobTrackerAddr;
  /** The adress of secondary tracker to fallback,
   * It is the local job tracker address
   * */
  private InetSocketAddress secondaryTrackerAddr = null;

  /**
   * The default constructor for the session info
   */
  public CoronaSessionInfo() {
  }

  /**
   * CoronaSessionInfo constructor given the handle and the address of teh
   * jobTracker rpc server
   * @param sessionHandle the handle of the session
   * @param jobTrackerAddr the address of the rpc server
   */
  public CoronaSessionInfo(String sessionHandle,
      InetSocketAddress jobTrackerAddr) {
    this.sessionHandle = sessionHandle;
    this.jobTrackerAddr = jobTrackerAddr;
  }

  /**
   * CoronaSessionInfo constructor given the handle and the addresses of the
   * jobTracker and secondary tracker RPC server
   * @param sessionHandle the handle of the session
   * @param jobTrackerAddr the address of the rpc server
   * @param secondaryTrackerAddr address for obtaining new job tracker address
   */
  public CoronaSessionInfo(String sessionHandle,
      InetSocketAddress jobTrackerAddr,
      InetSocketAddress secondaryTrackerAddr) {
    this.sessionHandle = sessionHandle;
    this.jobTrackerAddr = jobTrackerAddr;
    this.secondaryTrackerAddr = secondaryTrackerAddr;
  }

  public String getSessionHandle() {
    return sessionHandle;
  }

  public InetSocketAddress getJobTrackerAddr() {
    return jobTrackerAddr;
  }

  public InetSocketAddress getSecondaryTracker() {
    return secondaryTrackerAddr;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, sessionHandle);
    WritableUtils.writeString(out, jobTrackerAddr.getHostName());
    int port = jobTrackerAddr.getPort() % TCP_PORT_NUM;
    //Because the port number is a 2 bytes unsigned integer. 
    //When there is a secondaryTrackerAddr followed, 
    //we just make the current port to be port + TCP_PORT_NUM
    //otherwise we do nothing.
    if (secondaryTrackerAddr != null) {
      port += TCP_PORT_NUM;
      WritableUtils.writeVInt(out, port);
      InetSocketAddressWritable.writeAddress(out, secondaryTrackerAddr);
    } else {
      WritableUtils.writeVInt(out, port);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.sessionHandle = WritableUtils.readString(in);
    String hostName = WritableUtils.readString(in);
    int port = WritableUtils.readVInt(in);
    this.jobTrackerAddr = new InetSocketAddress(hostName, port % TCP_PORT_NUM);
    //When decoding, we just need to check if port / TCP_PORT_NUM > 0, 
    //if so, it means there is a secondaryTrackerAddr followed to be decoded.
    if (port / TCP_PORT_NUM > 0) {
      this.secondaryTrackerAddr = InetSocketAddressWritable.readAddress(in);
    }
  }

  /**
   * Encapsulates InetSocketAddress as Writable type
   */
  public static class InetSocketAddressWritable implements Writable {
    /** Encapsulated address */
    private InetSocketAddress address;

    /**
     * Default c'tor
     */
    public InetSocketAddressWritable() {
    }

    /**
     * Encapsulating c'tor
     * @param address an address to encapsulate
     */
    public InetSocketAddressWritable(InetSocketAddress address) {
      this.address = address;
    }

    public InetSocketAddress getAddress() {
      return address;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      writeAddress(out, address);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.address = readAddress(in);
    }

    /**
     * Writes InetSocketAddress to data out
     * @param out data output
     * @param addr InetSocketAddress
     * @throws IOException
     */
    public static void writeAddress(DataOutput out, InetSocketAddress addr)
        throws IOException {
      WritableUtils.writeString(out, addr.getHostName());
      WritableUtils.writeVInt(out, addr.getPort());
    }

    /**
     * Reads InetSocketAddress from data input
     * @param in data input
     * @return InetSocketAddress
     * @throws IOException
     */
    public static InetSocketAddress readAddress(DataInput in)
        throws IOException {
      String hostName = WritableUtils.readString(in);
      int port = WritableUtils.readVInt(in);
      return new InetSocketAddress(hostName, port);
    }

  }

}

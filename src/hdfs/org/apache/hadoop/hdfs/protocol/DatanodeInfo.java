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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

/** 
 * DatanodeInfo represents the status of a DataNode.
 * This object is used for communication in the
 * Datanode Protocol and the Client Protocol.
 */
@ThriftStruct
public class DatanodeInfo extends DatanodeID implements Node {
  protected long capacity;
  protected long dfsUsed;
  protected long remaining;
  protected long namespaceUsed;
  protected long lastUpdate;
  protected int xceiverCount;
  protected String location = NetworkTopology.DEFAULT_RACK;
  private static volatile boolean suspectNodes = true;

  /** HostName as suplied by the datanode during registration as its 
   * name. Namenode uses datanode IP address as the name.
   */
  protected String hostName = null;

  public static void write(DataOutput out, DatanodeInfo elem) throws IOException {
    DatanodeInfo node = new DatanodeInfo(elem);
    node.write(out);
  }

  public static DatanodeInfo read(DataInput in) throws IOException {
    DatanodeInfo node = new DatanodeInfo();
    node.readFields(in);
    return node;
  }

  public static void writeList(DataOutput out, List<DatanodeInfo> list) throws IOException {
    if (list != null) {
      out.writeInt(list.size());
      for (DatanodeInfo elem : list) {
        DatanodeInfo.write(out, elem);
      }
    } else {
      out.writeInt(-1);
    }
  }

  public static List<DatanodeInfo> readList(DataInput in) throws IOException {
    final int listSize = in.readInt();
    List<DatanodeInfo> list = (listSize < 0) ? null : new ArrayList<DatanodeInfo>(listSize);
    for (int i = 0; i < listSize; i++) {
      list.add(DatanodeInfo.read(in));
    }
    return list;
  }

  // administrative states of a datanode
  public enum AdminStates {
    NORMAL("In Service"), 
    DECOMMISSION_INPROGRESS("Decommission In Progress"), 
    DECOMMISSIONED("Decommissioned");

    final String value;

    AdminStates(final String v) {
      this.value = v;
    }

    public String toString() {
      return value;
    }
  }
  
  protected AdminStates adminState;
  protected boolean suspectFail = false;
  


  public DatanodeInfo() {
    super();
    adminState = null;
  }
  
  public DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.namespaceUsed = from.getNamespaceUsed();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.adminState;
    this.hostName = from.hostName;
    this.suspectFail = from.suspectFail;
  }

  public DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.namespaceUsed = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }

  @ThriftConstructor
  // Last ThriftField used in superclass: 4
  public DatanodeInfo(@ThriftField(1) String name, @ThriftField(2) String storageID,
      @ThriftField(3) int infoPort, @ThriftField(4) int ipcPort, @ThriftField(6) long capacity,
      @ThriftField(7) long dfsUsed, @ThriftField(8) long remaining,
      @ThriftField(9) long namespaceUsed, @ThriftField(10) long lastUpdate,
      @ThriftField(11) int xceiverCount, @ThriftField(12) String networkLocation,
      @ThriftField(13) String hostName, @ThriftField(14) int adminStateOrdinal) {
    super(name, storageID, infoPort, ipcPort);
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.namespaceUsed = namespaceUsed;
    this.lastUpdate = lastUpdate;
    this.xceiverCount = xceiverCount;
    this.location = networkLocation;
    this.hostName = hostName;
    setAdminState(AdminStates.values()[adminStateOrdinal]);
  }

  protected DatanodeInfo(DatanodeID nodeID, String location, String hostName) {
    this(nodeID);
    this.location = location;
    this.hostName = hostName;
  }

  @ThriftField(5)
  public DatanodeID getDatanodeID() {
    return new DatanodeID(this);
  }

  public boolean isSuspectFail() {
    return suspectFail;
  }

  public void setSuspectFail(boolean suspectFail) {
    this.suspectFail = suspectFail;
  }

  /** The raw capacity. */
  @ThriftField(6)
  public long getCapacity() { return capacity; }
  
  /** The used space by the data node. */
  @ThriftField(7)
  public long getDfsUsed() { return dfsUsed; }

  /** The used space by the data node. */
  public long getNonDfsUsed() { 
    long nonDFSUsed = capacity - dfsUsed - remaining;
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /** The used space by the data node as percentage of present capacity */
  public float getDfsUsedPercent() { 
    if (capacity <= 0) {
      return 100;
    }

    return ((float)dfsUsed * 100.0f)/(float)capacity; 
  }

  @ThriftField(9)
  public long getNamespaceUsed() { return namespaceUsed; }
  
  /** The used space by the data node as percentage of present capacity */
  public float getNamespaceUsedPercent() { 
    if (capacity <= 0) {
      return 100;
    }

    return ((float)namespaceUsed * 100.0f)/(float)capacity; 
  }

  /** The raw free space. */
  @ThriftField(8)
  public long getRemaining() { return remaining; }

  /** The remaining space as percentage of configured capacity. */
  public float getRemainingPercent() { 
    if (capacity <= 0) {
      return 0;
    }

    return ((float)remaining * 100.0f)/(float)capacity; 
  }

  /** The time when this information was accurate. */
  @ThriftField(10)
  public long getLastUpdate() { return lastUpdate; }

  /** number of active connections */
  @ThriftField(11)
  public int getXceiverCount() { return xceiverCount; }

  /** Sets raw capacity. */
  public void setCapacity(long capacity) { 
    this.capacity = capacity; 
  }

  /** Sets raw free space. */
  public void setRemaining(long remaining) { 
    this.remaining = remaining; 
  }

  /** Sets time when this information was accurate. */
  public void setLastUpdate(long lastUpdate) { 
    this.lastUpdate = lastUpdate; 
  }

  /** Sets number of active connections */
  public void setXceiverCount(int xceiverCount) { 
    this.xceiverCount = xceiverCount; 
  }

  /** rack name **/
  @ThriftField(12)
  public synchronized String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }
  
  @Deprecated
  public String getHostName() {
    return (hostName == null || hostName.length()==0) ? getHost() : hostName;
  }

  @ThriftField(value = 13, name = "hostName")
  public String getTHostName() {
    return hostName;
  }

  public void setHostName(String host) {
    hostName = host;
  }
  
  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    long nonDFSUsed = getNonDfsUsed();
    float usedPercent = getDfsUsedPercent();
    float remainingPercent = getRemainingPercent();

    buffer.append("Name: "+name+"\n");
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
    buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
    buffer.append("Non DFS Used: "+nonDFSUsed+" ("+StringUtils.byteDesc(nonDFSUsed)+")"+"\n");
    buffer.append("DFS Remaining: " +r+ "("+StringUtils.byteDesc(r)+")"+"\n");
    buffer.append("DFS Used%: "+StringUtils.limitDecimalTo2(usedPercent)+"%\n");
    buffer.append("DFS Remaining%: "+StringUtils.limitDecimalTo2(remainingPercent)+"%\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  public String dumpDatanode() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    buffer.append(name);
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + StringUtils.byteDesc(c)+")");
    buffer.append(" " + u + "(" + StringUtils.byteDesc(u)+")");
    buffer.append(" " + StringUtils.limitDecimalTo2(((1.0*u)/c)*100)+"%");
    buffer.append(" " + r + "(" + StringUtils.byteDesc(r)+")");
    buffer.append(" " + new Date(lastUpdate));
    return buffer.toString();
  }

  /**
   * Start decommissioning a node.
   * old state.
   */
  public void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Stop decommissioning a node.
   * old state.
   */
  public void stopDecommission() {
    adminState = null;
  }

  /**
   * Returns true if the node is in the process of being decommissioned
   */
  public boolean isDecommissionInProgress() {
    if (adminState == AdminStates.DECOMMISSION_INPROGRESS) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the node has been decommissioned.
   */
  public boolean isDecommissioned() {
    if (adminState == AdminStates.DECOMMISSIONED) {
      return true;
    }
    return false;
  }

  /**
   * Sets the admin state to indicate that decommision is complete.
   */
  public void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  /**
   * Retrieves the admin state of this node.
   */
  public AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  @ThriftField(14)
  public int getAdminStateOrdinal() {
    return getAdminState().ordinal();
  }

  /**
   * Sets the admin state of this node.
   */
  protected void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private int level; //which level of the tree the node resides
  private Node parent; //its parent

  /** Return this node's parent */
  public Node getParent() { return parent; }
  public void setParent(Node parent) {this.parent = parent;}
   
  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel() { return level; }
  public void setLevel(int level) {this.level = level;}

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfo(); }
       });
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    out.writeShort(ipcPort);

    out.writeLong(capacity);
    out.writeLong(dfsUsed);
    out.writeLong(remaining);
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
    Text.writeStringOpt(out, location);
    Text.writeStringOpt(out, hostName == null? "": hostName);
    WritableUtils.writeEnum(out, getAdminState());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    this.ipcPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readStringOpt(in);
    this.hostName = Text.readStringOpt(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }

  public static boolean shouldSuspectNodes() {
    return suspectNodes;
  }

  public static void enableSuspectNodes() {
    DatanodeInfo.suspectNodes = true;
  }
  
  public static void disableSuspectNodes() {
    DatanodeInfo.suspectNodes = false;
  }
}

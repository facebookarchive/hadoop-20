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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import java.util.EnumMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;

import org.apache.hadoop.io.ArrayOutputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from FSEditLogOp and are only
 * instantiated from Reader#readOp()
 */
public abstract class FSEditLogOp {
  public static final Log LOG = LogFactory.getLog(FSEditLogOp.class);
  public final FSEditLogOpCodes opCode;
  long txid;

  /**
   * Opcode size is limited to 1.5 megabytes
   */
  public static final int MAX_OP_SIZE = (3 * 1024 * 1024) / 2;

  @SuppressWarnings("deprecation")
  private static ThreadLocal<EnumMap<FSEditLogOpCodes, FSEditLogOp>> opInstances =
    new ThreadLocal<EnumMap<FSEditLogOpCodes, FSEditLogOp>>() {
      @Override
      protected EnumMap<FSEditLogOpCodes, FSEditLogOp> initialValue() {
        EnumMap<FSEditLogOpCodes, FSEditLogOp> instances 
          = new EnumMap<FSEditLogOpCodes, FSEditLogOp>(FSEditLogOpCodes.class);
        instances.put(OP_INVALID, new InvalidOp());
        instances.put(OP_ADD, new AddOp());
        instances.put(OP_CLOSE, new CloseOp());
        instances.put(OP_APPEND, new AppendOp());
        instances.put(OP_SET_REPLICATION, new SetReplicationOp());
        instances.put(OP_CONCAT_DELETE, new ConcatDeleteOp());
        instances.put(OP_RENAME, new RenameOp());
        instances.put(OP_DELETE, new DeleteOp());
        instances.put(OP_MKDIR, new MkdirOp());
        instances.put(OP_SET_GENSTAMP, new SetGenstampOp());
        instances.put(OP_DATANODE_ADD, new DatanodeAddOp());
        instances.put(OP_DATANODE_REMOVE, new DatanodeRemoveOp());
        instances.put(OP_SET_PERMISSIONS, new SetPermissionsOp());
        instances.put(OP_SET_OWNER, new SetOwnerOp());
        instances.put(OP_SET_NS_QUOTA, new SetNSQuotaOp());
        instances.put(OP_CLEAR_NS_QUOTA, new ClearNSQuotaOp());
        instances.put(OP_SET_QUOTA, new SetQuotaOp());
        instances.put(OP_TIMES, new TimesOp());
        instances.put(OP_START_LOG_SEGMENT,
                      new LogSegmentOp(OP_START_LOG_SEGMENT));
        instances.put(OP_END_LOG_SEGMENT,
                      new LogSegmentOp(OP_END_LOG_SEGMENT));
        instances.put(OP_HARDLINK, new HardLinkOp());
        instances.put(OP_MERGE, new MergeOp());
        return instances;
      }
  };

  /**
   * Constructor for an EditLog Op. EditLog ops cannot be constructed
   * directly, but only through Reader#readOp.
   */
  private FSEditLogOp(FSEditLogOpCodes opCode) {
    this.opCode = opCode;
    this.txid = 0;
  }

  public long getTransactionId() {
    return txid;
  }

  public void setTransactionId(long txid) {
    this.txid = txid;
  }
  
  @Override
  public String toString(){
    return "Record: txid: " + txid + " opcode: " + opCode;
  }

  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  abstract void writeFields(DataOutput out)
      throws IOException;

  @SuppressWarnings("unchecked")
  public static abstract class AddCloseOp extends FSEditLogOp {
    int length;
    public String path;
    long inodeId;
    short replication;
    long mtime;
    long atime;
    long blockSize;
    BlockInfo[] blocks;
    PermissionStatus permissions;
    String clientName;
    String clientMachine;
    
    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      assert(opCode == OP_ADD || opCode == OP_CLOSE);
    }
    
    public void set(long inodeId,
        String path,
        short replication,
        long mtime,
        long atime,
        long blockSize,
        BlockInfo[] blocks,
        PermissionStatus permissions,
        String clientName,
        String clientMachine) {
      this.inodeId = inodeId;
      this.path = path;
      this.replication = replication;
      this.mtime = mtime;
      this.atime = atime;
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.permissions = permissions;
      this.clientName = clientName;
      this.clientMachine = clientMachine;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(inodeId, out);
      FSImageSerialization.writeShort(replication, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
      FSImageSerialization.writeLong(blockSize, out);
      FSImageSerialization.writeBlocks(blocks, out);
      
      permissions.write(out);

      if (this.opCode == OP_ADD) {
        FSImageSerialization.writeString(clientName,out);
        FSImageSerialization.writeString(clientMachine,out);
      }
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // versions > 0 support per file replication
      // get name and replication
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
      }
      
      if (-7 == logVersion && length != 3||
          -17 < logVersion && logVersion < -7 && length != 4 ||
          (logVersion <= -17 && length != 5 && !LayoutVersion.supports(
              Feature.EDITLOG_OP_OPTIMIZATION, logVersion))) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);
      
      this.inodeId = readInodeId(in, logVersion);
      
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.replication = FSImageSerialization.readShort(in);
        this.mtime = FSImageSerialization.readLong(in);
      } else {
        this.replication = readShort(in);
        this.mtime = readLong(in);
      }

      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          this.atime = FSImageSerialization.readLong(in);
        } else {
          this.atime = readLong(in);
        }
      } else {
        this.atime = 0;
      }
      if (logVersion < -7) {
        if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          this.blockSize = FSImageSerialization.readLong(in);
        } else {
          this.blockSize = readLong(in);
        }
      } else {
        this.blockSize = 0;
      }

      // get blocks
      this.blocks = readBlocks(in, logVersion);

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }

      // clientname, clientMachine and block locations of last block.
      if (this.opCode == OP_ADD && logVersion <= -12) {
        this.clientName = FSImageSerialization.readString(in);
        this.clientMachine = FSImageSerialization.readString(in);
        if (-13 <= logVersion) {
          readDatanodeDescriptorArray(in);
        }
      } else {
        this.clientName = "";
        this.clientMachine = "";
      }
    }

    /** This method is defined for compatibility reason. */
    private static DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in)
        throws IOException {
      DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
        for (int i = 0; i < locations.length; i++) {
          locations[i] = new DatanodeDescriptor();
          locations[i].readFieldsFromFSEditLog(in);
        }
        return locations;
    }

    static BlockInfo[] readBlocks(
        DataInputStream in,
        int logVersion) throws IOException {
      int numBlocks = FSImageSerialization.readInt(in);
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        BlockInfo blk = new BlockInfo();
        if (logVersion <= -14) {
          blk.readFields(in);
          if (LayoutVersion.supports(Feature.BLOCK_CHECKSUM, logVersion)) {
            blk.setChecksum(FSImageSerialization.readInt(in));
          }
        } else {
          BlockTwo oldblk = new BlockTwo();
          oldblk.readFields(in);
          blk.set(oldblk.blkid, oldblk.len,
                  Block.GRANDFATHER_GENERATION_STAMP);
        }
        blocks[i] = blk;
      }
      return blocks;
    }
  }

  public static class AddOp extends AddCloseOp {
    private AddOp() {
      super(OP_ADD);
    }

    public static AddOp getUniqueInstance() {
      return new AddOp();
    }

    static AddOp getInstance() {
      return (AddOp)opInstances.get().get(OP_ADD);
    }
  }

  public static class CloseOp extends AddCloseOp {
    private CloseOp() {
      super(OP_CLOSE);
    }

    public static CloseOp getUniqueInstance() {
      return new CloseOp();
    }

    static CloseOp getInstance() {
      return (CloseOp)opInstances.get().get(OP_CLOSE);
    }
  }
  
  public static class AppendOp extends FSEditLogOp {
    public String path;
    BlockInfo[] blocks;
    String clientName;
    String clientMachine;
    
    private AppendOp() {
      super(OP_APPEND);
    }

    public static AppendOp getUniqueInstance() {
      return new AppendOp();
    }

    static AppendOp getInstance() {
      return (AppendOp)opInstances.get().get(OP_APPEND);
    }
    
    public void set(String path,
        BlockInfo[] blocks,
        String clientName,
        String clientMachine) {
      this.path = path;
      this.blocks = blocks;
      this.clientName = clientName;
      this.clientMachine = clientMachine;
    }
    
    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeBlocks(blocks, out);
      FSImageSerialization.writeString(clientName,out);
      FSImageSerialization.writeString(clientMachine,out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      this.blocks = AddCloseOp.readBlocks(in, logVersion);
      this.clientName = FSImageSerialization.readString(in);
      this.clientMachine = FSImageSerialization.readString(in);
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
    }

    public static SetReplicationOp getUniqueInstance() {
      return new SetReplicationOp();
    }

    static SetReplicationOp getInstance() {
      return (SetReplicationOp)opInstances.get()
        .get(OP_SET_REPLICATION);
    }
    
    void set(String path, short replication) {
      this.path = path;
      this.replication = replication;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShort(replication, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.replication = FSImageSerialization.readShort(in);
      } else {
        this.replication = readShort(in);
      }
    }
  }
  
  static class MergeOp extends FSEditLogOp {
    String parity;
    String source;
    String codecId;
    int[] checksums;
    long timestamp;

    private MergeOp() {
      super(OP_MERGE);
    }

    public static MergeOp getUniqueInstance() {
      return new MergeOp();
    }

    static MergeOp getInstance() {
      return (MergeOp)opInstances.get()
        .get(OP_MERGE);
    }
    
    void set(String parity, String source, String codecId, 
        int[] checksums, long timestamp) {
      this.parity = parity;
      this.source = source;
      this.codecId = codecId;
      this.checksums = checksums;
      this.timestamp = timestamp;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(parity, out);
      FSImageSerialization.writeString(source, out);
      FSImageSerialization.writeString(codecId, out);
      FSImageSerialization.writeInt(checksums.length, out);
      for(int i = 0; i < checksums.length; i++) {
        FSImageSerialization.writeInt(checksums[i], out);
      }
      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.parity = FSImageSerialization.readString(in);
      this.source = FSImageSerialization.readString(in);
      this.codecId = FSImageSerialization.readString(in);
      int numBlocks = FSImageSerialization.readInt(in);
      this.checksums = new int[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        this.checksums[i] = FSImageSerialization.readInt(in);
      }
      this.timestamp = FSImageSerialization.readLong(in);
    }
  }

  static class ConcatDeleteOp extends FSEditLogOp {
    int length;
    String trg;
    String[] srcs;
    long timestamp;

    private ConcatDeleteOp() {
      super(OP_CONCAT_DELETE);
    }

    public static ConcatDeleteOp getUniqueInstance() {
      return new ConcatDeleteOp();
    }

    static ConcatDeleteOp getInstance() {
      return (ConcatDeleteOp)opInstances.get()
        .get(OP_CONCAT_DELETE);
    }
    
    void set (String trg, String[] srcs, long timestamp) {
      this.trg = trg;
      this.srcs = srcs;
      this.timestamp = timestamp;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(trg, out);
      FSImageSerialization.writeInt(srcs.length, out);
      for(int i=0; i<srcs.length; i++) {
        FSImageSerialization.writeString(srcs[i], out);
      }
      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
        if (length < 3) { // trg, srcs.., timestamp
          throw new IOException("Incorrect data format. "
              + "Concat delete operation.");
        }
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = 0;
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        srcSize = FSImageSerialization.readInt(in);
      } else {
        srcSize = this.length - 1 - 1; // trg and timestamp
      }
      this.srcs = new String [srcSize];
      for(int i=0; i<srcSize;i++) {
        srcs[i]= FSImageSerialization.readString(in);
      }
      
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
    }
  }

  static class HardLinkOp extends FSEditLogOp {
    String src;
    String dst;
    long timestamp;

    private HardLinkOp() {
      super(OP_HARDLINK);
    }

    public static HardLinkOp getUniqueInstance() {
      return new HardLinkOp();
    }

    static HardLinkOp getInstance() {
      return (HardLinkOp)opInstances.get().get(OP_HARDLINK);
    }
    
    void set(String src, String dst, long timestamp) {
      this.src = src;
      this.dst = dst;
      this.timestamp = timestamp;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(dst, out);
      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = FSImageSerialization.readLong(in);
    }
  }

  static class RenameOp extends FSEditLogOp {
    int length;
    String src;
    String dst;
    long timestamp;

    private RenameOp() {
      super(OP_RENAME);
    }

    public static RenameOp getUniqueInstance() {
      return new RenameOp();
    }

    static RenameOp getInstance() {
      return (RenameOp)opInstances.get()
        .get(OP_RENAME);
    }
    
    void set(String src, String dst, long timestamp) {
      this.src = src;
      this.dst = dst;
      this.timestamp = timestamp;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(dst, out);
      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
        if (this.length != 3) {
          throw new IOException("Incorrect data format. "
              + "Old rename operation.");
        }
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
    }
  }

  public static class DeleteOp extends FSEditLogOp {
    int length;
    public String path;
    long timestamp;

    private DeleteOp() {
      super(OP_DELETE);
    }

    public static DeleteOp getUniqueInstance() {
      return new DeleteOp();
    }

    static DeleteOp getInstance() {
      return (DeleteOp)opInstances.get()
        .get(OP_DELETE);
    }

    void set(String path, long timestamp) {
      this.path = path;
      this.timestamp = timestamp;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
        if (this.length != 2) {
          throw new IOException("Incorrect data format. " + "delete operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
    }
  }

  public static class MkdirOp extends FSEditLogOp {
    int length;
    public String path;
    long inodeId;
    long timestamp;
    PermissionStatus permissions;

    private MkdirOp() {
      super(OP_MKDIR);
    }

    public static MkdirOp getUniqueInstance() {
      return new MkdirOp();
    }

    static MkdirOp getInstance() {
      return (MkdirOp)opInstances.get()
        .get(OP_MKDIR);
    }
    
    void set(long inodeId, String path, long timestamp, PermissionStatus permissions) {
      this.inodeId = inodeId;
      this.path = path;
      this.timestamp = timestamp;
      this.permissions = permissions;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(inodeId, out);
      FSImageSerialization.writeLong(timestamp, out); // mtime
      FSImageSerialization.writeLong(timestamp, out); // atime, unused at this
      permissions.write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {

      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
      }
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3
          && !LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      

      this.path = FSImageSerialization.readString(in);
      
      this.inodeId = readInodeId(in, logVersion);
      
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        /* unused this.atime = */
        if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          FSImageSerialization.readLong(in);
        } else {
          readLong(in);
        }
      }

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }
    }
  }

  static class SetGenstampOp extends FSEditLogOp {
    long genStamp;

    private SetGenstampOp() {
      super(OP_SET_GENSTAMP);
    }

    public static SetGenstampOp getUniqueInstance() {
      return new SetGenstampOp();
    }

    static SetGenstampOp getInstance() {
      return (SetGenstampOp)opInstances.get()
        .get(OP_SET_GENSTAMP);
    }

    void set(long genStamp) {
      this.genStamp = genStamp;
    }
    
    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeLong(genStamp, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStamp = FSImageSerialization.readLong(in);
    }
  }

  @SuppressWarnings("deprecation")
  static class DatanodeAddOp extends FSEditLogOp {
    private DatanodeAddOp() {
      super(OP_DATANODE_ADD);
    }

    static DatanodeAddOp getInstance() {
      return (DatanodeAddOp)opInstances.get()
        .get(OP_DATANODE_ADD);
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      throw new IOException("Deprecated, should not write");
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      //Datanodes are not persistent any more.
      FSImageSerialization.DatanodeImage.skipOne(in);
    }
  }

  @SuppressWarnings("deprecation")
  static class DatanodeRemoveOp extends FSEditLogOp {
    private DatanodeRemoveOp() {
      super(OP_DATANODE_REMOVE);
    }

    static DatanodeRemoveOp getInstance() {
      return (DatanodeRemoveOp)opInstances.get()
        .get(OP_DATANODE_REMOVE);
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      throw new IOException("Deprecated, should not write");
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      DatanodeID nodeID = new DatanodeID();
      nodeID.readFields(in);
      //Datanodes are not persistent any more.
    }
  }

  static class SetPermissionsOp extends FSEditLogOp {
    String src;
    FsPermission permissions;

    private SetPermissionsOp() {
      super(OP_SET_PERMISSIONS);
    }

    public static SetPermissionsOp getUniqueInstance() {
      return new SetPermissionsOp();
    }

    static SetPermissionsOp getInstance() {
      return (SetPermissionsOp)opInstances.get()
        .get(OP_SET_PERMISSIONS);
    }
    
    void set(String src, FsPermission permissions) {
      this.src = src;
      this.permissions = permissions;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(src, out);
      permissions.write(out);
     }
 
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.permissions = FsPermission.read(in);
    }
  }

  static class SetOwnerOp extends FSEditLogOp {
    String src;
    String username;
    String groupname;

    private SetOwnerOp() {
      super(OP_SET_OWNER);
    }

    public static SetOwnerOp getUniqueInstance() {
      return new SetOwnerOp();
    }

    static SetOwnerOp getInstance() {
      return (SetOwnerOp)opInstances.get()
        .get(OP_SET_OWNER);
    }
    
    void set(String src, String username, String groupname) {
      this.src = src;
      this.username = username;
      this.groupname = groupname;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(username == null ? "" : username, out);
      FSImageSerialization.writeString(groupname == null ? "" : groupname, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.username = FSImageSerialization.readString_EmptyAsNull(in);
      this.groupname = FSImageSerialization.readString_EmptyAsNull(in);
    }
  }

  static class SetNSQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;

    private SetNSQuotaOp() {
      super(OP_SET_NS_QUOTA);
    }

    static SetNSQuotaOp getInstance() {
      return (SetNSQuotaOp)opInstances.get()
        .get(OP_SET_NS_QUOTA);
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = FSImageSerialization.readLong(in);
    }
  }

  static class ClearNSQuotaOp extends FSEditLogOp {
    String src;

    private ClearNSQuotaOp() {
      super(OP_CLEAR_NS_QUOTA);
    }

    static ClearNSQuotaOp getInstance() {
      return (ClearNSQuotaOp)opInstances.get()
        .get(OP_CLEAR_NS_QUOTA);
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
    }
  }

  static class SetQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;
    long dsQuota;

    private SetQuotaOp() {
      super(OP_SET_QUOTA);
    }

    public static SetQuotaOp getUniqueInstance() {
      return new SetQuotaOp();
    }

    static SetQuotaOp getInstance() {
      return (SetQuotaOp)opInstances.get()
        .get(OP_SET_QUOTA);
    }
    
    void set(String src, long nsQuota, long dsQuota) {
      this.src = src;
      this.nsQuota = nsQuota;
      this.dsQuota = dsQuota;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeLong(nsQuota, out);
      FSImageSerialization.writeLong(dsQuota, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = FSImageSerialization.readLong(in);
      this.dsQuota = FSImageSerialization.readLong(in);
    }
  }

  static class TimesOp extends FSEditLogOp {
    int length;
    String path;
    long mtime;
    long atime;

    private TimesOp() {
      super(OP_TIMES);
    }

    public static TimesOp getUniqueInstance() {
      return new TimesOp();
    }

    static TimesOp getInstance() {
      return (TimesOp)opInstances.get()
        .get(OP_TIMES);
    }
    
    void set(String path, long mtime, long atime) {
      this.path = path;
      this.mtime = mtime;
      this.atime = atime;
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = FSImageSerialization.readInt(in);
        if (length != 3) {
          throw new IOException("Incorrect data format. " + "times operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.mtime = FSImageSerialization.readLong(in);
        this.atime = FSImageSerialization.readLong(in);
      } else {
        this.mtime = readLong(in);
        this.atime = readLong(in);
      }
    }
  }
  
  public static class LogSegmentOp extends FSEditLogOp {
    public LogSegmentOp(FSEditLogOpCodes code) {
      super(code);
      assert code == OP_START_LOG_SEGMENT ||
             code == OP_END_LOG_SEGMENT : "Bad op: " + code;
    }

    static LogSegmentOp getInstance(FSEditLogOpCodes code) {
      return (LogSegmentOp)opInstances.get().get(code);
    }

    @Override
    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // no data stored in these ops yet
    }

    @Override
    void writeFields(DataOutput out) throws IOException {
      // no data stored
    }
  }

  static class InvalidOp extends FSEditLogOp {
    private InvalidOp() {
      super(OP_INVALID);
    }

    static InvalidOp getInstance() {
      return (InvalidOp)opInstances.get().get(OP_INVALID);
    }

    @Override 
    void writeFields(DataOutput out) throws IOException {
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // nothing to read
    }
  }

  static private short readShort(DataInputStream in) throws IOException {
    return Short.parseShort(FSImageSerialization.readString(in));
  }

  public long readInodeId(DataInputStream in, int logVersion) throws IOException {
    if (LayoutVersion.supports(Feature.ADD_INODE_ID, logVersion)) {
      return FSImageSerialization.readLong(in);
    }
    return INodeId.GRANDFATHER_INODE_ID;
  }

  static private long readLong(DataInputStream in) throws IOException {
    return Long.parseLong(FSImageSerialization.readString(in));
  }

  /**
   * A class to read in blocks stored in the old format. The only two
   * fields in the block were blockid and length.
   */
  static class BlockTwo implements Writable {
    long blkid;
    long len;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (BlockTwo.class,
         new WritableFactory() {
          @Override
          public Writable newInstance() { return new BlockTwo(); }
         });
    }


    BlockTwo() {
      blkid = 0;
      len = 0;
    }
    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }

  /**
   * Class for writing editlog ops
   */
  public static class Writer {
    private final DataOutputBuffer buf;

    public Writer(DataOutputBuffer out) {
      this.buf = out;
    }

    public void writeInt(int v) throws IOException {
      buf.writeInt(v);
    }
    
    /**
     * Write an operation to the output stream
     * 
     * @param op The operation to write
     * @throws IOException if an error occurs during writing.
     */
    public void writeOp(FSEditLogOp op) throws IOException {
      int start = buf.getLength();
      buf.writeByte(op.opCode.getOpCode());
      buf.writeLong(op.txid);
      op.writeFields(buf);
      int end = buf.getLength();
      Checksum checksum = FSEditLog.getChecksumForWrite();
      checksum.reset();
      checksum.update(buf.getData(), start, end-start);
      int sum = (int)checksum.getValue();
      buf.writeInt(sum);
    }
    
    /**
     * Write an operation to the output stream
     * 
     * @param op The operation to write
     * @throws IOException if an error occurs during writing.
     */
    public static void writeOp(FSEditLogOp op, ArrayOutputStream os) throws IOException {
      // clear the buffer
      os.reset();
      os.writeByte(op.opCode.getOpCode());
      os.writeLong(op.txid);
      // write op fields
      op.writeFields(os);
      Checksum checksum = FSEditLog.getChecksumForWrite();
      checksum.reset();
      checksum.update(os.getBytes(), 0, os.size());
      int sum = (int)checksum.getValue();
      os.writeInt(sum);
    }
  }

  /**
   * Class for reading editlog ops from a stream
   */
  public static class Reader {
    private DataInputStream in;
    private final int logVersion;
    private final Checksum checksum;
    private long readChecksum;

    /**
     * Construct the reader
     * @param in The stream to read from.
     * @param logVersion The version of the data coming from the stream.
     */
    @SuppressWarnings("deprecation")
    public Reader(DataInputStream in, int logVersion) {
      this.logVersion = logVersion;
      if (LayoutVersion.supports(Feature.EDITS_CHESKUM, logVersion)) {
        this.checksum = FSEditLog.getChecksumForRead();
      } else {
        this.checksum = null;
      }

      if (this.checksum != null) {
        this.in = new DataInputStream(
            new CheckedInputStream(in, this.checksum));
      } else {
        this.in = in;
      }
    }
    
    public long getChecksum() {
      return readChecksum;
    }
    
    public void setInputStream(DataInputStream in) {
      this.in = in;
      if (this.checksum != null) {
        checksum.reset();
        this.in = new DataInputStream(
            new CheckedInputStream(in, this.checksum));
      } else {
        this.in = in;
      }
    }
    
    /**
     * Read an operation from the input stream.
     * 
     * Note that the objects returned from this method may be re-used by future
     * calls to the same method.
     * 
     * @param skipBrokenEdits    If true, attempt to skip over damaged parts of
     * the input stream, rather than throwing an IOException
     * @return the operation read from the stream, or null at the end of the 
     *         file
     * @throws IOException on error.  This function should only throw an
     *         exception when skipBrokenEdits is false.
     */
    public FSEditLogOp readOp(boolean skipBrokenEdits) throws IOException {
      while (true) {
        try {
          return decodeOp();
        } catch (IOException e) {
          in.reset();
          if (!skipBrokenEdits) {
            throw e;
          }
        } catch (RuntimeException e) {
          // FSEditLogOp#decodeOp is not supposed to throw RuntimeException.
          // However, we handle it here for recovery mode, just to be more
          // robust.
          in.reset();
          if (!skipBrokenEdits) {
            throw e;
          }
        } catch (Throwable e) {
          in.reset();
          if (!skipBrokenEdits) {
            throw new IOException("got unexpected exception " +
                e.getMessage(), e);
          }
        }
        // Move ahead one byte and re-try the decode process.
        if (in.skip(1) < 1) {
          return null;
        }
      }
    }

    /**
     * Read an operation from the input stream.
     * 
     * Note that the objects returned from this method may be re-used by future
     * calls to the same method.
     * 
     * @return the operation read from the stream, or null at the end of the file
     * @throws IOException on error.
     */
    public FSEditLogOp decodeOp() throws IOException {
      in.mark(MAX_OP_SIZE);
      if (checksum != null) {
        checksum.reset();
      }

      byte opCodeByte;
      try {
        opCodeByte = in.readByte();
      } catch (EOFException eof) {
        // EOF at an opcode boundary is expected
        return null;
      }

      FSEditLogOpCodes opCode = FSEditLogOpCodes.fromByte(opCodeByte);

      if (opCode == OP_INVALID) {
        LOG.info("Reached the end of the edit file...");
        return null;
      }

      FSEditLogOp op = opInstances.get().get(opCode);
      if (op == null) {
        throw new IOException("Read invalid opcode " + opCode);
      }

      if (LayoutVersion.supports(Feature.STORED_TXIDS, logVersion)) {
        // Read the txid
        op.setTransactionId(in.readLong());
      }

      op.readFields(in, logVersion);
      if (checksum != null)
        readChecksum = checksum.getValue();
      validateChecksum(in, checksum, op.txid);
      return op;
    }

    /**
     * Validate a transaction's checksum
     */
    private void validateChecksum(DataInputStream in,
                                  Checksum checksum,
                                  long txid)
        throws IOException {
      if (checksum != null) {
        int calculatedChecksum = (int)checksum.getValue();
        int readChecksum = FSImageSerialization.readInt(in); // read in checksum
        if (readChecksum != calculatedChecksum) {
          throw new ChecksumException(
              "Transaction is corrupt. Calculated checksum is " +
              calculatedChecksum + " but read checksum " + readChecksum, txid);
        }
      }
    }
  }
}

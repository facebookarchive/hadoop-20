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

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from FSEditLogOp and are only
 * instantiated from Reader#readOp()
 */
public abstract class FSEditLogOp {
  public static final Log LOG = LogFactory.getLog(FSEditLogOp.class);
  final FSEditLogOpCodes opCode;
  long txid;


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

  long getTransactionId() {
    return txid;
  }

  void setTransactionId(long txid) {
    this.txid = txid;
  }
  
  public String toString(){
    return "Record: txid: " + txid + " opcode: " + opCode;
  }

  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  abstract void writeFields(DataOutputStream out)
      throws IOException;

  @SuppressWarnings("unchecked")
  static abstract class AddCloseOp extends FSEditLogOp {
    int length;
    String path;
    short replication;
    long mtime;
    long atime;
    long blockSize;
    Block[] blocks;
    PermissionStatus permissions;
    String clientName;
    String clientMachine;
    
    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      assert(opCode == OP_ADD || opCode == OP_CLOSE);
    }
    
    void set(String path,
        short replication,
        long mtime,
        long atime,
        long blockSize,
        Block[] blocks,
        PermissionStatus permissions,
        String clientName,
        String clientMachine) {
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
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(5);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShortAsString(replication, out);
      FSImageSerialization.writeLongAsString(mtime, out);
      FSImageSerialization.writeLongAsString(atime, out);
      FSImageSerialization.writeLongAsString(blockSize, out);
      new ArrayWritable(Block.class, blocks).write(out);
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
      this.length = in.readInt();
      if (-7 == logVersion && length != 3||
          -17 < logVersion && logVersion < -7 && length != 4 ||
          (logVersion <= -17 && length != 5)) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);

      this.replication = readShort(in);
      this.mtime = readLong(in);

      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        this.atime = readLong(in);
      } else {
        this.atime = 0;
      }
      if (logVersion < -7) {
        this.blockSize = readLong(in);
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

    private static Block[] readBlocks(
        DataInputStream in,
        int logVersion) throws IOException {
      int numBlocks = in.readInt();
      Block[] blocks = new Block[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        Block blk = new Block();
        if (logVersion <= -14) {
          blk.readFields(in);
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

  static class AddOp extends AddCloseOp {
    private AddOp() {
      super(OP_ADD);
    }

    static AddOp getInstance() {
      return (AddOp)opInstances.get().get(OP_ADD);
    }
  }

  static class CloseOp extends AddCloseOp {
    private CloseOp() {
      super(OP_CLOSE);
    }

    static CloseOp getInstance() {
      return (CloseOp)opInstances.get().get(OP_CLOSE);
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
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
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShortAsString(replication, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      this.replication = readShort(in);
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
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(1 + srcs.length + 1);
      FSImageSerialization.writeString(trg, out);
      for(int i=0; i<srcs.length; i++) {
        FSImageSerialization.writeString(srcs[i], out);
      }
      FSImageSerialization.writeLongAsString(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (length < 3) { // trg, srcs.., timestamp
        throw new IOException("Incorrect data format. "
            + "Concat delete operation.");
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = this.length -1 -1;
      this.srcs = new String [srcSize];
      for(int i=0; i<srcSize;i++) {
        srcs[i]= FSImageSerialization.readString(in);
      }  
      this.timestamp = readLong(in);
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
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(3);
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(dst, out);
      FSImageSerialization.writeLongAsString(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (this.length != 3) {
        throw new IOException("Incorrect data format. "
            + "Old rename operation.");
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class DeleteOp extends FSEditLogOp {
    int length;
    String path;
    long timestamp;

    private DeleteOp() {
      super(OP_DELETE);
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
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(2);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLongAsString(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (this.length != 2) {
        throw new IOException("Incorrect data format. " + "delete operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class MkdirOp extends FSEditLogOp {
    int length;
    String path;
    long timestamp;
    PermissionStatus permissions;

    private MkdirOp() {
      super(OP_MKDIR);
    }
    
    static MkdirOp getInstance() {
      return (MkdirOp)opInstances.get()
        .get(OP_MKDIR);
    }
    
    void set(String path, long timestamp, PermissionStatus permissions) {
      this.path = path;
      this.timestamp = timestamp;
      this.permissions = permissions;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(3);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLongAsString(timestamp, out); // mtime
      FSImageSerialization.writeLongAsString(timestamp, out); // atime, unused at this
      permissions.write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {

      this.length = in.readInt();
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        /* unused this.atime = */
        readLong(in);
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

    static SetGenstampOp getInstance() {
      return (SetGenstampOp)opInstances.get()
        .get(OP_SET_GENSTAMP);
    }

    void set(long genStamp) {
      this.genStamp = genStamp;
    }
    
    @Override 
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
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

    static SetPermissionsOp getInstance() {
      return (SetPermissionsOp)opInstances.get()
        .get(OP_SET_PERMISSIONS);
    }
    
    void set(String src, FsPermission permissions) {
      this.src = src;
      this.permissions = permissions;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = FSImageSerialization.readLongAsString(in);
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
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
      out.writeInt(3);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLongAsString(mtime, out);
      FSImageSerialization.writeLongAsString(atime, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (length != 3) {
        throw new IOException("Incorrect data format. " + "times operation.");
      }
      this.path = FSImageSerialization.readString(in);

      this.mtime = readLong(in);
      this.atime = readLong(in);
    }
  }
  
  static class LogSegmentOp extends FSEditLogOp {
    private LogSegmentOp(FSEditLogOpCodes code) {
      super(code);
      assert code == OP_START_LOG_SEGMENT ||
             code == OP_END_LOG_SEGMENT : "Bad op: " + code;
    }

    static LogSegmentOp getInstance(FSEditLogOpCodes code) {
      return (LogSegmentOp)opInstances.get().get(code);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // no data stored in these ops yet
    }

    @Override
    void writeFields(DataOutputStream out) throws IOException {
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
    void writeFields(DataOutputStream out) throws IOException {
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
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

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
     * @return the operation read from the stream, or null at the end of the file
     * @throws IOException on error.
     */
    public FSEditLogOp readOp() throws IOException {
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
        int readChecksum = in.readInt(); // read in checksum
        if (readChecksum != calculatedChecksum) {
          throw new ChecksumException(
              "Transaction is corrupt. Calculated checksum is " +
              calculatedChecksum + " but read checksum " + readChecksum, txid);
        }
      }
    }
  }
}

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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Static utility functions for serializing various pieces of data in the correct
 * format for the FSImage file.
 *
 * Some members are currently public for the benefit of the Offline Image Viewer
 * which is located outside of this package. These members should be made
 * package-protected when the OIV is refactored.
 */
public class FSImageSerialization {

  // Static-only class
  private FSImageSerialization() {}
  
  /**
   * In order to reduce allocation, we reuse some static objects. However, the methods
   * in this class should be thread-safe since image-saving is multithreaded, so 
   * we need to keep the static objects in a thread-local.
   */
  static private final ThreadLocal<TLData> TL_DATA =
    new ThreadLocal<TLData>() {
    @Override
    protected TLData initialValue() {
      return new TLData();
    }
  };

  /**
   * Simple container "struct" for threadlocal data.
   */
  @SuppressWarnings("deprecation")
  static private final class TLData {
    final UTF8 U_STR = new UTF8();
    final FsPermission FILE_PERM = new FsPermission((short) 0);
  }

  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFileUnderConstruction readINodeUnderConstruction(
      DataInputStream in, FSDirectory fsDir, int imgVersion) throws IOException {
    byte[] name = readBytes(in);
    long inodeId = LayoutVersion.supports(Feature.ADD_INODE_ID, imgVersion) ? in
        .readLong() : fsDir.allocateNewInodeId();
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new BlockInfo();
      blocks[i].readFields(in);
      if (LayoutVersion.supports(Feature.BLOCK_CHECKSUM, imgVersion)) {
        blocks[i].setChecksum(in.readInt());
      }
    }
    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // These locations are not used at all
    int numLocs = in.readInt();
    DatanodeDescriptor[] locations = new DatanodeDescriptor[numLocs];
    for (int i = 0; i < numLocs; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFields(in);
    }
    
    return new INodeFileUnderConstruction(inodeId,
                                          name, 
                                          blockReplication, 
                                          modificationTime,
                                          preferredBlockSize,
                                          blocks,
                                          perm,
                                          clientName,
                                          clientMachine,
                                          null);
  }

  // Helper function that writes an INodeUnderConstruction
  // into the input stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out,
                                           INodeFileUnderConstruction cons,
                                           String path) 
                                           throws IOException {
    writeString(path, out);
    out.writeLong(cons.getId());
    out.writeShort(cons.getReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());
    int nrBlocks = cons.getBlocks().length;
    out.writeInt(nrBlocks);
    for (int i = 0; i < nrBlocks; i++) {
      cons.getBlocks()[i].write(out);
      out.writeInt(cons.getBlocks()[i].getChecksum());
    }
    cons.getPermissionStatus().write(out);
    writeString(cons.getClientName(), out);
    writeString(cons.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }
  
  /*
   * Save one inode's attributes to the image.
   */
  static void saveINode2Image(INode node,
                                      DataOutputStream out) throws IOException {
    byte[] name = node.getLocalNameBytes();
    out.writeShort(name.length);
    out.write(name);
    out.writeLong(node.getId());
    
    if (node instanceof INodeHardLinkFile) {
      // Process the hard link file:  
      // If the hard link has more than 1 reference cnt, then store its type with the hard link ID  
      // Otherwise, just store the regular INodeFile's inode type.  
      INodeHardLinkFile hardLink = (INodeHardLinkFile)node; 
      if (hardLink.getHardLinkFileInfo().getReferenceCnt() > 1) {
        out.writeByte(INode.INodeType.HARDLINKED_INODE.type); 
        WritableUtils.writeVLong(out, ((INodeHardLinkFile)node).getHardLinkID()); 
      } else {
        throw new IOException("Invalid reference count for the hardlink file: " +
            node.getFullPathName() + " with the hardlink ID: " + hardLink.getHardLinkID() +
            " and reference cnt: " + hardLink.getHardLinkFileInfo().getReferenceCnt());
      }
    } else if (node instanceof INodeFile && 
        ((INodeFile)node).getStorageType() == StorageType.RAID_STORAGE) {
      INodeFile raidFile = (INodeFile)node;
      INodeRaidStorage storage = (INodeRaidStorage)raidFile.getStorage();
      out.writeByte(INode.INodeType.RAIDED_INODE.type);
      WritableUtils.writeString(out, storage.getCodec().id);
    } else {
      // Process the regular files and directory: just store its inode type 
      out.writeByte(INode.INodeType.REGULAR_INODE.type); 
    }

    FsPermission filePerm = TL_DATA.get().FILE_PERM;
    if (!node.isDirectory()) {  // write file/hardlink inode
      INodeFile fileINode = (INodeFile)node;
      out.writeShort(fileINode.getReplication());
      out.writeLong(fileINode.getModificationTime());
      out.writeLong(fileINode.getAccessTime());
      out.writeLong(fileINode.getPreferredBlockSize());
      writeBlocks(fileINode.getBlocks(), out);
      filePerm.fromShort(fileINode.getFsPermissionShort());
      PermissionStatus.write(out, fileINode.getUserName(),
                             fileINode.getGroupName(),
                             filePerm);
    } else {   // write directory inode
      out.writeShort(0);  // replication
      out.writeLong(node.getModificationTime());
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-1);    // # of blocks
      out.writeLong(node.getNsQuota());
      out.writeLong(node.getDsQuota());
      filePerm.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             filePerm);
    }
  }

  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  @SuppressWarnings("deprecation")
  public static String readString(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return ustr.toString();
  }

  static String readString_EmptyAsNull(DataInputStream in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }

  @SuppressWarnings("deprecation")
  static void writeString(String str, DataOutput out) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(str, true);
    ustr.write(out);
  }

  /** read the long value */
  static long readLong(DataInputStream in) throws IOException {
    return in.readLong();
  }

  /** write the long value */
  static void writeLong(long value, DataOutput out) throws IOException {
    out.writeLong(value);
  }
  
  /** read the int value */
  static int readInt(DataInputStream in) throws IOException {
    return in.readInt();
  }

  /** write the int value */
  static void writeInt(int value, DataOutput out) throws IOException {
    out.writeInt(value);
  }

  /** read short value */
  static short readShort(DataInputStream in) throws IOException {
    return in.readShort();
  }

  /** write short value */
  static void writeShort(short value, DataOutput out) throws IOException {
    out.writeShort(value);
  }
  
  // Same comments apply for this method as for readString()
  @SuppressWarnings("deprecation")
  public static byte[] readBytes(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    int len = ustr.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(ustr.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  public static void writeBlocks(BlockInfo[] blocks, DataOutput out) throws IOException {
    out.writeInt(blocks.length); 
    for (BlockInfo blk : blocks) {
      blk.write(out);
      out.writeInt(blk.getChecksum());
    }
  }

  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static byte[][] readPathComponents(DataInputStream in)
      throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    
    ustr.readFields(in);
    return DFSUtil.bytes2byteArray(ustr.getBytes(),
      ustr.getLength(), (byte) Path.SEPARATOR_CHAR);
  }

  /**
   * DatanodeImage is used to store persistent information
   * about datanodes into the fsImage.
   */
  static class DatanodeImage implements Writable {
    DatanodeDescriptor node = new DatanodeDescriptor();

    static void skipOne(DataInput in) throws IOException {
      DatanodeImage nodeImage = new DatanodeImage();
      nodeImage.readFields(in);
    }
    
    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     * Public method that serializes the information about a
     * Datanode to be stored in the fsImage.
     */
    @Override
    public void write(DataOutput out) throws IOException {
      new DatanodeID(node).write(out);
      out.writeLong(node.getCapacity());
      out.writeLong(node.getRemaining());
      out.writeLong(node.getLastUpdate());
      out.writeInt(node.getXceiverCount());
    }

    /**
     * Public method that reads a serialized Datanode
     * from the fsImage.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      DatanodeID id = new DatanodeID();
      id.readFields(in);
      long capacity = in.readLong();
      long remaining = in.readLong();
      long lastUpdate = in.readLong();
      int xceiverCount = in.readInt();

      // update the DatanodeDescriptor with the data we read in
      node.updateRegInfo(id);
      node.setStorageID(id.getStorageID());
      node.setCapacity(capacity);
      node.setRemaining(remaining);
      node.setLastUpdate(lastUpdate);
      node.setXceiverCount(xceiverCount);
    }
  }
}
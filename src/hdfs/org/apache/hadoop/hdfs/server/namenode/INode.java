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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat.FSImageLoadingContext;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.BlockMetaInfoType;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.hdfs.util.LightWeightGSet.LinkedElement;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.util.StringUtils;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
public abstract class INode implements Comparable<byte[]>, FSInodeInfo, LinkedElement {
  public static final Log LOG = LogFactory.getLog(INode.class);
  protected byte[] name;
  protected INodeDirectory parent;
  protected long modificationTime;
  protected volatile long accessTime;
  private final long id; // Unique id for inode per namenode, not recyclable
  private LinkedElement next = null;
  
  public static enum INodeType {
    REGULAR_INODE((byte) 1),
    HARDLINKED_INODE((byte) 2),
    RAIDED_INODE((byte) 3),
    HARDLINK_RAIDED_INODE((byte) 4);
    
    public final byte type;
    INodeType(byte type) {
      this.type = type;
    }
  }
  
  /** Simple wrapper for two counters : 
   *  nsCount (namespace consumed) and dsCount (diskspace consumed).
   */
  static class DirCounts {
    long nsCount = 0;
    long dsCount = 0;
    
    /** returns namespace count */
    long getNsCount() {
      return nsCount;
    }

    /** returns diskspace count */
    long getDsCount() {
      return dsCount;
    }
  }
  
  //Only updated by updatePermissionStatus(...).
  //Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  /**
   * This is for testing
   */
  protected INode() {
    name = null;
    parent = null;
    modificationTime = 0;
    accessTime = 0;
    id = INodeId.GRANDFATHER_INODE_ID;
  }
  
  protected void updateINode(PermissionStatus permissions, long mTime, long atime) {
    this.modificationTime = mTime;
    setAccessTime(atime);
    setPermissionStatus(permissions);
  }

  INode(long id, PermissionStatus permissions, long mTime, long atime) {
    this.name = null;
    this.parent = null;
    this.modificationTime = mTime;
    this.id = id;
    setAccessTime(atime);
    setPermissionStatus(permissions);
  }

  protected INode(long id, String name, PermissionStatus permissions) {
    this(id, permissions, 0L, 0L);
    setLocalName(name);
  }
  
  /** copy constructor
   * 
   * @param other Other node to be copied
   */
  INode(INode other) {
    setLocalName(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatus(other.getPermissionStatus());
    setModificationTime(other.getModificationTime());
    setAccessTime(other.getAccessTime());
    this.id = other.getId();
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return name.length == 0;
  }

  /** Set the {@link PermissionStatus} */
  protected void setPermissionStatus(PermissionStatus ps) {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }
  /** Get the {@link PermissionStatus} */
  protected PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(),getGroupName(),getFsPermission());
  }
  private synchronized void updatePermissionStatus(
      PermissionStatusFormat f, long n) {
    permission = f.combine(n, permission);
  }
  /** Get user name */
  public String getUserName() {
    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }
  /** Set user */
  protected void setUser(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }
  /** Get group name */
  public String getGroupName() {
    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** Set group */
  protected void setGroup(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }
  /** Get the {@link FsPermission} */
  public FsPermission getFsPermission() {
    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link INode} */
  protected void setPermission(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE, permission.toShort());
  }
  /** Get iNode id */
  public long getId() {
    return id;
  }

  /**
   * Check whether it's a directory
   */
  public abstract boolean isDirectory();
  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this INode is deleted.
   * @param v blocks to be cleared and collected
   * @param blocksLimit the upper limit of blocks to be collected
   *        A parameter of 0 indicates that there is no limit
   * @param removedINodes inodes to be removed, in order to clear from inodeMap
   * @return the number of files deleted
   */
  abstract int collectSubtreeBlocksAndClear(List<BlockInfo> v, int blocksLimit, 
      List<INode> removedINodes);

  /** Compute {@link ContentSummary}. */
  public final ContentSummary computeContentSummary() {
    long[] a = computeContentSummary(new long[]{0,0,0,0});
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), 
                              a[3], getDsQuota());
  }
  /**
   * @return an array of three longs. 
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary);
  
  /**
   * Verify if file is regular storage, otherwise throw an exception
   */
  public static void enforceRegularStorageINode(INodeFile inode, String msg)
    throws IOException {
    if (inode.getStorageType() != StorageType.REGULAR_STORAGE) {
      LOG.error(msg);
      throw new IOException(msg);
    }
  }
  
  /**
   * Get the quota set for this inode
   * @return the quota if it is set; -1 otherwise
   */
  long getNsQuota() {
    return -1;
  }

  long getDsQuota() {
    return -1;
  }
  
  boolean isQuotaSet() {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total nubmer of names and total disk space taken under 
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts);
  
  /**
   * Get local file name
   * @return local file name
   */
  String getLocalName() {
    return name==null? null: DFSUtil.bytes2String(name);
  }

  /**
   * Get local file name
   * @return local file name
   */
  byte[] getLocalNameBytes() {
    return name;
  }

  /**
   * Set local file name
   */
  void setLocalName(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }

  /**
   * Set local file name
   */
  void setLocalName(byte[] name) {
    this.name = name;
  }

  @Override
  public String getFullPathName() throws IOException {
    // Get the full path name of this inode.
    return FSDirectory.getFullPathName(this);
  }

  @Override
  public String toString() {
    return "\"" + getLocalName() + "\":" + getPermissionStatus();
  }

  /**
   * Get parent directory 
   * @return parent INode
   */
  INodeDirectory getParent() {
    return this.parent;
  }

  /** 
   * Get last modification time of inode.
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  void setModificationTime(long modtime) {
    assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  void setModificationTimeForce(long modtime) {
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  void setAccessTime(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  boolean isUnderConstruction() {
    return false;
  }
  
  public int getStartPosForQuoteUpdate() {
    return 0;
  }

  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(String path) {
    return DFSUtil.splitAndGetPathComponents(path);
  }
  
  /**
   * Breaks file path into components.
   * @param path specified in byte[] format
   * @param len length of the path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(byte[] path) {
    return DFSUtil.bytes2byteArray(path, path.length, (byte) Path.SEPARATOR_CHAR);
  }
  
  /** Convert strings to byte arrays for path components. */
  static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] =  DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }

  /**
   * Breaks file path into names.
   * @param path
   * @return array of names 
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      return null;
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  boolean removeNode() {
    if (parent == null) {
      return false;
    } else {
      
      parent.removeChild(this);
      parent = null;
      return true;
    }
  }

  //
  // Comparable interface
  //
  
  /**
   * Compare names of the inodes
   * 
   * @return a negative integer, zero, or a positive integer 
   */
  public final int compareTo(byte[] name2) {
    if (name == name2)
      return 0;
    int len1 = (name == null ? 0 : name.length);
    int len2 = (name2 == null ? 0 : name2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i = 0; i < n; i++) {
      b1 = name[i];
      b2 = name2[i];
      if (b1 != b2)
        return b1 - b2;
    }
    return len1 - len2;
  }
  
  public boolean equals(Object that) {
    if (!(that instanceof INode)) {
      return false;
    }
    return getId() == ((INode) that).getId();
  }

  /** This is used in inodeMap in {@link FSDirectory} */
  public int hashCode() {
    return getHashCode(id); 
  }
  
  public static int getHashCode(long id) {
    return (int)(id ^ (id>>>32));
  }

  //
  // static methods
  //
  
  LocatedBlocks createLocatedBlocks(List<LocatedBlock> blocks,
      BlockMetaInfoType type,int namespaceid, int methodsFingerprint) {
    switch (type) {
    case VERSION_AND_NAMESPACEID:
      return new LocatedBlocksWithMetaInfo(
          computeContentSummary().getLength(), blocks,
          isUnderConstruction(), DataTransferProtocol.DATA_TRANSFER_VERSION,
          namespaceid, methodsFingerprint);
    case VERSION:
      return new VersionedLocatedBlocks(computeContentSummary().getLength(), blocks,
        isUnderConstruction(), DataTransferProtocol.DATA_TRANSFER_VERSION);
    default:
      return new LocatedBlocks(computeContentSummary().getLength(), blocks,
        isUnderConstruction());
    }
  }

  /**
   * Create an INode; the inode's name is not set yet
   *
   * @param id inode id
   * @param permissions permissions
   * @param blocks blocks if a file
   * @param symlink symblic link if a symbolic link
   * @param replication replication factor
   * @param modificationTime modification time
   * @param atime access time
   * @param nsQuota namespace quota
   * @param dsQuota disk quota
   * @param preferredBlockSize block size
   * @param inodeType The INode type
   * @param hardLinkID The HardLinkID
   * @param codec The raid codec for Raided file
   * @param parityReplication Replication of parity blocks for raided file
   * @param context The context when loading the fsImage
   * @return an inode
   */
  static INode newINode(long id,
                        PermissionStatus permissions,
                        BlockInfo[] blocks, 
                        short replication,
                        long modificationTime,
                        long atime,
                        long nsQuota,
                        long dsQuota,
                        long preferredBlockSize,
                        byte inodeType,
                        long hardLinkID,
                        RaidCodec codec,
                        FSImageLoadingContext context) {
    if (inodeType == INode.INodeType.REGULAR_INODE.type) {
      // Process the regular INode file
      if (blocks == null) { // directory
        if (nsQuota >= 0 || dsQuota >= 0) { // directory with quota
          return new INodeDirectoryWithQuota(
              id, permissions, modificationTime, nsQuota, dsQuota);
        }
        // regular directory
        return new INodeDirectory(id, permissions, modificationTime);
      }
      // file
      return new INodeFile(id, permissions, blocks, replication,
                           modificationTime, atime, preferredBlockSize, null);
    } else if (inodeType == INode.INodeType.HARDLINKED_INODE.type) {
      // Process the HardLink INode file
      // create and register the hard link file info  
      HardLinkFileInfo hardLinkFileInfo =   
        INodeHardLinkFile.loadHardLinkFileInfo(hardLinkID, context); 
      
      // Reuse the same blocks for the hardlinked files
      if (hardLinkFileInfo.getReferenceCnt() > 0) {
        blocks = hardLinkFileInfo.getHardLinkedFile(0).getBlocks();
      }
      
      // Create the INodeHardLinkFile and increment the reference cnt
      INodeHardLinkFile hardLinkFile = new INodeHardLinkFile(id,
                                                            permissions, 
                                                            blocks, 
                                                            replication,  
                                                            modificationTime, 
                                                            atime,  
                                                            preferredBlockSize,   
                                                            hardLinkFileInfo);
      hardLinkFile.incReferenceCnt();
      return hardLinkFile;
    } else if (inodeType == INode.INodeType.RAIDED_INODE.type) {
      return new INodeFile(id, permissions, blocks, replication,
                           modificationTime, atime, preferredBlockSize,
                           codec);
    } else {
      throw new IllegalArgumentException("Invalide inode type: " + inodeType);
    }
  }
  @Override
  public void setNext(LinkedElement next) {
    this.next = next;
  }
    
  @Override
  public LinkedElement getNext() {
    return next;
  }
}

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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat.FSImageLoadingContext;

public class INodeHardLinkFile extends INodeFile{
  private final HardLinkFileInfo hardLinkFileInfo;
  
  protected INodeHardLinkFile(INodeFile inodeFile, long hardLinkID) throws IOException {
    super(inodeFile);
    hardLinkFileInfo = new HardLinkFileInfo(hardLinkID);
  }

  protected INodeHardLinkFile(INodeHardLinkFile inodeHardLinkFile) {
    super(inodeHardLinkFile);
    this.hardLinkFileInfo = inodeHardLinkFile.getHardLinkFileInfo();
  }
  
  protected INodeHardLinkFile(long id, PermissionStatus permissions, BlockInfo[] blocks, 
      short replication, long modificationTime,  
      long atime, long preferredBlockSize,   
      HardLinkFileInfo hardLinkFileInfo) { 
     super(id,
         permissions,  
         blocks, 
         replication,  
         modificationTime,   
         atime,  
         preferredBlockSize,
         null);  
     this.hardLinkFileInfo = hardLinkFileInfo;
     
   }

  public HardLinkFileInfo getHardLinkFileInfo() {
    return hardLinkFileInfo;
  }

  public void incReferenceCnt() {
    this.hardLinkFileInfo.addLinkedFile(this);
  }

  public int getReferenceCnt() {
    return this.hardLinkFileInfo.getReferenceCnt();
  }

  public long getHardLinkID() {
    return this.hardLinkFileInfo.getHardLinkID();
  }

  @Override
  public boolean isHardlinkFile() {
    return true;
  }

  @Override
  int collectSubtreeBlocksAndClear(List<BlockInfo> v, 
                                   int blocksLimit, 
                                   List<INode> removedINodes) {
    parent = null;
    this.hardLinkFileInfo.removeLinkedFile(this);
    name = null;
    removedINodes.add(this);
    return 1;
  }
  
  public Set<INode> getAncestorSet() {
   return this.hardLinkFileInfo.getAncestorSet(null);
  }
  
  private Set<INode> getAncestorSetWithoutCurrentINode() {
    return this.hardLinkFileInfo.getAncestorSet(this);
  }
  
  public int getStartPosForQuoteUpdate() {
    int commonAncestorNum = 0;
    INode current = this;
    Set<INode> commonAncestorExclusiveSet = getAncestorSetWithoutCurrentINode();
    while ((current = current.parent) != null) {
      if (!commonAncestorExclusiveSet.contains(current)) {
        commonAncestorNum ++;
      }
    }
    
    return commonAncestorNum;
  }
  
  /**
   * Set the PermissionSatus for all the linked files
   * @param ps
   */
  @Override
  protected void setPermissionStatus(PermissionStatus ps) {
    setPermissionStatus(ps, true);
  }
  
  protected void setPermissionStatus(PermissionStatus ps, boolean recursive) {
    super.setPermissionStatus(ps);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setPermissionStatus(ps);
    }
  }

  /**
   * Set the user name for all the linked files
   * @param user
   */
  @Override
  protected void setUser(String user) {
    this.setUser(user, true);
  }
  
  protected void setUser(String user, boolean recursive) {
    super.setUser(user);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setUser(user);
    }
  }

  /**
   * Set the group name for all the linked files
   * @param group
   */
  @Override
  protected void setGroup(String group) {
    this.setGroup(group, true);
  }
  
  protected void setGroup(String group, boolean recursive) {
    super.setGroup(group);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setGroup(group);
    }
  }

  /**
   * Set the permission for all the linked files
   * @param permission
   */
  @Override
  protected void setPermission(FsPermission permission) {
    this.setPermission(permission, true);
  }
  
  protected void setPermission(FsPermission permission, boolean recursive) {
    super.setPermission(permission);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setPermission(permission);
    }
  }
  
  /**
   * Set the modtime for all the linked files
   * @param modtime
   */
  @Override
  protected void setModificationTime(long modtime) {
    this.setModificationTime(modtime, true);
  }
  
  protected void setModificationTime(long modtime, boolean recursive) {
    super.setModificationTimeForce(modtime);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setModificationTime(modtime);
    }
  }
  
  /**
  * Always set the last modification time of inode.
  * @param modtime
  */
  @Override
  protected void setModificationTimeForce(long modtime) {
    this.setModificationTimeForce(modtime, true);
  }
  
  protected void setModificationTimeForce(long modtime, boolean recursive) {
    super.setModificationTimeForce(modtime);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setModificationTimeForce(modtime);
    }
  }
  
  /**
   * Set the atime for all the linked files
   * @param modtime
   */
  @Override
  protected void setAccessTime(long atime) {
    this.setAccessTime(atime, true);
  }
  
  protected void setAccessTime(long atime, boolean recursive) {
    super.setAccessTime(atime);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setAccessTime(atime);
    }
  }
  
  /**
   * Set the replication factor for all the linked files
   * @param replication
   */
  @Override
  public void setReplication(short replication) {
    this.setReplication(replication, true);
  }
  
  protected void setReplication(short replication, boolean recursive) {
    super.setReplication(replication);
    if (this.hardLinkFileInfo != null && recursive) {
      this.hardLinkFileInfo.setReplication(replication);
    }
  }

  /** 
   * Create a HardLink file info if necessary and register to the hardLinkINodeIDToFileInfoMap  
   * And return the hardLinkFileInfo which is registered in the hardLinkINodeIDToFileInfoMap  
   *  
   * This function is not thread safe.  
   * @param inodeID
   * @param context The context when loading the fsImage
   * @return hardLinkFileInfo registered in the hardLinkINodeIDToFileInfoMap  
   */ 
  public static HardLinkFileInfo loadHardLinkFileInfo(long hardLinkID,
      FSImageLoadingContext context) {  
    // update the latest hard link ID 
    context.getFSDirectory().resetLastHardLinkIDIfLarge(hardLinkID);
  
    // create the hard link file info if necessary  
    HardLinkFileInfo fileInfo = context.getHardLinkFileInfo(hardLinkID);
    if (fileInfo == null) { 
      fileInfo = new HardLinkFileInfo(hardLinkID);
      context.associateHardLinkIDWithFileInfo(hardLinkID, fileInfo);
    }
    return fileInfo;  
  }
}
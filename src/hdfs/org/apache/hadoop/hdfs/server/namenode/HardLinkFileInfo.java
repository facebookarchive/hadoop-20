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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

/**
 * This HardLinkFileInfo represents the shared information across hard linked files.
 * The information includes the reference cnt and its hard link ID.
 * 
 * This class is not thread safe.
 *
 */
public class HardLinkFileInfo {
  private List<INodeHardLinkFile> linkedFiles;
  private long hardLinkID;
  
  public HardLinkFileInfo(long hardLinkID) {
    this.hardLinkID = hardLinkID;
    linkedFiles = new LinkedList<INodeHardLinkFile>();
  }
  
  public long getHardLinkID() {
    return this.hardLinkID;
  }
  
  public int getReferenceCnt() {
    return this.linkedFiles.size();
  }
  
  /**
   * Return the i th of the hardlinked file
   * @param i
   * @return the i th of the hardlinked files and return null if i is out of the boundary.
   */
  public INodeHardLinkFile getHardLinkedFile(int i) {
    if (i < this.linkedFiles.size()) {
      return this.linkedFiles.get(i);
    }
    return null;
  }
  
  /**
   * Add an INodeHardLinkFile to the linkedFiles
   * This function is not thread-safe. The caller is supposed to have a writeLock.
   * 
   * @param file The INodeHardLinkFile to be added
   */
  public void addLinkedFile(INodeHardLinkFile file) {
    linkedFiles.add(file);
  }
  
  /**
   * Remove an INodeHardLinkFile from the linkedFiles.
   * This function is not thread-safe. The caller is supposed to have a writeLock.
   * 
   * @param file The INodeHardLinkFile to be removed
   */
  public void removeLinkedFile(INodeHardLinkFile file) {
    // Remove the target file from the linkedFiles
    // Since the equal function in the INode only compares the name field, it would be safer to 
    // iterate the INodeHardLinkFile and compare the reference directly.
    for (int i = 0 ; i < linkedFiles.size(); i++) {
      // compare the reference !
      if (linkedFiles.get(i) == file) {
        // remove the file by index
        linkedFiles.remove(i);
        break;
      }
    }

    INodeFile newOwner = null;
    if (linkedFiles.size() == 1) {
      // revert the last INodeHardLinkFile to the regular INodeFile
      INodeHardLinkFile lastReferencedFile= linkedFiles.get(0);
      INodeFile inodeFile = new INodeFile(lastReferencedFile);
      lastReferencedFile.parent.replaceChild(inodeFile);
      
      // clear the linkedFiles
      linkedFiles.clear();
      // set the new owner
      newOwner = inodeFile;
    } else {
      if (file.getBlocks() != null && 
          file.getBlocks().length >0 && 
         (file.getBlocks()[0].getINode() == file)) {
        // need to find a new owner for all the block infos
        newOwner = linkedFiles.get(0);
      }    
    }
    if (newOwner != null) {
      for (BlockInfo blkInfo : file.getBlocks()) {
        blkInfo.setINode(newOwner);
      }
    }
  }
  
  /**
   * @return the set of the ancestor INodes for the linked INodes.
   */
  public Set<INode> getAncestorSet(INodeHardLinkFile excludedINode) {
    Set<INode> ancestors = new HashSet<INode>();
    INodeDirectory currentDirectory;
    
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      if (linkedFile == excludedINode) {
        continue;
      }
      currentDirectory = linkedFile.getParent();
      assert(currentDirectory != null);
      
      do {
        ancestors.add(currentDirectory);
      } while((currentDirectory = currentDirectory.getParent()) != null);
    }
    return ancestors;
   }
  
  /**
   * Set the PermissionSatus for all the linked files
   * @param ps
   */
  protected void setPermissionStatus(PermissionStatus ps) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setPermissionStatus(ps, false);
    }
  }

  /**
   * Set the user name for all the linked files
   * @param user
   */
  protected void setUser(String user) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setUser(user, false);
    }
  }

  /**
   * Set the group name for all the linked files
   * @param group
   */
  protected void setGroup(String group) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setGroup(group, false);
    }
  }

  /**
   * Set the permission for all the linked files
   * @param permission
   */
  protected void setPermission(FsPermission permission) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setPermission(permission, false);
    }
  }
  
  /**
   * Set the modtime for all the linked files
   * @param modtime
   */
  protected void setModificationTime(long modtime) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setModificationTime(modtime, false);
    }
  }
  
  /**
  * Always set the last modification time of inode.
  * @param modtime
  */
  protected void setModificationTimeForce(long modtime) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setModificationTimeForce(modtime, false);
    }
  }
  
  /**
   * Set the atime for all the linked files
   * @param modtime
   */
  public void setAccessTime(long atime) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setAccessTime(atime, false);
    }
  }
  
  /**
   * Set the replication factor for all the linked files
   * @param replication
   */
  public void setReplication(short replication) {
    for (INodeHardLinkFile linkedFile : linkedFiles) {
      linkedFile.setReplication(replication, false);
    }
  }
}
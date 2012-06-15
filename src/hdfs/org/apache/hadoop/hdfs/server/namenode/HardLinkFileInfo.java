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

import java.util.LinkedList;
import java.util.List;

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
    // remove the target file from the linkedFiles
    linkedFiles.remove(file);
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
}
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdfs.protocol.Block;

public class INodeHardLinkFile extends INodeFile{
  static final INodeType INODE_TYPE = INodeType.HARDLINKED_INODE;
  
  /** Keeps track of the lastest hard link ID */
  private static AtomicLong latestHardLinkID = new AtomicLong(0);
  
  private final HardLinkFileInfo hardLinkFileInfo;
  
  protected INodeHardLinkFile(INodeFile inodeFile) throws IOException {
    super(inodeFile);
    hardLinkFileInfo = new HardLinkFileInfo(latestHardLinkID.getAndIncrement());
  }
  
  protected INodeHardLinkFile(INodeHardLinkFile inodeHardLinkFile) {
    super(inodeHardLinkFile);
    this.hardLinkFileInfo = inodeHardLinkFile.getHardLinkFileInfo();
  }
  
  public HardLinkFileInfo getHardLinkFileInfo() {
    return hardLinkFileInfo;
  }

  public void incReferenceCnt(){
    this.hardLinkFileInfo.addLinkedFile(this);
  }
  
  public int getReferenceCnt(){
    return this.hardLinkFileInfo.getReferenceCnt();
  }
  
  public long getHardLinkID() {
    return this.hardLinkFileInfo.getHardLinkID();
  }

  @Override
  int collectSubtreeBlocksAndClear(List<Block> v, int blocksLimit) {
    parent = null;
    this.hardLinkFileInfo.removeLinkedFile(this);
    return 1;
  }
}
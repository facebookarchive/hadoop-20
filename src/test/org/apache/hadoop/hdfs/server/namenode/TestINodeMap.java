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

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.junit.Test;

public class TestINodeMap extends TestCase {

  @Test
  public void testGet() {
    INodeMap a = new INodeMap(100);
    for (int i = 1; i < 100; i++) {
      a.put(generateINode(i));
    }
    
    assertNotNull(a.get(1));
    assertNull(a.get(101));
  }
  
  private INode generateINode(long inodeId) {
    return new INode(inodeId, new PermissionStatus("", "", new FsPermission((short) 0)), 0, 0) {
      @Override
      long[] computeContentSummary(long[] summary) {
        return null;
      }

      @Override
      DirCounts spaceConsumedInTree(DirCounts counts) {
        return null;
      }

      @Override
      public boolean isDirectory() {
        return false;
      }

      @Override
      int collectSubtreeBlocksAndClear(List<BlockInfo> v, 
                                       int blocksLimit, 
                                       List<INode> removedINodes) {
        return 0;
      }
    };
  }
}

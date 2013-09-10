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

import org.apache.hadoop.hdfs.util.LightWeightGSet;

/**
 * A lighter weight implementation of LightWeightGSet optimized for inode query by id
 *
 */
public class INodeMap extends LightWeightGSet<INode, INode>{

  public INodeMap(int recommended_length) {
    super(recommended_length);
  }
  
  /**
   * This has to be consistent with {@link INode#hashCode()}
   * @param inodeId
   * @return
   */
  private int getIndex(final long inodeId) {
    return INode.getHashCode(inodeId) & hash_mask;
  }
  
  public INode get(long inodeId) {
    //find element
    final int index = getIndex(inodeId);
    for(LinkedElement e = entries[index]; e != null; e = e.getNext()) {
      INode n = (INode)e;
      if (n.getId() == inodeId) {
        return n;
      }
    }
    //element not found
    return null;
  }
  
}

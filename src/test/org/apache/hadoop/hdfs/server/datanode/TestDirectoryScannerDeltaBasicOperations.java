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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;

import junit.framework.TestCase;

public class TestDirectoryScannerDeltaBasicOperations extends TestCase {
  
  public void testDeltaBasicOperations() throws Exception {
    FSDatasetDelta delta = new FSDatasetDelta();
    delta.startRecordingDelta();
    int nsid = 1989;
    
    delta.addBlock(nsid, new Block(1));
    assertEquals(1, delta.size(nsid));
    
    // check delta is not accepting any blocks if
    // it is switched off
    delta.stopRecordingDelta();
    delta.addBlock(nsid, new Block(2));
    assertEquals(1, delta.size(nsid));
    
    delta.startRecordingDelta();
    
    delta.addBlock(nsid, new Block(2));
    assertEquals(2, delta.size(nsid));
    
    // the operation should overwrite the previous one
    delta.removeBlock(nsid, new Block(2, 100, 1001));
    assertEquals(2, delta.size(nsid));
    
    // this operation should be added
    delta.removeBlock(nsid, new Block(3));
    assertEquals(3, delta.size(nsid));
    
    delta.updateBlock(nsid, new Block(2), new Block(2, 101, 1002));
    assertEquals(3, delta.size(nsid));
  }

}

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

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Before;

import junit.framework.TestCase;

public class TestHost2NodesMap extends TestCase {
  private Host2NodesMap map;
  
  private final static DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
    new DatanodeDescriptor(new DatanodeID("127.0.0.1:5020"), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("127.0.0.1:5021"), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("127.0.0.1:5022"), "/d1/r2"),
    new DatanodeDescriptor(new DatanodeID("127.0.0.1:5030"), "/d1/r2"),
  };
  private final static DatanodeDescriptor NULL_NODE = null; 
  private final static DatanodeDescriptor NODE = 
    new DatanodeDescriptor(new DatanodeID("127.0.0.1:5040"), "/d1/r4");
  
  @Before
  public void setUp() {
    map = new Host2NodesMap();
    for(DatanodeDescriptor node:dataNodes) {
      map.add(node);
    }
    map.add(NULL_NODE);
  }
  
  public void testContains() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      assertTrue(map.contains(dataNodes[i]));
    }
    assertFalse(map.contains(NULL_NODE));
    assertFalse(map.contains(NODE));
  }

  public void testGetDatanodeByName() throws Exception {
    assertTrue(map.getDatanodeByName("127.0.0.1:5020")==dataNodes[0]);
    assertNull(map.getDatanodeByName("h1:5030"));
    assertTrue(map.getDatanodeByName("127.0.0.1:5021")==dataNodes[1]);
    assertNull(map.getDatanodeByName("h2:5030"));
    assertTrue(map.getDatanodeByName("127.0.0.1:5022")==dataNodes[2]);
    assertTrue(map.getDatanodeByName("127.0.0.1:5030")==dataNodes[3]);
    assertNull(map.getDatanodeByName("h3:5040"));
    assertNull(map.getDatanodeByName("127.0.0.1"));
    assertNull(map.getDatanodeByName(null));
  }

  public void testRemove() throws Exception {
    assertFalse(map.remove(NODE));
    assertTrue(map.remove(dataNodes[0]));
    
    assertNull(map.getDatanodeByName("127.0.0.1:5020"));
    assertTrue(map.remove(dataNodes[2]));

    assertNull(map.getDatanodeByName("127.0.0.1:5022"));
    assertTrue(map.getDatanodeByName("127.0.0.1:5021")==dataNodes[1]);
  }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;

import org.apache.hadoop.hdfs.protocol.Block;
import org.junit.Test;


public class TestBlockCrcFile {
  /*
   * Test BlockCrcFileReader can read what BlockCrcFileWrite wrote.
   */
  @Test
  public void testWriterReader() throws Exception {
    List<Map<Block, DatanodeBlockInfo>> mbds = new ArrayList<Map<Block, DatanodeBlockInfo>>();

    mbds.add(new HashMap<Block, DatanodeBlockInfo>());
    mbds.add(new HashMap<Block, DatanodeBlockInfo>());

    mbds.get(0).put(new Block(666, 0, 10666), new DatanodeBlockInfo(null, null, 0,
        true, true, 0, 256, true, 20666));
    mbds.get(0).put(new Block(888, 0, 10888), new DatanodeBlockInfo(null, null, 0,
        true, true, 0, 256, true, 20888));
    mbds.get(1).put(new Block(777, 0, 10777), new DatanodeBlockInfo(null, null, 0,
        true, true, 0, 256, true, 20777));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    BlockCrcMapFlusher.writeToCrcFile(mbds, dos);
    dos.close();

    Set<Long> blockSet = new HashSet<Long>();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    BlockCrcFileReader reader = new BlockCrcFileReader(dis);
    reader.readHeader();
    for (int i = 0; i < 3; i++) {
      BlockCrcInfoWritable writable = reader.getNextRecord();
      TestCase.assertTrue(writable.blockId % 111 == 0
          && writable.blockId / 111 > 5 && writable.blockId / 111 < 9);
      TestCase.assertFalse(blockSet.contains(writable.blockId));
      blockSet.add(writable.blockId);
      TestCase.assertEquals(writable.blockGenStamp, 10000 + writable.blockId);
      TestCase.assertEquals(writable.blockCrc, 20000 + writable.blockId);
    }
    TestCase.assertNull(reader.getNextRecord());
    dis.close();
  }
}

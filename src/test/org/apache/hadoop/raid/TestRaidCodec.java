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

package org.apache.hadoop.raid;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.raid.ReedSolomonCode;
import org.apache.hadoop.raid.XORCode;

public class TestRaidCodec extends TestCase {
  String jsonStr =
      "    [\n" +
      "      {   \n" +
      "        \"id\"            : \"rs\",\n" +
      "        \"num_data_blocks\" : 10,\n" +
      "        \"num_parity_blocks\" : 4,\n" +
      "        \"stripe_chunk_size\"      : 65536,\n" +
      "        \"parity_repl\"   : 1,\n" +
      "        \"min_source_repl\" : 1,\n" + 
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.ReedSolomonCode\",\n" +
      "        \"description\"   : \"ReedSolomonCode code\",\n" +
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"xor\",\n" +
      "        \"num_data_blocks\" : 10, \n" +
      "        \"num_parity_blocks\" : 1,\n" +
      "        \"stripe_chunk_size\"      : -1,\n" +
      "        \"parity_repl\"   : 2,\n" +
      "        \"min_source_repl\" : 2,\n" + 
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.XORCode\",\n" +
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"sr\",\n" +
      "        \"num_data_blocks\" : 10, \n" +
      "        \"num_parity_blocks\" : 5, \n" +
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.SimpleRegeneratingCode\",\n" +
      "        \"stripe_chunk_size\"      : 200,\n" +
      "        \"parity_repl\"   : 3,\n" +
      "        \"min_source_repl\" : 3,\n" + 
      "        \"description\"   : \"SimpleRegeneratingCode code\",\n" +
      "      },  \n" +
      "    ]\n";
  
  public void testCreation() throws Exception {
    Configuration conf = new Configuration();
    
    conf.set(RaidCodec.HDFS_RAID_CODEC_JSON, jsonStr);
    RaidCodec.initializeCodecs(conf);

    assertEquals("xor", RaidCodec.getCodec("xor").id);
    assertEquals("rs", RaidCodec.getCodec("rs").id);
    assertEquals("sr", RaidCodec.getCodec("sr").id);

    List<RaidCodec> codecs = RaidCodec.getCodecs();

    assertEquals(3, codecs.size());

    assertEquals("rs", codecs.get(0).id);
    assertEquals(10, codecs.get(0).numDataBlocks);
    assertEquals(4, codecs.get(0).numParityBlocks);
    assertEquals(65536, codecs.get(0).stripeChunkSize);
    assertEquals(1, codecs.get(0).parityReplication);
    assertEquals(1, codecs.get(0).minSourceReplication);
    assertEquals("ReedSolomonCode code", codecs.get(0).description);

    assertEquals("xor", codecs.get(1).id);
    assertEquals(10, codecs.get(1).numDataBlocks);
    assertEquals(1, codecs.get(1).numParityBlocks);
    assertEquals(-1, codecs.get(1).stripeChunkSize);
    assertEquals(2, codecs.get(1).parityReplication);
    assertEquals(2, codecs.get(1).minSourceReplication);
    assertEquals("", codecs.get(1).description);
    
    assertEquals("sr", codecs.get(2).id);
    assertEquals(10, codecs.get(2).numDataBlocks);
    assertEquals(5, codecs.get(2).numParityBlocks);
    assertEquals(200, codecs.get(2).stripeChunkSize);
    assertEquals(3, codecs.get(2).parityReplication);
    assertEquals(3, codecs.get(2).minSourceReplication);
    assertEquals("SimpleRegeneratingCode code", codecs.get(2).description);

    assertTrue(codecs.get(0).createErasureCode(conf) instanceof ReedSolomonCode);
    assertTrue(codecs.get(1).createErasureCode(conf) instanceof XORCode);
  }
  
  public void testMultiThreadCreation() 
      throws InterruptedException, ExecutionException {
    final Configuration conf = new Configuration();
    
    conf.set(RaidCodec.HDFS_RAID_CODEC_JSON, jsonStr);
    
    int numThreads = 100;
    ExecutorService excutor = Executors.newFixedThreadPool(numThreads);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    for (int i=0; i<numThreads; i++) {
      futures.add(excutor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          RaidCodec.initializeCodecs(conf);
          return true;
        }
      }));
    }
    
    for (int i=0; i<numThreads; i++) {
      assertTrue(futures.get(i).get());
    }
  }

}
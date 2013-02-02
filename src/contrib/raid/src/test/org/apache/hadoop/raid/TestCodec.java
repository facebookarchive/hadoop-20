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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

public class TestCodec extends TestCase {
  String jsonStr =
      "    [\n" +
      "      {   \n" +
      "        \"id\"            : \"rs\",\n" +
      "        \"parity_dir\"    : \"/raidrs\",\n" +
      "        \"stripe_length\" : 10,\n" +
      "        \"parity_length\" : 4,\n" +
      "        \"priority\"      : 300,\n" +
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.ReedSolomonCode\",\n" +
      "        \"description\"   : \"ReedSolomonCode code\",\n" +
      "        \"simulate_block_fix\"  : true,\n" +
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"xor\",\n" +
      "        \"parity_dir\"    : \"/raid\",\n" +
      "        \"stripe_length\" : 10, \n" +
      "        \"parity_length\" : 1,\n" +
      "        \"priority\"      : 100,\n" +
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.XORCode\",\n" +
      "        \"simulate_block_fix\"  : false,\n" + 
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"sr\",\n" +
      "        \"parity_dir\"    : \"/raidsr\",\n" +
      "        \"stripe_length\" : 10, \n" +
      "        \"parity_length\" : 5, \n" +
      "        \"degree\"        : 2,\n" +
      "        \"erasure_code\"  : \"org.apache.hadoop.raid.SimpleRegeneratingCode\",\n" +
      "        \"priority\"      : 200,\n" +
      "        \"description\"   : \"SimpleRegeneratingCode code\",\n" +
      "        \"simulate_block_fix\"  : false,\n" +
      "      },  \n" +
      "    ]\n";
  
  public void testCreation() throws Exception {
    Configuration conf = new Configuration();
    
    conf.set("raid.codecs.json", jsonStr);
    Codec.initializeCodecs(conf);

    assertEquals("xor", Codec.getCodec("xor").id);
    assertEquals("rs", Codec.getCodec("rs").id);
    assertEquals("sr", Codec.getCodec("sr").id);

    List<Codec> codecs = Codec.getCodecs();

    assertEquals(3, codecs.size());

    assertEquals("rs", codecs.get(0).id);
    assertEquals(10, codecs.get(0).stripeLength);
    assertEquals(4, codecs.get(0).parityLength);
    assertEquals(300, codecs.get(0).priority);
    assertEquals("/raidrs", codecs.get(0).parityDirectory);
    assertEquals("/tmp/raidrs", codecs.get(0).tmpParityDirectory);
    assertEquals("/tmp/raidrs_har", codecs.get(0).tmpHarDirectory);
    assertEquals("ReedSolomonCode code", codecs.get(0).description);
    assertEquals(true, codecs.get(0).simulateBlockFix);

    assertEquals("sr", codecs.get(1).id);
    assertEquals(10, codecs.get(1).stripeLength);
    assertEquals(5, codecs.get(1).parityLength);
    assertEquals(200, codecs.get(1).priority);
    assertEquals("/raidsr", codecs.get(1).parityDirectory);
    assertEquals("/tmp/raidsr", codecs.get(1).tmpParityDirectory);
    assertEquals("/tmp/raidsr_har", codecs.get(1).tmpHarDirectory);
    assertEquals("SimpleRegeneratingCode code", codecs.get(1).description);
    assertEquals(false, codecs.get(1).simulateBlockFix);

    assertEquals("xor", codecs.get(2).id);
    assertEquals(10, codecs.get(2).stripeLength);
    assertEquals(1, codecs.get(2).parityLength);
    assertEquals(100, codecs.get(2).priority);
    assertEquals("/raid", codecs.get(2).parityDirectory);
    assertEquals("/tmp/raid", codecs.get(2).tmpParityDirectory);
    assertEquals("/tmp/raid_har", codecs.get(2).tmpHarDirectory);
    assertEquals("", codecs.get(2).description);
    assertEquals(false, codecs.get(2).simulateBlockFix);

    assertTrue(codecs.get(0).createErasureCode(conf) instanceof ReedSolomonCode);
    assertTrue(codecs.get(2).createErasureCode(conf) instanceof XORCode);
  }
  
  public void testMultiThreadCreation() 
      throws InterruptedException, ExecutionException {
    final Configuration conf = new Configuration();
    
    conf.set("raid.codecs.json", jsonStr);
    
    int numThreads = 100;
    ExecutorService excutor = Executors.newFixedThreadPool(numThreads);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    for (int i=0; i<numThreads; i++) {
      futures.add(excutor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Codec.initializeCodecs(conf);
          return true;
        }
      }));
    }
    
    for (int i=0; i<numThreads; i++) {
      assertTrue(futures.get(i).get());
    }
  }

}

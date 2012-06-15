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
package org.apache.hadoop.hdfs.util;

import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.util.LightWeightBitSet;

public class TestLightWeightBitSet extends TestCase {

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestLightWeightBitSet");
  private boolean[] set;
  private int numSet;
  private final int NUM = 100;
  private Random rand;

  protected void setUp() {
    rand = new Random(System.currentTimeMillis());
    set = new boolean[NUM];
    numSet = 0;

    for (int i = 0; i < NUM; i++) {
      boolean isSet = rand.nextBoolean();
      if (isSet)
        numSet++;
      set[i] = isSet;
    }
  }

  public void testEmpty() {
    LOG.info("Test empty");

    long[] bitset = LightWeightBitSet.getBitSet(0);
    // bit set should be 0
    assertEquals(0, bitset.length);
    try {
      LightWeightBitSet.set(bitset, 1);
      assertTrue(false);
    } catch (Exception e) {
      // IndexOutOfBoundsException should be thrown
    }
    assertFalse(LightWeightBitSet.get(bitset, 0));

    LOG.info("Test empty -- DONE");
  }

  public void testMulti() {
    LOG.info("Test multi basic");

    long[] bitset = LightWeightBitSet.getBitSet(set.length);
    for (int i = 0; i<NUM; i++){
      if(set[i]){
        LightWeightBitSet.set(bitset, i);
      } else {
        LightWeightBitSet.clear(bitset, i);
      }
    }
    
    // check the cardinality (number of set bits
    assertEquals(numSet, LightWeightBitSet.cardinality(bitset));
    // check if the proper bits are set
    for(int i = 0; i<NUM; i++){
      assertEquals(set[i], LightWeightBitSet.get(bitset, i));
    }
    int capacity = LightWeightBitSet.getMaxCapacity(bitset);
    // the rest should not be set
    for(int i=NUM; i<capacity; i++) {
      assertFalse(LightWeightBitSet.get(bitset, i));
    }
    // out of bounds should return false
    assertFalse(LightWeightBitSet.get(bitset, capacity));
    
    try {
      LightWeightBitSet.set(bitset, capacity);
      assertTrue(false);
    } catch (Exception e) {
      // Should get IndexOutOfBoundException
    }
    
    LOG.info("Test multi - DONE");
  }
}

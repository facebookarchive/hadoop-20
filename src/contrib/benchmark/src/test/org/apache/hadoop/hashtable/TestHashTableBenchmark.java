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

package org.apache.hadoop.hashtable;

/**
 * TestHashTableBenchmark
 *
 * This class contains several benchamrks for HashTable related issues:
 *
 * HashTableBenchmark(String filename, int which, int capacity, int count, boolean linkedElements)
 *
 * filename - file containing a list of block ids
 *
 * which - 0 - read file with block ids (filename must be valid)
 *         1 - generate ids with Java.Random
 *         2 - generate ids with MT random generator
 *         3 - generate ids with R250_521 random generator
 *
 * capacity - the number of entries in the hash table (for hash tests, it's used to
 *            compute the number of entries in each bucket
 *
 * count  - the number of ids to generate (ignored when using a block file)
 *
 * linkedElements - must be true for testLightweightSetHashing()
 *                  (creates linked elements used for this test)
 *                  must be false for testMultiHashing()
 *
 * 1. testing Hash functions - testHashFunctions()
 *    - test distribution of entries across buckets for several hash functions
 * 2. testLightweightSetHashing()
 *    - LightWeightGSet used currently in the NN
 *    - LightWeightGSetMulti - variation of the former that uses two hash functions
 * 3. testMultiHashing(x)
 * x= 0 - HashTable with Linear Probing collision resolution (cr)
 *    1 - HashTable with Quadratic Probing cr
 *    2 - HashTable with Double Hashing cr
 *    3 - HashTable with Cuckoo Hashing cr
 *    NOTICE - in all 4 cases "count" must be less than "capacity",
 *    as the rehashing mechanisms are not implemented.
 *
 */
import junit.framework.TestCase;

public class TestHashTableBenchmark extends TestCase {

  private final int million = 1024 * 1024;

  public void testHashes() throws Exception {
    HashTableBenchmark th = new HashTableBenchmark("", 1, 1 * million , 2 * million, false);
    th.testHashFunctions();
  }

  public void testLightWeightSets() throws Exception {
    HashTableBenchmark t1 = new HashTableBenchmark("", 1, 1 * million, 2 * million, true);
    t1.testLightweightSetHashing(0);
    t1.testLightweightSetHashing(1);
  }

  public void testHashTables() throws Exception {
    HashTableBenchmark t2 = new HashTableBenchmark("", 1, 4 * million, 2 * million, false);
    t2.testMultiHashing(0);
    t2.testMultiHashing(1);
    t2.testMultiHashing(2);
    t2.testMultiHashing(3);
  }
}

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
 * Fixed size hashtable with Linear/Quad Hashing collision resolution. Not fully
 * functional - may fail when putting a new element.
 *
 * @author tomasz
 *
 */
public class QuadHash implements THashSet {

  private final int shiftAttempts = 100;
  private int hash_mask;
  private Long[] entries;
  private int failed;
  private int[] hops;
  private int c = 1;

  public QuadHash(int size, int c) {
    entries = new Long[size];
    hash_mask = size - 1;
    hops = new int[50];
    // c = 0 -> linear probing, c=1 -> quad probing
    this.c = c;
  }

  public Long get(Long id) {
    int hash1 = getHash(id.longValue());
    int pos = hash1;
    int k = 1;
    while ((entries[pos] != null) && (!entries[pos].equals(id))) {
      pos = (hash1 + k + c * k * k) & (hash_mask);
      k++;
      if (k > shiftAttempts) {
        return null;
      }
    }
    return entries[pos];
  }

  public Long put(Long id) {
    int hash1 = getHash(id.longValue());
    int pos = hash1;

    int k = 1;
    while ((entries[pos] != null) && (!entries[pos].equals(id))) {
      pos = (hash1 + k + c * k * k) & (hash_mask);
      k++;
      if (k > shiftAttempts) {
        failed++;
        return null;
      }
    }
    if (k < hops.length)
      hops[k]++;
    entries[pos] = id;
    return id;
  }

  public int getFailed() {
    return failed;
  }

  public String toString() {
    String ret = "QuadHash: c=" + c + " : ";
    for (int i = 0; i < hops.length; i++) {
      ret += "h[" + i + "]=" + hops[i] + "] ";
    }
    return ret + "\n";
  }

  public Long remove(Long id) {
    int hash1 = getHash(id.longValue());
    int pos = hash1;
    int k = 1;
    while ((entries[pos] != null) && (!entries[pos].equals(id))) {
      pos = (hash1 + k + k * k) & (hash_mask);
      k++;
      if (k > shiftAttempts) {
        return null;
      }
    }
    Long temp = entries[pos];
    entries[pos] = null;
    return temp;
  }

  private int getHash(long id) {
    return Hashes.getHash(id, 0) & hash_mask;
  }

}

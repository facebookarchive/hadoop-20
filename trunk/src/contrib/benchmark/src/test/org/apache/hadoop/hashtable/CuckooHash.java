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
 * Fixed size hashtable with CuckooHashing collision resolution. Not fully
 * functional - does not do rehashing, and may remove already present elements
 * when putting a new element.
 *
 * @author tomasz
 *
 */
public class CuckooHash implements THashSet {

  private final int shiftAttempts = 100;
  private int hash_mask;
  private Long[] entries;
  private int failed;

  public CuckooHash(int size) {
    entries = new Long[size];
    hash_mask = size - 1;
  }

  public Long get(Long id) {
    if (entries[getHash1(id)].equals(id))
      return id;
    if (entries[getHash2(id)].equals(id))
      return id;
    return null;
  }

  public Long put(Long id) {
    int n = 0;
    long originalId = id;
    int pos = getHash1(id);
    if ((entries[pos] == null) || (entries[pos].equals(id))) {
      entries[pos] = id;
      return id;
    }
    int pos2 = getHash1(id);
    if ((entries[pos2] == null) || (entries[pos2].equals(id))) {
      entries[pos2] = id;
      return id;
    }

    while (n < shiftAttempts) {
      if (entries[pos] == null) {
        entries[pos] = id;
        return originalId;
      }
      Long temp = entries[pos];
      entries[pos] = id;
      id = temp;
      n++;
      int hash1 = getHash1(id);
      if (pos == hash1) {
        pos = getHash2(id);
      } else {
        pos = hash1;
      }
    }
    // when "put" fails it expells a previously present element
    // this implementation is only for testing purposes
    failed++;
    return id;
  }

  public int getFailed() {
    return failed;
  }

  public Long remove(Long id) {
    if (entries[getHash1(id)].equals(id)) {
      entries[getHash1(id)] = null;
      return id;
    }
    if (entries[getHash2(id)].equals(id)) {
      entries[getHash2(id)] = null;
      return id;
    }
    return null;
  }

  private int getHash1(long id) {
    return Hashes.getHash(id, 0) & hash_mask;
  }

  private int getHash2(long id) {
    return Hashes.getHash(id, 1) & hash_mask;
  }
}

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

package org.apache.hadoop.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K,V> {

  private static final float   hashTableLoadFactor = 0.75f;

  private LinkedHashMap<K,V>   map;
  private int                  cacheSize;

 /**
  * Creates a new LRU cache.
  * @param cacheSize the maximum number of entries in this cache.
  */
  public LRUCache(int cacheSize) {
    this.cacheSize = cacheSize;
    int hashTableCapacity = (int)Math.ceil(cacheSize / hashTableLoadFactor) + 1;
    map = new LinkedHashMap<K,V>(hashTableCapacity, hashTableLoadFactor, true) {
      private static final long serialVersionUID = 1;
      @Override 
      protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
         return size() > LRUCache.this.cacheSize; }}; 
  }

  /**
  * Retrieves an entry from the cache.<br>
  * The retrieved entry becomes the most recently used entry.
  * @param key the key whose associated value is to be returned.
  * @return the value associated to this key, or null if no value with this key exists in the cache.
  */
  public synchronized V get(K key) {
   return map.get(key); 
  }

  /**
  * Adds an entry to this cache.
  * The new entry becomes the most recently used entry.
  * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
  * If the cache is full, the LRU (least recently used) entry is removed from the cache.
  * @param key    the key with which the specified value is to be associated.
  * @param value  a value to be associated with the specified key.
  */
  public synchronized void put(K key, V value) {
    map.put(key, value); 
  }

  /**
  * Remove an entry from this cache.
  * If an entry with the specified key already exists in the cache, it is removed.
  * If the entry does not exist, then null is returned.
  * @param key    the key with which the specified value is to be associated.
  * @param value  the value that was associated with the specified key, null otherwise.
  */
  public synchronized V remove(K key) {
    return map.remove(key);
  }

  /**
   * Empty the cache.
   */
  public synchronized void clear() {
    map.clear();
  }

  /**
   * Returns the current size of the cache.
   * @return the number of entries currently in the cache.
   */
  public synchronized int size() {
   return map.size(); 
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.mortbay.log.Log;

/** 
 * A Floyd variation of PriorityQueue. For a simple description see:
 *     http://en.wikipedia.org/wiki/Heapsort#Variations
 *   
 * Floyd variation works better if the following two assumptions hold:
 *   1. Key comparisons are expensive
 *   2. Key distribution from different input streams are not skewed
 *      (which means the new key in the heap is expected to be landed
 *      at the leaf level (which consists of half of all nodes).
 */
public abstract class PriorityQueueFloyd<T> extends PriorityQueue<T> {
  @Override
  protected void downHeap() {
    int i = 1;
    T node = heap[i];       // save top node
    int j = i << 1;         // find smaller child
    int k = j + 1;
    while (k <= size) {
      if (lessThan(heap[k], heap[j])) {
        j = k;
      }
      heap[i] = heap[j];
      i = j;
      j = i << 1;
      k = j + 1;
    }
    // set node to the leaf and push upHeap
    if (j > size) { // i is a leaf node
      j = i;
    } else {        // j is the last leaf node
      assert(j == size);
      heap[i] = heap[j];
    }
    heap[j] = node;
    upHeap(j);
  }
}

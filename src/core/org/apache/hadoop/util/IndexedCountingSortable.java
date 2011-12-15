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

/**
 * Liner time inplace counting sort
 */
public abstract class IndexedCountingSortable {
  abstract public int getKey(int i);

  abstract public int get(int i);

  abstract public void put(int i, int v);

  final int[] counts;
  final int[] starts;
  final int total;

  public IndexedCountingSortable(int[] counts, int total) {
    this.total = total;
    this.counts = counts;
    this.starts = new int[counts.length];
    for (int i = 1; i < counts.length; i++) {
      starts[i] = starts[i - 1] + counts[i - 1];
    }
    assert (starts[counts.length - 1] + counts[counts.length - 1] == total);
  }

  public void sort() {
    int[] dest = new int[total];
    for (int i = 0; i < total; i++) {
      int p = getKey(i);
      dest[starts[p]++] = get(i);
    }
    for (int i = 0; i < total; i++) {
      put(i, dest[i]);
    }
  }

  private int findSwapPosition(int partition) {
    while (counts[partition] > 0) {
      counts[partition]--;
      int pos = starts[partition] + counts[partition];
      int part = getKey(pos);
      if (part != partition) {
        return part;
      }
    }
    return -1;
  }

  public void sortInplace() {
    for (int i = 0; i < counts.length; i++) {
      while (true) {
        int part = findSwapPosition(i);
        if (part < 0) {
          break;
        }
        int hole = starts[i] + counts[i];
        int tempOffset = get(hole);
        while (true) {
          int next = findSwapPosition(part);
          int pos = starts[part] + counts[part];
          int temp = get(pos);
          put(pos, tempOffset);
          tempOffset = temp;
          if (i == next) {
            put(hole, tempOffset);
            break;
          }
          part = next;
        }
      }
    }
  }
}

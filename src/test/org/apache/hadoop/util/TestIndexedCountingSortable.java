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

import java.util.Random;

import junit.framework.TestCase;

public class TestIndexedCountingSortable extends TestCase {
  final static int INPUTSIZE = 2500000;
  final static int PARTITION = 500;
  final int[] values = new int[INPUTSIZE];
  final int[] indexes = new int[INPUTSIZE];
  final int[] counts = new int[PARTITION];

  void makeInput() {
    Random r = new Random();
    r.setSeed(10);
    for (int i = 0; i < values.length; ++i) {
      indexes[i] = i;
      values[i] = r.nextInt(PARTITION);
      counts[values[i]]++;
    }
  }

  void validate() {
    int [] has = new int[INPUTSIZE];
    for (int i=0;i<INPUTSIZE;i++) {
      if (i>0) {
        int l = values[indexes[i]];
        int r = values[indexes[i-1]];
        assertTrue(l>=r);
      }
      has[indexes[i]]++;
    }
    for (int i=0;i<INPUTSIZE;i++) {
      assertTrue(has[i]==1);
    }
  }

  public void runCountingSort() throws Exception {
    makeInput();
    IndexedCountingSortable sorter = new
      IndexedCountingSortable(counts, INPUTSIZE) {
        @Override
        public int get(int i) {
          return indexes[i];
        }

        @Override
        public void put(int i, int v) {
          indexes[i] = v;
        }

        @Override
        public int getKey(int i) {
          return values[indexes[i]];
        }
      };
    long start = System.nanoTime();
    sorter.sort();
    long end = System.nanoTime();
    System.out.printf("Counting sort: %.3f\n", (end - start) / 1000000000.0);
    validate();
  }

  public void runInplaceCountingSort() throws Exception {
    makeInput();
    IndexedCountingSortable sorter = new
      IndexedCountingSortable(counts, INPUTSIZE) {
        @Override
        public int get(int i) {
          return indexes[i];
        }

        @Override
        public void put(int i, int v) {
          indexes[i] = v;
        }

        @Override
        public int getKey(int i) {
          return values[indexes[i]];
        }
      };
    long start = System.nanoTime();
    sorter.sortInplace();
    long end = System.nanoTime();
    System.out.printf("Inplcae counting sort: %.3f\n", 
                      (end - start) / 1000000000.0);
    validate();
  }
  
  public void runQuickSort() throws Exception {
    makeInput();
    IndexedSortable dest = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        int temp = indexes[i];
        indexes[i] = indexes[j];
        indexes[j] = temp;
      }
      
      @Override
      public int compare(int i, int j) {
        return values[indexes[i]] - values[indexes[j]];
      }
    };
    QuickSort sorter = new QuickSort();
    long start = System.nanoTime();
    sorter.sort(dest, 0, INPUTSIZE, null);
    long end = System.nanoTime();
    System.out.printf("Quick sort: %.3f\n", (end - start) / 1000000000.0);
  }
  
  public void testSort() throws Exception {
    runCountingSort();
    runInplaceCountingSort();
    runQuickSort();
  }
}

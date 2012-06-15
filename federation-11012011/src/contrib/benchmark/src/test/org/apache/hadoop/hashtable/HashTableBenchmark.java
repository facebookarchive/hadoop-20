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

import java.util.HashMap;
import java.util.Map;
import java.io.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HashTableBenchmark {
  private static final Log LOG = LogFactory.getLog(HashTableBenchmark.class);

  private int capacity = 64 * 1024 * 1024;
  private int maxsize = 90000000;
  private Long[] ids;
  private LongInfo[] idsLI;
  private int divider = 100000000;

  private int NUM_NODES = 0;
  private int hash_mask = capacity - 1;
  private String blockFile;
  private boolean linkedElements = false;
  private RandomGen rg;

  public HashTableBenchmark(String filename, int which, int capacity,
      int count, boolean linkedElements) {
    this.capacity = capacity;
    this.hash_mask = capacity - 1;
    this.maxsize = count;
    this.blockFile = filename;
    this.linkedElements = linkedElements;
    switch (which) {
    case 0:
      readBlockFile();
      break;
    default:
      rg = new RandomGen(which);
      generateRandom();
    }
  }

  private String getHistogram(int[] entries) {
    Map<Integer, Integer> hist = new HashMap<Integer, Integer>();

    for (int i = 0; i < entries.length; i++) {
      Integer count = hist.get(entries[i]);
      if (count == null) {
        hist.put(entries[i], 1);
      } else {
        hist.put(entries[i], count + 1);
      }
    }
    return "HISTOGRAM: entriesLen: " + entries.length + " -- "
        + hist.toString();
  }

  // //////////////// READ + RANDOM GENERATORS

  private void readBlockFile() {
    try {
      LOG.info("----> READ BLOCK FILE : START");
      initArray();
      FileInputStream fstream = new FileInputStream(blockFile);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new DataInputStream(fstream)));
      String strLine;
      NUM_NODES = 0;
      while ((strLine = br.readLine()) != null) {
        if (NUM_NODES % divider == 0)
          LOG.info("Processed : " + NUM_NODES);
        updateArray(NUM_NODES, Long.parseLong(strLine));
        NUM_NODES++;
      }
      in.close();
      LOG.info("----> READ BLOCK FILE : DONE: Read " + NUM_NODES + " block ids");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void generateRandom() {
    initArray();
    long start, stop;
    LOG.info("---------->GENERATING RANDOM IDS ---------->");
    start = System.currentTimeMillis();
    for (int i = 0; i < maxsize; i++) {
      updateArray(i, rg.next());
    }
    NUM_NODES = maxsize;
    stop = System.currentTimeMillis();
    LOG.info("---------->GENERATING RANDOM IDS DONE -- TIME: "
        + ((stop - start) / 1000.0) + " GENRATED: " + NUM_NODES + " ids ");
  }

  // //////////////////////////////////////////////////////////

  private void initArray() {
    if (linkedElements) {
      idsLI = new LongInfo[maxsize];
    } else {
      ids = new Long[maxsize];
    }
  }

  private void updateArray(int i, long id) {
    if (linkedElements) {
      idsLI[i] = new LongInfo(id);
    } else {
      ids[i] = new Long(id);
    }
  }

  // //////////////////////////////////////////////////////////

  public void testMultiHashing(int mode) {
    LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
    LOG.info("-------------------->MULTIHASHING------------------->");
    long start, stop;
    THashSet c = null;
    if (mode == 0) {
      c = new QuadHash(capacity, 0);
      LOG.info("LINEAR COLLISION RESOLUTION");
    } else if (mode == 1) {
      c = new QuadHash(capacity, 1);
      LOG.info("QUAD COLLISION RESOLUTION");
    } else if (mode == 2) {
      c = new DoubleHash(capacity);
      LOG.info("DOUBLE HASH COLLISION RESOLUTION");
    } else if (mode == 3) {
      c = new CuckooHash(capacity);
      LOG.info("CUCKOO HASH COLLISION RESOLUTION");
    }

    start = System.currentTimeMillis();
    for (int i = 0; i < NUM_NODES; i++) {
      c.put(ids[i]);
    }
    stop = System.currentTimeMillis();

    LOG.info("--------------->MULTIHASHING PUT DONE--------------->");
    LOG.info(" TIME: " + ((stop - start) / 1000.0));
    LOG.info(" FAILED : " + c.getFailed());

    start = System.currentTimeMillis();
    int present = 0;
    for (int i = 0; i < NUM_NODES; i++) {
      Long getElem = c.get(ids[i]);
      if (getElem != null && getElem.equals(ids[i])) {
        present++;
      }
    }

    stop = System.currentTimeMillis();
    LOG.info("--------------->MULTIHASHING GET DONE--------------->");
    LOG.info(" TIME: " + ((stop - start) / 1000.0));
    LOG.info(" NOT PRESENT: " + (NUM_NODES - present));
  }

  public void testLightweightSetHashing(int mode) {
    LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
    LOG.info("------------------>LIGHTWEIGHTGSET------------------>");
    long start, stop;
    LightWeightSet c = null;
    if (mode == 0) {
      c = new LightWeightGSet(capacity);
      LOG.info("SET VERSION: ONE HASH");
    } else {
      c = new LightWeightGSetMulti(capacity);
      LOG.info("SET VERSION: DOUBLE HASH");
    }
    start = System.currentTimeMillis();
    for (int i = 0; i < NUM_NODES; i++) {
      c.put(idsLI[i]);
    }
    stop = System.currentTimeMillis();
    LOG.info("------------->LIGHTWEIGHTGSET PUT DONE-------------->");
    LOG.info(" TIME: " + ((stop - start) / 1000.0));
    start = System.currentTimeMillis();
    int present = 0;

    LongInfo tempi = new LongInfo();
    for (int i = 0; i < NUM_NODES; i++) {
      tempi.setData(idsLI[i].data);
      LongInfo getElem = c.get(tempi);
      if (getElem != null && getElem.equals(tempi))
        present++;
    }

    stop = System.currentTimeMillis();
    LOG.info("------------->LIGHTWEIGHTGSET GET DONE-------------->");
    LOG.info(" TIME: " + ((stop - start) / 1000.0));
    LOG.info(" NOT PRESENT: " + (NUM_NODES - present));
  }

  public void testHashFunctions() {
    long start, stop;
    int[] map;

    LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
    for (int hash = 0; hash < 7; hash++) {
      LOG.info("------------------>" + Hashes.getHashDesc(hash)
          + "------------------>");
      map = new int[capacity];
      start = System.currentTimeMillis();
      for (int i = 0; i < NUM_NODES; i++) {
        map[Hashes.getHash(ids[i], hash) & hash_mask]++;
      }
      stop = System.currentTimeMillis();
      LOG.info("TIME: " + ((stop - start) / 1000.0));
      LOG.info("HIST :" + getHistogram(map));
    }

    LOG.info("================> Double Hashing ================>");

    map = new int[capacity];
    start = System.currentTimeMillis();
    for (int i = 0; i < NUM_NODES; i++) {
      int hash1 = Hashes.getHash32ShiftMul((int) (ids[i].longValue()))
          & hash_mask;
      if (map[hash1] == 0)
        map[hash1]++;
      else
        map[Hashes.getHash6432shift(ids[i]) & hash_mask]++;
    }
    stop = System.currentTimeMillis();
    LOG.info("TIME: " + ((stop - start) / 1000.0));
    LOG.info("HIST :" + getHistogram(map));

  }
}

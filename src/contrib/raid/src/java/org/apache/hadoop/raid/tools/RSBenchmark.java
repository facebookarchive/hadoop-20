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
package org.apache.hadoop.raid.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.raid.ErasureCode;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.ReedSolomonCode;
import org.apache.hadoop.raid.JerasureCode;

public class RSBenchmark {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.tools.RSBenchmark");
  public final static int DEFAULT_DATALEN = 1024*1024;
  public final static int MAX_ITERATION = 10;
  private Random rand = null;
  private int dataLen = DEFAULT_DATALEN;
  private ErasureCode rsCode = null;
  private int decodeLocation = 0;
  private List<Integer> locations = new ArrayList<Integer>();
  private int[] erasedLocations;
  private byte[] decodeVec;
  private byte[] expectVec;
  private byte[][] sourceVecTmp;
  private byte[][] sourceVec;
  private byte[][] parityVec;
  private byte[][] totalVec;
  private boolean verify = false;
  private boolean useNative = false;
  private int decodePos = 0;
  private int decodeLen = DEFAULT_DATALEN;
  
  public RSBenchmark(boolean newVerify, String encodeMethod,
      int dpos, int dlen, int elen, boolean useNative) {
    this(newVerify, encodeMethod, System.currentTimeMillis(),
        dpos, dlen, elen, useNative);
  }
  
  public RSBenchmark(boolean newVerify, String encodeMethod, long seed,
      int dpos, int dlen, int elen,
      boolean newUseNative) {
    LOG.info("Random seed: " + seed);
    rand = new Random(seed);
    decodePos = dpos;
    decodeLen = dlen;
    dataLen = elen;
    if (encodeMethod.equals("jerasure")) {
      rsCode = new JerasureCode();
    } else {
      rsCode = new ReedSolomonCode();
    }
    Codec rsCodec = Codec.getCodec(encodeMethod); 
    rsCode.init(rsCodec.stripeLength, rsCodec.parityLength);
    sourceVecTmp = new byte[rsCode.stripeSize()][];
    sourceVec = new byte[rsCode.stripeSize()][];
    parityVec = new byte[rsCode.paritySize()][];
    totalVec = new byte[rsCode.stripeSize() + rsCode.paritySize()][];
    for (int i = 0; i < parityVec.length; i++) {
      parityVec[i] = new byte[dataLen];
      locations.add(i);
      totalVec[i] = parityVec[i];
      useNative = newUseNative;
    }
    for (int i = 0; i < sourceVec.length; i++) {
      sourceVec[i] = new byte[dataLen];
      sourceVecTmp[i] = new byte[dataLen];
      locations.add(i + rsCode.paritySize());
      totalVec[rsCode.paritySize() + i] = sourceVec[i];
    }
    decodeVec = new byte[dataLen];
    erasedLocations = new int[rsCode.paritySize()];
    verify = newVerify;
  }
  
  private void init() {
    // populate with random data
    for (int i = 0; i < rsCode.stripeSize(); i++) {
      rand.nextBytes(sourceVecTmp[i]);
      for (int j = 0; j < dataLen; j++) {
        sourceVec[i][j] = sourceVecTmp[i][j];
      }
    }
  }
  
  public void benchmarkEncoding() {
    try {
      long totalTime = 0;
      for (int iter = 0; iter < MAX_ITERATION; iter++) {
        init();
        long startTime = System.currentTimeMillis();
        rsCode.encodeBulk(sourceVec, parityVec, useNative);
        totalTime += System.currentTimeMillis() - startTime;
      }
      System.out.println("ENCODING\t" + (totalTime / MAX_ITERATION) + "ms/iter");
    } catch (IOException e) {
      LOG.error("ENCODING failed with exception: ", e);
    }
  }
  
  public void benchmarkDecoding() {
    try {
      long totalTime = 0;
      long succeeded = 0;
      for (int iter = 0; iter < MAX_ITERATION; iter++) {
        init();
        if (verify) {
          // sourceVecTmp will be erased after encodeBulk
          rsCode.encodeBulk(sourceVecTmp, parityVec, useNative);
        }
        Collections.shuffle(locations, rand);
        for (int i = 0;  i < rsCode.paritySize(); i++) {
          erasedLocations[i] = locations.get(i);
          if (i == rsCode.paritySize() - 1) {
            decodeLocation = erasedLocations[rsCode.paritySize() - 1];
          }
          if (verify) {
            if (i == rsCode.paritySize() - 1) {
              expectVec = Arrays.copyOf(totalVec[decodeLocation], dataLen); 
            }
            Arrays.fill(totalVec[erasedLocations[i]], 0, dataLen, (byte)0);
          }
        }
      
        long startTime = System.currentTimeMillis();
        rsCode.decodeOneBlock(totalVec, decodeVec, dataLen, erasedLocations,
          decodeLocation, decodePos, decodeLen, useNative);
        totalTime += System.currentTimeMillis() - startTime;
        if (verify) {
          boolean diff = false;
          for (int i = decodePos; i < decodePos + decodeLen; i++) {
            if (expectVec[i] != decodeVec[i]) {
              diff = true;
              break;
            }
          }
          if (!diff) {
            succeeded++;
          }
        }
      }
      if (verify) {
        String passStr = " passed " + succeeded + "/" + MAX_ITERATION + " tests";
        if (succeeded == MAX_ITERATION) {
          LOG.info("Decode succeeded:" + passStr);
        } else {
          LOG.info("Decode failed:" + passStr);
        }
      }
      System.out.println("DECODING\t" + (totalTime / MAX_ITERATION) + "ms/iter");
    } catch (IOException e) {
      LOG.error("DECODING failed with exception: ", e);
    }
  }
  
  public void run() {
    benchmarkEncoding();
    benchmarkDecoding();
  }
}

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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReedSolomonCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(ReedSolomonCode.class);

  private int stripeSize;
  private int paritySize;
  private int[] generatingPolynomial;
  private int PRIMITIVE_ROOT = 2;
  private int[] primitivePower;
  private GaloisField GF = GaloisField.getInstance();
  private int[] errSignature;
  private int[] paritySymbolLocations;
  private int[] dataBuff;

  @Deprecated
  public ReedSolomonCode(int stripeSize, int paritySize) {
    init(stripeSize, paritySize);
  }

  public ReedSolomonCode() {
  }

  @Override
  public void init(int stripeSize, int paritySize) {
    LOG.info("Initialized " + ReedSolomonCode.class +
        " stripeLength:" + stripeSize +
        " parityLength:" + paritySize);
    assert(stripeSize + paritySize < GF.getFieldSize());
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.errSignature = new int[paritySize];
    this.paritySymbolLocations = new int[paritySize];
    this.dataBuff = new int[paritySize + stripeSize];
    for (int i = 0; i < paritySize; i++) {
      paritySymbolLocations[i] = i;
    }

    this.primitivePower = new int[stripeSize + paritySize];
    // compute powers of the primitive root
    for (int i = 0; i < stripeSize + paritySize; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySize; i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert(message.length == stripeSize && parity.length == paritySize);
    for (int i = 0; i < paritySize; i++) {
      dataBuff[i] = 0;
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySize] = message[i];
    }
    GF.remainder(dataBuff, generatingPolynomial);
    for (int i = 0; i < paritySize; i++) {
      parity[i] = dataBuff[i];
    }
  }
  
  public void printArray(byte[][] outputs) {
    for (int i = 0; i < outputs.length; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append("Row " + i + ":");
      for (int j = 0; j < outputs[i].length; j++) {
        sb.append(outputs[i][j] + " ");
      }
      LOG.info(sb.toString());
    }
  }
  
  /**
   * This function (actually, the GF.remainder() function) will modify
   * the "inputs" parameter.
   */
  @Override
  public void encodeBulk(byte[][] inputs, byte[][] outputs) throws IOException {
    encodeBulk(inputs, outputs, true);
  }
  
  @Override
  public void encodeBulk(byte[][] inputs, byte[][] outputs, boolean useNative)
  throws IOException {
    final int stripeSize = stripeSize();
    final int paritySize = paritySize();
    assert (stripeSize == inputs.length);
    assert (paritySize == outputs.length);
    
    if (useNative) {
      LOG.info("ReedSolomon is doing an Non-Native Java Implementation");
    }

    for (int i = 0; i < outputs.length; i++) {
      Arrays.fill(outputs[i], (byte)0);
    }
      
    byte[][] data = new byte[stripeSize + paritySize][];
  
    for (int i = 0; i < paritySize; i++) {
      data[i] = outputs[i];
    }
    for (int i = 0; i < stripeSize; i++) {
      data[i + paritySize] = inputs[i];
    }
    // Compute the remainder
    GF.remainder(data, generatingPolynomial);
  }

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    if (erasedLocation.length == 0) {
      return;
    }
    assert(erasedLocation.length == erasedValue.length);
    for (int i = 0; i < erasedLocation.length; i++) {
      data[erasedLocation[i]] = 0;
    }
    for (int i = 0; i < erasedLocation.length; i++) {
      errSignature[i] = primitivePower[erasedLocation[i]];
      erasedValue[i] = GF.substitute(data, primitivePower[i]);
    }
    GF.solveVandermondeSystem(errSignature, erasedValue, erasedLocation.length);
  }
  
  @Override
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
                               int[] erasedLocation) throws IOException {
    decodeBulk(readBufs, writeBufs, erasedLocation, 0, readBufs[0].length);
  }
  
  @Override
  public void decodeOneBlock(byte[][] readBufs, byte[] decodeVec, int dataLen, 
      int[] erasedLocation, int decodeLocation, int decodePos, int decodeLen,
      boolean useNative) throws IOException {
    int numErasedLocation = erasedLocation.length; 
    if (numErasedLocation == 0) {
      return;
    }
    int pos = -1;
    for (int i = 0; i < numErasedLocation; i++) {
      if (erasedLocation[i] == decodeLocation) {
        pos = i;  
        break;
      }
    }
    if (pos == -1) {
      LOG.error("Location " + decodeLocation + " is not in the erasedLocation");
      return; 
    }
    byte[][] tmpBufs = new byte[numErasedLocation][];
    for (int i = 0; i < numErasedLocation; i++) {
      tmpBufs[i] = new byte[dataLen];
    }
    decodeBulk(readBufs, tmpBufs, erasedLocation, decodePos, decodeLen, useNative);
    System.arraycopy(tmpBufs[pos], decodePos, decodeVec, decodePos, decodeLen);
  }
  
  @Override
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
      int[] erasedLocation, int dataStart, int dataLen) throws IOException {
    decodeBulk(readBufs, writeBufs, erasedLocation, dataStart, dataLen, true); 
  }
  
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
      int[] erasedLocation, int dataStart, int dataLen, boolean useNative)
  throws IOException {
    if (erasedLocation.length == 0) {
      return;
    }
    
    if (useNative) {
      LOG.info("ReedSolomon is doing an Non-Native Java Implementation");
    }
    // cleanup the write buffer
    for (int i = 0; i < writeBufs.length; i++) {
      Arrays.fill(writeBufs[i], dataStart, dataStart + dataLen, (byte)0);
    }
    for (int i = 0; i < erasedLocation.length; i++) {
      errSignature[i] = primitivePower[erasedLocation[i]];
      GF.substitute(readBufs, writeBufs[i], primitivePower[i], dataStart, dataLen);
    }
    GF.solveVandermondeSystem(
        errSignature, writeBufs, erasedLocation.length, dataStart, dataLen);
  }

  @Override
  public int stripeSize() {
    return this.stripeSize;
  }

  @Override
  public int paritySize() {
    return this.paritySize;
  }

  @Override
  public int symbolSize() {
    return (int) Math.round(Math.log(GF.getFieldSize()) / Math.log(2));
  }

  /**
   * Given parity symbols followed by message symbols, return the locations of
   * symbols that are corrupted. Can resolve up to (parity length / 2) error
   * locations.
   * @param data The message and parity. The parity should be placed in the
   *             first part of the array. In each integer, the relevant portion
   *             is present in the least significant bits of each int.
   *             The number of elements in data is stripeSize() + paritySize().
   *             <b>Note that data may be changed after calling this method.</b>
   * @param errorLocations The set to put the error location results
   * @return true If the locations can be resolved, return true.
   */
  public boolean computeErrorLocations(int[] data,
      Set<Integer> errorLocations) {
    assert(data.length == paritySize + stripeSize && errorLocations != null);
    errorLocations.clear();
    int maxError = paritySize / 2;
    int[][] syndromeMatrix = new int[maxError][];
    for (int i = 0; i < syndromeMatrix.length; ++i) {
      syndromeMatrix[i] = new int[maxError + 1];
    }
    int[] syndrome = new int[paritySize];

    if (computeSyndrome(data, syndrome)) {
      // Parity check OK. No error location added.
      return true;
    }
    for (int i = 0; i < maxError; ++i) {
      for (int j = 0; j < maxError + 1; ++j) {
        syndromeMatrix[i][j] = syndrome[i + j];
      }
    }
    GF.gaussianElimination(syndromeMatrix);
    int[] polynomial = new int[maxError + 1];
    polynomial[0] = 1;
    for (int i = 0; i < maxError; ++i) {
      polynomial[i + 1] = syndromeMatrix[maxError - 1 - i][maxError];
    }
    for (int i = 0; i < paritySize + stripeSize; ++i) {
      int possibleRoot = GF.divide(1, primitivePower[i]);
      if (GF.substitute(polynomial, possibleRoot) == 0) {
        errorLocations.add(i);
      }
    }
    // Now recover with error locations and check the syndrome again
    int[] locations = new int[errorLocations.size()];
    int k = 0;
    for (int loc : errorLocations) {
      locations[k++] = loc;
    }
    int [] erasedValue = new int[locations.length];
    decode(data, locations, erasedValue);
    for (int i = 0; i < locations.length; ++i) {
      data[locations[i]] = erasedValue[i];
    }
    return computeSyndrome(data, syndrome);
  }

  /**
   * Compute the syndrome of the input [parity, message]
   * @param data [parity, message]
   * @param syndrome The syndromes (checksums) of the data
   * @return true If syndromes are all zeros
   */
  private boolean computeSyndrome(int[] data, int [] syndrome) {
    boolean corruptionFound = false;
    for (int i = 0; i < paritySize; i++) {
      syndrome[i] = GF.substitute(data, primitivePower[i]);
      if (syndrome[i] != 0) {
        corruptionFound = true;
      }
    }
    return !corruptionFound;
  }
}

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
import org.apache.hadoop.util.NativeJerasure;

public class JerasureCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(JerasureCode.class);

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
  public JerasureCode(int stripeSize, int paritySize) {
    init(stripeSize, paritySize);
  }

  public JerasureCode() {
  }

  @Override
  public void init(int stripeSize, int paritySize) {
    LOG.info("Initialized " + JerasureCode.class +
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

    if (NativeJerasure.isAvailable()) {
      NativeJerasure.nativeInit(stripeSize, paritySize);
    }
  }

  public void printArray(byte[][] outputs) {
    for (int i = 0; i< outputs.length; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append("Row " + i + ":");
      for (int j = 0; j < outputs[i].length; j++) {
        sb.append(outputs[i][j] + " ");
      }
      LOG.info(sb.toString());
    }
  }
  
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
    
    if (NativeJerasure.isAvailable() && useNative) {
      NativeJerasure.encodeBulk(inputs, outputs, stripeSize, paritySize);
    } else {
      throw new IOException("Jerasure Encoding is using Native C Implementation");
    }
  }

  @Override
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
                               int[] erasedLocation)  throws IOException {
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
      int[] erasedLocation, int dataStart, int dataLen) throws IOException {
    decodeBulk(readBufs, writeBufs, erasedLocation, dataStart, dataLen, true); 
  }
  
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs, int[] erasedLocation,
      int dataStart, int dataLen, boolean useNative) throws IOException{
    if (erasedLocation.length == 0) {
      return;
    }
    
    if (NativeJerasure.isAvailable() && useNative) {
      NativeJerasure.decodeBulk(readBufs, writeBufs, erasedLocation,
          dataStart, dataLen, stripeSize, paritySize);
    } else {
      throw new IOException("Jerasure Encoding is using Native C Implementation");
    }
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

}

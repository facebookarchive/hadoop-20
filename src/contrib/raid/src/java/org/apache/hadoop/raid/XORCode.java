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

import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class XORCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(XORCode.class);

  private int stripeSize;
  private int paritySize;
  private int[] dataBuff;

  @Deprecated
  public XORCode(int stripeSize, int paritySize) {
    init(stripeSize, paritySize);
  }

  public XORCode() {
  }

  @Override
  public void init(Codec codec) {
    init(codec.stripeLength, codec.parityLength);
    LOG.info("Initialized " + XORCode.class +
             " stripeLength:" + codec.stripeLength +
             " parityLength:" + codec.parityLength);
  }

  private void init(int stripeSize, int paritySize) {
    assert(paritySize == 1);
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.dataBuff = new int[paritySize + stripeSize];
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert(message.length == stripeSize && parity.length == 1);
    int val = 0;
    parity[0] = message[0];
    for (int i = 1; i < message.length; i++) {
      parity[0] ^= message[i];
    }
  }

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    if (erasedLocation.length != 1) {
      return;
    }
    assert(erasedLocation.length == erasedValue.length);
    int skipIndex = erasedLocation[0];
    int val = 0;
    for (int i = 0; i < data.length; i++) {
      if (i == skipIndex) {
        continue;
      }
      val ^= data[i];
    }
    erasedValue[0] = val;
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
    return 8;
  }

  @Override
  public void encodeBulk(byte[][] inputs, byte[][] outputs) {
    byte[] output = outputs[0];
    int bufSize = output.length;
    // Get the first buffer's data.
    for (int j = 0; j < bufSize; j++) {
      output[j] = inputs[0][j];
    }
    // XOR with everything else.
    for (int i = 1; i < inputs.length; i++) {
      for (int j = 0; j < bufSize; j++) {
        output[j] ^= inputs[i][j];
      }
    }
  }

  @Override
  public void decodeBulk(
    byte[][] readBufs, byte[][] writeBufs, int[] erasedLocations) {
    assert(erasedLocations.length == writeBufs.length);
    assert(erasedLocations.length <= 1);
    byte[] output = writeBufs[0];
    int erasedIdx = erasedLocations[0];
    // Set the output to zeros.
    for (int j = 0; j < output.length; j++) {
      output[j] = 0;
    }
    // Process the inputs.
    for (int i = 0; i < readBufs.length; i++) {
      // Skip the erased location.
      if (i == erasedIdx) {
        continue;
      }
      byte[] input = readBufs[i];
      for (int j = 0; j < input.length; j++) {
        output[j] ^= input[j];
      }
    }
  }
}

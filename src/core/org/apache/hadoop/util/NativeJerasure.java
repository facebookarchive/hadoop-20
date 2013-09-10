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

import java.nio.ByteBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NativeJerasure {
  public static final Log LOG = LogFactory.getLog(NativeJerasure.class);
  /**
   * Return true if the JNI-based native Jerasure extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded();
  }
  
  public static native void nativeInit(int stripeSize, int paritySize);

  public static void encodeBulk(byte[][] inputs, byte[][] outputs,
      int stripeSize, int paritySize) {
    ByteBuffer[] inputBuffers = new ByteBuffer[inputs.length];
    ByteBuffer[] outputBuffers = new ByteBuffer[outputs.length];
    int bufferLen = inputs[0].length;
    for (int i = 0; i < outputs.length; i++) {
      outputBuffers[i] = ByteBuffer.allocateDirect(bufferLen);
    }
    for (int i = 0; i < inputs.length; i++) {
      inputBuffers[i] = directify(inputs[i], 0, bufferLen);
    }
    nativeEncodeBulk(inputBuffers, outputBuffers, stripeSize, paritySize, bufferLen);
    for (int i = 0; i < outputs.length; i++) {
      outputBuffers[i].get(outputs[i]);
    }
  }
  
  private static native void nativeEncodeBulk(ByteBuffer[] inputBuffers,
      ByteBuffer[] outputBuffers, int stripeSize, int paritySize, int dataLen);
  
  public static void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
      int[] erasedLocation, int dataStart, int dataLen, int stripeSize,
      int paritySize) {
    ByteBuffer[] inputBuffers = new ByteBuffer[readBufs.length];
    ByteBuffer[] outputBuffers = new ByteBuffer[writeBufs.length];
    for (int i = 0; i < writeBufs.length; i++) {
      outputBuffers[i] = ByteBuffer.allocateDirect(dataLen);
    }
    for (int i = 0; i < readBufs.length; i++) {
      inputBuffers[i] = directify(readBufs[i], dataStart, dataLen);
    }
    nativeDecodeBulk(inputBuffers, outputBuffers, erasedLocation,
        dataLen, stripeSize, paritySize, erasedLocation.length);
    for (int i = 0; i < writeBufs.length; i++) {
      outputBuffers[i].get(writeBufs[i], dataStart, dataLen);
    }
  }
  
  private static native void nativeDecodeBulk(ByteBuffer[] inputBuffers,
      ByteBuffer[] outputBuffers, int[] erasedLocation, int dataLen,
      int stripeSize, int paritySize, int erasedLocationCount);
  
  private static ByteBuffer directify(byte[] readBufs, int dataStart, int dataLen) {
    ByteBuffer newBuf = null;
    newBuf = ByteBuffer.allocateDirect(dataLen);
    newBuf.position(0);
    newBuf.mark();
    newBuf.put(readBufs, dataStart, dataLen);
    newBuf.reset();
    newBuf.limit(dataLen);
    return newBuf;
  }
}

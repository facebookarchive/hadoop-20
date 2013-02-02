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

public class BadXORCode extends XORCode {
  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    erasedValue[0] = -1; // Write something bad
  }
  
  @Override
  public void decodeBulk(
    byte[][] readBufs, byte[][] writeBufs, int[] erasedLocations) {
    decodeBulk(readBufs, writeBufs, erasedLocations, 0, readBufs[0].length);
  }

  @Override
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
      int[] erasedLocations, int dataStart, int dataLen) {
    assert(erasedLocations.length == writeBufs.length);
    assert(erasedLocations.length <= 1);
    byte[] output = writeBufs[0];
    // Set the output to zeros.
    for (int j = dataStart; j < dataStart + dataLen; j++) {
      output[j] = -1;
    }
  }
}
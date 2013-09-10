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
import java.util.ArrayList;
import java.util.List;

public abstract class ErasureCode {
  /**
   * Encodes the given message.
   * 
   * @param message
   *          The data of the message. The data is present in the least
   *          significant bits of each int. The number of data bits is
   *          symbolSize(). The number of elements of message is stripeSize().
   * @param parity
   *          (out) The information is present in the least significant bits of
   *          each int. The number of parity bits is symbolSize(). The number of
   *          elements in the code is paritySize().
   */
  public abstract void encode(int[] message, int[] parity);

  /**
   * Generates missing portions of data.
   * 
   * @param data
   *          The message and parity. The parity should be placed in the first
   *          part of the array. In each integer, the relevant portion is
   *          present in the least significant bits of each int. The number of
   *          elements in data is stripeSize() + paritySize().
   * @param erasedLocations
   *          The indexes in data which are not available.
   * @param erasedValues
   *          (out)The decoded values corresponding to erasedLocations.
   */
  public abstract void decode(int[] data, int[] erasedLocations,
      int[] erasedValues);

  /**
   * Figure out which locations need to be read to decode erased locations. The
   * locations are specified as integers in the range [ 0, stripeSize() +
   * paritySize() ). Values in the range [ 0, paritySize() ) represent parity
   * data. Values in the range [ paritySize(), paritySize() + stripeSize() )
   * represent message data.
   * 
   * @param erasedLocations
   *          The erased locations.
   * @return The locations to read.
   */
  public List<Integer> locationsToReadForDecode(List<Integer> erasedLocations)
      throws TooManyErasedLocations {
    List<Integer> locationsToRead = new ArrayList<Integer>(stripeSize());
    int limit = stripeSize() + paritySize();
    // Loop through all possible locations in the stripe.
    for (int loc = limit - 1; loc >= 0; loc--) {
      // Is the location good.
      if (erasedLocations.indexOf(loc) == -1) {
        locationsToRead.add(loc);
        if (stripeSize() == locationsToRead.size()) {
          break;
        }
      }
    }
    // If we are are not able to fill up the locationsToRead list,
    // we did not find enough good locations. Throw TooManyErasedLocations.
    if (locationsToRead.size() != stripeSize()) {
      String locationsStr = "";
      for (Integer erasedLocation : erasedLocations) {
        locationsStr += " " + erasedLocation;
      }
      throw new TooManyErasedLocations("Locations " + locationsStr);
    }
    return locationsToRead;
  }
  
  public abstract void init(int stripeSize, int paritySize);

  /**
   * The number of elements in the message.
   */
  public abstract int stripeSize();

  /**
   * The number of elements in the code.
   */
  public abstract int paritySize();

  public abstract int symbolSize();

  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own encodeBulk behavior. 
   */
  public abstract void encodeBulk(byte[][] inputs, byte[][] outputs,
      boolean useNative) throws IOException;
  
  public void encodeBulk(byte[][] inputs, byte[][] outputs) throws IOException {
    final int stripeSize = stripeSize();
    final int paritySize = paritySize();
    assert (stripeSize == inputs.length);
    assert (paritySize == outputs.length);
    int[] data = new int[stripeSize];
    int[] code = new int[paritySize];

    for (int j = 0; j < outputs[0].length; j++) {
      for (int i = 0; i < paritySize; i++) {
        code[i] = 0;
      }
      for (int i = 0; i < stripeSize; i++) {
        data[i] = inputs[i][j] & 0x000000FF;
      }
      encode(data, code);
      for (int i = 0; i < paritySize; i++) {
        outputs[i][j] = (byte) code[i];
      }
    }
  }
  
  /**
   * position decode. 
   */
  public abstract void decodeBulk(byte[][] readBufs, byte[][] writeBufs, 
      int[] erasedLocation, int dataStart, int dataLen) throws IOException;
  
  public abstract void decodeOneBlock(byte[][] readBufs, byte[] decodeVec, int dataLen, 
      int[] erasedLocation, int decodeLocation, int decodePos, int decodeLen,
      boolean useNative) throws IOException;

  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own decodeBulk behavior. 
   */
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
      int[] erasedLocations) throws IOException {
    int[] tmpInput = new int[readBufs.length];
    int[] tmpOutput = new int[erasedLocations.length];

    int numBytes = readBufs[0].length;
    for (int idx = 0; idx < numBytes; idx++) {
      for (int i = 0; i < tmpOutput.length; i++) {
        tmpOutput[i] = 0;
      }
      for (int i = 0; i < tmpInput.length; i++) {
        tmpInput[i] = readBufs[i][idx] & 0x000000FF;
      }
      decode(tmpInput, erasedLocations, tmpOutput);
      for (int i = 0; i < tmpOutput.length; i++) {
        writeBufs[i][idx] = (byte) tmpOutput[i];
      }
    }
  }
}

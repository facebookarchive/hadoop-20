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
package org.apache.hadoop.hdfs.util;

/**
 * LightWeightBitSet is a class providing static methods for creating a bit set
 * representing membership of objects in a set.
 */

public class LightWeightBitSet {

  public static long[] emptySet = new long[0];

  private static final int LONG_MASK = 0x3f;
  private static final int LONG_SHIFT = 6;

  /**
   * Get an array of longs that will fit membership information about the given
   * number of elements
   * 
   * @param size - the number of elements to fit into the set
   * @return an array with sufficient number of longs
   */
  public static long[] getBitSet(int size) {
    if (size <= 0) {
      return emptySet;
    }
    return new long[getBitArraySize(size)];
  }

  /**
   * Get maximum number of elements that can be represented by the given array
   * of longs.
   */
  public static int getMaxCapacity(long[] bits) {
    if (bits == null)
      return 0;
    return bits.length << LONG_SHIFT;
  }

  /**
   * Set the bit for the given position to true.
   * 
   * @param bits - bitset
   * @param pos - position to be set to true
   */
  public static void set(long[] bits, int pos) {
    int offset = pos >> LONG_SHIFT;
    if (offset >= bits.length)
      throw new IndexOutOfBoundsException();
    bits[offset] |= 1L << pos;
  }

  /**
   * Set the bit for the given position to false.
   * 
   * @param bits - bitset
   * @param pos - position to be set to false
   */
  public static void clear(long[] bits, int pos) {
    int offset = pos >> LONG_SHIFT;
    if (offset >= bits.length)
      throw new IndexOutOfBoundsException();
    bits[offset] &= ~(1L << pos);
  }

  /**
   * Gets the bit for the given position.
   * 
   * @param bits
   *          - bitset
   * @param pos
   *          - position to retrieve.
   */
  public static boolean get(long[] bits, int pos) {
    int offset = pos >> LONG_SHIFT;
    if (offset >= bits.length)
      return false;
    return (bits[offset] & (1L << pos)) != 0;
  }

  /**
   * Checks the number of bits set to 1.
   * 
   * @param bits - bitset
   * @return the number of bits set to 1
   */
  public static int cardinality(long[] bits) {
    int card = 0;
    for (int i = bits.length - 1; i >= 0; i--) {
      long a = bits[i];
      if (a == 0)
        continue;
      if (a == -1) {
        card += 64;
        continue;
      }
      // Successively collapse alternating bit groups into a sum.
      a = ((a >> 1) & 0x5555555555555555L) + (a & 0x5555555555555555L);
      a = ((a >> 2) & 0x3333333333333333L) + (a & 0x3333333333333333L);
      int b = (int) ((a >>> 32) + a);
      b = ((b >> 4) & 0x0f0f0f0f) + (b & 0x0f0f0f0f);
      b = ((b >> 8) & 0x00ff00ff) + (b & 0x00ff00ff);
      card += ((b >> 16) & 0x0000ffff) + (b & 0x0000ffff);
    }
    return card;
  }

  /**
   * Get the number of longs to store the membership information about spaceSize
   * elements.
   * 
   * @param size - number of elements to be represented
   * @return the needed number of longs
   */
  public static int getBitArraySize(int size) {
    int bitMapSize = size >>> LONG_SHIFT;
    if ((size & LONG_MASK) != 0)
      bitMapSize++;
    return bitMapSize;
  }
}

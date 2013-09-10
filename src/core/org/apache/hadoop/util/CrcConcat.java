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

/**
 * Utility class to Concatenate two CRCs
 */
public class CrcConcat {

  /**
   * Lookup tables for calculating the CRC after adding zeros to a byte array from the original CRC.
   */
  static final LookupTable[] lookupTables = {
    new LookupTable(256 * 1024 * 1024, CrcConcatLookupTables.T256mb),
    new LookupTable(16 * 1024 * 1024, CrcConcatLookupTables.T16mb),    
    new LookupTable(1024 * 1024, CrcConcatLookupTables.T1mb),    
    new LookupTable(16 * 1024, CrcConcatLookupTables.T16kb),
    new LookupTable(512, CrcConcatLookupTables.T512bytes),
    new LookupTable(64, CrcConcatLookupTables.T64bytes)
  };

  /**
   * Lookup table for calculating the CRC after adding zeros to a byte array from the original CRC.
   * order is how many zeros are added. lookupTable is the lookup table for it. 
   */
  static class LookupTable {
    private LookupTable(int order, int[][] lookupTable) {
      this.order = order;
      this.lookupTable = lookupTable;
    }
    public int getOrder() {
      return order;
    }
    public int[][] getLookupTable() {
      return lookupTable;
    }

    int order;
    int[][] lookupTable;
  }
  
  /**
   * Helper function to transform a CRC using lookup table. Currently it is used
   * for calculating CRC after adding bytes zeros to source byte array. The special
   * lookup table needs to be passed in for this specific transformation
   * 
   * @param crc
   * @param lookupTable
   *          the first dimension is for which byte it applies to, the second
   *          dimension is to lookup the byte and generate the outputs.
   * @return the result CRC
   */
  static int transform(int crc, int[][] lookupTable) {
    int cb1 = lookupTable[0][crc & 0xff];
    int cb2 = lookupTable[1][(crc >>>= 8) & 0xff];
    int cb3 = lookupTable[2][(crc >>>= 8) & 0xff];
    int cb4 = lookupTable[3][(crc >>>= 8) & 0xff];
    return cb1 ^ cb2 ^ cb3 ^ cb4;
  }

  /**
   * Concatenate two CRCs
   * 
   * @param crc1
   *          the CRC of the first byte array
   * @param crc2
   *          the CRC of the second byte array
   * @param order
   *          the length of the second byte array
   * @return CRC of of the byte array, which is the concatenation of the two
   *         byte arrays with given crc's
   */
  static public int concatCrc(int crc1, int crc2, int order) {
    // Calculate CRC of crc1 + order's 0
    int crcForCrc1 = crc1;
    int orderRemained = order;

    // Fast transforming CRCs for adding 0 to the end of the byte array by table
    // look-up
    for (LookupTable lookupTable : lookupTables) {
      while (orderRemained >= lookupTable.getOrder()) {
        crcForCrc1 = transform(crcForCrc1, lookupTable.getLookupTable());
        orderRemained -= lookupTable.getOrder();
      }      
    }

    if (orderRemained > 0) {
      // We continue the first byte array's CRC calculating
      // and adding 0s to it. And then we plus it with CRC2
      //
      // Doing that, we need to offset the CRC initial value of CRC2 by
      // subtracting a CRC value of empty string.
      //
      // For example, A1A2A3's CRC is C1C2C3C4,
      // while B1 B2 B3's CRc is C5C6C7C8 and we wnat to concatenate them,
      // it means (our initial value is FF FF FF FF):
      //    FF FF FF FF A1 A2 A3 C1 C2 C3 C4
      //             FF FF FF FF B1 B2 B3 C5 C6 C7 C8
      // both are multiple of generation polynomial.
      // By continue CRC by adding zeros, actually, we calculated
      // the CRC C1'C2'C3'C4, so that
      //    FF FF FF FF A1 A2 A3 00 00 00 C1'C2'C3'C4'
      // is the multiple of generation polynomial.
      // By adding C5C6C7C8 and C1'C2'C3'C4', what we got is not
      // the CRC for
      //    FF FF FF FF A1 A2 A3 B1 B2 B3
      // which we expect, but this string plus:
      //             FF FF FF FF 00 00 00
      // To offset the impact, the only thing we need to do, is
      // to subtract the result by the CRC value for 00 00 00.
      //
      int initial = CrcConcatLookupTables.initCrcMap[orderRemained];

      NativeCrc32 pjc = new NativeCrc32();
      pjc.setValue(crcForCrc1);
      byte[] zeros = new byte[orderRemained];
      pjc.update(zeros, 0, zeros.length);
      crcForCrc1 = (int) pjc.getValue() ^ initial;      
    }
    return crcForCrc1 ^ crc2;
  }
}

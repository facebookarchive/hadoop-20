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
package org.apache.hadoop.hdfs;

import java.util.HashMap;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.util.CrcConcat;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Before;
import org.junit.Test;


public class TestCrcConcat {
  byte[] data = new byte[2 * 1024 * 1024 + 888];
  DataChecksum checksum;
  HashMap<String, Integer> checksumMap;

  
  @Before
  public void setUpClass() throws Exception {

    checksum = DataChecksum.newDataChecksum(
        DataChecksum.CHECKSUM_CRC32, 4);
    checksumMap = new HashMap<String, Integer>();
  }
  
  /**
   * Get checksum of data array, from offset with length.
   * Use a cache to avoid to calculated repeatly.
   * @param off
   * @param length
   * @return
   */
  private int getChecksum(int off, int length) {
    Integer co = checksumMap.get("" + off + ":" + length);
    if (co == null) {
      checksum.reset();
      checksum.update(data, off, length);
      int newC = (int) checksum.getValue();
      checksumMap.put("" + off + ":" + length, newC);
      return newC;
    } else {
      return co.intValue();
    }
  }
  
  private void verifyCat(int off, int len1, int len2) {
    int crc1 = getChecksum(off, len1);
    int crc2 = getChecksum(off + len1, len2);
    int combineCrc = getChecksum(off, len1 + len2);
    int combineByCrcConcat = CrcConcat.concatCrc(crc1, crc2, len2);
    TestCase.assertEquals(combineCrc, combineByCrcConcat);
  }

  @Test
  public void testConcatCrc() throws Exception {
    Random random = new Random(1);
    random.nextBytes(data);

    // small remain
    verifyCat(0, 4, 1);
    verifyCat(0, 4, 2);
    verifyCat(0, 4, 3);
    verifyCat(0, 4, 4);
    verifyCat(0, 6, 1);
    verifyCat(0, 6, 3);
    verifyCat(0, 1, 4);
    verifyCat(0, 3, 6);
    verifyCat(0, 512, 130);
    verifyCat(0, 513, 132);
    verifyCat(0, 513, 131);
    verifyCat(0, 513, 130);
    verifyCat(0, 513, 129);
    verifyCat(0, 513, 128);
    verifyCat(0, 131, 194);
    verifyCat(0, 515, 515);
    
    // byte level
    verifyCat(0, 512, 512);
    verifyCat(0, 1024, 512);
    verifyCat(0, 1536, 512);
    verifyCat(0, 1024, 1024);
    verifyCat(0, 1024, 1277);
    verifyCat(0, 2048, 253);

    // MB byte level
    verifyCat(0, 1024 * 1024, 1024 * 1024);
    verifyCat(0, 2 * 1024 * 1024, 888);
    verifyCat(0, 1536, 512);
    verifyCat(0, 1024, 1024);
    verifyCat(0, 1024, 1277);
    verifyCat(0, 2048, 253);
  }

  @Test
  public void testConcatCrcRandom() throws Exception {
    checksumMap.clear();

    Random random = new Random(System.currentTimeMillis());
    random.nextBytes(data);

    // small remain
    verifyCat(0, 4, 1);
    verifyCat(0, 4, 2);
    verifyCat(0, 4, 3);
    verifyCat(0, 4, 4);
    verifyCat(0, 6, 1);
    verifyCat(0, 6, 3);
    verifyCat(0, 1, 4);
    verifyCat(0, 3, 6);
    verifyCat(0, 512, 130);
    verifyCat(0, 513, 132);
    verifyCat(0, 513, 131);
    verifyCat(0, 513, 130);
    verifyCat(0, 513, 129);
    verifyCat(0, 513, 128);
    verifyCat(0, 131, 194);
    verifyCat(0, 515, 515);

    // byte level
    verifyCat(0, 512, 512);
    verifyCat(0, 1024, 512);
    verifyCat(0, 1536, 512);
    verifyCat(0, 1024, 1024);
    verifyCat(0, 1024, 1277);
    verifyCat(0, 2048, 253);

    // MB byte level
    verifyCat(0, 1024 * 1024, 1024 * 1024);
    verifyCat(0, 2 * 1024 * 1024, 888);
    verifyCat(0, 1536, 512);
    verifyCat(0, 1024, 1024);
    verifyCat(0, 1024, 1277);
    verifyCat(0, 2048, 253);
  }
  
  
  /**
   * Calculate CRC checksum for number bytes from the random generator
   * @param random
   * @param numBytes
   * @return
   */
  private int getChecksum(Random random, int numBytes) {
    byte[] data = new byte[1024];
    checksum.reset();
    for (int i = 0; i < numBytes / 1024; i++) {
      random.nextBytes(data);
      checksum.update(data, 0, 1024);
    }
    if (numBytes % 1024 != 0) {
      byte[] data1 = new byte[numBytes % 1024];
      random.nextBytes(data1);
      checksum.update(data1, 0, numBytes % 1024);
    }
    return (int) checksum.getValue();
  }

  @Test
  public void testConcatCrcBlocks() throws Exception {
    int lastBlockLength = 38888889;
    
    Random random;

    // Verify concatenating two 256MB blocks
    random = new Random(1);
    int checksumb1 = getChecksum(random, 256 * 1024 * 1024);
    int checksumb2 = getChecksum(random, 256 * 1024 * 1024);
    int checksumb3 = getChecksum(random, lastBlockLength);
    
    // Verify concatenating two blocks' CRC
    random = new Random(1);
    int checksum2b = getChecksum(random, 2 * 256 * 1024 * 1024);;
    int concatedChcksum2b = CrcConcat.concatCrc(checksumb1, checksumb2, 256 * 1024 * 1024);
    TestCase.assertEquals(checksum2b, concatedChcksum2b);

    // Verify full CRC.
    random = new Random(1);
    int checksum_all = getChecksum(random, 2 * 256 * 1024 * 1024 + lastBlockLength);
    int concatedChcksum_all = CrcConcat.concatCrc(checksum2b, checksumb3, lastBlockLength);
    TestCase.assertEquals(checksum_all, concatedChcksum_all);
  }
}

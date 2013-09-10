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
package org.apache.hadoop.io;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

public class TestArrayOutputStream {

  @Test
  public void testWriting() throws IOException {
    // compare this implementation
    ArrayOutputStream aos = new ArrayOutputStream(10);
    for (int i = 0; i < 10; i++) {
      // make sure reset clears up the buffer
      aos.reset();
      // compare with standard java.io backed implementation
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      runComparison(aos, dos, bos);
    }
  }

  private void runComparison(ArrayOutputStream aos, DataOutputStream dos,
      ByteArrayOutputStream bos) throws IOException {
    Random r = new Random();
    // byte
    int b = r.nextInt(128);
    aos.write(b);
    dos.write(b);

    // byte[]
    byte[] bytes = new byte[10];
    r.nextBytes(bytes);
    aos.write(bytes, 0, 10);
    dos.write(bytes, 0, 10);

    // Byte
    aos.writeByte(b);
    dos.writeByte(b);

    // boolean
    boolean bool = r.nextBoolean();
    aos.writeBoolean(bool);
    dos.writeBoolean(bool);

    // short
    short s = (short) r.nextInt();
    aos.writeShort(s);
    dos.writeShort(s);

    // char
    int c = r.nextInt();
    aos.writeChar(c);
    dos.writeChar(c);

    // int
    int i = r.nextInt();
    aos.writeInt(i);
    dos.writeInt(i);

    // long
    long l = r.nextLong();
    aos.writeLong(l);
    dos.writeLong(l);

    // float
    float f = r.nextFloat();
    aos.writeFloat(f);
    dos.writeFloat(f);

    // double
    double d = r.nextDouble();
    aos.writeDouble(d);
    dos.writeDouble(d);

    // strings
    String str = RandomStringUtils.random(20);
    aos.writeBytes(str);
    aos.writeChars(str);
    aos.writeUTF(str);
    dos.writeBytes(str);
    dos.writeChars(str);
    dos.writeUTF(str);

    byte[] expected = bos.toByteArray();
    assertEquals(expected.length, aos.size());
    
    byte[] actual = new byte[aos.size()];
    System.arraycopy(aos.getBytes(), 0, actual, 0, actual.length);
    // serialized bytes should be the same
    assertTrue(Arrays.equals(expected, actual));
  }
}

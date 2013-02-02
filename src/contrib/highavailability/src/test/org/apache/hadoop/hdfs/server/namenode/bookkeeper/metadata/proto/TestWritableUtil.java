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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class TestWritableUtil {

  // A sample writable class for this unit test
  private static class WritableForTest implements Writable {

    boolean aBoolean;
    byte aByte;
    char aChar;
    int anInt;
    long aLong;
    String aString;

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(aBoolean);
      out.writeByte(aByte);
      out.writeChar(aChar);
      out.writeInt(anInt);
      out.writeLong(aLong);
      out.writeUTF(aString);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      aBoolean = in.readBoolean();
      aByte = in.readByte();
      aChar = in.readChar();
      anInt = in.readInt();
      aLong = in.readLong();
      aString = in.readUTF();
    }

    public void testEquality(WritableForTest other) {
      assertNotNull(other);
      assertEquals(aBoolean, other.aBoolean);
      assertEquals(aByte, other.aByte);
      assertEquals(aChar, other.aChar);
      assertEquals(anInt, other.anInt);
      assertEquals(aLong, other.aLong);
      assertEquals(aString, other.aString);
    }

  }

  /**
   * Tests {@link WritableUtil#readWritableFromByteArray(byte[], Writable)}
   * and {@link WritableUtil#writableToByteArray(Writable)}
   */
  @Test
  public void testWritableToByteArrayAndReadWritableFromByteArray()
      throws Exception {
    WritableForTest aWritableIn = new WritableForTest();
    aWritableIn.aBoolean = false;
    aWritableIn.aByte = 'b';
    aWritableIn.aByte = 'c';
    aWritableIn.anInt = 0xfaceb00c;
    aWritableIn.aLong = 0xfaceb0000000000cl;
    aWritableIn.aString = "hadoop";
    byte[] aWritableBytes = WritableUtil.writableToByteArray(aWritableIn);
    WritableForTest aWritableOut = WritableUtil.readWritableFromByteArray(
        aWritableBytes, new WritableForTest());

    // Test that we read what we wrote
    aWritableIn.testEquality(aWritableOut);

    // readWritableFromByteArray() and writableToByteArray() must be inverse
    // of each other
    byte[] aWritableBytesOther = WritableUtil.writableToByteArray(aWritableOut);
    assertEquals("resulting byte arrays must be equal length!",
        aWritableBytes.length, aWritableBytesOther.length);
    assertEquals("resulting byte arrays must have same content!",
        WritableComparator.compareBytes(
            aWritableBytes, 0, aWritableBytes.length,
            aWritableBytesOther, 0, aWritableBytesOther.length),
        0);
  }

}

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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Simple implementation of OutputStream, backed by an array of bytes. The
 * capacity of the array expands as needed. Similar to
 * java.io.ByteArrayOutputStream. However, this implementation is not
 * thread-safe, and exposes the underlying byte array directly. Alos implements
 * DataOutput, to be able to write other primitives directly.
 */
public class ArrayOutputStream extends OutputStream implements DataOutput {

  // underlying byte array
  private byte bytes[];
  private int count;
  private DataOutputStream dos;

  /**
   * Creates a new byte array output stream.
   */
  public ArrayOutputStream(int size) {
    bytes = new byte[size];
    dos = new DataOutputStream(this);
  }

  /**
   * Expand the underlying byte array to fit size bytes.
   */
  private void expandIfNecessary(int size) {
    if (bytes.length >= size) {
      // no need to expand
      return;
    }
    // either double, or expand to fit size
    int newlength = Math.max(2 * bytes.length, size);
    bytes = Arrays.copyOf(bytes, newlength);
  }

  /**
   * Writes the given byte into the output stream.
   */
  public void write(int b) {
    expandIfNecessary(count + 1);
    bytes[count] = (byte) b;
    count += 1;
  }

  /**
   * Writes array of bytes into the output stream.
   */
  public void write(byte b[], int off, int len) {
    expandIfNecessary(count + len);
    System.arraycopy(b, off, bytes, count, len);
    count += len;
  }

  /**
   * Clears the buffer.
   */
  public void reset() {
    count = 0;
  }

  /**
   * Return underlying byte array.
   */
  public byte getBytes()[] {
    return bytes;
  }

  /**
   * Returns the current size of the buffer.
   */
  public int size() {
    return count;
  }

  // implement DataOutput methods

  @Override
  public void writeBoolean(boolean v) throws IOException {
    dos.writeBoolean(v);
  }

  @Override
  public void writeByte(int v) throws IOException {
    dos.writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    dos.writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    dos.writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    dos.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    dos.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    dos.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    dos.writeDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    dos.writeBytes(s);
  }

  @Override
  public void writeChars(String s) throws IOException {
    dos.writeChars(s);
  }

  @Override
  public void writeUTF(String s) throws IOException {
    dos.writeUTF(s);
  }
}

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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


import org.apache.commons.logging.*;

/** A WritableComparable for strings that uses the UTF8 encoding.
 * 
 * <p>Also includes utilities for efficiently reading and writing UTF-8.
 *
 * @deprecated replaced by Text
 */
public class UTF8 implements WritableComparable {
  private static final Log LOG= LogFactory.getLog(UTF8.class);
  
  /* Used for string conversions*/
  private static final int MAX_STRING_LENGTH = 8000;
  private static final int MAX_BYTE_LENGTH = 1000;
  
  private static final ThreadLocal<TempArrays> arrays = new ThreadLocal<TempArrays>() {
    protected TempArrays initialValue() {
      TempArrays ta = new TempArrays();
      ta.byteArray = new byte[MAX_BYTE_LENGTH];
      ta.charArray = new char[MAX_STRING_LENGTH];
      return ta;
    }
  };
  
  public static final char[] getCharArray(int len) {
    if(len <= MAX_STRING_LENGTH) {
      // return cached array
      return arrays.get().charArray; 
    }
    // otherwise allocate temporary char[]
    return new char[len];
  }
  
  public static final byte[] getByteArray(int len) {
    if(len <= MAX_BYTE_LENGTH) {
      // return cached array
      return arrays.get().byteArray; 
    }
    // otherwise allocate temporary byte[]
    return new byte[len];
  }
  
  public static final TempArrays getArrays(int len) {
    // both stored arrays are sufficient
    if (len <= MAX_BYTE_LENGTH && len <= MAX_STRING_LENGTH) {
      return arrays.get();
    }

    // otherwise allocate temporary object
    TempArrays temp = new TempArrays();
    temp.byteArray = new byte[len];
    temp.charArray = new char[len];
    return temp;
  }
  
  public static class TempArrays {
    byte[] byteArray;
    char[] charArray;
  }
  
  public static final byte MIN_ASCII_CODE = 0;
  public static final byte MAX_ASCII_CODE = 0x7f;
 
  private static final ThreadLocal<DataInputBuffer> IBUF_FACTORY = 
    new ThreadLocal<DataInputBuffer>() {
    @Override
    protected DataInputBuffer initialValue() {
      return new DataInputBuffer();
    }
  };

  private static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] bytes = EMPTY_BYTES;
  private int length;

  public UTF8() {
    //set("");
  }

  /** Construct from a given string. */
  public UTF8(String string) {
    set(string);
  }
  
  public UTF8(String string, boolean optimized) {
    set(string, optimized);
  }

  /** Construct from a given string. */
  public UTF8(UTF8 utf8) {
    set(utf8);
  }

  /** The raw bytes. */
  public byte[] getBytes() {
    return bytes;
  }

  /** The number of bytes in the encoded string. */
  public int getLength() {
    return length;
  }

  /** Set to contain the contents of a string. 
   * @param optimize - optimize for setting ASCII strings. */
  public void set(String string, boolean optimized) {
    int len = string.length();
    if (len > 0xffff/3) {             // maybe too long
      LOG.warn("truncating long string: " + string.length()
               + " chars, starting with " + string.substring(0, 20));
      string = string.substring(0, 0xffff/3);
      len = string.length();
    }
    
    if (optimized) {
      // speculate that we can do fast conversion
      char[] charArray = getCharArray(len);
      length = len;
      if (bytes == null || length > bytes.length) // grow buffer
        bytes = new byte[length];
      
      if (copyStringToBytes(string, charArray, bytes, len))
        // done, length is set
        // bytes are already copied
        return;
    }
    
    // we need to do full conversion
    length = utf8Length(string);                  // compute length
    if (length > 0xffff)                          // double-check length
      throw new RuntimeException("string too long!");

    if (bytes == null || length > bytes.length)   // grow buffer
      bytes = new byte[length];

    try {                                         // avoid sync'd allocations
      writeChars(bytes, string, 0, string.length());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /** Set to contain the contents of a string. */
  public void set(String string) {
    set(string, false);
  }

  /** Set to contain the contents of a string. */
  public void set(UTF8 other) {
    length = other.length;
    if (bytes == null || length > bytes.length)   // grow buffer
      bytes = new byte[length];
    System.arraycopy(other.bytes, 0, bytes, 0, length);
  }

  public void readFields(DataInput in) throws IOException {
    length = in.readUnsignedShort();
    if (bytes == null || bytes.length < length)
      bytes = new byte[length];
    in.readFully(bytes, 0, length);
  }

  /** Skips over one UTF8 in the input. */
  public static void skip(DataInput in) throws IOException {
    int length = in.readUnsignedShort();
    WritableUtils.skipFully(in, length);
  }

  public void write(DataOutput out) throws IOException {
    out.writeShort(length);
    out.write(bytes, 0, length);
  }

  /** Compare two UTF8s. */
  public int compareTo(Object o) {
    UTF8 that = (UTF8)o;
    return WritableComparator.compareBytes(bytes, 0, length,
                                           that.bytes, 0, that.length);
  }

  /** Convert to a String. */
  public String toString() {
    try {
      DataInputBuffer ibuf = IBUF_FACTORY.get();
      ibuf.reset(bytes, length);
      return readChars(ibuf, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns true iff <code>o</code> is a UTF8 with the same contents.  */
  public boolean equals(Object o) {
    if (!(o instanceof UTF8))
      return false;
    UTF8 that = (UTF8)o;
    if (this.length != that.length)
      return false;
    else
      return WritableComparator.compareBytes(bytes, 0, length,
                                             that.bytes, 0, that.length) == 0;
  }

  public int hashCode() {
    return WritableComparator.hashBytes(bytes, length);
  }

  /** A WritableComparator optimized for UTF8 keys. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(UTF8.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int n1 = readUnsignedShort(b1, s1);
      int n2 = readUnsignedShort(b2, s2);
      return compareBytes(b1, s1+2, n1, b2, s2+2, n2);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(UTF8.class, new Comparator());
  }

  /// STATIC UTILITIES FROM HERE DOWN

  /// These are probably not used much anymore, and might be removed...

  /** Convert a string to a UTF-8 encoded byte array.
   * @see String#getBytes(String)
   */
  public static byte[] getBytes(String string) {
    byte[] result = new byte[utf8Length(string)];
    try {                                         // avoid sync'd allocations
      writeChars(result, string, 0, string.length());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /** Read a UTF-8 encoded string.
   *
   * @see DataInput#readUTF()
   */
  public static String readString(DataInput in) throws IOException {
    int bytes = in.readUnsignedShort();
    return readChars(in, bytes);
  }

  private static String readChars(DataInput in, int nBytes)
    throws IOException {
    TempArrays ta = getArrays(nBytes);
    byte[] bytes = ta.byteArray;
    char[] charArray = ta.charArray;
    
    in.readFully(bytes, 0, nBytes);
    int i = 0;
    int strLen = 0;
    while (i < nBytes) {
      byte b = bytes[i++];
      if ((b & 0x80) == 0) {
        charArray[strLen++] = (char) b;
      } else if ((b & 0xE0) != 0xE0) {
        charArray[strLen++] = ((char)(((b & 0x1F) << 6)
                             | (bytes[i++] & 0x3F)));
      } else {
        charArray[strLen++] = (char)(((b & 0x0F) << 12)
                             | ((bytes[i++] & 0x3F) << 6)
                             |  (bytes[i++] & 0x3F));
      }
    }
    return new String(charArray, 0, strLen);
  }
  
  public static int writeStringOpt(DataOutput out, String s) throws IOException {
    int len = s.length();
    TempArrays ta = getArrays(len);
    byte[] bytes = ta.byteArray;
    char[] charArray = ta.charArray;

    s.getChars(0, len, charArray, 0);
    if (copyStringToBytes(s, charArray, bytes, len)) {
      out.writeShort(len);
      out.write(bytes, 0, len);
      return len;
    }
    return writeString(out, s);
  }

  /** Write a UTF-8 encoded string.
   *
   * @see DataOutput#writeUTF(String)
   */
  public static int writeString(DataOutput out, String s) throws IOException {
    if (s.length() > 0xffff/3) {         // maybe too long
      LOG.warn("truncating long string: " + s.length()
               + " chars, starting with " + s.substring(0, 20));
      s = s.substring(0, 0xffff/3);
    }
    
    int len = utf8Length(s);
    if (len > 0xffff)                             // double-check length
      throw new IOException("string too long!");
      
    out.writeShort(len);
    writeChars(out, s, 0, s.length());
    return len;
  }
  
  private static boolean copyStringToBytes(String s, char[] charArray,
      byte[] bytes, int len) {
    s.getChars(0, len, charArray, 0);
    for (int i = 0; i < len; i++) {
      if (charArray[i] > MAX_ASCII_CODE) {
        return false;
      }
      bytes[i] = (byte) charArray[i];
    }
    return true;
  }

  /** Returns the number of bytes required to write this. */
  private static int utf8Length(String string) {
    int stringLength = string.length();
    int utf8Length = 0;
    for (int i = 0; i < stringLength; i++) {
      int c = string.charAt(i);
      if (c <= 0x007F) {
        utf8Length++;
      } else if (c > 0x07FF) {
        utf8Length += 3;
      } else {
        utf8Length += 2;
      }
    }
    return utf8Length;
  }

  private static void writeChars(DataOutput out,
                                 String s, int start, int length)
    throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      int code = s.charAt(i);
      if (code <= 0x7F) {
        out.writeByte((byte)code);
      } else if (code <= 0x07FF) {
        out.writeByte((byte)(0xC0 | ((code >> 6) & 0x1F)));
        out.writeByte((byte)(0x80 |   code       & 0x3F));
      } else {
        out.writeByte((byte)(0xE0 | ((code >> 12) & 0X0F)));
        out.writeByte((byte)(0x80 | ((code >>  6) & 0x3F)));
        out.writeByte((byte)(0x80 |  (code        & 0x3F)));
      }
    }
  }
  
  private static void writeChars(byte[] out, String s, int start, int length)
      throws IOException {
    final int end = start + length;
    int count = 0;
    for (int i = start; i < end; i++) {
      int code = s.charAt(i);
      if (code <= 0x7F) {
        out[count++] = (byte) code;
      } else if (code <= 0x07FF) {
        out[count++] = (byte) (0xC0 | ((code >> 6) & 0x1F));
        out[count++] = (byte) (0x80 | code & 0x3F);
      } else {
        out[count++] = (byte) (0xE0 | ((code >> 12) & 0X0F));
        out[count++] = (byte) (0x80 | ((code >> 6) & 0x3F));
        out[count++] = (byte) (0x80 | (code & 0x3F));
      }
    }
  }

}

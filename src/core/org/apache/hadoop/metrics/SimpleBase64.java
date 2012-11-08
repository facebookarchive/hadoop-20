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
package org.apache.hadoop.metrics;


// This code was mostly copied from the Apache Base64 library.  We don't link
// with the original Base64 jar, as that jar will not be in the classpaths of
// the client (e.g., HBase) of HDFS.  As an unintentional side benefit, the
// implementation here makes minor performance improvements over the original
// since we do not need to be general purpose.  In particular, this version
// computes the encoding with single call to a static method.  The original
// keeps global state in a Base64 objects and requires multiple calls to an
// instance method.
class SimpleBase64 {
  /**
   * This array is a lookup table that translates 6-bit positive integer index
   * values into their "Base64 Alphabet" equivalents as specified in Table 1 of
   * RFC 2045.
   * 
   * Thanks to "commons" project in ws.apache.org for this code.
   * http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/
   */
  private static final byte[] STANDARD_ENCODE_TABLE = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
  };

  private static final int BYTES_PER_UNENCODED_BLOCK = 3;
  private static final int BYTES_PER_ENCODED_BLOCK = 4;
  private static final char PAD = '=';

  /**
   * Base64 uses 6-bit fields. 
   */
  /** Mask used to extract 6 bits, used when encoding */
  private static final int MASK_6BITS = 0x3f;

  public static String encode(byte[] in) throws java.io.UnsupportedEncodingException {
    int inLen = in.length;
    int padLen = ((BYTES_PER_UNENCODED_BLOCK -
                   (inLen % BYTES_PER_UNENCODED_BLOCK)) %
                  BYTES_PER_UNENCODED_BLOCK);
    int outLen = (BYTES_PER_ENCODED_BLOCK * (inLen + padLen) /
                  BYTES_PER_UNENCODED_BLOCK);

    byte[] buffer = new byte[outLen];

    for (int i=0; i<buffer.length; i++)
      buffer[i] = '_';

    byte[] encodeTable = STANDARD_ENCODE_TABLE;
    int inPos = 0;
    int pos = 0;
    int bitWorkArea = 0;
    int modulus = 0;

    int lineLength = 0;
    int currentLinePos = 0;

    // iterate over main body of the bytes where we can map 3 input bytes to 4
    // output bytes
    for (int i = 0; i < in.length; i++) {
      modulus = (modulus+1) % BYTES_PER_UNENCODED_BLOCK;
      int b = in[inPos++];
      if (b < 0) {
        b += 256;
      }
      bitWorkArea = (bitWorkArea << 8) + b; //  BITS_PER_BYTE
      if (0 == modulus) { // 3 bytes = 24 bits = 4 * 6 bits to extract
        buffer[pos++] = encodeTable[(bitWorkArea >> 18) & MASK_6BITS];
        buffer[pos++] = encodeTable[(bitWorkArea >> 12) & MASK_6BITS];
        buffer[pos++] = encodeTable[(bitWorkArea >> 6) & MASK_6BITS];
        buffer[pos++] = encodeTable[bitWorkArea & MASK_6BITS];
      }
    }

    // handle corner case of end of string when it doesn't have an even number
    // of bytes
    switch (modulus) { // 0-2
    case 1 : // 8 bits = 6 + 2
      buffer[pos++] = encodeTable[(bitWorkArea >> 2) & MASK_6BITS]; // top 6 bits
      buffer[pos++] = encodeTable[(bitWorkArea << 4) & MASK_6BITS]; // remaining 2 
      // URL-SAFE skips the padding to further reduce size.
      if (encodeTable == STANDARD_ENCODE_TABLE) {
        buffer[pos++] = PAD;
        buffer[pos++] = PAD;
      }
      break;

    case 2 : // 16 bits = 6 + 6 + 4
      buffer[pos++] = encodeTable[(bitWorkArea >> 10) & MASK_6BITS];
      buffer[pos++] = encodeTable[(bitWorkArea >> 4) & MASK_6BITS];
      buffer[pos++] = encodeTable[(bitWorkArea << 2) & MASK_6BITS];
      // URL-SAFE skips the padding to further reduce size.
      if (encodeTable == STANDARD_ENCODE_TABLE) {
        buffer[pos++] = PAD;
      }
      break;
    }

    String v = new String(buffer, "UTF-8");
    return v;
  }
}
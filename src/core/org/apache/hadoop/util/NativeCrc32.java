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

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ChecksumException;

/**
 * Wrapper around JNI support code to do checksum computation
 * natively.
 */
public class NativeCrc32 implements Checksum {
  static {
    NativeCodeLoader.isNativeCodeLoaded();
  }
  /** the current CRC value, bit-flipped */
  private int crc;
  private final PureJavaCrc32 pureJavaCrc32 = new PureJavaCrc32();
  private final PureJavaCrc32C pureJavaCrc32C = new PureJavaCrc32C();
  private int checksumType = CHECKSUM_CRC32;

  private boolean isAvailable = true;
  // Local benchmarks show that for >= 128 bytes, NativeCrc32 performs
  // better than PureJavaCrc32.
  private static final int SMALL_CHECKSUM = 128;
  private static final Log LOG = LogFactory.getLog(NativeCrc32.class);

  public NativeCrc32(int checksumType) {
    this();
    if (checksumType != CHECKSUM_CRC32 && checksumType != CHECKSUM_CRC32C) {
      throw new IllegalArgumentException("Invalid checksum type");
    }
    this.checksumType = checksumType;
  }

  public NativeCrc32() {
    isAvailable = isAvailable();
    reset();
  }

  /** {@inheritDoc} */
  public long getValue() {
     return (~crc) & 0xffffffffL;
  }

  public void setValue(int crc) {
    this.crc = ~crc;
  }

  /** {@inheritDoc} */
  public void reset() {
     crc = 0xffffffff;
  }
  
  /**
   * Return true if the JNI-based native CRC extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded();
  }

  /**
   * Verify the given buffers of data and checksums, and throw an exception
   * if any checksum is invalid. The buffers given to this function should
   * have their position initially at the start of the data, and their limit
   * set at the end of the data. The position, limit, and mark are not
   * modified.
   * 
   * @param bytesPerSum the chunk size (eg 512 bytes)
   * @param checksumType the DataChecksum type constant
   * @param sums the DirectByteBuffer pointing at the beginning of the
   *             stored checksums
   * @param data the DirectByteBuffer pointing at the beginning of the
   *             data to check
   * @param basePos the position in the file where the data buffer starts 
   * @param fileName the name of the file being verified
   * @throws ChecksumException if there is an invalid checksum
   */
  public static void verifyChunkedSums(int bytesPerSum, int checksumType,
      ByteBuffer sums, ByteBuffer data, String fileName, long basePos)
      throws ChecksumException {
    nativeVerifyChunkedSums(bytesPerSum, checksumType,
        sums, sums.position(),
        data, data.position(), data.remaining(),
        fileName, basePos);
  }

  public void update(int b) {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    update(buf, 0, buf.length);
  }

  private void updatePureJava(byte[] buf, int offset, int len) {
    if (checksumType == CHECKSUM_CRC32) {
      pureJavaCrc32.setValueInternal(crc);
      pureJavaCrc32.update(buf, offset, len);
      crc = pureJavaCrc32.getCrcValue();
    } else {
      pureJavaCrc32C.setValueInternal(crc);
      pureJavaCrc32C.update(buf, offset, len);
      crc = pureJavaCrc32C.getCrcValue();
    }
  }

  public void update(byte[] buf, int offset, int len) {
    // To avoid JNI overhead, use native methods only for large checksum chunks.
    if (isAvailable && len >= SMALL_CHECKSUM) {
      try {
        crc = update(crc, buf, offset, len, checksumType);
      } catch (UnsatisfiedLinkError ule) {
        isAvailable = false;
        LOG.warn("Could not find native crc32 libraries," +
            " falling back to pure java", ule);
        updatePureJava(buf, offset, len);
      }
    } else {
      updatePureJava(buf, offset, len);
    }
  }

  public native int update(int crc, byte[] buf, int offset, int len, int checksumType);
  
  private static native void nativeVerifyChunkedSums(
      int bytesPerSum, int checksumType,
      ByteBuffer sums, int sumsOffset,
      ByteBuffer data, int dataOffset, int dataLength,
      String fileName, long basePos);

  // Copy the constants over from DataChecksum so that javah will pick them up
  // and make them available in the native code header.
  public static final int CHECKSUM_CRC32 = DataChecksum.CHECKSUM_CRC32;
  public static final int CHECKSUM_CRC32C = DataChecksum.CHECKSUM_CRC32C;
}

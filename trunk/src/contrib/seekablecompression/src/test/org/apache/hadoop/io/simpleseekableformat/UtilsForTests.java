package org.apache.hadoop.io.simpleseekableformat;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import junit.framework.Assert;

public class UtilsForTests {

  /**
   * A less-efficient but better-controlled version of Random.nextBytes(b).
   * This produces a deterministic sequence of bytes no matter how big array b is each time.
   */
  public static void nextBytes(Random random, byte[] b, int maxByte) {
    // random.nextInt is very slow.
    // To test speed, replace the following loop with:
    // Arrays.fill(b, (byte)0);
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte)random.nextInt(maxByte);
    }
  }

  public static void nextBytes(Random random, byte[] b) {
    nextBytes(random, b, 256);
  }

  /**
   * This function is already in junit 4.0: Assert.assertArrayEquals()
   */
  public static void assertArrayEquals(String message, byte[] expected, byte[] actual) {
    Assert.assertEquals(message + " length mismatch ", expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(message + " Byte " + i, expected[i], actual[i]);
    }
  }

  static class TruncatedOutputStream extends FilterOutputStream {

    final int maxSize;
    int currentSize;

    public TruncatedOutputStream(OutputStream out, int maxSize) {
      super(out);
      this.maxSize = maxSize;
      this.currentSize = 0;
    }

    @Override
    public void write(int b) throws IOException {
      if (maxSize > currentSize) {
        out.write(b);
        currentSize ++;
      }
    }

    @Override
    public void write(byte[] b, int start, int length) throws IOException {
      int toWrite = Math.min(maxSize - currentSize, length);
      if (toWrite > 0) {
        out.write(b, start, toWrite);
        currentSize += toWrite;
      }
    }
  }
}

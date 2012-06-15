package org.apache.hadoop.io.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

/**
 * Unittest for GzipCodec.
 */
public class TestGzipCodec extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestGzipCodec.class);
  
  private GzipCodec gzipCodec = null;
  private Configuration conf = null;
  
  protected void setUp() {
    conf = new Configuration();
    CompressionCodecFactory factory =
        new CompressionCodecFactory(conf);
    
    String className = GzipCodec.class.getName();
    gzipCodec = (GzipCodec) factory.getCodecByClassName(className);
  }
  
  public void testReadTruncatedSmallFile() throws IOException {
    readTruncatedFile(1024, Integer.MAX_VALUE);
  }
  
  public void testReadTruncatedLargeFile() throws IOException {
    readTruncatedFile(1024 * 1024 * 10, 10);
  }
  
  /**
   * This function will first generate a gzip file with decompressedLength
   * uncompressed size, tries to truncate in numTruncates possible ways
   * and read each truncated file.
   * 
   * If numTruncates is Integer.MAX_VALUE, this function will truncate
   * the generated compressed file by 1 byte step, i.e. all possible
   * ways to truncate.
   */
  private void readTruncatedFile(int decompressedLength, int numTruncates)
      throws IOException {
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      System.err.println(
        "Ignoring readTruncatedFile test because native zlib is not loaded");
      return;
    }
    try {
      byte[] compressed = getGzipArray(decompressedLength);
      LOG.info(String.format(
          "Decompressed Length / Compressed Length: %d / %d",
          decompressedLength,
          compressed.length));
      
      int step = Math.max(1, compressed.length / numTruncates);
      for (int i = 0; i < compressed.length; i += step) {
        byte[] truncated = Arrays.copyOf(compressed, i);

        try {
          readGzip(truncated);
          fail(String.format(
              "CodecPrematureEOFException is expected for truncated file." +
              " Original Length / Truncated Length: %d / %d",
              compressed.length,
              i));
        } catch (EOFException e) {
          // pass
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  /**
   * Generate random array of specified decompressedLength, compress it using
   * gzip codec and return compressed byte array.
   */
  private byte[] getGzipArray(int decompressedLength) throws IOException {
    ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
    OutputStream gzipOut = gzipCodec.createOutputStream(rawOut);
    
    Random rand = new Random(37);
    for (int i = 0; i < decompressedLength; ++i) {
      gzipOut.write(rand.nextInt(16));  // to allow ~50% of compression
    }
    gzipOut.close();
    
    return rawOut.toByteArray();
  }
  
  /**
   * Given a compressed array, read it using gzip codec.
   */
  private void readGzip(byte[] compressed) throws IOException {
    InputStream gzipIn = gzipCodec.createInputStream(
        new ByteArrayInputStream(compressed));

    while ((gzipIn.read()) != -1) {
      // do nothing
    }
    gzipIn.close();
  }
}

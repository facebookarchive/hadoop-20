package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.simpleseekableformat.InterleavedInputStream;
import org.apache.hadoop.io.simpleseekableformat.InterleavedOutputStream;


/**
 * TestCase for {@link InterleavedInputStream} and {@link InterleavedOutputStream}
 */
public class TestInterleavedStreams extends TestCase {

  Configuration conf = new Configuration();


  public void testNormalWriteAndRead() throws Exception {
    testNormalWriteAndRead(10, 90, 0);
    testNormalWriteAndRead(10, 90, 10);
    testNormalWriteAndRead(10, 90, 90);
    testNormalWriteAndRead(10, 90, 100);
    testNormalWriteAndRead(10, 90, 900);
    testNormalWriteAndRead(1024, 1023 * 1024, 0);
    testNormalWriteAndRead(1024, 1023 * 1024, 1);
    testNormalWriteAndRead(1024, 1023 * 1024, 1024);
    testNormalWriteAndRead(1024, 1023 * 1024, 3 * 1024 * 1024);
  }


  public void testNormalWriteAndRead(final int metaBlockLength, final int dataBlockLength,
      final int totalDataLength) throws Exception {
    // Not using loops here so we can know the exact parameter values from
    // the stack trace.
    testNormalWriteAndRead(metaBlockLength, dataBlockLength, totalDataLength, 0, 0);
    testNormalWriteAndRead(metaBlockLength, dataBlockLength, totalDataLength, 0, 7);
    testNormalWriteAndRead(metaBlockLength, dataBlockLength, totalDataLength, 7, 0);
    testNormalWriteAndRead(metaBlockLength, dataBlockLength, totalDataLength, 7, 7);
  }

  /**
   * @param writeSize  0: use "write(int)"; > 0: use "write(byte[])".
   * @param readSize   0: use "read()"; > 0: use "read(bytes[])".
   */
  public void testNormalWriteAndRead(final int metaBlockLength, final int dataBlockLength,
      final int totalDataLength, final int writeSize, final int readSize) throws Exception {

    // Random seed for metaData to be written
    final Random metaDataRandom = new Random(127);
    // Random seed for verifying metaData written
    final Random metaDataRandom2 = new Random(127);

    // Random seed for data to be written
    final Random dataRandom = new Random(111);
    // Random seed for verifying data written
    final Random dataRandom2 = new Random(111);

    // MetaData producer for the writing
    final InterleavedOutputStream.MetaDataProducer myMetaDataProducer =
      new InterleavedOutputStream.MetaDataProducer() {
        @Override
        public void writeMetaData(DataOutputStream out, int metaDataBlockSize)
            throws IOException {
          Assert.assertEquals(metaBlockLength, metaDataBlockSize);
          byte[] metaData = new byte[metaBlockLength];
          UtilsForTests.nextBytes(metaDataRandom, metaData);
          out.write(metaData);
        }
    };

    // The interleaved data
    ByteArrayOutputStream interleaved = new ByteArrayOutputStream();

    // Write to interleaved data
    InterleavedOutputStream outputStream = new InterleavedOutputStream(
        new DataOutputStream(interleaved), metaBlockLength,
        dataBlockLength, myMetaDataProducer);
    if (writeSize == 0) {
      byte[] chunk = new byte[1];
      for (int d = 0; d < totalDataLength; d++) {
        UtilsForTests.nextBytes(dataRandom, chunk);
        outputStream.write(chunk[0] < 0 ? chunk[0] + 256 : (int)chunk[0]);
      }
    } else {
      int written = 0;
      while (written < totalDataLength) {
        int toWrite = Math.min(writeSize, totalDataLength - written);
        byte[] chunk = new byte[toWrite];
        UtilsForTests.nextBytes(dataRandom, chunk);
        outputStream.write(chunk);
        written += toWrite;
      }
    }
    outputStream.close();

    // MetaData consumer for the reading
    final InterleavedInputStream.MetaDataConsumer myMetaDataConsumer =
      new InterleavedInputStream.MetaDataConsumer() {
        @Override
        public void readMetaData(InputStream in, int metaDataBlockSize)
            throws IOException {
          Assert.assertEquals(metaBlockLength, metaDataBlockSize);
          byte[] metaDataExpected = new byte[metaBlockLength];
          UtilsForTests.nextBytes(metaDataRandom2, metaDataExpected);
          byte[] metaData = new byte[metaBlockLength];
          // Note: DataInputStream.close() will close underlying stream, but we are OK because
          // it's ByteArrayInputStream which has a dummy close().
          DataInputStream dataInputStream = new DataInputStream(in);
          dataInputStream.readFully(metaData);
          UtilsForTests.assertArrayEquals("MetaData section", metaDataExpected, metaData);
        }
    };

    // Read from the interleaved data, and verify the data
    InterleavedInputStream inputStream = new InterleavedInputStream(
        new ByteArrayInputStream(interleaved.toByteArray()), metaBlockLength,
        dataBlockLength, myMetaDataConsumer);
    if (readSize == 0) {
      byte[] expected = new byte[1];
      for (int d = 0; d < totalDataLength; d++) {
        UtilsForTests.nextBytes(dataRandom2, expected);
        Assert.assertEquals("Data section byte " + d,
            expected[0] < 0 ? expected[0] + 256 : expected[0],
            inputStream.read());
      }
    } else {
      int read = 0;
      // Note: DataInputStream.close() will close underlying stream, but we are OK because
      // it's ByteArrayInputStream which has a dummy close().
      DataInputStream dataInputStream = new DataInputStream(inputStream);
      while (read < totalDataLength) {
        int toRead = Math.min(readSize, totalDataLength - read);
        byte[] expected = new byte[toRead];
        UtilsForTests.nextBytes(dataRandom2, expected);
        byte[] actual = new byte[toRead];
        dataInputStream.readFully(actual);
        UtilsForTests.assertArrayEquals("Data section", expected, actual);
        read += toRead;
      }
    }

    // Verify EOF
    Assert.assertEquals(-1, inputStream.read());
    byte[] temp = new byte[100];
    Assert.assertEquals(-1, inputStream.read(temp));
  }

}

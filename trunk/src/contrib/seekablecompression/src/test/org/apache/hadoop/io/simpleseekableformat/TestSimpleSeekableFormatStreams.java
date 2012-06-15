package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormatInputStream;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormatOutputStream;


/**
 * TestCase for {@link SimpleSeekableFormatInputStream} and {@link SimpleSeekableFormatOutputStream}
 */
public class TestSimpleSeekableFormatStreams extends TestCase {

  public void testNormalWriteAndRead() throws Exception {
    testNormalWriteAndRead(null);
    testNormalWriteAndRead(GzipCodec.class);
  }

  void testNormalWriteAndRead(final Class<? extends CompressionCodec> codecClass
      ) throws Exception {
    // Not using loops here so we can know the exact parameter values from
    // the stack trace.
    (new NormalWriteAndReadTester(codecClass, 1, 65536)).run();
    (new NormalWriteAndReadTester(codecClass, 200, 16384)).run();
    (new NormalWriteAndReadTester(codecClass, 1000, 4096)).run();
  }

  /**
   * Test the seekForward function.
   */
  public void testNormalWriteAndForwardRead() throws Exception {
    testNormalWriteAndForwardRead(null, false);
    testNormalWriteAndForwardRead(GzipCodec.class, false);
    // useFileSystem = true, for testing seek using Seekable
    testNormalWriteAndForwardRead(null, true);
    testNormalWriteAndForwardRead(GzipCodec.class, true);
  }

  void testNormalWriteAndForwardRead(final Class<? extends CompressionCodec> codecClass,
      boolean useFileSystem) throws Exception {
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 10, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 1024, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 500 * 1000, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 - 100, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 + 100, useFileSystem)).run();
    (new NormalWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 + 1024, useFileSystem)).run();
  }

  /**
   * Test the seekForward function with truncated files.
   */
  public void testTruncatedWriteAndForwardRead() throws Exception {
    testTruncatedWriteAndForwardRead(null, false);
    testTruncatedWriteAndForwardRead(GzipCodec.class, false);
    // useFileSystem = true, for testing seek using Seekable
    testTruncatedWriteAndForwardRead(null, true);
    testTruncatedWriteAndForwardRead(GzipCodec.class, true);
  }

  void testTruncatedWriteAndForwardRead(final Class<? extends CompressionCodec> codecClass,
      boolean useFileSystem) throws Exception {
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 10, 80, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 1024, 100 * 1000, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 500 * 1000, 1024 * 1024, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 - 100, 6 * 1024 * 1024, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024, 6 * 1024 * 1024, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 + 100, 6 * 1024 * 1024, useFileSystem)).run();
    (new TruncatedWriteAndForwardReadTester(codecClass, 1000, 16384, 3 * 1024 * 1024 + 1024, 6 * 1024 * 1024, useFileSystem)).run();
  }



  static class NormalWriteAndReadTester {
    /**
     * @param writeSize
     *          0: use "write(int)"; > 0: use "write(byte[])".
     * @param readSize
     *          0: use "read()"; > 0: use "read(bytes[])".
     */
    NormalWriteAndReadTester(final Class<? extends CompressionCodec> codecClass,
      final int numRecord, final int maxRecordSize) throws Exception {
      this.codecClass = codecClass;
      this.numRecord = numRecord;
      this.maxRecordSize = maxRecordSize;
    }

    protected final Class<? extends CompressionCodec> codecClass;
    protected final int numRecord;
    protected final int maxRecordSize;

    void run() throws Exception {

      // Random seed for data to be written
      final int randSeed = 333;

      // Write
      long startMs = System.currentTimeMillis();
      ByteArrayOutputStream inMemoryFile = write(new Random(randSeed));
      long writeDoneMs = System.currentTimeMillis();

      // Read
      read(new Random(randSeed), inMemoryFile);
      long readDoneMs = System.currentTimeMillis();

      // Output file size and time used for debugging purpose
      System.out.println("File size = " + inMemoryFile.size()
          + " writeMs=" + (writeDoneMs - startMs)
          + " readMs=" + (readDoneMs - writeDoneMs)
          + " numRecord=" + numRecord + " maxRecordSize=" + maxRecordSize
          + " codec=" + codecClass);

    }

    ByteArrayOutputStream write(final Random dataRandom) throws Exception {

      // Create the in-memory file and start to write to it.
      ByteArrayOutputStream inMemoryFile = new ByteArrayOutputStream();
      SimpleSeekableFormatOutputStream out = new SimpleSeekableFormatOutputStream(inMemoryFile);

      // Set compression Codec
      Configuration conf = new Configuration();
      if (codecClass != null) {
        conf.setClass(SimpleSeekableFormat.FILEFORMAT_SSF_CODEC_CONF, codecClass,
            CompressionCodec.class);
      }
      out.setConf(conf);

      // Write some data
      for (int r = 0; r < numRecord; r++) {
        byte[] b = new byte[dataRandom.nextInt(maxRecordSize)];
        // Generate some compressible random data
        UtilsForTests.nextBytes(dataRandom, b, 16);
        out.write(b);
        if (r % 100 == 99) {
          out.flush();
        }
      }
      out.close();

      return inMemoryFile;
    }

    void read(final Random dataRandom2, final ByteArrayOutputStream inMemoryFile)
        throws Exception {

      // Open the in-memory file for read
      ByteArrayInputStream fileForRead = new ByteArrayInputStream(inMemoryFile.toByteArray());
      SimpleSeekableFormatInputStream in = new SimpleSeekableFormatInputStream(fileForRead);
      DataInputStream dataIn = new DataInputStream(in);

      // Verify the data
      for (int r = 0; r < numRecord; r++) {
        // Regenerate the same random bytes
        byte[] b = new byte[dataRandom2.nextInt(maxRecordSize)];
        UtilsForTests.nextBytes(dataRandom2, b, 16);
        // Read from the file
        byte[] b2 = new byte[b.length];
        dataIn.readFully(b2);
        UtilsForTests.assertArrayEquals("record " + r + " with length " + b.length,
            b, b2);
      }

      // Verify EOF
      Assert.assertEquals(-1, in.read());
      byte[] temp = new byte[100];
      Assert.assertEquals(-1, in.read(temp));
    }
  }

  static class NormalWriteAndForwardReadTester extends NormalWriteAndReadTester {

    NormalWriteAndForwardReadTester(
        Class<? extends CompressionCodec> codecClass, int numRecord,
        int maxRecordSize, int availableBytes,
        boolean useFileSystem) throws Exception {
      super(codecClass, numRecord, maxRecordSize);
      this.availableBytes = availableBytes;
      this.useFileSystem = useFileSystem;
    }

    protected final int availableBytes;
    protected final boolean useFileSystem;

    @Override
    void read(final Random dataRandom2, final ByteArrayOutputStream inMemoryFile)
        throws Exception {

      byte[] data = inMemoryFile.toByteArray();
      // Open the in-memory file for read
      InputStream fileForRead = null;
      if (useFileSystem) {
        // Write data to a file and then test it.
        // This is useful for testing "seek" because FSInputStream is a Seekable.
        Configuration conf = new Configuration();
        FileSystem fs = LocalFileSystem.getLocal(conf);
        Path file = new Path(System.getProperty("user.dir") + "/test_seek.ssf");
        OutputStream out = fs.create(file);
        out.write(data);
        out.close();
        fileForRead = fs.open(file);
      } else {
        fileForRead = new ByteArrayInputStream(data) {
          /**
           * Only expose at most availableBytes until those bytes are all read.
           * This is to simulate a growing file.
           */
          @Override
          public int available() {
            if (pos < availableBytes) {
              return Math.min(super.available(), availableBytes - pos);
            } else {
              return super.available();
            }
          }
        };
      }

      SimpleSeekableFormatInputStream in = new SimpleSeekableFormatInputStream(fileForRead);
      DataInputStream dataIn = new DataInputStream(in);

      long seekedPosition = in.seekForward();
      {
        // We should not be at the beginning of the stream any more.
        InterleavedInputStream interleavedIn = in.getInterleavedIn();
        long blocks = interleavedIn.getRawOffset() / interleavedIn.getCompleteBlockSize();
        long blocksAvailable = (availableBytes - interleavedIn.getMetaDataBlockSize()) / interleavedIn.getCompleteBlockSize();
        blocksAvailable = Math.max(0, blocksAvailable);
        Assert.assertTrue(blocks >= blocksAvailable);
      }

      long currentUncompressedPosition = 0;
      for (int r = 0; r < numRecord; r++) {
        // Regenerate the same random bytes
        byte[] b = new byte[dataRandom2.nextInt(maxRecordSize)];
        UtilsForTests.nextBytes(dataRandom2, b, 16);
        if (currentUncompressedPosition >= seekedPosition) {
          // Read from the file
          byte[] b2 = new byte[b.length];
          dataIn.readFully(b2);
          UtilsForTests.assertArrayEquals("record " + r + " with length " + b.length,
              b, b2);
        }
        currentUncompressedPosition += b.length;
      }

      // Verify EOF
      Assert.assertEquals(-1, in.read());
      byte[] temp = new byte[100];
      Assert.assertEquals(-1, in.read(temp));
    }
  }

  static class TruncatedWriteAndForwardReadTester extends NormalWriteAndForwardReadTester {

    private int truncatedBytes;

    TruncatedWriteAndForwardReadTester(
        Class<? extends CompressionCodec> codecClass, int numRecord,
        int maxRecordSize, int availableBytes, int truncatedBytes,
        boolean useFileSystem) throws Exception {
      super(codecClass, numRecord, maxRecordSize, availableBytes, useFileSystem);
      this.truncatedBytes = truncatedBytes;
    }

    @Override
    ByteArrayOutputStream write(final Random dataRandom) throws Exception {
      ByteArrayOutputStream out = super.write(dataRandom);
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] fullArray = out.toByteArray();
      result.write(fullArray, 0, Math.min(fullArray.length, truncatedBytes));
      return result;
    }

    @Override
    void read(final Random dataRandom2, final ByteArrayOutputStream inMemoryFile)
      throws Exception {
      try {
        super.read(dataRandom2, inMemoryFile);
      } catch (EOFException e) {
        System.out.println("Hit EOF while testing truncated file...");
      }
    }

  }


}

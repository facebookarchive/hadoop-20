package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.simpleseekableformat.DataSegmentReader;
import org.apache.hadoop.io.simpleseekableformat.DataSegmentWriter;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * TestCase for {@link DataSegmentReader} and {@link DataSegmentWriter}.
 */
public class TestDataSegment extends TestCase {


  Configuration conf = new Configuration();

  public void testUncompressed() throws Exception {
    testWithCodec(null);
  }

  public void testCompressedGzipCodec() throws Exception {
    testWithCodec(GzipCodec.class);
  }

  void testWithCodec(Class<? extends CompressionCodec> codecClass) throws Exception {
    CompressionCodec codec =
      codecClass == null ? null : ReflectionUtils.newInstance(codecClass, conf);
    testNormalWriteAndRead(1, 1024, 16, codec);
    testNormalWriteAndRead(10, 1024, 16, codec);
    testNormalWriteAndRead(1, 1024, 256, codec);
    testNormalWriteAndRead(10, 1024, 256, codec);
  }

  SimpleSeekableFormat.Buffer createBuffer(Random random, int length, int byteMax) {
    SimpleSeekableFormat.Buffer data = new SimpleSeekableFormat.Buffer();
    for (int i = 0; i < length; i++) {
      data.write(random.nextInt(byteMax));
    }
    return data;
  }

  void testNormalWriteAndRead(final int segments, final int length, final int byteMax,
      final CompressionCodec codec) throws Exception {

    Random random = new Random(37);

    // Prepare data for testing
    SimpleSeekableFormat.Buffer[] data = new SimpleSeekableFormat.Buffer[segments];
    for (int s = 0; s < segments; s++) {
      data[s] = createBuffer(random, length, byteMax);
    }

    // Write data to dataSegmentOutput
    ByteArrayOutputStream dataSegmentOutput = new ByteArrayOutputStream();
    for (int s = 0; s < segments; s++) {
      DataSegmentWriter writer = new DataSegmentWriter(data[s], codec, null);
      writer.writeTo(new DataOutputStream(dataSegmentOutput));
    }

    // Read dataSegments back to a Buffer
    byte[][] newData = new byte[segments][];
    ByteArrayInputStream dataSegmentInput =
      new ByteArrayInputStream(dataSegmentOutput.toByteArray());
    Configuration conf = new Configuration();
    HashMap<Text, Decompressor> decompressorCache = new HashMap<Text, Decompressor>();
    for (int s = 0; s < segments; s++) {
      DataSegmentReader reader = new DataSegmentReader(
          new DataInputStream(dataSegmentInput), conf, decompressorCache);
      newData[s] = IOUtils.toByteArray(reader.getInputStream());
    }

    // Compare data and newData
    for (int s = 0; s < segments; s++) {
      UtilsForTests.assertArrayEquals("Segment[" + s + "]", data[s].toByteArray(), newData[s]);
    }
  }

}

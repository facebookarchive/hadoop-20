package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class holds the data related to a single data segment.
 */
class DataSegmentReader {


  static class EmptyDataSegmentException extends EOFException {
  }

  // uncompressed data stream
  private final InputStream uncompressedData;

  /**
   * May throw EOFException if InputStream does not have a
   * complete data segment.
   *
   * NOTE: This class holds reference to the Decompressor in
   * the decompressorCache, until the return value of
   * getInputStream() is closed.
   *
   * @param decompressorCache
   * @throws EmptyDataSegmentException  if there is nothing to read.
   * @throws EOFException  if the data segment is not complete.
   */
  DataSegmentReader(DataInputStream in, Configuration conf,
      HashMap<Text, Decompressor> decompressorCache)
      throws EmptyDataSegmentException, EOFException,
      ClassNotFoundException, IOException {

    // Read from DataInputStream
    // 1. Read length
    int length = 0;
    try {
      length = in.readInt();
    } catch (EOFException e) {
      throw new EmptyDataSegmentException();
    }

    // 2. Read codec
    int codecNameUTF8Length = in.readShort();
    byte[] codecNameUTF8 = new byte[codecNameUTF8Length];
    in.readFully(codecNameUTF8);
    Text codecNameText = new Text(codecNameUTF8);
    // 3. read CRC32 (only present when uncompressed)
    boolean hasCrc32 = (codecNameUTF8Length == 0);
    long crc32Value = 0;
    if (hasCrc32) {
      crc32Value = in.readLong();
    }
    // 4. read data
    byte[] storedData
        = new byte[length - (hasCrc32 ? 8 : 0)/*crc32*/
                   - 2/*codec length*/ - codecNameUTF8Length];
    in.readFully(storedData);

    // Verify the checksum
    if (hasCrc32) {
      CRC32 crc32 = new CRC32();
      crc32.update(storedData);
      if (crc32.getValue() != crc32Value) {
        throw new CorruptedDataException("Corrupted data segment with length " + length
            + " crc32 expected " + crc32Value + " but got " + crc32.getValue());
      }
    }

    // Uncompress the data if needed
    if (codecNameUTF8Length == 0) {
      // no compression
      uncompressedData = new ByteArrayInputStream(storedData);
    } else {
      CompressionCodec codec = getCodecFromName(codecNameText, conf);
      Decompressor decompressor = null;
      if (decompressorCache != null) {
        // Create decompressor and add to cache if needed.
        decompressor = decompressorCache.get(codecNameText);
        if (decompressor == null) {
          decompressor = codec.createDecompressor();
        } else {
          decompressor.reset();
        }
      }
      if (decompressor == null) {
        uncompressedData = codec.createInputStream(new ByteArrayInputStream(storedData));
      } else {
        uncompressedData = codec.createInputStream(new ByteArrayInputStream(storedData),
            decompressor);
      }
    }
  }

  InputStream getInputStream() {
    return uncompressedData;
  }

  /**
   * A simple cache to save the cost of reflection and object creation.
   */
  private static ConcurrentHashMap<Text, CompressionCodec> CODEC_CACHE
    = new ConcurrentHashMap<Text, CompressionCodec>();

  @SuppressWarnings("unchecked")
  private static CompressionCodec getCodecFromName(Text codecName, Configuration conf)
      throws ClassNotFoundException {
    CompressionCodec result = CODEC_CACHE.get(codecName);
    if (result == null) {
      Class<? extends CompressionCodec> codecClass =
        (Class<? extends CompressionCodec>)Class.forName(codecName.toString());
      result = ReflectionUtils.newInstance(codecClass, conf);
      CODEC_CACHE.put(codecName, result);
    }
    return result;
  }
}

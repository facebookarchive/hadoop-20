package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;

/**
 * This class holds the data related to a single data segment.
 */
class DataSegmentWriter {

  // empty string for no compression
  private final String codecName;
  private final byte[] codecNameUTF8;
  // either uncompressedData or compressedData
  private final SimpleSeekableFormat.Buffer storedData;
  // CRC32 value
  private final long crc32Value;

  /**
   * Create a new data segment from uncompressed data and a codec.
   * This is called by the writer.
   * @param compressor  for reusing a compressor.  It can be null.
   */
  DataSegmentWriter(SimpleSeekableFormat.Buffer uncompressedData,
      CompressionCodec codec,
      Compressor compressor) throws IOException {

    // Try compress
    if (codec != null) {
      SimpleSeekableFormat.Buffer compressedData = new SimpleSeekableFormat.Buffer();
      OutputStream out;
      if (compressor == null) {
        compressor = codec.createCompressor();
      } else {
        compressor.reset();
      }
      out = codec.createOutputStream(compressedData, compressor);
      out.write(uncompressedData.getData(), 0, uncompressedData.getLength());
      out.close();

      // Don't compress if the result is longer than uncompressed data.
      if (compressedData.getLength() + codec.getClass().getName().length()
          < uncompressedData.getLength() + 8) {
        codecName = codec.getClass().getName();
        storedData = compressedData;
      } else {
        codecName = "";
        storedData = uncompressedData;
      }
    } else {
      // no compression
      codecName = "";
      storedData = uncompressedData;
    }
    codecNameUTF8 = getCodecNameUTF8(codecName);

    // Calculate CRC32 only when there are no compression.
    if (codecName.length() == 0) {
      CRC32 crc32 = new CRC32();
      crc32.update(storedData.getData(), 0, storedData.getLength());
      crc32Value = crc32.getValue();
    } else {
      crc32Value = 0;
    }
  }

  /**
   * Returns the size of this Data Segment after (potential) compression.
   * This includes the length field.
   */
  int size() {
    return 4 /*length field*/
      + (codecNameUTF8.length == 0 ? 8 : 0) /*optional crc32*/
      + 2 /*utf8 length*/ + codecNameUTF8.length
      + storedData.getLength();
  }

  /**
   * Write this data segment into an OutputStream.
   */
  void writeTo(DataOutputStream out) throws IOException {

    // We do the UTF8 conversion ourselves instead of relying on DataOutput
    // to ensure we strictly follow UTF-8 standard, as well as better performance,
    // and save the code to count the UTF-8 bytes (we need that to calculate
    // the total length.
    int length = size() - 4;

    out.writeInt(length);
    out.writeShort(codecNameUTF8.length);
    out.write(codecNameUTF8);
    if (codecNameUTF8.length == 0) {
      out.writeLong(crc32Value);
    }
    out.write(storedData.getData(), 0, storedData.getLength());
  }


  /**
   * Utility static fields.
   */
  static final ConcurrentHashMap<String, byte[]> CODEC_NAME_CACHE = new ConcurrentHashMap<String, byte[]>();
  /**
   * Convert from String to UTF8 byte array.
   */
  static byte[] getCodecNameUTF8(String compressionCodecName) {
    byte[] codecNameBytes = CODEC_NAME_CACHE.get(compressionCodecName);
    if (codecNameBytes == null) {
      try {
        codecNameBytes = compressionCodecName.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      CODEC_NAME_CACHE.put(compressionCodecName, codecNameBytes);
    }
    return codecNameBytes;
  }

}

package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * SimpleSeekableFormat supports seek based on compressed byte offsets as well
 * as uncompressed byte offsets.
 *
 * File Format Description:
 *
 * 0. Definition
 * * Metadata block: a fixed size block of 1024 bytes storing metadata of the
 *                   file.
 * * Data block: a fixed size block of 1023K bytes storing actual data.
 * * Data segment: a variable-length segment of bytes storing a logical
 *                 data unit that needs to be processed together.
 *
 * 1. File Format Layout
 * Each 1K bytes at the beginning of x MB is a metadata block.
 * The rest of 1023K bytes are data blocks.
 *
 * 2. Metadata block (1024 bytes):
 * Each metadata block looks like this:
 * 32 bytes: "SSF_Magic_C17e5C697a00bB1A859aD\n"
 * 4 bytes: version number, now is 1.
 * 16 bytes: 8-byte of uncompressed data stream offset
 *           + 8-byte of compressed data stream offset
 *
 * 3. Data block (1023 * 1024 bytes):
 * All data blocks should be concatenated to be a stream.  The stream consists
 * of consecutive data segments, back by back.
 *
 * 4. Data segment:
 * Each data segment looks like this:
 * 4 bytes: length (implies that a single data segment cannot be longer than
 *          4GB).  It does not include the length field itself, but includes
 *          all following fields like codec name and crc32 checksum.
 * 2 bytes: byte length of compression codec class name. Or 0 for uncompressed.
 * x-bytes: UTF-8 encoded compression codec class name.
 * (Only when x = 0) 8 bytes: crc32 checksum of the data following.
 * (length - 2 - x - (x==0?8:0) ) bytes: actual data
 *
 * This class encapsulates all underlying logics of the SeekableFileFormat.
 *
 * NOTE: Requirement on the CompressionCodec InputStream: available() should
 * only return 0 when EOF.  Otherwise SeekableFileInputStream.available() will
 * break.
 */
public class SimpleSeekableFormat {

  public static final String FILEFORMAT_SSF_CODEC_CONF = "fileformat.ssf.codec";
  public static final String FILEFORMAT_SSF_MAX_UNCOMPRESSED_SEGMENT_LENGTH =
    "fileformat.ssf.max.uncompressed.segment.length";


  static final int METADATA_BLOCK_LENGTH = 1024;
  static final int DATA_BLOCK_LENGTH = 1024 * 1024 - METADATA_BLOCK_LENGTH;

  static final int VERSION = 1;
  static final String MAGIC_HEADER = "SSF_Magic_C17e5C697a00bB1A859aD\n";
  static final byte[] MAGIC_HEADER_BYTES;
  static {
    try {
      MAGIC_HEADER_BYTES = MAGIC_HEADER.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }


  public static class OffsetPair {
    private long uncompressedOffset;
    private long compressedOffset;
    public OffsetPair() {
      this(0L, 0L);
    }
    public OffsetPair(long uncompressedOffset, long compressedOffset) {
      this.uncompressedOffset = uncompressedOffset;
      this.compressedOffset = compressedOffset;
    }
    public long getUncompressedOffset() {
      return uncompressedOffset;
    }
    public void setUncompressedOffset(long uncompressedOffset) {
      this.uncompressedOffset = uncompressedOffset;
    }
    public long getCompressedOffset() {
      return compressedOffset;
    }
    public void setCompressedOffset(long compressedOffset) {
      this.compressedOffset = compressedOffset;
    }
  };

  public static class MetaData {
    // key: uncompressedOffset
    // value: DataStreamOffset
    SortedMap<Long, Long> offsetPairs;

    public void setOffsetPairs(final SortedMap<Long, Long> offsetPairs) {
      this.offsetPairs = offsetPairs;
    }
    public SortedMap<Long, Long> getOffsetPairs() {
      return offsetPairs;
    }
  }

  /**
   * MetaDataConsumer reads data from the stream and write to the MetaData class.
   */
  public static class MetaDataConsumer implements InterleavedInputStream.MetaDataConsumer {

    private MetaData metaData;

    public MetaDataConsumer(MetaData metaData) {
      this.metaData = metaData;
    }

    @Override
    public void readMetaData(InputStream in, int metaDataBlockSize)
        throws IOException {

      // Read in the whole MetaDataBlock and store it in a DataInputStream.
      byte[] metaDataBlock = new byte[metaDataBlockSize];
      (new DataInputStream(in)).readFully(metaDataBlock);
      DataInputStream din = new DataInputStream(new ByteArrayInputStream(metaDataBlock));

      // verify magic header
      byte[] magicHeaderBytes = new byte[MAGIC_HEADER_BYTES.length];
      din.readFully(magicHeaderBytes);
      if (!Arrays.equals(magicHeaderBytes, MAGIC_HEADER_BYTES)) {
        throw new IOException("Wrong Magic Header Bytes");
      }

      // verify version
      int version = din.readInt();
      if (version > VERSION) {
        throw new IOException("Unknown version " + version);
      }

      switch (version) {
        case 1: {
          // one pair of offsets
          long uncompressedOffset = din.readLong();
          long compressedOffset = din.readLong();
          SortedMap<Long, Long> offsetPairs = new TreeMap<Long, Long>();
          offsetPairs.put(uncompressedOffset, compressedOffset);
          metaData.setOffsetPairs(offsetPairs);
          // the rest is thrown away
        }
      }
    }
  }

  /**
   * This inner class provides the metadata block.
   * Note that it accesses the lastOffsets field.
   */
  public static class MetaDataProducer implements InterleavedOutputStream.MetaDataProducer {

    private MetaData metaData;

    public MetaDataProducer(MetaData metaData) {
      this.metaData = metaData;
    }

    /**
     * @param out  The raw output stream.
     */
    @Override
    public void writeMetaData(DataOutputStream out, int metaDataBlockSize) throws IOException {
      // Magic header and version
      out.write(SimpleSeekableFormat.MAGIC_HEADER_BYTES);
      out.writeInt(SimpleSeekableFormat.VERSION);
      // Write out the offset pair
      SortedMap<Long, Long> offsetPairs = metaData.getOffsetPairs();
      assert(offsetPairs.size() == 1);
      long uncompressedOffset = offsetPairs.firstKey();
      long compressedOffset = offsetPairs.get(uncompressedOffset);
      out.writeLong(uncompressedOffset);
      out.writeLong(compressedOffset);

      // Fill up the bytes
      int left = metaDataBlockSize - SimpleSeekableFormat.MAGIC_HEADER_BYTES.length - 4 - 8 - 8;
      while (left > 0) {
        int toWrite = Math.min(left, NULLS.length);
        out.write(NULLS, 0, toWrite);
        left -= toWrite;
      }
    }
  }

  private static byte[] NULLS = new byte[1024];
  static {
    Arrays.fill(NULLS, (byte)0);
  }


  static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }
    public void reset() { count = 0; }
  }

}

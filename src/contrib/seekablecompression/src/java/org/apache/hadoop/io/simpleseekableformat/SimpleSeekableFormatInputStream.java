package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPrematureEOFException;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.simpleseekableformat.DataSegmentReader.EmptyDataSegmentException;

/**
 * The reader for Seekable File Format.
 *
 * This class inherits CompressionInputStream because an instance of this will
 * be returned by SimpleSeekableFormatCodec.
 *
 * See {@link SimpleSeekableFormat}
 */
public class SimpleSeekableFormatInputStream extends CompressionInputStream {

  private final InterleavedInputStream interleavedIn;
  private final DataInputStream dataIn;
  private InputStream dataSegmentIn;

  // Stores the latest metaData block
  private final SimpleSeekableFormat.MetaData metaData;

  private final HashMap<Text, Decompressor> decompressorCache
    = new HashMap<Text, Decompressor>();

  private final Configuration conf = new Configuration();

  public SimpleSeekableFormatInputStream(InputStream in) {
    // we don't use the inherited field "in" at all:
    super(null);
    metaData = new SimpleSeekableFormat.MetaData();
    interleavedIn = createInterleavedInputStream(in,
        SimpleSeekableFormat.METADATA_BLOCK_LENGTH,
        SimpleSeekableFormat.DATA_BLOCK_LENGTH,
        new SimpleSeekableFormat.MetaDataConsumer(metaData));
    this.dataIn = new DataInputStream(interleavedIn);
  }

  /**
   * This factory method can be overwritten by subclass to provide different behavior.
   * It's only called in the constructor.
   */
  protected InterleavedInputStream createInterleavedInputStream(InputStream in,
      int metaDataBlockLength, int dataBlockLength,
      SimpleSeekableFormat.MetaDataConsumer consumer) {
    return new InterleavedInputStream(in, metaDataBlockLength, dataBlockLength, consumer);
  }

  protected InterleavedInputStream getInterleavedIn() {
    return interleavedIn;
  }

  protected SimpleSeekableFormat.MetaData getMetaData() {
    return metaData;
  }

  @Override
  public int read() throws IOException {
    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return -1;
      }
    }
    do {
      int result = dataSegmentIn.read();
      if (result != -1) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return -1;
      }
    } while (true);
  }

  @Override
  public int read(byte[] b, int start, int length) throws IOException {

    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return -1;
      }
    }
    do {
      int result = dataSegmentIn.read(b, start, length);
      if (result != -1) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return -1;
      }
    } while (true);
  }

  @Override
  public void close() throws IOException {
    clearDataSegment();
    dataIn.close();
  }

  /**
   * This function depends on that the underlying dataSegmentIn.available() only
   * returns 0 when EOF.  Otherwise it will break because it jumps over the dataSegmentIn
   * that has available() == 0.
   */
  @Override
  public int available() throws IOException {
    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return 0;
      }
    }
    do {
      int result = dataSegmentIn.available();
      if (result != 0) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return 0;
      }
    } while (true);
  }

  /**
   * Returns false if there are no more data segments.
   */
  private boolean moveToNextDataSegment() throws IOException {
    try {
      clearDataSegment();
      DataSegmentReader dataSegmentReader =
          new DataSegmentReader(dataIn, conf, decompressorCache);
      dataSegmentIn = dataSegmentReader.getInputStream();
    } catch (EmptyDataSegmentException e){
      // no data available
      return false;
    } catch (EOFException e) {
      // EOFException is thrown when the underlying data stream is truncated, e.g. truncated file.
      // This is considered as a normal case.
      throw new CodecPrematureEOFException("Truncated .SSF file detected.");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**
   * Called by subclass to clear out the current dataSegmentIn.
   */
  protected void clearDataSegment() throws IOException {
    if (dataSegmentIn != null) {
      dataSegmentIn.close();
      dataSegmentIn = null;
    }
  }

  @Override
  public void resetState() throws IOException {
    throw new RuntimeException("SeekableFileInputFormat does not support resetState()");
  }

  /**
   * This function seeks forward using all "available" bytes.
   * It returns the offset after the seek.
   *
   * This function throws EOFException if there are no available complete metaDataBlock
   * or the metaDataBlock points to a position after the file end (e.g. truncated files).
   */
  public long seekForward() throws IOException {
    // Try to read the last metadata block
    interleavedIn.skipToLastAvailableMetaDataBlock();
    if (!interleavedIn.readMetaDataIfNeeded()) {
      throw new EOFException("Cannot get a complete metadata block");
    }

    // Move the interleavedIn to the beginning of a dataSegment
    SortedMap<Long, Long> offsetPairs = metaData.getOffsetPairs();
    // The last key in the offsetPair points to the farthest position that we can seek to.
    long uncompressedDataOffset = offsetPairs.lastKey();
    long compressedDataOffset = offsetPairs.get(uncompressedDataOffset);
    long toSkip = compressedDataOffset - interleavedIn.getDataOffset();
    if (toSkip < 0) {
      throw new CorruptedDataException("SSF format error: The last offset pair is before the current position in InterleaveStream!");
    }
    try {
      interleavedIn.skipExactly(toSkip);
    } catch (EOFException e) {
      // Ignore this exception
      // This is the PTail use case.  We don't care about this CodecPrematureEOFException
    }

    clearDataSegment();
    return uncompressedDataOffset;
  }

}

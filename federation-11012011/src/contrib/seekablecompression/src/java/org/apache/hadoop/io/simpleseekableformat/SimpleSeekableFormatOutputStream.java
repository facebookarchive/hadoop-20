package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat.MetaData;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat.OffsetPair;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Write data in Seekable File Format.
 * Data from a single write will be in a single data segment.
 *
 * See {@link SimpleSeekableFormat}
 */
public class SimpleSeekableFormatOutputStream extends CompressionOutputStream implements Configurable {

  /**
   * This is a hint.  The actual max can go beyond this number if a lot of data are
   * sent via a single write.  A single write will always be in the same data segment.
   */
  private static final int DEFAULT_MAX_UNCOMPRESSED_SEGMENT_LENGTH = 1024 * 1024;

  /**
   * dataSegmentOut is a wrapper stream that automatically inserts MetaDataBlocks
   * while writing out data segments.
   */
  final InterleavedOutputStream dataSegmentOut;
  /**
   * dataSegmentDataOut is a DataOutputStream wrapping dataSegmentOut.
   */
  private final DataOutputStream dataSegmentDataOut;

  private final MetaData metadata;

  private Configuration conf;
  private Class<? extends CompressionCodec> codecClass;
  private CompressionCodec codec;
  private Compressor codecCompressor;
  private int thresholdUncompressedSegmentLength;


  private final SimpleSeekableFormat.Buffer currentDataSegmentBuffer = new SimpleSeekableFormat.Buffer();

  public SimpleSeekableFormatOutputStream(OutputStream out) {
    this(new DataOutputStream(out));
  }

  /**
   * DataOutputStream allows easy write of integer, string etc.
   */
  protected SimpleSeekableFormatOutputStream(DataOutputStream out) {
    // We don't use the inherited field "out" at all.
    super(null);

    metadata = new MetaData();
    SortedMap<Long, Long> offsetPairs = new TreeMap<Long, Long>();
    offsetPairs.put(0L, 0L);
    metadata.setOffsetPairs(offsetPairs);

    this.dataSegmentOut =
        new InterleavedOutputStream(out,
            SimpleSeekableFormat.METADATA_BLOCK_LENGTH,
            SimpleSeekableFormat.DATA_BLOCK_LENGTH,
            new SimpleSeekableFormat.MetaDataProducer(metadata)
          );
    this.dataSegmentDataOut = new DataOutputStream(dataSegmentOut);
  }


  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // Set the codec
    codecClass = conf.getClass(SimpleSeekableFormat.FILEFORMAT_SSF_CODEC_CONF, null,
        CompressionCodec.class);
    if (codecClass == null) {
      codec = null;
    } else {
      codec = ReflectionUtils.newInstance(codecClass, conf);
      codecCompressor = codec.createCompressor();
    }
    // Set the max segment length
    thresholdUncompressedSegmentLength = conf.getInt(
        SimpleSeekableFormat.FILEFORMAT_SSF_MAX_UNCOMPRESSED_SEGMENT_LENGTH,
        DEFAULT_MAX_UNCOMPRESSED_SEGMENT_LENGTH);
  }

  @Override
  public void write(int b) throws IOException {
    currentDataSegmentBuffer.write(b);
    flushIfNeeded();
  }

  /**
   * This function makes sure the whole buffer is written into the same data segment.
   */
  @Override
  public void write(byte[] b, int start, int length) throws IOException {
    currentDataSegmentBuffer.write(b, start, length);
    flushIfNeeded();
  }

  @Override
  public void close() throws IOException {
    if (currentDataSegmentBuffer.size() > 0) {
      flush();
    }
    dataSegmentDataOut.close();
  }

  private void flushIfNeeded() throws IOException {
    if (currentDataSegmentBuffer.size() >= thresholdUncompressedSegmentLength) {
      flush();
    }
  }


  private void updateMetadata(long uncompressedSegmentSize,
      long compressedSegmentSize) {
    SortedMap<Long, Long> offsetPairs = metadata.getOffsetPairs();
    long lastUncompressedOffset = offsetPairs.firstKey();
    long lastCompressedOffset = offsetPairs.get(lastUncompressedOffset);
    long uncompressedOffset = lastUncompressedOffset + uncompressedSegmentSize;
    long compressedOffset = lastCompressedOffset + compressedSegmentSize;
    offsetPairs.clear();
    offsetPairs.put(uncompressedOffset, compressedOffset);
  }

  /**
   * Take the current data segment, optionally compress it,
   * calculate the crc32, and then write it out.
   *
   * The method sets the lastOffsets to the end of the file before it starts
   * writing.  That means the offsets in the MetaDataBlock will be after the
   * end of the current data block.
   */
  @Override
  public void flush() throws IOException {

    // Do not do anything if no data has been written
    if (currentDataSegmentBuffer.size() == 0) {
      return;
    }

    // Create the current DataSegment
    DataSegmentWriter currentDataSegment =
        new DataSegmentWriter(currentDataSegmentBuffer, codec, codecCompressor);

    // Update the metadata
    updateMetadata(currentDataSegmentBuffer.size(), currentDataSegment.size());

    // Write out the DataSegment
    currentDataSegment.writeTo(dataSegmentDataOut);

    // Clear out the current buffer. Note that this has to be done after
    // currentDataSegment.writeTo(...), because currentDataSegment can
    // keep a reference to the currentDataSegmentBuffer.
    currentDataSegmentBuffer.reset();

    // Flush out the underlying stream
    dataSegmentDataOut.flush();
  }

  @Override
  public void finish() throws IOException {
    // we don't need to do anything for finish().
  }

  @Override
  public void resetState() throws IOException {
    // we don't need to do anything for resetState().
  }

}

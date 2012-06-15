package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This OutputStream calls metaDataProducer to produce
 * metadata at fixed periodic locations in the output stream.
 */
class InterleavedOutputStream extends OutputStream {

  public interface MetaDataProducer {
    /**
     * This function should write a metadata block with size metaDataBlockSize
     * @param out  The raw output stream
     */
    void writeMetaData(DataOutputStream out, int metaDataBlockSize) throws IOException;
  }

  private final DataOutputStream out;
  private final int metaDataBlockSize;
  private final int dataBlockSize;
  private final MetaDataProducer metaDataProducer;

  private long completeMetaDataBlocks;
  private int currentDataBlockSize;

  InterleavedOutputStream(DataOutputStream out,
      int metaDataBlockSize, int dataBlockSize,
      MetaDataProducer metaDataProducer) {
    this.out = out;
    this.metaDataBlockSize = metaDataBlockSize;
    this.dataBlockSize = dataBlockSize;
    this.metaDataProducer = metaDataProducer;
    // indicate that we need to write a metadata block right away.
    currentDataBlockSize = dataBlockSize;
  }

  private void writeMetaDataIfNeeded() throws IOException {
    if (currentDataBlockSize == dataBlockSize) {
      metaDataProducer.writeMetaData(out, metaDataBlockSize);
      completeMetaDataBlocks ++;
      currentDataBlockSize = 0;
    }
  }

  // Number of bytes written to the underlying stream
  public long getOffset() {
    return completeMetaDataBlocks * metaDataBlockSize
      + getDataOffset();
  }

  // Number of data bytes written to the underlying stream
  public long getDataOffset() {
    return (completeMetaDataBlocks - 1) * dataBlockSize
        + currentDataBlockSize;
  }

  @Override
  public void write(int b) throws IOException {
    writeMetaDataIfNeeded();
    out.write(b);
    currentDataBlockSize ++;
  }

  @Override
  public void write(byte[] b, int start, int length) throws IOException {
    while (length > 0) {
      writeMetaDataIfNeeded();
      int dataToWrite = Math.min(length, dataBlockSize - currentDataBlockSize);
      out.write(b, start, dataToWrite);
      start += dataToWrite;
      length -= dataToWrite;
      currentDataBlockSize += dataToWrite;
    }
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}

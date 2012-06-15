package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Seekable;

/**
 * This InputStream removes the metadata from the underlying stream.
 *
 * Note that skip(long) is optimized by calling Seekable.seek(long) if possible.
 */
public class InterleavedInputStream extends InputStream {

  public interface MetaDataConsumer {
    /**
     * This function should read a metadata block with size metaDataBlockSize.
     * This function should throw EOFException if there are not enough bytes
     * in the InputStream.
     * @param in  The raw input stream
     */
    void readMetaData(InputStream in, int metaDataBlockSize) throws IOException;
  }

  public static class DefaultMetaDataConsumer implements MetaDataConsumer {
    @Override
    public void readMetaData(InputStream in, int metaDataBlockSize)
        throws IOException {
      // Read in the whole MetaDataBlock and store it in a byte array.
      byte[] metaDataBlock = new byte[metaDataBlockSize];
      (new DataInputStream(in)).readFully(metaDataBlock);
    }
  }

  private final InputStream in;
  private final Seekable seekableIn;
  private final int metaDataBlockSize;
  private final int dataBlockSize;
  private final MetaDataConsumer metaDataConsumer;

  /**
   * The number of complete blocks (metadata + data) processed.
   */
  private long completeBlocks;
  /**
   * The raw offset inside the current block (metadata + data).
   * 0 <= rawBlockOffset < metaDataBlockSize + dataBlockSize
   */
  private int rawBlockOffset;
  /**
   * Whether we have reached EOF.
   */
  private boolean eofReached;


  public int getMetaDataBlockSize() {
    return metaDataBlockSize;
  }
  public int getDataBlockSize() {
    return dataBlockSize;
  }
  public int getCompleteBlockSize() {
    return metaDataBlockSize + dataBlockSize;
  }

  /**
   * This function sets the internal offset.
   * This should only be called by a subclass that is capable of
   * seeking the InputStream.
   */
  protected void setPosition(long completeBlocks, int rawBlockOffset) {
    this.completeBlocks = completeBlocks;
    this.rawBlockOffset = rawBlockOffset;
    this.eofReached = false;
  }

  public InterleavedInputStream(InputStream in,
      int metaDataBlockSize, int dataBlockSize,
      MetaDataConsumer metaDataConsumer) {
    this.in = in;
    this.seekableIn = (in instanceof Seekable) ? (Seekable)in : null;
    this.metaDataBlockSize = metaDataBlockSize;
    this.dataBlockSize = dataBlockSize;
    this.metaDataConsumer = metaDataConsumer;
    // Signal that we need to read metadata block first.
    eofReached = false;
  }

  /**
   * @param in  in.available() should return > 0 unless EOF
   */
  public InterleavedInputStream(InputStream in,
      int metaDataBlockSize, int dataBlockSize) {
    this(in, metaDataBlockSize, dataBlockSize, new DefaultMetaDataConsumer());
  }

  /**
   * Number of bytes read from the underlying stream.
   */
  public long getRawOffset() {
    return completeBlocks * getCompleteBlockSize()
      + rawBlockOffset;
  }

  /**
   * Number of data bytes read from the underlying stream.
   */
  public long getDataOffset() {
    return completeBlocks * dataBlockSize
        + Math.max(0, (rawBlockOffset - metaDataBlockSize));
  }

  /**
   * Returns whether we've reached EOF.
   */
  public boolean readMetaDataIfNeeded() throws IOException {
    if (eofReached) {
      return false;
    }
    if (rawBlockOffset == 0) {
      try {
        metaDataConsumer.readMetaData(in, metaDataBlockSize);
        rawBlockOffset += metaDataBlockSize;
      } catch (EOFException e) {
        eofReached = true;
        return false;
      }
    }
    return true;
  }

  private void moveForward(long bytes) {
    rawBlockOffset += bytes;
    if (rawBlockOffset == getCompleteBlockSize()) {
      completeBlocks ++;
      rawBlockOffset = 0;
    }
  }

  @Override
  public int read() throws IOException {
    if (!readMetaDataIfNeeded()) {
      return -1;
    }
    int result = in.read();
    if (result >= 0) {
      // don't do this if read() returns -1, which means EOF.
      moveForward(1);
    } else {
      eofReached = true;
    }
    return result;
  }

  /**
   * Note: When at the beginning of a metadata block, reading with length = 0 will
   * consume the MetaDataBlock.
   */
  @Override
  public int read(byte[] b, int start, int length) throws IOException {
    if (!readMetaDataIfNeeded()) {
      return -1;
    }
    int toRead = (int)Math.min(length, getCompleteBlockSize() - rawBlockOffset);
    int read = in.read(b, start, toRead);
    if (read >= 0) {
      moveForward(read);
    } else {
      eofReached = true;
    }
    return read;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  /**
   * Returns the number of bytes in the raw stream.
   */
  public long rawAvailable() throws IOException {
    return in.available();
  }

  private long rawOffsetToDataOffset(long rawSize) {
    long blocks = rawSize / getCompleteBlockSize();
    long rawLeft = rawSize % getCompleteBlockSize();
    return blocks * dataBlockSize + Math.max(rawLeft - metaDataBlockSize, 0);
  }

  private long dataOffsetToRawOffset(long pos) {
    long blocks = pos / getDataBlockSize();
    long left = pos % getDataBlockSize();
    long rawOffset = blocks * getCompleteBlockSize()
        + (left == 0 ? 0 : getMetaDataBlockSize() + left);
    return rawOffset;
  }

  protected long dataIncrementToRawIncrement(long bytes) {
    long targetRawOffset = dataOffsetToRawOffset(getDataOffset() + bytes);
    return targetRawOffset - getRawOffset();
  }

  /**
   * Note: bytes can be negative iff seekableIn != null.
   */
  @Override
  public long skip(long bytes) throws IOException {
    skipExactly(bytes);
    return bytes;
  }

  public void skipExactly(long bytes) throws IOException {
    rawSkip(dataIncrementToRawIncrement(bytes), false);
  }

  protected boolean seekToNewSource(long targetPos) throws IOException {
    long bytes = targetPos - getDataOffset();
    return rawSkip(dataIncrementToRawIncrement(bytes), true);
  }

  /**
   * Returns the amount of data bytes available to read.
   * This function depends on the underlying InputStream to tell
   * the actual available bytes for reading in available().
   */
  @Override
  public int available() throws IOException {
    int rawAvailable = in.available();

    return (int)(rawOffsetToDataOffset(rawBlockOffset + rawAvailable)
        - rawOffsetToDataOffset(rawBlockOffset));
  }

  /**
   * This function depends on the underlying
   * @param toNewSource  only useful when seekableIn != null.
   * @return when toNewSource is true, return whether we seeked to a new source.
   *         otherwise return true.
   */
  private boolean seekOrSkip(long bytes, boolean toNewSource) throws IOException {
    if (seekableIn != null) {
      // Use Seekable interface to speed up skip.
      int available = in.available();
      try {
        if (toNewSource) {
          return seekableIn.seekToNewSource(seekableIn.getPos() + bytes);
        } else {
          seekableIn.seek(seekableIn.getPos() + bytes);
          return true;
        }
      } catch (IOException e) {
        if (bytes > available && "Cannot seek after EOF".equals(e.getMessage())) {
          eofReached = true;
          throw new EOFException(e.getMessage());
        }
      }
    } else {
      // Do raw skip.
      long toSkip = bytes;
      while (toSkip > 0) {
        long skipped = in.skip(toSkip);
        if (skipped <= 0) {
          throw new EOFException("skip returned " + skipped);
        }
        toSkip -= skipped;
      };
    }
    return true;
  }

  private void setRawOffset(long rawOffset) {
    completeBlocks = rawOffset / getCompleteBlockSize();
    rawBlockOffset = (int)(rawOffset % getCompleteBlockSize());
  }

  /**
   * Skip some bytes from the raw InputStream.
   * @param  toNewSource - only useful when seekableIn is not null.
   * @return when toNewSource is true, return whether we seeked to a new source.
   *         otherwise return true.
   */
  protected boolean rawSkip(long bytes, boolean toNewSource) throws IOException {

    boolean result = seekOrSkip(bytes, toNewSource);
    setRawOffset(getRawOffset() + bytes);

    // Check validity
    if (rawBlockOffset > 0 && rawBlockOffset < metaDataBlockSize) {
      throw new IOException("Cannot jump into the middle of a MetaDataBlock. MetaDataBlockSize = "
          + metaDataBlockSize + " and we are at " + rawBlockOffset);
    }
    return result;
  }

  /**
   * This function is only for applications that needs to "fast-forward" to the
   * current end of the file.  Typically the file is growing and "available()"
   * returns the currently available bytes.
   *
   * Skip to the last "available" meta data block.
   * This is an estimate based on in.available().
   */
  public void skipToLastAvailableMetaDataBlock() throws IOException {
    long totalSize = getRawOffset() + rawAvailable();
    long blocks = (totalSize - getMetaDataBlockSize()) / getCompleteBlockSize();
    blocks = Math.max(0, blocks);

    long bytesToSkip = (blocks - completeBlocks) * getCompleteBlockSize()
        - rawBlockOffset;

    seekOrSkip(bytesToSkip, false);

    completeBlocks = blocks;
    rawBlockOffset = 0;
  }
}

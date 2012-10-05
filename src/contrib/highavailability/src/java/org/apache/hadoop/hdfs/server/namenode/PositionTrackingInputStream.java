package org.apache.hadoop.hdfs.server.namenode;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Stream wrapper that keeps track of the current stream position.
 */
public class PositionTrackingInputStream extends FilterInputStream {
  private long curPos = 0;
  private long markPos = -1;

  public PositionTrackingInputStream(InputStream is) {
    super(is);
  }
  
  public PositionTrackingInputStream(InputStream is, long position) {
    super(is);
    curPos = position;
  }

  public int read() throws IOException {
    int ret = super.read();
    if (ret != -1) curPos++;
    return ret;
  }

  public int read(byte[] data) throws IOException {
    int ret = super.read(data);
    if (ret > 0) curPos += ret;
    return ret;
  }

  public int read(byte[] data, int offset, int length) throws IOException {
    int ret = super.read(data, offset, length);
    if (ret > 0) curPos += ret;
    return ret;
  }

  public void mark(int limit) {
    super.mark(limit);
    markPos = curPos;
  }

  public void reset() throws IOException {
    if (markPos == -1) {
      throw new IOException("Not marked!");
    }
    super.reset();
    curPos = markPos;
    markPos = -1;
  }

  public long getPos() {
    return curPos;
  }
}

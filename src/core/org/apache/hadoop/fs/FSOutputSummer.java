/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.util.DataChecksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */

abstract public class FSOutputSummer extends OutputStream {
  // data checksum
  private Checksum sum;
  // internal buffer for storing data before it is checksumed
  private byte buf[];
  // internal buffer for storing checksum
  private byte checksum[];
  // The number of valid bytes in the buffer.
  private int count;
  // bytes already sent in current chunk.
  protected int bytesSentInChunk;
  
  protected final FsWriteProfile profileData;

  
  protected FSOutputSummer(Checksum sum, int maxChunkSize, int checksumSize,
      FsWriteProfile profileData) {
    this.sum = sum;
    this.buf = new byte[maxChunkSize];
    this.checksum = new byte[checksumSize];
    this.count = 0;
    this.profileData = profileData;
    this.bytesSentInChunk = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len</code>, and its checksum
   */
  protected abstract void writeChunk(byte[] b, int offset, int len, byte[] checksum)
  throws IOException;
  
  protected abstract boolean shouldKeepPartialChunkData() throws IOException;

  /** Write one byte */
  public synchronized void write(int b) throws IOException {
    eventStartWrite();

    try {
      sum.update(b);
      buf[count++] = (byte) b;
      if (bytesSentInChunk + count == buf.length) {
        flushBuffer(true, shouldKeepPartialChunkData());
      }
    } finally {
      eventEndWrite();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids uneccessary data copy.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  public synchronized void write(byte b[], int off, int len)
  throws IOException {
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    for (int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
    incMetrics(len);
  }
  
  /**
   * overrided in DFSOutputStream, to collect information for DFSClientMetrics
   */
  protected void incMetrics(int len) {}
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   */
  private int write1(byte b[], int off, int len) throws IOException {
    eventStartWrite();

    try {
      if(count==0 && bytesSentInChunk + len>=buf.length) {
        // local buffer is empty and user data can fill the current chunk
        // checksum and output data
        final int length = buf.length - bytesSentInChunk;
        sum.update(b, off, length);
        writeChecksumChunk(b, off, length, false);
        // start a new chunk
        bytesSentInChunk = 0;
        return length;
      }
    
      // copy user data to local buffer
      int bytesToCopy = buf.length - bytesSentInChunk - count;
      bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
      sum.update(b, off, bytesToCopy);
      System.arraycopy(b, off, buf, count, bytesToCopy);
      count += bytesToCopy;
      if (count + bytesSentInChunk == buf.length) {
        // local buffer is full
        flushBuffer(true, shouldKeepPartialChunkData());
      } 
      return bytesToCopy;
    } finally {
      eventEndWrite();
    }
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream.  If keep is true, then the state of 
   * this object remains intact.
   */
  protected synchronized void flushBuffer(boolean chunkComplete,
      boolean keepPartialData) throws IOException {
    if (count != 0) {
      int chunkLen = count;
      count = 0;
      writeChecksumChunk(buf, 0, chunkLen, keepPartialData && !chunkComplete);
      if (!chunkComplete) {
        if (!keepPartialData) {
          bytesSentInChunk += chunkLen;
        } else {
          count = chunkLen;
        }
      } else {
        bytesSentInChunk = 0;
      }
    }
  }
  
  /** Generate checksum for the data chunk and output data chunk & checksum
   * to the underlying output stream. If keep is true then keep the
   * current checksum intact, do not reset it.
   */
  private void writeChecksumChunk(byte b[], int off, int len, boolean keep)
  throws IOException {
    int tempChecksum = (int)sum.getValue();
    if (!keep) {
      sum.reset();
    }
    int2byte(tempChecksum, checksum);
    writeChunk(b, off, len, checksum);
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
    return int2byte((int)sum.getValue(), new byte[checksumSize]);
  }

  static byte[] int2byte(int integer, byte[] bytes) {
    DataChecksum.writeIntToBuf(integer, bytes, 0);
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void resetChecksumChunk() {
    sum.reset();
    this.count = 0;
  }
  
  protected void eventStartWrite() {
    if (profileData != null) {
      profileData.startWrite();
    }
  }
  
  protected void eventEndWrite() {
    if (profileData != null) {
      profileData.endWrite();
    }
  }
}

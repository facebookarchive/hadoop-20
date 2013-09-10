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
package org.apache.hadoop.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BufferedByteInputStream uses an underlying in-memory staging buffer for
 * reading bytes from the underlying input stream. The actual reading is done
 * by a separate thread.
 */
public class BufferedByteInputStream extends InputStream {

  static final Log LOG = LogFactory.getLog(BufferedByteInputStream.class
      .getName());
  
  // staging buffer used to store bytes read from underlying stream
  private final BufferedByteInputOutput buffer;
  
  // reader thread that reads bytes from underlying stream
  // and writes to the buffer
  private final ReadThread readThread;
  
  // actual input stream to read from
  private final InputStream underlyingInputStream;
  
  private boolean closed = false;
  
  /**
   * Wrap given input stream with BufferedByteInputOutput.
   * This is the only way to instantiate the buffered input stream.
   * @param is underlying input stream
   * @param bufferSize size of the in memory buffer
   * @param readBufferSize size of the buffer used for reading from is
   */
  public static DataInputStream wrapInputStream(InputStream is, int bufferSize,
      int readBufferSize) {
    // wrapping BufferedByteInputStream in BufferedInputStream decreases
    // pressure on BBIS internal locks, and we read from the BBIS in
    // bigger chunks
    return new DataInputStream(new BufferedInputStream(
        new BufferedByteInputStream(is, bufferSize, readBufferSize)));
  }

  /**
   * Construct BuffredByteInputStream
   * @param is underlying input stream
   * @param bufferSize size of the in-memory buffer
   * @param readBufferSize size of the transfer chunks 
   *        (from input stream to staging buffer)
   */
  private BufferedByteInputStream(InputStream is, int bufferSize,
      int readBufferSize) {
    buffer = new BufferedByteInputOutput(bufferSize);
    readThread = new ReadThread(is, buffer, readBufferSize);
    readThread.setDaemon(true);
    readThread.start();
    underlyingInputStream = is;
  }

  // override InputStream methods
  // each of the calls will fail after the stream is closed
  // after reading all bytes available in the buffer!
  
  public int read() throws IOException {
    return checkOutput(buffer.read());
  }

  public int read(byte[] buf) throws IOException {
    return checkOutput(buffer.read(buf, 0, buf.length));
  }

  public int read(byte[] buf, int off, int len) throws IOException {
    return checkOutput(buffer.read(buf, off, len));
  }
  
  /**
   * How many bytes are available in the buffer.
   * After closing this call will fail.
   */
  public int available() throws IOException {
    checkOutput(-1);
    return buffer.available();
  }
  
  /**
   * Close the input stream. Joins the thread and
   * closes the underlying input stream. Can be
   * called multiple times.
   */
  public void close() throws IOException {
    // multiple close should return with no errors
    // readThread will close underlying buffer
    readThread.close();
    try {
      readThread.join();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  /////////////////////////////////////
  
  /**
   * Check if the read thread died, if so, throw an exception.
   */
  private int checkOutput(int readBytes) throws IOException {
    if (readBytes > -1) {
      return readBytes;
    }
    if (closed) {
      throw new IOException("The stream has been closed");
    }
    if (readThread.error != null) {
      throw new IOException(readThread.error.getMessage());
    }
    return readBytes;
  }
  
  /**
   * Reader thread which will copy bytes from the underlying input stream
   * to the byte buffer.
   */
  private class ReadThread extends Thread implements Runnable {

    private final InputStream is;
    private final BufferedByteInputOutput buffer;
    private final int readBufferSize;
    
    volatile Throwable error = null;

    ReadThread(InputStream is, BufferedByteInputOutput os, int readBufferSize) {
      this.buffer = os;
      this.is = is;
      this.readBufferSize = readBufferSize;
    }

    /** 
     * Close input stream.
     * Closes the buffer and underlying input stream;
     */
    void close() throws IOException {
      closed = true;
      buffer.close();
      is.close();
    }

    public void run() {
      byte[] buf = new byte[readBufferSize];
      int bytesRead;
      try {
        while (((bytesRead = is.read(buf)) > 0)) {
          buffer.write(buf, 0, bytesRead);
        }
      } catch (Exception e) {
        // after closing we might get an error here
        // but we can ignore it
        if (!closed) {
          error = e;
          LOG.warn("Exception", e);
        }
      } finally {
        // close buffer and input stream
        buffer.close();
        try {
          is.close();
        } catch (IOException e) {
          error = e;
          LOG.error("Exception when closing underlying input stream", e);
        }
      }
    }
  }
}

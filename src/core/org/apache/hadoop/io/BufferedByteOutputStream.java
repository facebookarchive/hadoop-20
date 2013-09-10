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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionHandler;

/**
 * BufferedByteOutputStream uses an underlying in-memory buffer for
 * writing bytes to the underlying output stream. The actual writing is done
 * by a separate thread.
 */
public class BufferedByteOutputStream extends OutputStream {
  
  static final Log LOG = LogFactory.getLog(BufferedByteOutputStream.class.getName());
  
  //staging buffer used to store bytes to be written to underlying stream
  private final BufferedByteInputOutput buffer;
  
  // writer thread that reads bytes from the staging buffer
  // and writes to the underlying stream
  private final WriteThread writeThread;
  
  //actual output stream to write from
  private final OutputStream underlyingOutputStream;
  
  private boolean closed = false;
  
  
  /**
   * Wrap given output stream with BufferedByteInputOutput.
   * This is the only way to instantiate the buffered output stream.
   * @param os underlying output stream
   * @param bufferSize size of the in memory buffer
   * @param writeBufferSize size of the buffer used for writing to is
   */
  public static DataOutputStream wrapOutputStream(OutputStream os,
      int bufferSize, int writeBufferSize) {
    // wrapping BufferedByteOutputStream in BufferedOutputStream decreases
    // pressure on BBOS internal locks, and we read from the BBOS in
    // bigger chunks
    return new DataOutputStream(new BufferedOutputStream(
        new BufferedByteOutputStream(os, bufferSize, writeBufferSize)));
  }
  
  /**
   * Construct BuffredByteInputStream
   * @param os underlying output stream
   * @param bufferSize size of the in memory buffer
   * @param writeBufferSize size of the transfer chunks 
   *        (from staging buffer to output stream)
   */
  private BufferedByteOutputStream(OutputStream os, int bufferSize, int writeBufferSize) {
    buffer = new BufferedByteInputOutput(bufferSize);
    writeThread = new WriteThread(os, buffer, writeBufferSize);
    writeThread.setDaemon(true);
    writeThread.start();
    underlyingOutputStream = os;
  }
  
  /**
   * Close the output stream. Joins the thread and
   * closes the underlying output stream.
   */
  public void close() throws IOException {   
    if (closed) {
      checkWriteThread();
      return;
    }
    try {
      buffer.close(); 
      // writeThread should exit after this
      // and close underlying stream
      try {
        writeThread.join();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    } finally {
      checkWriteThread();
    }
  }
  
  /**
   * Waits until the buffer has been written to underlying stream.
   * Flushes the underlying stream.
   */
  public void flush() throws IOException {
    // check if the stream has been closed
    checkError();
    
    // how many bytes were written to the buffer
    long totalBytesWritten = buffer.totalWritten();
    
    // unblock reads from the buffer
    buffer.unblockReads();
    
    // wait until the write thread transfers everything from the
    // buffer to the stream
    while (writeThread.totalBytesTransferred < totalBytesWritten) {
      BufferedByteInputOutput.sleep(1);
    }
    
    InjectionHandler.processEvent(
        InjectionEventCore.BUFFEREDBYTEOUTPUTSTREAM_FLUSH,
        writeThread.totalBytesTransferred);
    
    // check error
    checkError();
    
    // block reads
    buffer.blockReads();
    
    // flush the underlying buffer
    underlyingOutputStream.flush();
  }
  
  //override OutputStream methods
  
  public void write(int b) throws IOException {
    checkError();
    buffer.write(b);
  }
  
  public void write(byte[] buf) throws IOException {
    checkError();
    buffer.write(buf, 0, buf.length);
  }
  
  public void write(byte[] buf, int off, int len) throws IOException {
    checkError();
    buffer.write(buf, off, len);
  }
  
  /**
   * Check if the read thread died, if so, throw an exception.
   */
  private void checkError() throws IOException {
    if (closed) {
      throw new IOException("The stream has been closed");
    }
    checkWriteThread();
  }
  
  private void checkWriteThread() throws IOException {
    if (writeThread.error != null) {
      throw new IOException(writeThread.error.getMessage());
    }
  }

  /////////////////////////////////////
  
  /**
   * Writer thread which will copy bytes from the byte buffer to the underlying
   * output stream.
   */
  private class WriteThread extends Thread implements Runnable {

    private final OutputStream os;
    private final BufferedByteInputOutput buffer;
    private final int writeBufferSize;
    
    volatile long totalBytesTransferred = 0;   
    volatile Throwable error = null;

    WriteThread(OutputStream os, BufferedByteInputOutput is, int writeBufferSize) {
      this.os = os;
      this.buffer = is;
      this.writeBufferSize = writeBufferSize;
    }

    public void run() {
      byte[] buf = new byte[writeBufferSize];
      int bytesRead;
      try {
        while ((bytesRead = buffer.read(buf, 0, buf.length)) >= 0) {
          if (bytesRead > 0) {
            os.write(buf, 0, bytesRead);
            totalBytesTransferred += bytesRead;
          }
        }
      } catch (Exception e) {
        error = e;
        LOG.warn("Exception", e);
      } finally {
        // close buffer and output stream
        buffer.close();
        try {
          os.close();
        } catch (Exception e) {
          LOG.error("Exception when closing underlying output stream", e);
        }
        closed = true;
      }
    }
  }
}

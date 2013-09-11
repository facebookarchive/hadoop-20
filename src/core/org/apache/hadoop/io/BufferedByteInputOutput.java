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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BufferedByteInputOutput implements a byte buffer with standard read and write 
 * operations for byte and byte[]. The buffer has a bounded capacity. The buffer
 * presents the abstraction of a blocking queue of bytes.
 */
public class BufferedByteInputOutput {
  
  static final Log LOG = LogFactory.getLog(BufferedByteInputOutput.class.getName());

  // bytes are stored in this buffer
  private final byte[] bytes;
  private final int length;


  private volatile int readCursor = 0;
  private volatile int writeCursor = 0;
  private final AtomicInteger availableCount = new AtomicInteger(0);
  
  private volatile long totalRead = 0;
  private volatile long totalWritten = 0;

  private volatile boolean closed = false;
  
  // if set the reads are non-blocking
  // only the byte[] reads are non blocking
  private volatile boolean nonBlockingRead = false;
  
  private final ReentrantLock lockR = new ReentrantLock();
  private final ReentrantLock lockW = new ReentrantLock();

  public BufferedByteInputOutput(int size) {
    bytes = new byte[size];
    length = bytes.length;
  }

  /**
   * Return number of available bytes to read.
   */
  public int available() {
    return availableCount.get();
  }
  
  /**
   * All available data is already in the buffer.
   * Reads will return -1.
   * Writes will fail.
   */
  public void close() {
    closed = true;
  }
  
  public boolean isClosed() {
    return closed;
  }
  
  /**
   * The reads will become non-blocking
   */
  public void unblockReads() {
    nonBlockingRead = true;
  }
  
  /**
   * The reads will become blocking
   */
  public void blockReads() {
    nonBlockingRead = false;
  }

  /**
   * Read a single byte.
   * Blocks until data is available, or fail if buffer is closed.
   */
  public int read() throws IOException {
    while (true) {
      lockR.lock();
      try {
        if (availableCount.get() > 0) {
          // as long as there is available data
          // serve it even if closed
          int b = bytes[readCursor] & 0xFF;
          incReadCursor(1);
          availableCount.decrementAndGet();
          totalRead++;
          return b;
        } else if (closed){
          // when no bytes are left and buffer is closed
          // return -1
          return -1;
        }
      } finally {
        lockR.unlock();
      }
      sleep(1);
    }
  }

  /**
   * Read data to the buffer, starting at offset off.
   * Will block until data available, or the input has been closed.
   */
  public int read(byte[] buf, int off, int len) throws IOException {
    while (true) {
      lockR.lock();
      try {
        int available = availableCount.get();
        if (available > 0) {
          // as long as there is available data
          // serve it even if closed
          final int lenToRead = Math.min(available, len);
          final int lenForward = Math.min(lenToRead, length - readCursor);
          final int lenRemaining = lenToRead - lenForward;
          
          // after readCursor
          if (lenForward > 0) {
            System.arraycopy(bytes, readCursor, buf, off, lenForward);
            incReadCursor(lenForward);
          }
          
          // before readCursor
          if (lenRemaining > 0) {
            System.arraycopy(bytes, 0, buf, off + lenForward, lenRemaining);
            incReadCursor(lenRemaining);
          } 
          availableCount.addAndGet(-1 * lenToRead);
          totalRead += lenToRead;
          return lenToRead;
        } else if (nonBlockingRead) {
          // we do not serve any bytes
          return 0;
        } else if (closed) {
          // when no bytes are left and buffer is closed
          // return -1
          return -1;
        }
      } finally {
        lockR.unlock();
      }
      sleep(1);
    }
  }

  /**
   * Write buf to the buffer, will block until it can write len bytes.
   * Will fail if buffer is closed.
   */
  public void write(byte[] buf, int off, int len) throws IOException {
    while (true) {
      lockW.lock();
      try {
        // fail if closed
        checkClosed();
        
        final int lenToWrite = Math.min(len, length - availableCount.get());
        final int lenForward = Math.min(lenToWrite, length - writeCursor);
        final int lenRemaining = lenToWrite - lenForward;
        
        // after writeCursor
        if (lenForward > 0) {
          System.arraycopy(buf, off, bytes, writeCursor, lenForward);
          incWriteCursor(lenForward);
        }
        
        // before writeCursor
        if (lenRemaining > 0) {
          System.arraycopy(buf, off + lenForward, bytes, 0, lenRemaining);
          incWriteCursor(lenRemaining);
        } 
        
        availableCount.addAndGet(lenToWrite);
        totalWritten += lenToWrite;
        
        // modify offset and len for next iteration
        off += lenToWrite;
        len -= lenToWrite;        
        if (len == 0) {
          return;
        }
      } finally {
        lockW.unlock();
      }
      sleep(1);
    }
  }

  /**
   * Write a single byte to the buffer.
   * Will block until the byte can be written or fail when buffer is closed.
   */
  public void write(int b) throws IOException {
    while (true) {
      lockW.lock();
      try {
        // fail if closed
        checkClosed();
        
        if (length - availableCount.get() > 0) {
          bytes[writeCursor] = (byte) b;
          incWriteCursor(1);
          availableCount.incrementAndGet();
          totalWritten++;
          return;
        }
      } finally {
        lockW.unlock();
      }
      sleep(1);
    }
  }
  
  /**
   * Get total number of bytes read from this buffer.
   */
  public long totalRead() {
    return totalRead;
  }
  
  /**
   * Get total number of bytes written to this buffer.
   */
  public long totalWritten() {
    return totalWritten;
  }

  // ////////////////////// helpers

  static void sleep(long n) {
    try {
      Thread.sleep(n);
    } catch (InterruptedException e) {
      LOG.warn("Thread interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  // inc read cursor
  private void incReadCursor(int inc) {
    readCursor = (readCursor + inc) % length;
  }
  
  // inc write cursor
  private void incWriteCursor(int inc) {
    writeCursor = (writeCursor + inc) % length;
  }
  
  /**
   * If the output was closed, no more writes will be accepted.
   */
  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException("Buffer has been closed.");
    }
  }
}

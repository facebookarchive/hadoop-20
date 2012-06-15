/*
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

package org.apache.hadoop.io.compress.lzma;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Compressor} based on the lzma algorithm.
 * http://www.7zip.org
 * 
 */
public class LzmaCompressor implements Compressor {
  private static final Log LOG = 
    LogFactory.getLog(LzmaCompressor.class.getName());

  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  private static final int DEFAULT_COMPRESS_LEVEL = 7;
  
  // HACK - Use this as a global lock in the JNI layer
  private static Class clazz = LzmaCompressor.class;
  
  private long stream;
  private int level;
  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private Buffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufOff = 0, uncompressedDirectBufLen = 0;
  private Buffer compressedDirectBuf = null;
  private boolean finish, finished;

  private static boolean nativeLzmaLoaded = true;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      try {
        initIDs();
        nativeLzmaLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to load/initialize native-lzma
      	LOG.error("initIDs() failed in LzmaCompressor..." + t);
      	LOG.error(t.getCause());
      	LOG.error(t.getMessage());
        nativeLzmaLoaded = false;
      }
    } else {
      LOG.error("Cannot load " + LzmaCompressor.class.getName() + 
                " without native-hadoop library!");
      nativeLzmaLoaded = false;
     }
   }
  
  /**
   * Check if lzma compressors are loaded and initialized.
   * 
   * @return <code>true</code> if lzma compressors are loaded & initialized,
   *         else <code>false</code> 
   */
  public static boolean isNativeLzmaLoaded() {
    return nativeLzmaLoaded;
  }

  /** 
   * Creates a new compressor using the specified level {@link CompressionLevel}.
   * 
   * @param level lzma compression algorithm to use
   * @param directBufferSize size of the direct buffer to be used.
   */
  public LzmaCompressor(int compress_level, int directBufferSize) {
    this.level = compress_level;
    this.directBufferSize = directBufferSize;
    
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    
    stream = init(this.level);
  }
  
  /**
   * Creates a new compressor with the default compression level.
   */
  public LzmaCompressor() {
    this(DEFAULT_COMPRESS_LEVEL, 
         DEFAULT_DIRECT_BUFFER_SIZE);	
  }
  
  public synchronized void setInput(byte[] b, int off, int len) {
    if (b== null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    setInputFromSavedData();
    
    // Reinitialize lzma's output direct buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData() {
    uncompressedDirectBufOff = 0;
    uncompressedDirectBufLen = userBufLen;
    if (uncompressedDirectBufLen > directBufferSize) {
      uncompressedDirectBufLen = directBufferSize;
    }

    // Reinitialize lzma's input direct buffer
    uncompressedDirectBuf.rewind();
    ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff,  
                                            uncompressedDirectBufLen);

    // Note how much data is being fed to lzma
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  public synchronized void setDictionary(byte[] b, int off, int len) {
    // nop
  }

  /** {@inheritDoc} */
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if lzma has consumed all input
    if (uncompressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  public synchronized void finish() {
    finish = true;
  }
  
  public synchronized boolean finished() {
    // Check if 'lzma' says its 'finished' and
    // all compressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0); 
  }

  public synchronized int compress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    int n = 0;
    
    // Check if there is compressed data
    n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize the lzma's output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = compressBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  public synchronized long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  public synchronized long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  public synchronized void reset() {
    checkStream();    
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
    end();
    stream = init(this.level);
  }
  
  public synchronized void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs();
  private native static long init(int level);
  private native int compressBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static void end(long strm);  
}

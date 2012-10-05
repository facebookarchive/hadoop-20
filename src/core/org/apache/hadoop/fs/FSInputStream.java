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

import java.io.*;
import java.util.List;
import java.util.LinkedList;
import java.nio.ByteBuffer;

/****************************************************************
 * FSInputStream is a generic old InputStream with a little bit
 * of RAF-style seek ability.
 *
 *****************************************************************/
public abstract class FSInputStream extends InputStream
    implements Seekable, PositionedReadable {
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Return the current offset from the start of the file
   */
  public abstract long getPos() throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if 
   * found a new source, false otherwise.
   */
  public abstract boolean seekToNewSource(long targetPos) throws IOException;

  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    synchronized (this) {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }
    
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position+nread, buffer, offset+nread, length-nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
  }
    
  public void readFully(long position, byte[] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  /**
   * This can be optimized to avoid buffer copies. It allows
   * the underlying system to  return a list of ByteBuffers that
   * contain the data.
   * A FileSystem implementation can override this method to make it
   * avoid redundant memory copies.
   */
  public List<ByteBuffer> readFullyScatterGather(long position, int length)
    throws IOException {
    byte[] buf = new byte[length];
    readFully(position, buf, 0, length);
    LinkedList<ByteBuffer> results = new LinkedList<ByteBuffer>();
    results.add(ByteBuffer.wrap(buf, 0, length));
    return results;
  }
}

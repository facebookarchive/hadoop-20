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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import com.google.common.base.Joiner;

/**
 * Output stream for uploading image to journal nodes. The stream maintains
 * http-based channels to each of the nodes. It extends OutputStream, so it can
 * be directly used for writing the image.
 */
public class HttpImageUploadStream extends OutputStream {
  static final Log LOG = LogFactory.getLog(HttpImageUploadStream.class);

  private final String journalId;
  private final long txid;

  private final List<HttpImageUploadChannel> uploadChannels 
    = new ArrayList<HttpImageUploadChannel>();

  private ByteArrayOutputStream buffer;
  private int flushSize;

  public HttpImageUploadStream(List<String> uris, String journalId,
      NamespaceInfo nsinfo, long txid, long epoch, int flushSize,
      int maxBufferedChunks) throws IOException {

    // create 20% larger buffer to minimize chances of expanding it
    this.flushSize = flushSize;
    this.buffer = new ByteArrayOutputStream((int) (1.2 * flushSize));

    this.journalId = journalId;
    this.txid = txid;

    if (uris.size() % 2 == 0) {
      throwIOException("The number of upload channels should not be even: "
          + uris.size());
    }

    for (String uri : uris) {
      HttpImageUploadChannel ch = new HttpImageUploadChannel(uri, journalId,
          nsinfo, txid, epoch, maxBufferedChunks);
      uploadChannels.add(ch);
    }

    // initialize upload for each channel
    for (HttpImageUploadChannel ch : uploadChannels) {
      ch.start();
    }
    checkState();
  }

  public String toString() {
    return "ImageUploadStream to [" + Joiner.on(",").join(uploadChannels)
        + "], for journal: " + journalId + ", txid= " + txid;
  }

  /**
   * At each operation, this function is used to ensure that we still have a
   * majority of successful channels to which we can write.
   */
  void checkState() throws IOException {
    int majority = getMajoritySize();
    int numDisabled = 0;
    for (HttpImageUploadChannel ch : uploadChannels) {
      numDisabled += ch.isDisabled() ? 1 : 0;
    }
    if (numDisabled >= majority) {
      Map<HttpImageUploadChannel, Void> successes 
        = new HashMap<HttpImageUploadChannel, Void>();
      Map<HttpImageUploadChannel, Throwable> exceptions 
        = new HashMap<HttpImageUploadChannel, Throwable>();
      for (HttpImageUploadChannel ch : uploadChannels) {
        if (ch.isDisabled()) {
          exceptions.put(ch, ch.getErrorStatus());
        } else {
          successes.put(ch, null);
        }
      }
      throw QuorumException.create("Failed when uploading", successes,
          exceptions);
    }
  }

  /**
   * Checks if the size of the temporary buffer exceeds maximum allowed size. If
   * so, send the current buffer to the upload channels, and allocate new
   * buffer.
   */
  void checkBuffer() throws IOException {
    if (buffer.size() >= flushSize) {
      flushBuffer(false);
    }
    // check if we can continue
    checkState();
  }

  /**
   * Flushes the buffer to the upload channels and allocates a new buffer.
   */
  private void flushBuffer(boolean close) {
    for (HttpImageUploadChannel ch : uploadChannels) {
      ch.send(buffer);
    }
    if (!close) {
      // allocate new buffer, since the old one is used by the channels for
      // reading
      buffer = new ByteArrayOutputStream((int) (1.2 * flushSize));
    }
  }

  /**
   * @return the number of nodes which are required to obtain a quorum.
   */
  int getMajoritySize() {
    return uploadChannels.size() / 2 + 1;
  }

  static void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }

  // OutputStream methods

  @Override
  public void write(byte[] b) throws IOException {
    buffer.write(b);
    checkBuffer();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    buffer.write(b, off, len);
    checkBuffer();
  }

  @Override
  public void write(int b) throws IOException {
    buffer.write(b);
    checkBuffer();
  }

  @Override
  public void close() throws IOException {
    // flush whatever we have left
    flushBuffer(true);
    for (HttpImageUploadChannel ch : uploadChannels) {
      ch.close();
    }
    // check to see it the we have the quorum
    checkState();
  }

  @Override
  public void flush() throws IOException {
    // send the buffer through the channels
    flushBuffer(false);
    // ensure that we still have a quorum
    checkState();
  }
}

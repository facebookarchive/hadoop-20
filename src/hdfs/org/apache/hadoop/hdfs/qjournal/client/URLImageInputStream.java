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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.io.MD5Hash;

/**
 * URLImageInputStream wraps a http input stream for an image from a journal
 * node. The class has the functionality of creating the http connection to the
 * journal node.
 */
public class URLImageInputStream extends InputStream {
  private final URL url;
  private final int httpTimeout;

  private InputStream inputStream;
  private MD5Hash digest;
  private long advertisedSize;
  private volatile boolean initialized = false;

  /**
   * Create image input for given URL.
   */
  public URLImageInputStream(AsyncLogger logger, long txid, int httpTimeout)
      throws IOException {
    this.url = logger.buildURLToFetchImage(txid);
    this.httpTimeout = httpTimeout;
    // setup the stream
    setupInputStream();
  }

  /**
   * Get input stream for the image through http connection.
   */
  private void setupInputStream() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    // set timeout for connecting and reading
    connection.setConnectTimeout(httpTimeout);
    connection.setReadTimeout(httpTimeout);

    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Fetch of " + url + " failed with status code "
          + connection.getResponseCode() + "\nResponse message:\n"
          + connection.getResponseMessage());
    }

    String contentLength = connection
        .getHeaderField(TransferFsImage.CONTENT_LENGTH);
    if (contentLength != null) {
      // store image size
      advertisedSize = Long.parseLong(contentLength);
      if (advertisedSize <= 0) {
        throw new IOException("Invalid " + TransferFsImage.CONTENT_LENGTH
            + " header: " + contentLength);
      }
    } else {
      throw new IOException(TransferFsImage.CONTENT_LENGTH
          + " header is not provided " + "by the server when trying to fetch "
          + url);
    }

    // get the digest
    digest = TransferFsImage.parseMD5Header(connection);
    if (digest == null) {
      // digest must be provided, otherwise the image is not valid
      throw new IOException("Image digest not provided for url: " + url);
    }

    // get the input stream directly from the connection
    inputStream = connection.getInputStream();
    initialized = true;
  }

  /**
   * Get image md5 digest advertised by the server.
   */
  public MD5Hash getImageDigest() {
    return digest;
  }

  /**
   * Get size of the image advertised by the server.
   */
  public long getSize() throws IOException {
    init();
    return advertisedSize;
  }

  @Override
  public String toString() {
    return "QJM Image: " + url;
  }

  private synchronized void init() throws IOException {
    if (!initialized) {
      setupInputStream();
    }
  }

  // InputStream methods
  @Override
  public int available() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read() throws IOException {
    init();
    return inputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    init();
    return inputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    init();
    return inputStream.read(b, off, len);
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public long skip(long n) {
    throw new UnsupportedOperationException("Not supported");
  }
}

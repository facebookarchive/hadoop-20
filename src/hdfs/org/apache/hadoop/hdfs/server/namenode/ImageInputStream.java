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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.InputStream;

import org.apache.hadoop.io.MD5Hash;

/**
 * Wrapper class used for loading images to the namesystem. It contains
 * information about the md5 digest, size and the string representation of the
 * underlying image location.
 */
public class ImageInputStream {

  final long txid;
  final InputStream inputStream;
  MD5Hash digest;
  final String name;
  final long size;

  public ImageInputStream(long txid, InputStream inputStream, MD5Hash digest,
      String name, long size) {
    this.txid = txid;
    this.inputStream = inputStream;
    this.digest = digest;
    this.name = name;
    this.size = size;
  }

  /**
   * Get size of the image stored in the underlying location.
   */
  public long getSize() {
    return size;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * Get input stream for reading.
   */
  public InputStream getInputStream() {
    return inputStream;
  }

  /**
   * Update image digest for this image.
   */
  public void setImageDigest(MD5Hash digest) {
    this.digest = digest;
  }

  /**
   * Return digest associated with this image location.
   */
  public MD5Hash getDigest() {
    return digest;
  }
}

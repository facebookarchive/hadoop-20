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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto;

import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Additional utilities for type-safe reading and writing of Writables to
 * byte arrays
 * @see Writable
 */
public class WritableUtil {

  /**
   * Write a writable to a byte array, e.g., to set metadata in a ZNode
   * @param writable The writable object to write
   * @return Writable object serialized to be a byte array
   * @throws IOException If there is an error serializable the writable
   */
  public static <T extends Writable> byte[] writableToByteArray(T writable)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    writable.write(dos);
    dos.flush();
    return bos.toByteArray();
  }

  /**
   * Read a writable from a byte array, e.g., to parse metadata from a ZNode
   * @param bytes The byte array to read into a writable
   * @param writable Writable object to read into
   * @throws IOException If there an error de-serializing the byte array into
   *                     the writable object
   */
  public static <T extends Writable> T readWritableFromByteArray(
      byte[] bytes, T writable) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bis);
    writable.readFields(dis);
    return writable;
  }
}

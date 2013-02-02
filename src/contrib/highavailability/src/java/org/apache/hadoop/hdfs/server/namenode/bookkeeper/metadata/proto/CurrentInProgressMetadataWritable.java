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

import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.CurrentInProgressMetadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable representation for {@link CurrentInProgressMetadata}
 */
public class CurrentInProgressMetadataWritable implements Writable {

  private String id;
  private String path;

  /**
   * Set the id and path fields
   * @param id A unique identifier for the caller's process
   * @param path The path to the ZNode holding the metadata for the
   *             ledger containing the current in-progress segment
   */
  public void set(String id, String path) {
    this.id = id;
    this.path = path;
  }

  /**
   * Returns the id of the process that wrote this data
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the path to the ZNode holding the metadata for the
   * ledger containing the current in-progress segment
   */
  public String getPath() {
    return path;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeStringOpt(out, id);
    Text.writeStringOpt(out, path);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = Text.readStringOpt(in);
    path = Text.readStringOpt(in);
  }
}

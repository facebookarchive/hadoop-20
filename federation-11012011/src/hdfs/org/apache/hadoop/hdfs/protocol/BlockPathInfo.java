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
package org.apache.hadoop.hdfs.protocol;

import java.io.*;

import org.apache.hadoop.io.*;

/**
 * Path information for a block. This includes the
 * blocks metadata including the full pathname of the block on
 * the local file system.
 */
public class BlockPathInfo extends Block {
  static final WritableFactory FACTORY = new WritableFactory() {
    public Writable newInstance() { return new BlockPathInfo(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(BlockPathInfo.class, FACTORY);
  }

  private String localBlockPath = "";  // local file storing the data
  private String localMetaPath = "";   // local file storing the checksum

  public BlockPathInfo() {}

  public BlockPathInfo(Block b, String file, String metafile) {
    super(b);
    this.localBlockPath = file;
    this.localMetaPath = metafile;
  }

  public String getBlockPath() {return localBlockPath;}
  public String getMetaPath() {return localMetaPath;}

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, localBlockPath);
    Text.writeString(out, localMetaPath);
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    localBlockPath = Text.readString(in);
    localMetaPath = Text.readString(in);
  }
}

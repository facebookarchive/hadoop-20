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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

/**
 * The interface for a block replica to be read.
 */
public interface ReplicaToRead {
  /**
   * @return file path on the local disk to read. NULL if the block is not ready
   *         to be read (for now it means it's a block still being transferring
   *         from another data node or another block)
   */
  public File getDataFileToRead();

  /**
   * @return The length of the bytes available for clients to read. (data have
   *         been saved in all replicas in the pipeline). For finalized blocks,
   *         the value returned will be the same as file size on disk
   *         (getLengthWritten).
   * @throws IOException
   */
  public long getBytesVisible() throws IOException;

  /**
   * @return The length of the bytes flushed to local file of current machine.
   *         For finalized blocks, it will be the same as bytes available to
   *         read (getLengthVisible()).
   * @throws IOException
   */
  public long getBytesWritten() throws IOException;

  public boolean isInlineChecksum();
  
  public int getChecksumType();

  public int getBytesPerChecksum();

  InputStream getBlockInputStream(DataNode datanode, long offset)
      throws IOException;
  
  public boolean isFinalized();

  boolean hasBlockCrcInfo();

  int getBlockCrc() throws IOException;

  BlockDataFile getBlockDataFile() throws IOException;
}

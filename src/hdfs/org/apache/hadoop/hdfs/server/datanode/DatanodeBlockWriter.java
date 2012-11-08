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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.DataChecksum;

/**
 * Base class for block writing in data node. It reads input packet
 * buffer and write data and checksum to local files.
 *
 */
abstract class DatanodeBlockWriter implements java.io.Closeable {
  static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  protected int bytesPerChecksum;
  protected int checksumSize;
  protected Block block; // the block to receive
  protected String inAddr;
  protected int namespaceId;
  protected DataNode datanode = null;

  public abstract void initializeStreams(int bytesPerChecksum,
      int checksumSize, Block block, String inAddr, int namespaceId,
      DataNode datanode) throws FileNotFoundException, IOException;
  
  protected void setParameters(int bytesPerChecksum, int checksumSize,
      Block block, String inAddr, int namespaceId, DataNode datanode) {
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumSize = checksumSize;
    this.block = block;
    this.inAddr = inAddr;
    this.namespaceId = namespaceId;
    this.datanode = datanode;
  }

  /**
   * Write out checksum header
   * 
   * @param checksum
   * @throws IOException
   */
  public abstract void writeHeader(DataChecksum checksum) throws IOException;

  /**
   * Write the data in the packet buffer to local disk.
   * 
   * Input format:
   * Both of data and checksum are in pktBuf.
   * data has len bytes and are from dataOff.
   * checksums are from checksumOff, followed by checksums of chunks. 
   * 
   * @param pktBuf        packet buffer
   * @param len           length of data in the buffer
   * @param dataOff       starting data offset in pktBuf
   * @param checksumOff   starting checksum offset in pktBuf
   * @param numChunks     number of chunks expected in the pktBuf
   * @throws IOException
   */
  public abstract void writePacket(byte pktBuf[], int len, int dataOff,
      int checksumOff, int numChunks) throws IOException;

  public abstract void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock,
      DataChecksum checksum) throws IOException;

  public abstract void flush(boolean forceSync) throws IOException;
}

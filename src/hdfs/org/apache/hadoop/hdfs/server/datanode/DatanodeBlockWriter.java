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

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.InjectionHandler;

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
  
  protected int partialCrcInt;
  protected int firstChunkOffset;

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
      int pktBufStartOff, int numChunks, int packetVersion) throws IOException;

  public abstract void setPosAndRecomputeChecksumIfNeeded(long offsetInBlock,
      DataChecksum checksum) throws IOException;

  public abstract void flush(boolean forceSync) throws IOException;

  /**
   * Issue a file range sync with the last bytes of the data stream it is
   * writing to
   * 
   * @param lastBytesToSync
   *          the number of bytes to sync in the end of the block. It's counted
   *          as block sizes instead of bytes on disk, for inline checksum, it
   *          may translate to more bytes to sync.
   * @param flags
   *          the flags for sync_file_range
   * @throws IOException
   */
  public abstract void fileRangeSync(long lastBytesToSync, int flags)
      throws IOException;

  public abstract void fadviseStream(int advise, long offset, long len)
      throws IOException;

  public abstract void fadviseStream(int advise, long offset, long len,
      boolean sync) throws IOException;

  /**
   * Sets the posix fadvise for the caching behavior required from block after
   * we finalize the block.
   * 
   * @param fadvise
   *          the type of advice
   * @throws IOException
   */
  public abstract void close(int fadvise) throws IOException;
}

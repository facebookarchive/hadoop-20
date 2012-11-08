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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

/** This is a block reader for fast reads. It tries to reduce
 * the number of diskops needed to satisfy a read request.
 */
class BlockReaderAccelerator implements java.io.Closeable {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  private Configuration conf;
  private InetSocketAddress targetAddress;
  private DatanodeInfo datanodeInfo;
  private int dataTransferVersion;
  private int namespaceId;
  private String clientName;
  private  Socket sock;
  private  String hdfsfile; 
  private  LocatedBlock blk;
  private  long startOffset; 
  private  long length;
  private  boolean verifyChecksum;
  private  DFSClientMetrics metrics;
  private DataInputStream in;
  private Checksum checker;
  private int bytesPerChecksum;
  private byte[] cksumBuffer;
  private byte[] dataBuffer;
  private ByteBuffer byteBuffer;
  
  /**
   * object constructor
   */
  public BlockReaderAccelerator(
    Configuration conf,
    InetSocketAddress targetAddress,
    DatanodeInfo chosenNode, 
    int dataTransferVersion,
    int namespaceId,
    String clientName,
    Socket sock,
    String hdfsfile, 
    LocatedBlock blk,
    long startOffset, 
    long length,
    boolean verifyChecksum,
    DFSClientMetrics metrics) throws IOException {

    this.conf = conf;
    this.targetAddress = targetAddress;
    this.datanodeInfo = chosenNode;
    this.dataTransferVersion = dataTransferVersion;
    this.namespaceId = namespaceId;
    this.clientName = clientName;
    this.sock = sock;
    this.hdfsfile = hdfsfile;
    this.blk = blk;
    this.startOffset = startOffset;
    this.length = length;
    this.verifyChecksum = verifyChecksum;
    this.metrics = metrics;

     // create a checksum checker
     if (this.verifyChecksum) {
       this.checker = new PureJavaCrc32();
     }
  }
  
  /**
   * Return all the data [startOffset , length] in one shot!
   */
  public ByteBuffer readAll() throws IOException {

    // in and out will be closed when sock is closed (by the caller)
    DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT)));

    //write the header.
    ReadBlockAccelaratorHeader readBlockAccelaratorHeader =
        new ReadBlockAccelaratorHeader(dataTransferVersion, namespaceId,
            blk.getBlock().getBlockId(), blk.getBlock().getGenerationStamp(),
            startOffset, length, clientName);
    readBlockAccelaratorHeader.writeVersionAndOpCode(out);
    readBlockAccelaratorHeader.write(out);
    out.flush();
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockReaderAccelerator client blkid " + blk.getBlock().getBlockId() +
                " offset " + startOffset + " length " + length);
    }

    in = new DataInputStream(NetUtils.getInputStream(sock));

    // read the checksum header. 
    // 1 byte of checksum type and 4 bytes of bytes-per-checksum
    byte[] cksumHeader = new byte[DataChecksum.HEADER_LEN];
    in.readFully(cksumHeader);
    DataChecksum dsum = DataChecksum.newDataChecksum(cksumHeader, 0);
    this.bytesPerChecksum = dsum.getBytesPerChecksum();

    // align the startOffset with the previous crc chunk
    long delta = startOffset % bytesPerChecksum;
    long newOffset = startOffset - delta;
    long newlength = length + delta;
 
    // align the length to encompass the entire last checksum chunk
    long del = newlength % bytesPerChecksum;
    if (del != 0) {
      del = bytesPerChecksum - del;
      newlength += del;
    }

    // find the number of checksum chunks
    long numChunks = newlength / bytesPerChecksum;
    long sizeChecksumData = numChunks * dsum.getChecksumSize();

    // read in all checksums and data in one shot.
    this.dataBuffer = new byte[(int)newlength + (int)sizeChecksumData];
    in.readFully(dataBuffer);
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockReaderAccelerator client read in " + dataBuffer.length + 
                " bytes.");
    }

    // verify checksums of all chunks
    if (this.verifyChecksum) {
      for (int i = 0; i < numChunks; i++) {
        long dataOffset = sizeChecksumData + i * bytesPerChecksum;
        checker.reset();
        checker.update(dataBuffer, (int)dataOffset, bytesPerChecksum);

        int ckOffset = i * dsum.getChecksumSize();
        long expected = FSInputChecker.checksum2long(dataBuffer, ckOffset, 
                         dsum.getChecksumSize());

        if (expected != checker.getValue()) {
          String msg = "Checksum failure for file " + hdfsfile +
                       " block " + blk.getBlock() +
                       " at blockoffet " + (startOffset + i * bytesPerChecksum) +
                       " chunk " + i +
                       " expected " + expected +
                       " got " + checker.getValue();
          LOG.warn(msg);
          throw new ChecksumException(msg, startOffset + i * bytesPerChecksum);
        }
      }
    }
    // The offset in the ByteBuffer skips over the
    // portion that stores the checksums. It also skips over the additional
    // data portion that was read while aligning with the previous chunk boundary
    return ByteBuffer.wrap(dataBuffer, 
                           (int)(sizeChecksumData + delta),
                           (int)length);
  }

  /**
   * Close all open resources, if any
   */
  public void close() throws IOException {
  }
}


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
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
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
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

/** This is a local block reader. if the DFS client is on
 * the same machine as the datanode, then the client can read
 * files directly from the local file system rathen than going
 * thorugh the datanode. This improves performance dramatically.
 */
public class BlockReaderLocal extends BlockReader {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  private Configuration conf;
  private long length;
  private BlockPathInfo pathinfo;
  private FileInputStream dataIn;  // reader for the data file
  private FileInputStream checksumIn;
  private boolean clearOsBuffer;
  private DFSClientMetrics metrics;
  
  static private volatile ProtocolProxy<ClientDatanodeProtocol> datanode;
  static private final LRUCache<Block, BlockPathInfo> cache = 
    new LRUCache<Block, BlockPathInfo>(10000);
  static private final Path src = new Path("/BlockReaderLocal:localfile");
  
  /**
   * The only way this object can be instantiated.
   */
  public static BlockReaderLocal newBlockReader(Configuration conf,
    String file, int namespaceid, Block blk, DatanodeInfo node, 
    long startOffset, long length,
    DFSClientMetrics metrics, boolean verifyChecksum,
    boolean clearOsBuffer) throws IOException {
    // check in cache first
    BlockPathInfo pathinfo = cache.get(blk);

    if (pathinfo == null) {
      // cache the connection to the local data for eternity.
      if (datanode == null) {
        datanode = DFSClient.createClientDNProtocolProxy(node, conf, 0);
      }
      // make RPC to local datanode to find local pathnames of blocks
      if (datanode.isMethodSupported("getBlockPathInfo", int.class, Block.class)) {
        pathinfo = datanode.getProxy().getBlockPathInfo(namespaceid, blk);
      } else {
        pathinfo = datanode.getProxy().getBlockPathInfo(blk);
      }
      if (pathinfo != null) {
        cache.put(blk, pathinfo);
      }
    }
    
    // check to see if the file exists. It may so happen that the
    // HDFS file has been deleted and this block-lookup is occuring
    // on behalf of a new HDFS file. This time, the block file could
    // be residing in a different portion of the fs.data.dir directory.
    // In this case, we remove this entry from the cache. The next
    // call to this method will repopulate the cache.
    try {

      // get a local file system
      File blkfile = new File(pathinfo.getBlockPath());
      FileInputStream dataIn = new FileInputStream(blkfile);
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("New BlockReaderLocal for file " +
                  blkfile + " of size " + blkfile.length() +
                  " startOffset " + startOffset +
                  " length " + length);
      }

      if (verifyChecksum) {
      
        // get the metadata file
        File metafile = new File(pathinfo.getMetaPath());
        FileInputStream checksumIn = new FileInputStream(metafile);
    
        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(new DataInputStream(checksumIn), new PureJavaCrc32());
        short version = header.getVersion();
      
        if (version != FSDataset.METADATA_VERSION) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + blk + " ignoring ...");
        }
        DataChecksum checksum = header.getChecksum();

        return new BlockReaderLocal(conf, file, blk, startOffset, length,
            pathinfo, metrics, checksum, verifyChecksum, dataIn, checksumIn,
            clearOsBuffer);
      }
      else {
        return new BlockReaderLocal(conf, file, blk, startOffset, length,
            pathinfo, metrics, dataIn, clearOsBuffer);
      }
      
    } catch (FileNotFoundException e) {
      cache.remove(blk);    // remove from cache
      DFSClient.LOG.warn("BlockReaderLoca: Removing " + blk +
                         " from cache because local file " +
                         pathinfo.getBlockPath() + 
                         " could not be opened.");
      throw e;
    }
  }

  private BlockReaderLocal(Configuration conf, String hdfsfile, Block block,      
                          long startOffset, long length,
                          BlockPathInfo pathinfo, DFSClientMetrics metrics,
                          FileInputStream dataIn, boolean clearOsBuffer)
                          throws IOException {
    super(
        src, // dummy path, avoid constructing a Path object dynamically
        1);
    
    this.pathinfo = pathinfo;
    this.startOffset = startOffset;
    this.length = length;    
    this.metrics = metrics;
    this.dataIn = dataIn;
    this.clearOsBuffer = clearOsBuffer;
    
    dataIn.skip(startOffset);
  }
  
  private BlockReaderLocal(Configuration conf, String hdfsfile, Block block,      
                          long startOffset, long length,
                          BlockPathInfo pathinfo, DFSClientMetrics metrics,
                          DataChecksum checksum, boolean verifyChecksum,
                          FileInputStream dataIn, FileInputStream checksumIn,
                          boolean clearOsBuffer)
                          throws IOException {
    super(
        src, // dummy path, avoid constructing a Path object dynamically
        1, 
        checksum,
        verifyChecksum);

    this.pathinfo = pathinfo;
    this.startOffset = startOffset;
    this.length = length;    
    this.metrics = metrics;
    
    this.dataIn = dataIn;
    this.checksumIn = checksumIn;
    this.checksum = checksum;
    this.clearOsBuffer = clearOsBuffer;
    
    long blockLength = pathinfo.getNumBytes();
       
    /* If bytesPerChecksum is very large, then the metadata file
     * is mostly corrupted. For now just truncate bytesPerchecksum to
     * blockLength.
     */        
    bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
      checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
                                 Math.max((int)blockLength, 10*1024*1024),
                              new PureJavaCrc32());
      bytesPerChecksum = checksum.getBytesPerChecksum();        
    }

    checksumSize = checksum.getChecksumSize();
    
    // if the requested size exceeds the currently known length of the file
    // then check the blockFile to see if its length has grown. This can
    // occur if the file is being concurrently written to while it is being
    // read too. If the blockFile has grown in size, then update the new
    // size in our cache.
    if (startOffset > blockLength
        || (length + startOffset) > blockLength) {
      File blkFile = new File(pathinfo.getBlockPath());
      long newlength = blkFile.length();
      LOG.warn("BlockReaderLocal found short block " + blkFile +
               " requested offset " +
               startOffset + " length " + length +
               " but known size of block is " + blockLength +
               ", size on disk is " + newlength);
      if (newlength > blockLength) {
        blockLength = newlength;
        pathinfo.setNumBytes(newlength);
      }
    }
    long endOffset = blockLength;
    if (startOffset < 0 || startOffset > endOffset
        || (length + startOffset) > endOffset) {
      String msg = " Offset " + startOffset + " and length " + length
      + " don't match block " + block + " ( blockLen " + endOffset + " )";
      LOG.warn("BlockReaderLocal requested with incorrect offset: " + msg);
      throw new IOException(msg);
    }
   
    firstChunkOffset = (startOffset - (startOffset % bytesPerChecksum));
      
    if (length >= 0) {
      // Make sure endOffset points to end of a checksumed chunk.
      long tmpLen = startOffset + length;
      if (tmpLen % bytesPerChecksum != 0) {
        tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
      }
      if (tmpLen < endOffset) {
        endOffset = tmpLen;
      }
    }

    // seek to the right offsets
    if (firstChunkOffset > 0) {
      dataIn.getChannel().position(firstChunkOffset);
      
      long checksumSkip = (firstChunkOffset / bytesPerChecksum) * checksumSize;
      // note blockInStream is  seeked when created below
      if (checksumSkip > 0) {
        checksumIn.skip(checksumSkip);
      }
    }

    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;
  }
  
  @Override
  public synchronized int read(byte[] buf, int off, int len)
                           throws IOException {    
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem read off " + off + " len " + len);
    }   
    metrics.readsFromLocalFile.inc();
    int byteRead;
    if (checksum == null) {
      byteRead = dataIn.read(buf, off, len);
      updateStatsAfterRead(byteRead);
    }
    else {
      byteRead = super.read(buf, off, len);
    }
    if (clearOsBuffer) {
      // drop all pages from the OS buffer cache
      NativeIO.posixFadviseIfPossible(dataIn.getFD(), off, len, 
                                      NativeIO.POSIX_FADV_DONTNEED);
    }
    return byteRead;
  }
  
  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem skip " + n);
    }
    if (checksum == null) {
      return dataIn.skip(n);
    }
    else {
      return super.skip(n);
    }
  }
  
  @Override
  public synchronized void seek(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem seek " + n);
    }
    throw new IOException("Seek() is not supported in BlockReaderLocal");
  }

  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset,
                                       int len, byte[] checksumBuf)
                                       throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading chunk from position " + pos + " at offset " +
          offset + " with length " + len);
    }

    if ( gotEOS ) {
      if ( startOffset < 0 ) {
        //This is mainly for debugging. can be removed.
        throw new IOException( "BlockRead: already got EOS or an error" );
      }
      startOffset = -1;
      return -1;
    }

    if (checksumBuf.length != checksumSize) {
      throw new IOException("Cannot read checksum into provided buffer. The buffer must be exactly '" +
          checksumSize + "' bytes long to hold the checksum bytes.");
    }
    
    if ( (pos + firstChunkOffset) != lastChunkOffset ) {
      throw new IOException("Mismatch in pos : " + pos + " + " + 
                            firstChunkOffset + " != " + lastChunkOffset);
    }
    
    int nRead = dataIn.read(buf, offset, bytesPerChecksum);
    if (nRead < bytesPerChecksum) {
      gotEOS = true;
      
    }
    
    lastChunkOffset += nRead;
    lastChunkLen = nRead;
    
    // If verifyChecksum is false, we omit reading the checksum
    if (checksumIn != null) {
      int nChecksumRead = checksumIn.read(checksumBuf);
      
      if (nChecksumRead != checksumSize) {
        throw new IOException("Could not read checksum at offset " + 
            checksumIn.getChannel().position() + " from the meta file.");      
      }
    }
       
    return nRead;    
  }

  /**
   * Maps in the relevant portion of the file. This avoid copying the data from
   * OS pages into this process's page. It will be automatically unmapped when
   * the ByteBuffer that is returned here goes out of scope. This method is
   * currently invoked only by the FSDataInputStream ScatterGather api.
   */
  public ByteBuffer readAll() throws IOException {
    MappedByteBuffer bb = dataIn.getChannel().map(FileChannel.MapMode.READ_ONLY,
                                 startOffset, length);
    return bb;  
  }

  @Override
  public synchronized void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem close");
    }
    dataIn.close();
    if (checksumIn != null) {
      checksumIn.close();
    }
  }  
}


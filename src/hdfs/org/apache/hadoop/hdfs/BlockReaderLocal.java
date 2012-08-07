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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.LRUCache;
import org.apache.hadoop.util.PureJavaCrc32;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** This is a local block reader. if the DFS client is on
 * the same machine as the datanode, then the client can read
 * files directly from the local file system rathen than going
 * thorugh the datanode. This improves performance dramatically.
 */
public class BlockReaderLocal extends BlockReader {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final Object lock = new Object();

  static final class LocalBlockKey {
    private int namespaceid;
    private Block block;

    LocalBlockKey(int namespaceid, Block block) {
      this.namespaceid = namespaceid;
      this.block = block;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof LocalBlockKey)) {
        return false;
      }

      LocalBlockKey that = (LocalBlockKey) o;
      return this.namespaceid == that.namespaceid &&
          this.block.equals(that.block);
    }

    @Override
    public int hashCode() {
      return 31 * namespaceid + block.hashCode();
    }

    @Override
    public String toString() {
      return namespaceid + ":" + block.toString();
    }
  }

  private static final LRUCache<LocalBlockKey, BlockPathInfo> cache =
      new LRUCache<LocalBlockKey, BlockPathInfo>(10000);

  private static class LocalDatanodeInfo {
    private volatile ProtocolProxy<ClientDatanodeProtocol> datanode;
    private boolean namespaceIdSupported;

    LocalDatanodeInfo() {
    }

    public BlockPathInfo getOrComputePathInfo(
        int namespaceid,
        Block block,
        DatanodeInfo node,
        Configuration conf
    ) throws IOException {
      LocalBlockKey blockKey = new LocalBlockKey(namespaceid, block);
      BlockPathInfo pathinfo = cache.get(blockKey);
      if (pathinfo != null) {
        return pathinfo;
      }
      // make RPC to local datanode to find local pathnames of blocks
      ClientDatanodeProtocol proxy = getDatanodeProxy(node, conf);
      try {
        if (namespaceIdSupported) {
          pathinfo = proxy.getBlockPathInfo(namespaceid, block);
        } else {
          pathinfo =  proxy.getBlockPathInfo(block);
        }
        if (pathinfo != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Cached location of block " + blockKey + " as " +
                pathinfo);
          }
          setBlockLocalPathInfo(blockKey, pathinfo);
        }

      } catch (IOException ioe) {
        datanode = null;
        RPC.stopProxy(proxy);
        throw ioe;
      }
      return pathinfo;
    }

    private synchronized ClientDatanodeProtocol getDatanodeProxy(
        DatanodeInfo node, Configuration conf
    ) throws IOException{

      if (datanode == null) {
        datanode = DFSClient.createClientDNProtocolProxy(node, conf, 0);
        this.namespaceIdSupported = datanode.isMethodSupported(
            "getBlockPathInfo", int.class, Block.class
        );
      }
      return datanode.getProxy();
    }

    private void setBlockLocalPathInfo(LocalBlockKey b, BlockPathInfo info) {
      cache.put(b, info);
    }

    private void removeBlockLocalPathInfo(int namespaceid, Block block) {
      cache.remove(new LocalBlockKey(namespaceid, block));
    }
  }

  private long length;
  private FileInputStream dataIn;  // reader for the data file
  private FileInputStream checksumIn;
  private boolean clearOsBuffer;
  private DFSClientMetrics metrics;

  static private final Path src = new Path("/BlockReaderLocal:localfile");

  /**
   * The only way this object can be instantiated.
   */
  public static BlockReaderLocal newBlockReader(
      Configuration conf,
      String file,
      int namespaceid,
      Block blk,
      DatanodeInfo node,
      long startOffset,
      long length,
      DFSClientMetrics metrics,
      boolean verifyChecksum,
      boolean clearOsBuffer) throws IOException {

    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node);

    BlockPathInfo pathinfo = localDatanodeInfo.getOrComputePathInfo(namespaceid,
        blk, node, conf);

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
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(
            new DataInputStream(checksumIn), new PureJavaCrc32());
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
      localDatanodeInfo.removeBlockLocalPathInfo(namespaceid, blk);
      DFSClient.LOG.warn("BlockReaderLoca: Removing " + blk +
          " from cache because local file " +
          pathinfo.getBlockPath() +
          " could not be opened.");
      throw e;
    }
  }

  // ipc port to LocalDatanodeInfo mapping to properly handles the
  // case of multiple data nodes running on a local machine
  private static ConcurrentMap<Integer, LocalDatanodeInfo>
      ipcPortToLocalDatanodeInfo =
      new ConcurrentHashMap<Integer, LocalDatanodeInfo>();

  private static LocalDatanodeInfo getLocalDatanodeInfo(DatanodeInfo node) {
    LocalDatanodeInfo ldInfo = ipcPortToLocalDatanodeInfo.get(
        node.getIpcPort());

    if (ldInfo == null) {
      LocalDatanodeInfo miss = new LocalDatanodeInfo();
      ldInfo = ipcPortToLocalDatanodeInfo.putIfAbsent(node.getIpcPort(), miss);
      if(ldInfo == null) {
        ldInfo = miss;
      }
    }
    return ldInfo;
  }

  private BlockReaderLocal(Configuration conf, String hdfsfile, Block block,
                           long startOffset, long length,
                           BlockPathInfo pathinfo, DFSClientMetrics metrics,
                           FileInputStream dataIn, boolean clearOsBuffer)
      throws IOException {
    super(
        src, // dummy path, avoid constructing a Path object dynamically
        1);

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
      throw new IOException("Cannot read checksum into provided buffer. " +
          "The buffer must be exactly '" +
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
    return dataIn.getChannel().map(FileChannel.MapMode.READ_ONLY,
        startOffset, length);
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


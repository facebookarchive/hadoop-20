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
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumReader;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumReader.GenStampAndChecksum;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.LRUCache;
import org.apache.hadoop.util.NativeCrc32;
import org.apache.hadoop.hdfs.BlockReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/** This is a local block reader. if the DFS client is on
 * the same machine as the datanode, then the client can read
 * files directly from the local file system rathen than going
 * through the datanode. This improves performance dramatically.
 */
public abstract class BlockReaderLocalBase extends BlockReader {
  
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
      InjectionHandler
          .processEventIO(InjectionEvent.BLOCK_READ_LOCAL_GET_PATH_INFO);

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
  
  protected long length;
  protected boolean clearOsBuffer;
  protected boolean positionalReadMode;
  protected DFSClientMetrics metrics;

  protected static final Path src = new Path("/BlockReaderLocal:localfile");

  /**
   * The only way this object can be instantiated.
   */
  public static BlockReaderLocalBase newBlockReader(
      Configuration conf,
      String file,
      int namespaceid,
      Block blk,
      DatanodeInfo node,
      long startOffset,
      long length,
      DFSClientMetrics metrics,
      boolean verifyChecksum,
      boolean clearOsBuffer,
      boolean positionalReadMode) throws IOException {

    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node);

    BlockPathInfo pathinfo = localDatanodeInfo.getOrComputePathInfo(namespaceid,
        blk, node, conf);
    
    // Another alternative is for datanode to pass whether it is an inline checksum
    // file and checksum metadata through BlockPathInfo, which is a cleaner approach.
    // However, we need to worry more about protocol compatible issue. We avoid this
    // trouble for now. We can always change to the other approach later.
    //
    boolean isInlineChecksum = Block.isInlineChecksumBlockFilename(new Path(
        pathinfo.getBlockPath()).getName());

    // check to see if the file exists. It may so happen that the
    // HDFS file has been deleted and this block-lookup is occuring
    // on behalf of a new HDFS file. This time, the block file could
    // be residing in a different portion of the fs.data.dir directory.
    // In this case, we remove this entry from the cache. The next
    // call to this method will repopulate the cache.
    try {

      // get a local file system
      FileChannel dataFileChannel;
      FileDescriptor dataFileDescriptor;
      File blkfile = new File(pathinfo.getBlockPath());
      FileInputStream fis = new FileInputStream(blkfile);
      dataFileChannel = fis.getChannel();
      dataFileDescriptor = fis.getFD();
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("New BlockReaderLocal for file " +
            pathinfo.getBlockPath() + " of size " + blkfile.length() +
                  " startOffset " + startOffset +
                  " length " + length);
      }
      
      DataChecksum checksum = null;
      if (isInlineChecksum) {
        GenStampAndChecksum gac = BlockInlineChecksumReader
            .getGenStampAndChecksumFromInlineChecksumFile(new Path(pathinfo
                .getBlockPath()).getName());
        checksum = DataChecksum.newDataChecksum(gac.getChecksumType(),
            gac.getBytesPerChecksum());
        
        if (verifyChecksum) {

          return new BlockReaderLocalInlineChecksum(conf, file, blk,
              startOffset, length, pathinfo, metrics, checksum, verifyChecksum,
              dataFileChannel, dataFileDescriptor, clearOsBuffer,
              positionalReadMode);
        }
        else {
          return new BlockReaderLocalInlineChecksum(conf, file, blk,
              startOffset, length, pathinfo, metrics, checksum,
              dataFileChannel, dataFileDescriptor, clearOsBuffer,
              positionalReadMode);
        }
      } else if (verifyChecksum) {
        FileChannel checksumInChannel = null;
        // get the metadata file
        File metafile = new File(pathinfo.getMetaPath());
        FileInputStream checksumIn = new FileInputStream(metafile);
        checksumInChannel = checksumIn.getChannel();
        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(
            new DataInputStream(checksumIn), new NativeCrc32());
        short version = header.getVersion();

        if (version != FSDataset.FORMAT_VERSION_NON_INLINECHECKSUM) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + blk + " ignoring ...");
        }
        checksum = header.getChecksum();

        return new BlockReaderLocalWithChecksum(conf, file, blk, startOffset,
            length, pathinfo, metrics, checksum, verifyChecksum,
            dataFileChannel, dataFileDescriptor, checksumInChannel,
            clearOsBuffer, positionalReadMode);
      }
      else {
        return new BlockReaderLocalWithChecksum(conf, file, blk, startOffset,
            length, pathinfo, metrics, dataFileChannel, dataFileDescriptor,
            clearOsBuffer, positionalReadMode);
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

  protected BlockReaderLocalBase(Configuration conf, String hdfsfile,
      Block block, long startOffset, long length, BlockPathInfo pathinfo,
      DFSClientMetrics metrics, boolean clearOsBuffer,
      boolean positionalReadMode) throws IOException {
   super(
        src, // dummy path, avoid constructing a Path object dynamically
        1);

    this.startOffset = startOffset;
    this.length = length;    
    this.metrics = metrics;
    this.clearOsBuffer = clearOsBuffer;   
    this.positionalReadMode = positionalReadMode;
  }
  
  protected BlockReaderLocalBase(Configuration conf, String hdfsfile,
      Block block, long startOffset, long length, BlockPathInfo pathinfo,
      DFSClientMetrics metrics, DataChecksum checksum, boolean verifyChecksum,
      boolean clearOsBuffer, boolean positionalReadMode) throws IOException {
    super(
        src, // dummy path, avoid constructing a Path object dynamically
        1,
        checksum,
        verifyChecksum);

    this.startOffset = startOffset;
    this.length = length;
    this.metrics = metrics;
    
    this.checksum = checksum;
    this.clearOsBuffer = clearOsBuffer;
    this.positionalReadMode = positionalReadMode;

    long blockLength = pathinfo.getNumBytes();

    /* If bytesPerChecksum is very large, then the metadata file
    * is mostly corrupted. For now just truncate bytesPerchecksum to
    * blockLength.
    */
    bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
      checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
          Math.max((int)blockLength, 10*1024*1024),
          new NativeCrc32());
      bytesPerChecksum = checksum.getBytesPerChecksum();
    }

    checksumSize = checksum.getChecksumSize();
  }

  public synchronized void seek(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem seek " + n);
    }
    throw new IOException("Seek() is not supported in BlockReaderLocal");
  }

  @Override
  protected abstract int readChunk(long pos, byte[] buf, int offset,
                                       int len, byte[] checksumBuf)
      throws IOException;

  /**
   * For non inline checksum: Maps in the relevant portion of the file. This avoid
   * copying the data from OS pages into this process's page. It will be
   * automatically unmapped when the ByteBuffer that is returned here goes out
   * of scope. This method is currently invoked only by the FSDataInputStream
   * ScatterGather api.
   * 
   * For inline checksum: just keep the same return using normal read()
   * implementation.
   */
  public abstract ByteBuffer readAll() throws IOException;

  @Override
  public abstract void close() throws IOException;
}


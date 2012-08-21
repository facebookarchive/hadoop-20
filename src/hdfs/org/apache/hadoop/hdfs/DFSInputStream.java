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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient.DataNodeSlowException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
public class DFSInputStream extends FSInputStream {
  private Socket s = null;
  private boolean closed = false;
  
  private final DFSClient dfsClient;

  private String src = null;
  private long prefetchSize;
  private BlockReader blockReader = null;
  private ConcurrentHashMap<Long, BlockReaderLocal> localBlockReaders =
      new ConcurrentHashMap<Long, BlockReaderLocal>(0, 0.75f, 2);
  private boolean verifyChecksum;
  private boolean clearOsBuffer;
  private DFSLocatedBlocks locatedBlocks = null;
  private DatanodeInfo currentNode = null;
  private Block currentBlock = null;
  private boolean isCurrentBlockUnderConstruction;
  private long pos = 0;
  private long blockEnd = -1;
  private LocatedBlocks blocks = null;
  private int namespaceId;  // the namespace that this file belongs to

  private int timeWindow = 3000; // wait time window (in msec) if BlockMissingException is caught

  /* XXX Use of CocurrentHashMap is temp fix. Need to fix
   * parallel accesses to DFSInputStream (through ptreads) properly */
  private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes =
             new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>(0, 0.75f, 1);
  private int buffersize = 1;

  private byte[] oneByteBuf = new byte[1]; // used for 'int read()'

  void addToDeadNodes(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }

  DFSInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum,
                 boolean clearOsBuffer) throws IOException {
    this.src = src;
    this.dfsClient = dfsClient;    
    init(buffersize, verifyChecksum, clearOsBuffer);
  }

  /**
   * Used for snapshot
   */
  DFSInputStream(DFSClient dfsClient, LocatedBlocksWithMetaInfo blocks, int buffersize,
      boolean verifyChecksum) throws IOException {
    this.blocks = blocks;
    this.namespaceId = blocks.getNamespaceID();
    this.dfsClient = dfsClient;
    
    dfsClient.updateDataTransferProtocolVersionIfNeeded(blocks.getDataProtocolVersion());
    dfsClient.getNewNameNodeIfNeeded(blocks.getMethodFingerPrint());
    init(buffersize, verifyChecksum, false);
  }


  private void init(int buffersize, boolean verifyChecksum,
                    boolean clearOsBuffer) throws IOException {
    this.verifyChecksum = verifyChecksum;
    this.buffersize = buffersize;
    this.clearOsBuffer = clearOsBuffer;
    prefetchSize = dfsClient.conf.getLong("dfs.read.prefetch.size",
    		10 * dfsClient.defaultBlockSize);
    timeWindow = dfsClient.conf.getInt(
    		"dfs.client.baseTimeWindow.waitOn.BlockMissingException",
    		timeWindow);

    try {
      openInfo();
    } catch (IOException e) {
      dfsClient.incReadExpCntToStats();
      throw e;
    }
  }

  /**
   * Grab the open-file info from namenode
   */
  synchronized void openInfo() throws IOException {
    if (src == null && blocks == null) {
      throw new IOException("No fine provided to open");
    }

    LocatedBlocks newInfo = src != null ? 
                            getLocatedBlocks(src, 0, prefetchSize) : blocks;
    if (newInfo == null) {
      throw new IOException("Cannot open filename " + src);
    }

    // I think this check is not correct. A file could have been appended to
    // between two calls to openInfo().
    if (locatedBlocks != null && !locatedBlocks.isUnderConstruction() &&
        !newInfo.isUnderConstruction()) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
      Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
      while (oldIter.hasNext() && newIter.hasNext()) {
        if (! oldIter.next().getBlock().equals(newIter.next().getBlock())) {
          throw new IOException("Blocklist for " + src + " has changed!");
        }
      }
    }

    // if the file is under construction, then fetch size of last block
    // from datanode.
    if (newInfo.isUnderConstruction() && newInfo.locatedBlockCount() > 0) {
      LocatedBlock last = newInfo.get(newInfo.locatedBlockCount()-1);
      if (last.getLocations().length > 0) {
        try {
          Block newBlock = getBlockInfo(last);
          // only if the block has data (not null)
          if (newBlock != null) {
            long newBlockSize = newBlock.getNumBytes();
            newInfo.setLastBlockSize(newBlock.getBlockId(), newBlockSize);
          }
        } catch (IOException e) {
          DFSClient.LOG.debug("DFSClient file " + src + 
                    " is being concurrently append to" +
                    " but datanodes probably does not have block " +
                    last.getBlock(), e);
        }
      }
    }
    this.locatedBlocks = new DFSLocatedBlocks(newInfo);
    this.currentNode = null;
  }
  
  private void checkLocatedBlocks(LocatedBlocks locatedBlocks)
      throws IOException {
    if (null == locatedBlocks) {
      return;
    }
    if(!locatedBlocks.isUnderConstruction()) {
      return;
    }
    List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
    if (lbs == null) {
      return;
    }
    for (int i = 0; i < lbs.size() - 1; i++) {
      if (lbs.get(i).getBlockSize() <= 1) {
        throw new IOException(
            "File is under construction and namenode hasn't received the second last block yet.");
      }
    }
  }
  
  private LocatedBlocks getLocatedBlocks(String src, long start, long length)
  throws IOException {
    try {
      if (dfsClient.namenodeProtocolProxy != null &&
          dfsClient.namenodeProtocolProxy.isMethodSupported("openAndFetchMetaInfo",
              String.class, long.class, long.class)) {
        LocatedBlocksWithMetaInfo locs = 
            dfsClient.namenode.openAndFetchMetaInfo(src, start, length);
        if (locs != null) {
          this.namespaceId = locs.getNamespaceID();
          dfsClient.updateDataTransferProtocolVersionIfNeeded(locs.getDataProtocolVersion());
          dfsClient.getNewNameNodeIfNeeded(locs.getMethodFingerPrint());
        }
        checkLocatedBlocks(locs);
        return locs;
      } else if (dfsClient.namenodeProtocolProxy != null && 
          dfsClient.namenodeProtocolProxy.isMethodSupported("open", String.class,
              long.class, long.class)) {
        VersionedLocatedBlocks locs = dfsClient.namenode.open(src, start, length);
        if (locs != null) {
          dfsClient.updateDataTransferProtocolVersionIfNeeded(locs.getDataProtocolVersion());
        }
        checkLocatedBlocks(locs);
        return locs;
      } else {
        LocatedBlocks locs = dfsClient.namenode.getBlockLocations(src, start, length);
        checkLocatedBlocks(locs);
        return locs;
      }
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                    FileNotFoundException.class);
    }
  }
  
  /** Get block info from a datanode */
  private Block getBlockInfo(LocatedBlock locatedblock) throws IOException {
    if (locatedblock == null || locatedblock.getLocations().length == 0) {
      return null;
    }
    int replicaNotFoundCount = locatedblock.getLocations().length;

    for(DatanodeInfo datanode : locatedblock.getLocations()) {
      ProtocolProxy<ClientDatanodeProtocol> cdp = null;

      try {
        cdp = DFSClient.createClientDNProtocolProxy(datanode,
            dfsClient.conf, dfsClient.socketTimeout);

        final Block newBlock;
        if (cdp.isMethodSupported("getBlockInfo", int.class, Block.class)) {
          newBlock = cdp.getProxy().getBlockInfo(
              namespaceId, locatedblock.getBlock());
        } else {
          newBlock = cdp.getProxy().getBlockInfo(locatedblock.getBlock());
        }

        if (newBlock == null) {
          // special case : replica might not be on the DN, treat as 0 length
          replicaNotFoundCount--;
        } else {
          return newBlock;
        }
      }
      catch(IOException ioe) {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Failed to getBlockInfo from datanode "
              + datanode + " for block " + locatedblock.getBlock(), ioe);
        }
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp.getProxy());
        }
      }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
      return null;
    }

    throw new IOException("Cannot obtain block info for " + locatedblock);
  }

  /**
   * Returns whether the file opened is under construction.
   */
  public synchronized boolean isUnderConstruction() {
    return locatedBlocks.isUnderConstruction();
  }

  public long getFileLength() {
    return locatedBlocks.getFileLength();
  }

  public DFSLocatedBlocks fetchLocatedBlocks() {
    return locatedBlocks;
  }

  /**
   * Returns the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return currentNode;
  }

  /**
   * Returns the block containing the target position.
   */
  public Block getCurrentBlock() {
    return currentBlock;
  }

  /**
   * Return collection of blocks that has already been located.
   */
  synchronized List<LocatedBlock> getAllBlocks() throws IOException {
    return getBlockRange(0, this.getFileLength());
  }

  /**
   * Get block at the specified position. Fetch it from the namenode if not
   * cached.
   * 
   * @param offset
   * @param updatePosition
   * @param throwWhenNoFound
   *          when no block found for the offset return null instead of
   *          throwing an exception
   * @return located block
   * @throws IOException
   */
  private LocatedBlock getBlockAt(long offset, boolean updatePosition,
      boolean throwWhenNotFound)
      throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";
    // search cached blocks first
    LocatedBlock blk = locatedBlocks.getBlockContainingOffset(offset);
    if (blk == null) { // block is not cached
      // fetch more blocks
      LocatedBlocks newBlocks;
      newBlocks = getLocatedBlocks(src, offset, prefetchSize);
      if (newBlocks == null) {
        if (!throwWhenNotFound) {
          return null;
        }
        throw new IOException("Could not find target position " + offset);
      }
      locatedBlocks.insertRange(newBlocks.getLocatedBlocks());
      locatedBlocks.setFileLength(newBlocks.getFileLength());
    }
    blk = locatedBlocks.getBlockContainingOffset(offset);
    if (blk == null) {
      if (!throwWhenNotFound) {
        return null;
      }
      throw new IOException("Failed to determine location for block at "
          + "offset=" + offset);
    }
    if (updatePosition) {
      // update current position
      this.pos = offset;
      this.blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
      this.currentBlock = blk.getBlock();
      isCurrentBlockUnderConstruction = locatedBlocks
          .isUnderConstructionBlock(this.currentBlock);
    }
    return blk;

  }

  /**
   * Get blocks in the specified range. The locations of all blocks
   * overlapping with the given segment of the file are retrieved. Fetch them
   * from the namenode if not cached.
   *
   * @param offset the offset of the segment to read
   * @param length the length of the segment to read
   * @return consequent segment of located blocks
   * @throws IOException
   */
  private List<LocatedBlock> getBlockRange(final long offset,
      final long length) throws IOException {
    List<LocatedBlock> blockRange = new ArrayList<LocatedBlock>();
    // Zero length. Not sure this ever happens in practice.
    if (length == 0)
      return blockRange;

    // A defensive measure to ensure that we never loop here eternally.
    // With a 256 M block size, 10000 blocks will correspond to 2.5 TB.
    // No one should read this much data at once in practice.
    int maxLoops = 10000;

    // Copy locatedBlocks to a local data structure. This ensures that 
    // a concurrent invocation of openInfo() works OK, the reason being
    // that openInfo may completely replace locatedBlocks.
    DFSLocatedBlocks locatedBlocks = this.locatedBlocks;

    if (locatedBlocks == null) {
      // Make this an IO exception because this is input/output code error.
      throw new IOException("locatedBlocks is null");
    }

    long remaining = length;
    long curOff = offset;
    while (remaining > 0) {
      // a defensive check to bail out of this loop at all costs
      if (--maxLoops < 0) {
        String msg = "Failed to getBlockRange at offset " + offset +
                     ", length=" + length +
                     ", curOff=" + curOff +
                     ", remaining=" + remaining +
                     ". Aborting...";
        DFSClient.LOG.warn(msg);
        throw new IOException(msg); 
      }

      LocatedBlock blk = locatedBlocks.getBlockContainingOffset(curOff);
      if (blk == null) {
        LocatedBlocks newBlocks;
        newBlocks = getLocatedBlocks(src, curOff, remaining);
        if (newBlocks == null) {
          throw new IOException("Could not get block locations for curOff=" +
              curOff + ", remaining=" + remaining + " (offset=" + offset +
              ")");
        }
        locatedBlocks.insertRange(newBlocks.getLocatedBlocks());
        continue;
      }

      blockRange.add(blk);
      long bytesRead = blk.getStartOffset() + blk.getBlockSize() - curOff;
      remaining -= bytesRead;
      curOff += bytesRead;
    }

    DFSClient.checkBlockRange(blockRange, offset, length);

    return blockRange;
  }

  protected synchronized DatanodeInfo blockSeekTo(long target) throws IOException {
    return blockSeekTo(target, true);
  }
  
  /**
   * Open a DataInputStream to a DataNode so that it can be read from.
   * We get block ID and the IDs of the destinations at startup, from the namenode.
   */
  private synchronized DatanodeInfo blockSeekTo(long target,
      boolean throwWhenNotFound) throws IOException {
    // We only allow to seek before the end of the file, or the end of the file
    // and allowSeedtoEnd, which is the case called by available().
    //
    if (target > getFileLength() || (target == getFileLength() && throwWhenNotFound)) {
      throw new IOException("Attempted to read past end of file");
    }

    if ( blockReader != null ) {
      blockReader.close();
      blockReader = null;
    }

    if (s != null) {
      s.close();
      s = null;
    }

    //
    // Compute desired block.
    //
    LocatedBlock targetBlock = getBlockAt(target, true, throwWhenNotFound);
    // Given target<= fileLength, when and only whenallowSeektoEnd is true and
    // there is no block for the file yet, getBlockAt() returns null, in this
    // case we should simply return null.
    //
    if (targetBlock == null) {
      assert target == 0;
      return null;
    }
    assert (target==this.pos) : "Wrong postion " + pos + " expect " + target;
    long offsetIntoBlock = target - targetBlock.getStartOffset();

    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    DatanodeInfo chosenNode = null;
    while (s == null) {
      DNAddrPair retval = chooseDataNode(targetBlock);
      chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;

      // try reading the block locally. if this fails, then go via
      // the datanode
      Block blk = targetBlock.getBlock();
      try {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.warn("blockSeekTo shortCircuitLocalReads "
                   + dfsClient.shortCircuitLocalReads +
                   " localhost " + dfsClient.localHost +
                   " targetAddr " + targetAddr);
        }
        if (dfsClient.shortCircuitLocalReads && dfsClient.localHost != null &&
            (targetAddr.equals(dfsClient.localHost) ||
             targetAddr.getHostName().startsWith("localhost"))) {
          blockReader = BlockReaderLocal.newBlockReader(dfsClient.conf, src,
                                                 namespaceId, blk,
                                                 chosenNode,
                                                 offsetIntoBlock,
                                                 blk.getNumBytes() - offsetIntoBlock,
                                                 dfsClient.metrics, 
                                                 this.verifyChecksum,
                                                 this.clearOsBuffer,
                                                 false);
          blockReader.setReadLocal(true);
          blockReader.setFsStats(dfsClient.stats);
          return chosenNode;
        }
      } catch (IOException ex) {
        DFSClient.LOG.info("Failed to read block " + targetBlock.getBlock() +
                 " on local machine " + dfsClient.localHost +
                 ". Try via the datanode on " + targetAddr + ":"
                  + StringUtils.stringifyException(ex));
      }

      try {
        s = dfsClient.socketFactory.createSocket();
        NetUtils.connect(s, targetAddr, dfsClient.socketTimeout,
            dfsClient.ipTosValue);
        s.setSoTimeout(dfsClient.socketTimeout);

        long minReadSpeedBps = (dfsClient.numNodeLeft(targetBlock.getLocations(),
            deadNodes) > 1) ? dfsClient.minReadSpeedBps : -1;
        blockReader = BlockReader.newBlockReader(
            dfsClient.getDataTransferProtocolVersion(), namespaceId,
            s, src, blk.getBlockId(),
            blk.getGenerationStamp(),
            offsetIntoBlock, blk.getNumBytes() - offsetIntoBlock,
            buffersize, verifyChecksum,
            dfsClient.clientName, minReadSpeedBps);
        boolean isLocalHost = NetUtils.isLocalAddressWithCaching(targetAddr
            .getAddress());
        blockReader.setReadLocal(isLocalHost);
        if (!isLocalHost) {
          blockReader
              .setReadRackLocal(dfsClient.isInLocalRack(targetAddr.getAddress()));
        }
        blockReader.setFsStats(dfsClient.stats);

        return chosenNode;
      } catch (IOException ex) {
        // Put chosen node into dead list, continue
        DFSClient.LOG.warn("Failed to connect to " + targetAddr, ex);
        addToDeadNodes(chosenNode);
        if (s != null) {
          try {
            s.close();
          } catch (IOException iex) {
          }
        }
        s = null;
      }
    }
    return chosenNode;
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    dfsClient.checkOpen();

    if ( blockReader != null ) {
      blockReader.close();
      blockReader = null;
    }
    
    for (BlockReaderLocal brl : localBlockReaders.values()) {
      try {
        brl.close();
      } catch (IOException ioe) {
        Log.warn("Error when closing local block reader", ioe);
      }
    }
    localBlockReaders = null;

    if (s != null) {
      s.close();
      s = null;
    }
    super.close();
    closed = true;
  }

  @Override
  public synchronized int read() throws IOException {
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksuFileSystem
   */
  private synchronized int readBuffer(byte buf[], int off, int len)
                                                  throws IOException {
    IOException ioe;

    /* we retry current node only once. So this is set to true only here.
     * Intention is to handle one common case of an error that is not a
     * failure on datanode or client : when DataNode closes the connection
     * since client is idle. If there are other cases of "non-errors" then
     * then a datanode might be retried by setting this to true again.
     */
    boolean retryCurrentNode = true;

    while (true) {
      // retry as many times as seekToNewSource allows.
      try {
        int bytesRead = blockReader.read(buf, off, len);
        
        // update length of file under construction if needed
        if (isCurrentBlockUnderConstruction
            && blockReader.isBlkLenInfoUpdated()) {
          locatedBlocks.setLastBlockSize(currentBlock.getBlockId(),
              blockReader.getUpdatedBlockLength());
          this.blockEnd = locatedBlocks.getFileLength() - 1;
          blockReader.resetBlockLenInfo();
          // if the last block is finalized, get file info from name-node.
          // It is necessary because there might be new blocks added to
          // the file. The client needs to check with the name-node whether
          // it is the case, or the file has been finalized.
          if (blockReader.isBlockFinalized() && src != null) {
            openInfo();
          }
          
        }
        return bytesRead;     
      } catch (DataNodeSlowException dnse) {
        DFSClient.LOG.warn("Node " + currentNode + " is too slow when reading blk "
            + this.currentBlock + ". Try another datanode.");
        ioe = dnse;
        retryCurrentNode = false;
      } catch ( ChecksumException ce ) {
        DFSClient.LOG.warn("Found Checksum error for " + currentBlock + " from " +
                 currentNode.getName() + " at " + ce.getPos());
        dfsClient.reportChecksumFailure(src, currentBlock, currentNode);
        ioe = ce;
        retryCurrentNode = false;
      } catch ( IOException e ) {
        if (!retryCurrentNode) {
          DFSClient.LOG.warn("Exception while reading from " + currentBlock +
                   " of " + src + " from " + currentNode + ": " +
                   StringUtils.stringifyException(e));
        }
        ioe = e;
      }
      boolean sourceFound = false;
      if (retryCurrentNode) {
        /* possibly retry the same node so that transient errors don't
         * result in application level failures (e.g. Datanode could have
         * closed the connection because the client is idle for too long).
         */
        sourceFound = seekToBlockSource(pos, len != 0);
      } else {
        addToDeadNodes(currentNode);
        sourceFound = seekToNewSource(pos, len != 0);
      }
      if (!sourceFound) {
        throw ioe;
      } else {
        dfsClient.incReadExpCntToStats();

      }
      retryCurrentNode = false;
    }
  }

  /**
   * Read the entire buffer.
   */
  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed) {
      dfsClient.incReadExpCntToStats();

      throw new IOException("Stream closed");
    }
    DFSClient.dfsInputStreamfailures.set(0);
			long start = System.currentTimeMillis();
    
    if (pos < getFileLength() || (pos == getFileLength() && len == 0)) {
      int retries = 2;
      while (retries > 0) {
        try {
          // If position equals or is larger than the end position of the
          // block, we try to seek to the next block, unless:
          // 1. user tries to read 0 bytes (usually by available() call), AND
          // 2. there is at least a known block for the file (blockEnd != -1), AND
          // 3. pos is the end of the file, AND
          // 4. the end of the block is the end of the file
          //    (the current block is the known last block of the file)
          // For this case, we want to stay in the current block, as in the case
          // that it is the last block (which is almost always true given
          // len == 0), the current block is the under-construction block whose size
          // you want to update.
          //
          if (len == 0) { // called by available()
            if (blockEnd == -1 // No current block selected
                || pos == getFileLength()) { // at the end of the file
              currentNode = blockSeekTo(pos, false);
              if (currentNode == null) {
                // In this case, user wants to know available information of
                // the file, but the file doesn't have any block created yet (it
                // is a 0 size file). Simply 0 should be returned.
                return 0;
              }
            } else {
              throw new IOException(
                  "Try to read 0 bytes while current position is not the end of the file");
            }
          } else if (pos > blockEnd) {
            currentNode = blockSeekTo(pos, true);
          }
          
          int realLen = (int) Math.min((long) len, (blockEnd - pos + 1L));
          int result = readBuffer(buf, off, realLen);

          if (result >= 0) {
            pos += result;
          } else if (len != 0){
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          if (dfsClient.stats != null && result != -1) {
            dfsClient.stats.incrementBytesRead(result);
          }
          long timeval = System.currentTimeMillis() - start;
          dfsClient.metrics.incReadTime(timeval);
          dfsClient.metrics.incReadSize(result);
          dfsClient.metrics.incReadOps();
          return (result >= 0) ? result : 0;
        } catch (ChecksumException ce) {
          dfsClient.incReadExpCntToStats();

          throw ce;
        } catch (IOException e) {
          dfsClient.incReadExpCntToStats();

          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
          }
          blockEnd = -1;
          if (currentNode != null) { addToDeadNodes(currentNode); }
          if (--retries == 0) {
            throw e;
          }
        }
      }
    }
    return -1;
  }


  private DNAddrPair chooseDataNode(LocatedBlock block)
    throws IOException {
    while (true) {
      DatanodeInfo[] nodes = block.getLocations();
      DatanodeInfo chosenNode = null;
      try {
        chosenNode = DFSClient.bestNode(nodes, deadNodes);
        InetSocketAddress targetAddr =
                          NetUtils.createSocketAddr(chosenNode.getName());
        return new DNAddrPair(chosenNode, targetAddr);
      } catch (IOException ie) {
        int failureTimes = DFSClient.dfsInputStreamfailures.get();
        String blockInfo = block.getBlock() + " file=" + src;
        if (failureTimes >= dfsClient.maxBlockAcquireFailures
            || failureTimes >= block.getLocations().length) {
          throw new BlockMissingException(src, "Could not obtain block: " + 
              blockInfo, block.getStartOffset());
        }

        if (nodes == null || nodes.length == 0) {
          DFSClient.LOG.info("No node available for block: " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain block " + block.getBlock() +
                 " from node:  " +
                 (chosenNode == null ? "" : chosenNode.getHostName()) + ie +
                 ". Will get new block locations from namenode and retry...");       
        try {
          // Introducing a random factor to the wait time before another retry.
          // The wait time is dependent on # of failures and a random factor.
          // At the first time of getting a BlockMissingException, the wait time
          // is a random number between 0..3000 ms. If the first retry
          // still fails, we will wait 3000 ms grace period before the 2nd retry.
          // Also at the second retry, the waiting window is expanded to 6000 ms
          // alleviating the request rate from the server. Similarly the 3rd retry
          // will wait 6000ms grace period before retry and the waiting window is
          // expanded to 9000ms.
          // waitTime = grace period for the last round of attempt + 
          // expanding time window for each failure
          double waitTime = timeWindow * failureTimes + 
            timeWindow * (failureTimes + 1) * DFSClient.r.nextDouble(); 
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failureTimes + 1) + 
              " IOException, will wait for " + waitTime + " msec.", ie);
						Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {
        }
        // Following statements seem to make the method thread unsafe, but
        // not all the callers hold the lock to call the method. Shall we
        // fix it?
        //
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo();
        block = getBlockAt(block.getStartOffset(), false, true);
        DFSClient.dfsInputStreamfailures.set(failureTimes+1);
        continue;
      }
    }
  }

  private void fetchBlockByteRange(LocatedBlock block, long start,
                                   long end, byte[] buf, int offset) throws IOException {
    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    Socket dn = null;

    while (true) {
      // cached block locations may have been updated by chooseDatNode()
      // or fetchBlockAt(). Always get the latest list of locations at the
      // start of the loop.
      block = getBlockAt(block.getStartOffset(), false, true);
      DNAddrPair retval = chooseDataNode(block);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      BlockReader reader = null;
      int len = (int) (end - start + 1);
      
      try {
         if (DFSClient.LOG.isDebugEnabled()) {
           DFSClient.LOG.debug("fetchBlockByteRange shortCircuitLocalReads " +
                    dfsClient.shortCircuitLocalReads +
                    " localhst " + dfsClient.localHost +
                    " targetAddr " + targetAddr);
         }
         // first try reading the block locally.
         if (dfsClient.shortCircuitLocalReads && 
             NetUtils.isLocalAddressWithCaching(targetAddr.getAddress())) {
           reader = BlockReaderLocal.newBlockReader(dfsClient.conf, src,
                                                namespaceId, block.getBlock(),
                                                chosenNode,
                                                start,
                                                len,
                                                dfsClient.metrics,
                                                verifyChecksum,
                                                this.clearOsBuffer,
                                                false);
           reader.setReadLocal(true);
           reader.setFsStats(dfsClient.stats);
          } else {
            // go to the datanode

            dn = dfsClient.socketFactory.createSocket();
            NetUtils.connect(dn, targetAddr, dfsClient.socketTimeout,
                dfsClient.ipTosValue);
            dn.setSoTimeout(dfsClient.socketTimeout);
            reader = BlockReader.newBlockReader(dfsClient.getDataTransferProtocolVersion(),
                                            namespaceId,
                                            dn, src,
                                            block.getBlock().getBlockId(),
                                            block.getBlock().getGenerationStamp(),
                                            start, len, buffersize,
                                            verifyChecksum, dfsClient.clientName,
                                            dfsClient.minReadSpeedBps);
            boolean isLocalHost = NetUtils.isLocalAddressWithCaching(targetAddr
              .getAddress());
            reader.setReadLocal(isLocalHost);
            if (!isLocalHost) {
              reader.setReadRackLocal(
                  dfsClient.isInLocalRack(targetAddr.getAddress()));
            }
            reader.setFsStats(dfsClient.stats);
          }
          int nread = reader.readAll(buf, offset, len);
          if (nread != len) {
            throw new IOException("truncated return from reader.read(): " +
                                  "excpected " + len + ", got " + nread);
          }
          return;
      } catch (ChecksumException e) {
        DFSClient.LOG.warn("fetchBlockByteRange(). Got a checksum exception for " +
                 src + " at " + block.getBlock() + ":" +
                 e.getPos() + " from " + chosenNode.getName());
        dfsClient.reportChecksumFailure(src, block.getBlock(), chosenNode);
      } catch (IOException e) {
        DFSClient.LOG.warn("Failed to connect to " + targetAddr +
                 " for file " + src +
                 " for block " + block.getBlock().getBlockId() + ":"  +
                 StringUtils.stringifyException(e));
      } finally {
        IOUtils.closeStream(reader);
        IOUtils.closeSocket(dn);
      }
      // Put chosen node into dead list, continue
      addToDeadNodes(chosenNode);
    }
  }

  /**
   * This is highly optimized for preads. Reduce number of buffercopies.
   * Its is similar to doing a scatter/gather kind of io, all data to be
   * returned in a ByteBuffer.
   */
  private ByteBuffer fetchBlockByteRangeScatterGather(LocatedBlock block, 
                      long start, long len) throws IOException {
    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    Socket dn = null;

    while (true) {
      // cached block locations may have been updated by chooseDatNode()
      // or fetchBlockAt(). Always get the latest list of locations at the
      // start of the loop.
      block = getBlockAt(block.getStartOffset(), false, true);
      DNAddrPair retval = chooseDataNode(block);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      ByteBuffer result = null;
      BlockReaderLocal localReader = null;
      BlockReaderAccelerator remoteReader = null;

       try {
         if (DFSClient.LOG.isDebugEnabled()) {
           DFSClient.LOG.debug("fetchBlockByteRangeScatterGather " +
                    " localhst " + dfsClient.localHost +
                    " targetAddr " + targetAddr);
         }
         
         // first try reading the block locally.
         if (dfsClient.shortCircuitLocalReads && 
             NetUtils.isLocalAddressWithCaching(targetAddr.getAddress())) {
           localReader = BlockReaderLocal.newBlockReader(dfsClient.conf, src,
                                                namespaceId, block.getBlock(),
                                                chosenNode,
                                                start,
                                                len,
                                                dfsClient.metrics,
                                                verifyChecksum,
                                                this.clearOsBuffer,
                                                false);
           localReader.setReadLocal(true);
           localReader.setFsStats(dfsClient.stats);
           result = localReader.readAll();

         } else {
         
           // go to the datanode
           dn = dfsClient.socketFactory.createSocket();
           NetUtils.connect(dn, targetAddr, dfsClient.socketTimeout,
               dfsClient.ipTosValue);
           dn.setSoTimeout(dfsClient.socketTimeout);
           remoteReader = new BlockReaderAccelerator(dfsClient.conf,
                                          targetAddr,
                                          chosenNode,
                                          dfsClient.getDataTransferProtocolVersion(),
                                          namespaceId, dfsClient.clientName,
                                          dn, src,
                                          block,
                                          start, len,
                                          verifyChecksum, dfsClient.metrics);
           result = remoteReader.readAll();
          }
          if (result.remaining() != len) {
            throw new IOException("truncated return from reader.read(): " +
                                "expected " + len + ", got " + 
                                  result.remaining());
          }
          if (NetUtils.isLocalAddressWithCaching(targetAddr.getAddress())) {
            dfsClient.stats.incrementLocalBytesRead(len);
            dfsClient.stats.incrementRackLocalBytesRead(len);
          } else if (dfsClient.isInLocalRack(targetAddr.getAddress())) {
            dfsClient.stats.incrementRackLocalBytesRead(len);
          }

          return result;
      } catch (ChecksumException e) {
        DFSClient.LOG.warn("fetchBlockByteRangeScatterGather(). Got a checksum exception for " +
                 src + " at " + block.getBlock() + ":" +
                 e.getPos() + " from " + chosenNode.getName());
        dfsClient.reportChecksumFailure(src, block.getBlock(), chosenNode);
      } catch (IOException e) {
        DFSClient.LOG.warn("Failed to connect to " + targetAddr +
                 " for file " + src +
                 " for block " + block.getBlock().getBlockId() + ":"  +
                 StringUtils.stringifyException(e));
      } finally {
        IOUtils.closeStream(localReader);
        IOUtils.closeStream(remoteReader);
        IOUtils.closeSocket(dn);
      }
       dfsClient.incReadExpCntToStats();
      // Put chosen node into dead list, continue
      addToDeadNodes(chosenNode);
    }
  }
  
  /**
   * Check whether the pread operation qualifies the local read using cached
   * local block reader pool and execute the read if possible. The condition is:
   * 
   * 1. short circuit read is enabled 2. checksum verification is disabled 3.
   * Only read from one block.
   * 
   * (Condition 2 and 3 can be potentially relaxed as improvements)
   * 
   * It will first check whether there is a local block reader cached for the
   * block. Otherwise, it first tries to determine whether the block qualifies
   * local short circuit read. If yes, create a local block reader and insert it
   * to the local block reader map. Finally, it reads data using the block
   * reader from the cached or created local block reader.
   * 
   * @param blockRange
   * @param position
   * @param buffer
   * @param offset
   * @param length
   * @param realLen
   * @param startTime
   * @return true if the data is read using local block reader pool. Otherwise,
   *         false.
   * @throws IOException
   */
  private boolean tryPreadFromLocal(List<LocatedBlock> blockRange,
      long position, byte[] buffer, int offset, int length, int realLen,
      long startTime) throws IOException {
    if (!dfsClient.shortCircuitLocalReads || verifyChecksum
        || blockRange.size() != 1) {
      return false;
    }
    // Try to optimize by using cached local block readers.
    LocatedBlock lb = blockRange.get(0);
    BlockReaderLocal brl = localBlockReaders.get(lb.getStartOffset());
    if (brl == null) {
      // Try to create a new local block reader for the block and
      // put it to the block reader map.
      DNAddrPair retval = chooseDataNode(lb);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      if (NetUtils.isLocalAddressWithCaching(targetAddr.getAddress())) {
        BlockReaderLocal newBrl = BlockReaderLocal.newBlockReader(
            dfsClient.conf, src, namespaceId, lb.getBlock(), chosenNode,
            startTime, realLen, dfsClient.metrics, verifyChecksum,
            this.clearOsBuffer, true);
        newBrl.setReadLocal(true);
        newBrl.setFsStats(dfsClient.stats);

        // Put the new block reader to the map if another thread didn't put
        // another one for the same block in it. Otherwise, reuse the one
        // created by another thread and close the current one.
        //
        brl = localBlockReaders.putIfAbsent(lb.getStartOffset(), newBrl);
        if (brl == null) {
          brl = newBrl;
        } else {
          newBrl.close();
        }
      }
    }
    if (brl != null) {
      int nread = -1;
      try {
        nread = brl.read(position - lb.getStartOffset(), buffer, offset,
            realLen);
      } catch (IOException ioe) {
        Log.warn("Exception when issuing local read", ioe);
        // We are conservative here so for any exception, we close the
        // local block reader and remove it from the reader list to isolate
        // the failure of one reader from future reads.
        localBlockReaders.remove(lb.getStartOffset(), brl);
        brl.close();
        throw ioe;
      }
      if (nread != realLen) {
        throw new IOException("truncated return from reader.read(): "
            + "excpected " + length + ", got " + nread);
      }
    }
    return (brl != null);
  }

  /**
   * Read bytes starting from the specified position.
   *
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   *
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {      
    // sanity checks
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    DFSClient.dfsInputStreamfailures.set(0);
			long start = System.currentTimeMillis();
    long filelen = getFileLength();
    if ((position < 0) || (position >= filelen)) {
      return -1;
    }
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }

    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    
    if (!tryPreadFromLocal(blockRange, position, buffer, offset, length,
        realLen, start)) {
      // non-local or multiple block read.
      int remaining = realLen;
      for (LocatedBlock blk : blockRange) {
        long targetStart = position - blk.getStartOffset();
        long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
        fetchBlockByteRange(blk, targetStart,
                            targetStart + bytesToRead - 1, buffer, offset);
        remaining -= bytesToRead;
        position += bytesToRead;
        offset += bytesToRead;
      }
      assert remaining == 0 : "Wrong number of bytes read.";
    }
    
    if (dfsClient.stats != null) {
      dfsClient.stats.incrementBytesRead(realLen);
    }
    long timeval = System.currentTimeMillis() - start;
    dfsClient.metrics.incPreadTime(timeval);
    dfsClient.metrics.incPreadSize(realLen);
    dfsClient.metrics.incPreadOps();
    return realLen;
  }

  /**
   * Read bytes starting from the specified position. This is optimized
   * for fast preads from an application with minimum of buffer copies.
   *
   * @param position start read from this position
   * @param length number of bytes to read
   *
   * @return A list of Byte Buffers that represent all the data that was
   * read from the underlying system.
   */
  @Override
  public List<ByteBuffer> readFullyScatterGather(long position, int length)
    throws IOException {      

    // if the server does not support scatter-gather, 
    // then use default implementation from FSDataInputStream.
    if (dfsClient.dataTransferVersion < DataTransferProtocol.SCATTERGATHER_VERSION) {
      return super.readFullyScatterGather(position, length);
    }
    // sanity checks
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    DFSClient.dfsInputStreamfailures.set(0);
    long start = System.currentTimeMillis();
    long filelen = getFileLength();
    if ((position < 0) || (position > filelen)) {
      String msg = " Invalid position " + position +
                   ". File " + src + " is of size " + filelen;
      DFSClient.LOG.warn(msg);
      throw new IOException(msg);
    }
    List<ByteBuffer> results = new LinkedList<ByteBuffer>();
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }
    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    int remaining = realLen;
    for (LocatedBlock blk : blockRange) {
      long targetStart = position - blk.getStartOffset();
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      ByteBuffer bb = fetchBlockByteRangeScatterGather(blk, targetStart,
                          bytesToRead);
      results.add(bb);
      remaining -= bytesToRead;
      position += bytesToRead;
    }
    assert remaining == 0 : "Wrong number of bytes read.";
    if (dfsClient.stats != null) {
      dfsClient.stats.incrementBytesRead(realLen);
    }
    long timeval = System.currentTimeMillis() - start;
    dfsClient.metrics.incPreadTime(timeval);
    dfsClient.metrics.incPreadSize(realLen);
    dfsClient.metrics.incPreadOps();
    return results;
  }

  @Override
  public long skip(long n) throws IOException {
    if ( n > 0 ) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if( n+curPos > fileLen ) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new IOException("Cannot seek after EOF");
    }
    boolean done = false;
    if (pos <= targetPos && targetPos <= blockEnd) {
      //
      // If this seek is to a positive position in the current
      // block, and this piece of data might already be lying in
      // the TCP buffer, then just eat up the intervening data.
      //
      int diff = (int)(targetPos - pos);
      if (diff <= DFSClient.TCP_WINDOW_SIZE) {
        try {
          pos += blockReader.skip(diff);
          if (pos == targetPos) {
            done = true;
          }
        } catch (IOException e) {//make following read to retry
          dfsClient.incReadExpCntToStats();

          DFSClient.LOG.debug("Exception while seek to " + targetPos + " from "
                    + currentBlock +" of " + src + " from " + currentNode +
                    ": " + StringUtils.stringifyException(e));
        }
      }
    }
    if (!done) {
      pos = targetPos;
      blockEnd = -1;
    }
  }

  /**
   * Same as {@link #seekToNewSource(long)} except that it does not exclude
   * the current datanode and might connect to the same node.
   */
  private synchronized boolean seekToBlockSource(long targetPos,
      boolean throwWhenNotFound) throws IOException {
    currentNode = blockSeekTo(targetPos, throwWhenNotFound);
    return true;
  }

  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true.
   * If another node could not be found, then returns false.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return seekToNewSource(targetPos, true);
  }
  
  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true.
   * If another node could not be found, then returns false.
   */
  public synchronized boolean seekToNewSource(long targetPos,
      boolean throwWhenNotFound) throws IOException {
    boolean markedDead = deadNodes.containsKey(currentNode);
    addToDeadNodes(currentNode);
    DatanodeInfo oldNode = currentNode;
    DatanodeInfo newNode = blockSeekTo(targetPos, throwWhenNotFound);
    if (!markedDead) {
      /* remove it from deadNodes. blockSeekTo could have cleared
       * deadNodes and added currentNode again. Thats ok. */
      deadNodes.remove(oldNode);
    }
    if (!oldNode.getStorageID().equals(newNode.getStorageID())) {
      currentNode = newNode;
      return true;
    } else {
      return false;
    }
  }    

  /**
   */
  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /**
   * WARNING: This method does not work with files larger than 2GB.
   * Use getFileLength() - getPos() instead.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    long length = getFileLength() - pos;
    
    if (!isUnderConstruction() || length > 0) {
      return (int) length;
    }

    read(DFSClient.emptyByteArray);
    return (int) (getFileLength() - pos);
  }

  /**
   * We definitely don't support marks
   */
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int readLimit) {
  }
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }
  
  /** Utility class to encapsulate data node info and its ip address. */
  private static class DNAddrPair {
    DatanodeInfo info;
    InetSocketAddress addr;
    DNAddrPair(DatanodeInfo info, InetSocketAddress addr) {
      this.info = info;
      this.addr = addr;
    }
  }
}
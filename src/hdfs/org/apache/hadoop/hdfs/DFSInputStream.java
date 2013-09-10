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
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSClientReadProfilingData;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient.DataNodeSlowException;
import org.apache.hadoop.hdfs.profiling.DFSReadProfilingData;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
public class DFSInputStream extends FSInputStream {
  private final long DEFAULT_EXPIRATION_WINDOW_IN_MSECS = 20 * 60 * 1000L;
  private final long DEFAULT_DEAD_NODES_LIST_UPDATE_INTERVAL_IN_MSECS = 60000L;

  private final long NOT_WAITING_FOR_MORE_BYTES = -1;
  private boolean closed = false;
  
  private final SocketCache socketCache;
  private final DFSClient dfsClient;

  private String src = null;
  private long prefetchSize;
  private BlockReader blockReader = null;
  private ConcurrentHashMap<Long, BlockReaderLocalBase> localBlockReaders =
      new ConcurrentHashMap<Long, BlockReaderLocalBase>(0, 0.75f, 2);
  private boolean verifyChecksum;
  private boolean clearOsBuffer;
  private volatile DFSLocatedBlocks locatedBlocks = null;
  private volatile DatanodeInfo currentNode = null;
  private volatile Block currentBlock = null;
  private boolean isCurrentBlockUnderConstruction;
  private long pos = 0;
  private long blockEnd = -1;
  private LocatedBlocks blocks = null;

  private int timeWindow = 3000; // wait time window (in msec) if BlockMissingException is caught
  private volatile long timeDeadNodesListLastChecked = 0; // time (in msec) we last updated the deadNodes List
  private Lock timeDeadNodesListLastCheckedLock = new ReentrantLock(); // lock to protect access to timeDeadNodesListLastChecked
  private long deadNodesListUpdateInterval;  // interval (in msec) for checking the deadNodes list
  long expirationTimeWindow; // time (in msec) after which the dead node is removed from the deadNodes list
  private int locatedBlockExpireTimeout = -1;  // interval (in msec) for checking the deadNodes list

  private long timeoutGetAvailFromDatanode = 600000L;
  private long timeShowNoAvailableByte = NOT_WAITING_FOR_MORE_BYTES;
  
  /* XXX Use of CocurrentHashMap is temp fix. Need to fix
   * parallel accesses to DFSInputStream (through ptreads) properly.
   * Its a map of DatanodeInfo -> timeInMilliSecs. The entries in the map are
   * updated on every read() call, where if the entry was made earlier than
   * EXPIRATION_WINDOW_IN_MILLISECS, the entry is removed from the map. */
  ConcurrentHashMap<DatanodeInfo, Long> deadNodes =
             new ConcurrentHashMap<DatanodeInfo, Long>(0, 0.75f, 1);
  Collection<DatanodeInfo> unfavoredNodes = null;
  private int buffersize = 1;

  private byte[] oneByteBuf = new byte[1]; // used for 'int read()'
  private int nCachedConnRetry;
  
  private FSClientReadProfilingData cliData = null;

  private ReadOptions options = new ReadOptions();

  void addToDeadNodes(DatanodeInfo dnInfo) {
    InjectionHandler
        .processEvent(InjectionEvent.DFSCLIENT_BEFORE_ADD_DEADNODES);
    deadNodes.put(dnInfo, System.currentTimeMillis());
  }
  
  synchronized void setUnfavoredNodes(Collection<DatanodeInfo> unfavoredNodes) {
    this.unfavoredNodes = unfavoredNodes;
    // Force to choose another data node for next read()
    blockEnd = -1;
  }

  void updateDeadNodesListPeriodically() {
    final Long currentTime = System.currentTimeMillis();
    if ((currentTime - timeDeadNodesListLastChecked) >
        deadNodesListUpdateInterval) {
      // If somebody else has the lock then just return
      if (timeDeadNodesListLastCheckedLock.tryLock()) {
        try {
          updateDeadNodesList(currentTime);
          this.timeDeadNodesListLastChecked = currentTime;
        }
        finally {
          timeDeadNodesListLastCheckedLock.unlock();
        }
      }
    }
  }

  void updateDeadNodesList(long currentTime) {
    Iterator<ConcurrentHashMap.Entry<DatanodeInfo, Long>> iterator =
      this.deadNodes.entrySet().iterator();
    ConcurrentHashMap.Entry<DatanodeInfo, Long> entry = null;
    while (iterator.hasNext()) {
      entry = iterator.next();
      if ((currentTime - entry.getValue()) > expirationTimeWindow) {
        iterator.remove();
      }
    }
  }

  DFSInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum,
                 boolean clearOsBuffer, ReadOptions options) throws IOException {
    this.src = src;
    this.dfsClient = dfsClient;    
    this.socketCache = dfsClient.socketCache;
    this.options = options;
    init(buffersize, verifyChecksum, clearOsBuffer);
  }

  /**
   * Used for snapshot
   */
  DFSInputStream(DFSClient dfsClient, LocatedBlocksWithMetaInfo blocks, int buffersize,
      boolean verifyChecksum) throws IOException {
    this.blocks = blocks;
    this.dfsClient = dfsClient;
    this.socketCache = dfsClient.socketCache;
    
    dfsClient.updateNamespaceIdIfNeeded(blocks.getNamespaceID());
    dfsClient.updateDataTransferProtocolVersionIfNeeded(blocks.getDataProtocolVersion());
    dfsClient.getNewNameNodeIfNeeded(blocks.getMethodFingerPrint());
    init(buffersize, verifyChecksum, false);
  }
  
  private int getBlockExpireTime() {
    int blockExpireTimeout = dfsClient.conf.getInt(
        "dfs.client.locatedblock.expire.timeout",
        DFSLocatedBlocks.DEFAULT_BLOCK_EXPIRE_TIME);

    if (blockExpireTimeout > 0) {
      int blockExpireTimeoutRandom = dfsClient.conf.getInt(
          "dfs.client.locatedblock.expire.random.timeout", -1);

      if (blockExpireTimeoutRandom == -1) {
        blockExpireTimeoutRandom =
            (int) ((double) blockExpireTimeout *
                DFSLocatedBlocks.DEFAULT_BLOCK_EXPIRE_RANDOM_FACTOR);
      }
      if (blockExpireTimeoutRandom > 0) {
        blockExpireTimeout += new Random().nextInt(blockExpireTimeoutRandom);
      }
    }
    return blockExpireTimeout;
  }

  String getFileName() {
    return this.src;
  }


  private void init(int buffersize, boolean verifyChecksum,
                    boolean clearOsBuffer) throws IOException {
    this.verifyChecksum = verifyChecksum;
    this.buffersize = buffersize;
    this.clearOsBuffer = clearOsBuffer;
    prefetchSize = dfsClient.conf.getLong("dfs.read.prefetch.size",
    		10 * dfsClient.defaultBlockSize);
    this.timeoutGetAvailFromDatanode = dfsClient.conf.getLong(
        "dfs.timeout.get.available.from.datanode", 600000L);
    this.expirationTimeWindow = dfsClient.conf.getLong("dfs.deadnodes.expiration.window",
        DEFAULT_EXPIRATION_WINDOW_IN_MSECS);
    this.deadNodesListUpdateInterval = dfsClient.conf.getLong("dfs.deadnodes.update.interval",
        DEFAULT_DEAD_NODES_LIST_UPDATE_INTERVAL_IN_MSECS);
    this.timeDeadNodesListLastChecked = System.currentTimeMillis();
    
    this.locatedBlockExpireTimeout = getBlockExpireTime();
    timeWindow = dfsClient.conf.getInt(
    		"dfs.client.baseTimeWindow.waitOn.BlockMissingException",
    		timeWindow);
    nCachedConnRetry = dfsClient.conf.getInt(
        DFSClient.DFS_CLIENT_CACHED_CONN_RETRY_KEY, 
        DFSClient.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);

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
      throw new IOException("No file provided to open");
    }

    LocatedBlocks newInfo = src != null ? 
                            getLocatedBlocks(src, 0, prefetchSize) : blocks;
                            
    if (newInfo == null) {
      throw new FileNotFoundException("Cannot open filename " + src);
    }

    // I think this check is not correct. A file could have been appended to
    // between two calls to openInfo().
    if (locatedBlocks != null && !locatedBlocks.isUnderConstruction() &&
        !newInfo.isUnderConstruction()) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocksCopy()
          .iterator();
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
    this.locatedBlocks = new DFSLocatedBlocks(newInfo, locatedBlockExpireTimeout);
    this.currentNode = null;
    if (!newInfo.isUnderConstruction()) {
      // No block should be under construction if the file is finalized.
      isCurrentBlockUnderConstruction = false;
    }
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
    InjectionHandler.processEventIO(InjectionEvent.DFSCLIENT_GET_LOCATED_BLOCKS);
    
    try {
      if (dfsClient.namenodeProtocolProxy != null &&
          dfsClient.namenodeProtocolProxy.isMethodSupported("openAndFetchMetaInfo",
              String.class, long.class, long.class)) {
        LocatedBlocksWithMetaInfo locs = 
            dfsClient.namenode.openAndFetchMetaInfo(src, start, length);
        if (locs != null) {
          dfsClient.updateNamespaceIdIfNeeded(locs.getNamespaceID());
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
              dfsClient.namespaceId, locatedblock.getBlock());
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
      DFSClient.LOG
          .warn("Cannot get block info from any datanode having block "
              + locatedblock.getBlock() + " for file " + src);
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
   * If updatePosition is true, it is required that the the method is
   * called inside the synchronized block, since the current block information
   * will be updated.
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
    locatedBlocks.blockLocationInfoExpiresIfNeeded();
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

    locatedBlocks.blockLocationInfoExpiresIfNeeded();

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
  
  class BlockSeekContext{
    LocatedBlock targetBlock;

    LocatedBlock getTargetBlock() {
      return targetBlock;
    }

    void setTargetBlock(LocatedBlock targetBlock) {
      this.targetBlock = targetBlock;
    }

    BlockSeekContext(LocatedBlock targetBlock) {
      this.targetBlock = targetBlock;
    }
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

    // Will be getting a new BlockReader.
    if ( blockReader != null ) {
      closeBlockReader(blockReader, false);
      blockReader = null;
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
    BlockSeekContext seekContext = new BlockSeekContext(targetBlock);
    while (true) {
      DNAddrPair retval = chooseDataNode(seekContext);
      chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      final InetAddress chosenDataNodeAddress = targetAddr.getAddress();
      final boolean isChosenDatanodeLocal = NetUtils.isLocalAddressWithCaching(chosenDataNodeAddress)
          || (dfsClient.localHost != null && dfsClient.localHost.equals(chosenDataNodeAddress));
      
      // try reading the block locally. if this fails, then go via
      // the datanode
      Block blk = seekContext.getTargetBlock().getBlock();
      try {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.warn("blockSeekTo shortCircuitLocalReads "
                   + dfsClient.shortCircuitLocalReads +
                   " localhost " + dfsClient.localHost +
                   " targetAddr " + targetAddr);
        }
        if (dfsClient.shortCircuitLocalReads && isChosenDatanodeLocal) {
          blockReader = BlockReaderLocalBase.newBlockReader(dfsClient.conf, src,
                                                 dfsClient.namespaceId, blk,
                                                 chosenNode,
                                                 offsetIntoBlock,
                                                 blk.getNumBytes() - offsetIntoBlock,
                                                 dfsClient.metrics, 
                                                 this.verifyChecksum,
                                                 this.clearOsBuffer,
                                                 false);
          if (blockReader != null) {
            blockReader.setReadLocal(true);
            blockReader.setFsStats(dfsClient.stats);
            if (cliData != null) {
              cliData.recordReadBlockSeekToTime();
            }
            return chosenNode;
          } else if (!dfsClient.shortcircuitDisableWhenFail) {
            throw new IOException(
                "Short circuit local read not supported for inline checksum file.");
          }
        }
      } catch (IOException ex) {
        DFSClient.LOG.info("Failed to read block " + seekContext.getTargetBlock().getBlock() +
                 " on local machine " + dfsClient.localHost +
                 ". Try via the datanode on " + targetAddr + ":"
                  + StringUtils.stringifyException(ex));
      }

      try {
        long minReadSpeedBps = (dfsClient.numNodeLeft(seekContext.getTargetBlock().getLocations(),
            deadNodes) > 1) ? dfsClient.minReadSpeedBps : -1;
        blockReader = getBlockReader(
            dfsClient.getDataTransferProtocolVersion(), dfsClient.namespaceId,
            targetAddr, src, blk.getBlockId(),
            blk.getGenerationStamp(),
            offsetIntoBlock, blk.getNumBytes() - offsetIntoBlock,
            buffersize, verifyChecksum,
            dfsClient.clientName, dfsClient.bytesToCheckReadSpeed,
            minReadSpeedBps, false, cliData);
        blockReader.setReadLocal(isChosenDatanodeLocal);
        blockReader.enableThrowForSlow = dfsClient.enableThrowForSlow;
        if (!isChosenDatanodeLocal) {
          blockReader
              .setReadRackLocal(dfsClient.isInLocalRack(targetAddr));
        }
        blockReader.setFsStats(dfsClient.stats);
        if (cliData != null) {
          cliData.recordReadBlockSeekToTime();
        }
        return chosenNode;
      } catch (IOException ex) {
        // Put chosen node into dead list, continue
        DFSClient.LOG.warn("Failed to connect to " + targetAddr, ex);
        addToDeadNodes(chosenNode);
      }
    }
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      if (closed) {
        return;
      }
      dfsClient.checkOpen();

      if (blockReader != null) {
        closeBlockReader(blockReader, false);
        blockReader = null;
      }

      for (BlockReaderLocalBase brl : localBlockReaders.values()) {
        try {
          brl.close();
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Error when closing local block reader", ioe);
        }
      }
      localBlockReaders = null;

      super.close();
      closed = true;
    } finally {
      // Avoid memory leak by making sure we remove the file from the renewal
      // threads map even in case of failures during close.
      if (dfsClient.blockLocationRenewal != null) {
        dfsClient.blockLocationRenewal.remove(this);
      }
    }
  }

  @Override
  public synchronized int read() throws IOException {
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksumFileSystem
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
        InjectionHandler.processEventIO(InjectionEvent.DFSCLIENT_READBUFFER_BEFORE,
            deadNodes, locatedBlocks);
        
        int bytesRead = blockReader.read(buf, off, len);

        InjectionHandler.processEventIO(
            InjectionEvent.DFSCLIENT_READBUFFER_AFTER, blockReader);

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
        if (cliData != null) {
          cliData.recordReadBufferTime();
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
          } else if (pos > blockEnd
              || (this.isCurrentBlockUnderConstruction && blockReader != null
                  && blockReader.eos && blockReader.available() == 0)) {
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
        } catch (InterruptedIOException iie) {
          throw iie;
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
            if (len != 0) {
              throw e;
            } else {
              // When called by available(). No need to fail the query. In that case
              // available() value might not be updated, but it's OK.
              return 0;
            }
          }
        }
      }
    }
    return -1;
  }


  private DNAddrPair chooseDataNode(BlockSeekContext seekContext)
    throws IOException {
    return chooseDataNode(seekContext, null);
  }

  private DNAddrPair chooseDataNode(BlockSeekContext seekContext,
                              Collection<DatanodeInfo> ignoredNodes)
    throws IOException {
    updateDeadNodesListPeriodically();

    while (true) {
      DatanodeInfo[] nodes = seekContext.getTargetBlock().getLocations();
      DatanodeInfo chosenNode = bestNode(nodes, deadNodes, unfavoredNodes, ignoredNodes);
      if (chosenNode != null) {
        InetSocketAddress targetAddr =
            NetUtils.createSocketAddr(chosenNode.getName());
        return new DNAddrPair(chosenNode, targetAddr);
      } else {
        String errMsg = getBestNodeErrorString(nodes, deadNodes, unfavoredNodes, ignoredNodes);
        
        int failureTimes = DFSClient.dfsInputStreamfailures.get();
        String blockInfo = seekContext.getTargetBlock().getBlock() + " file="
            + src;
        if (failureTimes >= dfsClient.maxBlockAcquireFailures
            || failureTimes >= seekContext.getTargetBlock().getLocations().length) {
          DFSClient.LOG.warn("Could not obtain block "
              + seekContext.getTargetBlock().getBlock()
              + errMsg + ". Throw a BlockMissingException");
          throw new BlockMissingException(src, "Could not obtain block: "
              + blockInfo, seekContext.getTargetBlock().getStartOffset());
        }

        if (nodes == null || nodes.length == 0) {
          DFSClient.LOG.info("No node available for block: " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain block "
            + seekContext.getTargetBlock().getBlock() + " from node:  "
            + (chosenNode == null ? "" : chosenNode.getHostName()) + " " + errMsg
            + ". Will get new block locations from namenode and retry...");
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
              " IOException, will wait for " + waitTime + " msec. " + errMsg);
						Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {
          throw new InterruptedIOException();
        }
        // Following statements seem to make the method thread unsafe, but
        // not all the callers hold the lock to call the method. Shall we
        // fix it?
        //
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo();
        seekContext.setTargetBlock(getBlockAt(seekContext.getTargetBlock()
            .getStartOffset(), false, true));
        DFSClient.dfsInputStreamfailures.set(failureTimes+1);
        continue;
      }
    }
  }

  private Callable<ByteBuffer> getFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
      final ByteBuffer bb, final int trial, 
      final FSClientReadProfilingData cliData,
      final ReadOptions options) {
    return new Callable<ByteBuffer>() {
      public ByteBuffer call() throws IOException {
        byte []buf = bb.array();
        int offset = bb.position();
        
        try {
          actualGetFromOneDataNode(datanode, block, start, end, buf, offset,
              cliData, options);
        } finally {
          if (cliData != null) {
            cliData.endRead();
          }
        }
        return bb;
      }
    };
  }
  
  private void actualGetFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
      byte []buf, int offset, FSClientReadProfilingData cliData,
      ReadOptions options)
          throws IOException {
    
    InjectionHandler.processEvent(InjectionEvent.DFSCLIENT_START_FETCH_FROM_DATANODE);
    
    boolean success = false;
    DatanodeInfo chosenNode = datanode.info;
    InetSocketAddress targetAddr = datanode.addr;
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
      if (dfsClient.shortCircuitLocalReads && NetUtils.isLocalAddress(targetAddr.getAddress())) {
        reader = BlockReaderLocalBase.newBlockReader(dfsClient.conf, src,
                                             dfsClient.namespaceId, block.getBlock(),
                                             chosenNode,
                                             start,
                                             len,
                                             dfsClient.metrics,
                                             verifyChecksum,
                                             this.clearOsBuffer,
                                             false);
      
        if (reader != null) {
           reader.setReadLocal(true);
           reader.setFsStats(dfsClient.stats);
        } else if (!dfsClient.shortcircuitDisableWhenFail) {
          throw new IOException(
              "Short circuit local read not supported for this scase");
        }
      }
      if (reader == null) {
        // go to the datanode
        reader = getBlockReader(dfsClient.getDataTransferProtocolVersion(),
            dfsClient.namespaceId,
            targetAddr, src,
            block.getBlock().getBlockId(),
            block.getBlock().getGenerationStamp(),
            start, len, buffersize,
            verifyChecksum, dfsClient.clientName,
            dfsClient.bytesToCheckReadSpeed,
            dfsClient.minReadSpeedBps, true,
            cliData, options);
        boolean isLocalHost = NetUtils.isLocalAddressWithCaching(targetAddr
          .getAddress());
        reader.setReadLocal(isLocalHost);
        if (!isLocalHost) {
          reader.setReadRackLocal(
              dfsClient.isInLocalRack(targetAddr));
        }
        reader.setFsStats(dfsClient.stats);
        if (cliData != null) {
          cliData.recordPreadGetBlockReaderTime();
        }
      }
      int nread = reader.readAll(buf, offset, len);
      if (cliData != null) {
        cliData.recordPreadAllTime();
      }
      if (nread != len) {
        throw new IOException("truncated return from reader.read(): " +
                              "excpected " + len + ", got " + nread);
      }
      
      success = true;
    } catch (ChecksumException e) {
      DFSClient.LOG.warn("getFromOneDataNode(). Got a checksum exception for " +
               src + " at " + block.getBlock() + ":" +
               e.getPos() + " from " + chosenNode.getName());
      dfsClient.reportChecksumFailure(src, block.getBlock(), chosenNode);
      throw e;
    } catch (IOException e) {
      DFSClient.LOG.warn("Failed to connect to " + targetAddr +
               " for file " + src +
               " for block " + block.getBlock().getBlockId() + ":"  +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (reader != null) {
        closeBlockReader(reader, true);
      }
      
      // Put chosen node into dead list, continue
      if (!success) {
        addToDeadNodes(chosenNode);
      }
    }
  }

  private void fetchBlockByteRangeSpeculative(LocatedBlock block, long start,
                                   long end, byte[] buf, int offset,
                                   ReadOptions options) throws IOException {
    
    ArrayList<Future<ByteBuffer>> futures = null;
    ArrayList<DatanodeInfo> ignored = null;
    int trial = 0;
    ByteBuffer bb = null;

    int len = (int) (end - start + 1);
    BlockSeekContext seekContext = new BlockSeekContext(getBlockAt(
        block.getStartOffset(), false, true));
    DFSReadProfilingData pData = DFSClient.getDFSReadProfilingData();
    
    while (true) {
      FSClientReadProfilingData cliData = null;
      if (pData != null) {
        cliData = new FSClientReadProfilingData();
        pData.addDFSClientReadProfilingData(cliData);
        cliData.startRead();
      }
      
      DNAddrPair retval = chooseDataNode(seekContext, ignored);
      DatanodeInfo chosenNode = retval.info;

      // futures is null iff there is no request already executing.
      if (futures == null) {
        bb = ByteBuffer.wrap(buf, offset, len);
      }
      else {
        bb = ByteBuffer.allocate(len);
      }
      
      Callable<ByteBuffer> getFromDataNodeCallable = 
          getFromOneDataNode(retval, block, start, end, bb, ++trial, cliData,
              options);
      
      Future<ByteBuffer> future = dfsClient.parallelReadsThreadPool.submit(getFromDataNodeCallable);

      if (futures == null) { 
        try {
          // wait for 500 ms.
          future.get(dfsClient.quorumReadThresholdMillis, TimeUnit.MILLISECONDS);
          return;
        } catch (TimeoutException e) {
          DFSClient.LOG.debug("Waited for " +
              dfsClient.quorumReadThresholdMillis + "ms . Still not complete. Spawning new task");
          if (ignored == null) { 
            ignored = new ArrayList<DatanodeInfo>();
          }
          ignored.add(chosenNode);
          dfsClient.quorumReadMetrics.incParallelReadOps();
          
          futures = new ArrayList<Future<ByteBuffer>>();
          futures.add(future);
          continue; // no need to refresh block locations
        } catch (InterruptedException ie) {
          // Ignore
        } catch (ExecutionException e) {
          // Ignore already logged in the call.
        }
      } else {
        futures.add(future);
        // if not succeded. Submit callables for each datanode
        // in a loop, wait for 50 ms each.
        // get the result from the fastest one.
        try {
          ByteBuffer result = getFirst(futures, 50);
  
          // cancel the rest.
          cancelAll(futures);
  
          if (result.array() != buf) { // compare the array pointers
            dfsClient.quorumReadMetrics.incParallelReadWins();
            System.arraycopy(result.array(),  result.position(), buf, offset, len);
          }
          return;
        } catch (InterruptedException ie) {
          // Ignore
        } catch (ExecutionException e) {
          // exception already handled in the call method.
          // getFirst will remove the failing future from the list.
          // nothing more to do.
        }
        ignored.add(chosenNode);
      }
      
      // executed if we get an error from a data node 
      block = getBlockAt(block.getStartOffset(), false, true);
    }
  }

  private ByteBuffer getFirst(ArrayList<Future<ByteBuffer>> futures, long timeout) 
      throws ExecutionException, InterruptedException {
    while (true) {
      for(Future<ByteBuffer> future: futures) {
        if (future.isDone()) {
          try {
            return future.get();
          } catch (ExecutionException e) {
            // already logged in the Callable
            futures.remove(future);
            throw e;
          }
        }
      }
      Thread.sleep(timeout);
    }
  }

  private void cancelAll(List<Future<ByteBuffer>> futures) {
    for(Future<ByteBuffer> future: futures) {
      future.cancel(true);
    }
  }

  private void fetchBlockByteRange(LocatedBlock block, long start,
                                   long end, byte[] buf, int offset,
                                   ReadOptions options) throws IOException {
    //
    // Connect to best DataNode for desired Block, with potential offset
    //

    // cached block locations may have been updated by chooseDatNode()
    // or fetchBlockAt(). Always get the latest list of locations before the
    // start of the loop.
    BlockSeekContext seekContext = new BlockSeekContext(getBlockAt(
        block.getStartOffset(), false, true));
    if (cliData != null) {
      cliData.recordPreadBlockSeekContextTime();
    }
    while (true) {
      DNAddrPair retval = chooseDataNode(seekContext);
      if (cliData != null) {
        cliData.recordPreadChooseDataNodeTime();
      }
      try {
        actualGetFromOneDataNode(retval, block, start, end, buf, offset,
            cliData, options);
        if (cliData != null) {
          cliData.recordPreadActualGetFromOneDNTime();
        }
        return;
      } catch(InterruptedIOException iie) {
        throw iie;
      } catch (IOException e) {
        // Ignore. Already processed inside the function.
        // Loop through to try the next node.
      }
    }
  }
  
  /**
   * Close the given BlockReader and cache its socket.
   */
  private void closeBlockReader(BlockReader reader, boolean reuseConnection) 
      throws IOException {
    if (reader.hasSentStatusCode()) {
      Socket oldSock = reader.takeSocket();
      if (dfsClient.getDataTransferProtocolVersion() < 
          DataTransferProtocol.READ_REUSE_CONNECTION_VERSION ||
          !reuseConnection) {
          // close the sock for old datanode.
        if (oldSock != null) {
          IOUtils.closeSocket(oldSock);
        }
      } else {
        socketCache.put(oldSock);
      }
    }
    reader.close();
  }
  
  protected BlockReader getBlockReader(int protocolVersion, int namespaceId,
      InetSocketAddress dnAddr, String file, long blockId,
      long generationStamp, long startOffset, long len, int bufferSize,
      boolean verifyChecksum, String clientName, long bytesToCheckReadSpeed,
      long minReadSpeedBps, boolean reuseConnection,
      FSClientReadProfilingData cliData)
      throws IOException {
    return getBlockReader(protocolVersion, namespaceId, dnAddr, file, blockId,
        generationStamp, startOffset, len, bufferSize, verifyChecksum,
        clientName, bytesToCheckReadSpeed, minReadSpeedBps, reuseConnection,
        cliData, options);
  }

  /**
   * Retrieve a BlockReader suitable for reading.
   * This method will reuse the cached connection to the DN if appropriate.
   * Otherwise, it will create a new connection.
   * 
   * @param protocolVersion
   * @param namespaceId
   * @param dnAddr
   * @param file
   * @param blockId
   * @param generationStamp
   * @param startOffset
   * @param len
   * @param bufferSize
   * @param verifyChecksum
   * @param clientName
   * @param minReadSpeedBps
   * @return
   * @throws IOException
   */
  protected BlockReader getBlockReader(int protocolVersion,
                                       int namespaceId,
                                       InetSocketAddress dnAddr,
                                       String file,
                                       long blockId,
                                       long generationStamp,
                                       long startOffset,
                                       long len,
                                       int bufferSize,
                                       boolean verifyChecksum,
                                       String clientName,
                                       long bytesToCheckReadSpeed,
                                       long minReadSpeedBps,
                                       boolean reuseConnection,
      FSClientReadProfilingData cliData, ReadOptions options)
        throws IOException {
    IOException err = null;
    
    boolean fromCache = true;
    
    // back compatible with the old datanode.
    if (protocolVersion < DataTransferProtocol.READ_REUSE_CONNECTION_VERSION ||
        reuseConnection == false) {
      Socket sock = dfsClient.socketFactory.createSocket();
      sock.setTcpNoDelay(true);
      NetUtils.connect(sock, dnAddr, dfsClient.socketTimeout);
      sock.setSoTimeout(dfsClient.socketTimeout);
      
      BlockReader reader = 
          BlockReader.newBlockReader(protocolVersion, namespaceId, sock, src, 
              blockId, generationStamp, startOffset, len, buffersize, 
              verifyChecksum, clientName, bytesToCheckReadSpeed,
              minReadSpeedBps, false, cliData, options);
      return reader;
    }
    
    // Allow retry since there is no way of knowing whether the cached socket
    // is good until we actually use it
    for (int retries = 0; retries <= nCachedConnRetry && fromCache; ++retries) {
      Socket sock = socketCache.get(dnAddr);
      if (sock == null) {
        fromCache = false;
        
        sock = dfsClient.socketFactory.createSocket();
        /**
         * TCP_NODELAY is crucial here because of bad interactions between 
         * Nagle's alglrithm and delayed ACKs. With connection keepalive 
         * between the client and DN, the conversation looks like:
         * 1. Client -> DN: Read block X
         * 2. DN -> client: data for block X;
         * 3. Client -> DN: Status OK (successful read)
         * 4. Client -> DN: Read block Y
         * 
         * The fact that step #3 and #4 are both in the client -> DN direction
         * triggers Nagling. If the DN is using delayed ACKS, this results in
         * a delay of 40ms or more.
         * 
         * TCP_NODELAY disables nagling and thus avoid this performance 
         * disaster.
         */
        sock.setTcpNoDelay(true);
        
        NetUtils.connect(sock, dnAddr, dfsClient.socketTimeout);
        sock.setSoTimeout(dfsClient.socketTimeout);
      }
      
      try {
        // The OP_READ_BLOCK request is sent as we make the BlockReader
        BlockReader reader = 
            BlockReader.newBlockReader(protocolVersion, namespaceId, sock, src, 
                blockId, generationStamp, startOffset, len, buffersize, 
                verifyChecksum, clientName, bytesToCheckReadSpeed,
                minReadSpeedBps, true, cliData, options);
        return reader;
      } catch (IOException ex) {
        // Our socket is no good.
        DFSClient.LOG.debug("Error making BlockReader. Closing stale " + sock, ex);
        sock.close();
        err = ex;
      }
    }
    
    throw err;
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

    // cached block locations may have been updated by chooseDatNode()
    // or fetchBlockAt(). Always get the latest list of locations before the
    // start of the loop.
    BlockSeekContext seekContext = new BlockSeekContext(getBlockAt(
        block.getStartOffset(), false, true));
    while (true) {
      DNAddrPair retval = chooseDataNode(seekContext);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      ByteBuffer result = null;
      BlockReaderLocalBase localReader = null;
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
           localReader = BlockReaderLocalBase.newBlockReader(dfsClient.conf, src,
                                                dfsClient.namespaceId, block.getBlock(),
                                                chosenNode,
                                                start,
                                                len,
                                                dfsClient.metrics,
                                                verifyChecksum,
                                                this.clearOsBuffer,
                                                false);
           if (localReader != null) {
             localReader.setReadLocal(true);
             localReader.setFsStats(dfsClient.stats);
             result = localReader.readAll();
           } else if (!dfsClient.shortcircuitDisableWhenFail) {
            throw new IOException(
                "Short circuit local read not supported for this scase");
           }
         }
         if (localReader == null) {

           // go to the datanode
           dn = dfsClient.socketFactory.createSocket();
           NetUtils.connect(dn, targetAddr, dfsClient.socketTimeout,
               dfsClient.ipTosValue);
           dn.setSoTimeout(dfsClient.socketTimeout);
           remoteReader = new BlockReaderAccelerator(dfsClient.conf,
                                          targetAddr,
                                          chosenNode,
                                          dfsClient.getDataTransferProtocolVersion(),
                                          dfsClient.namespaceId, dfsClient.clientName,
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
          } else if (dfsClient.isInLocalRack(targetAddr)) {
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
      long startTime) throws InterruptedIOException {
    try {
      if (!dfsClient.shortCircuitLocalReads || blockRange.size() != 1) {
        return false; 
      }
      // Try to optimize by using cached local block readers.
      BlockSeekContext seekContext = new BlockSeekContext(blockRange.get(0));
      BlockReaderLocalBase brl = localBlockReaders.get(seekContext
          .getTargetBlock().getStartOffset());
      if (brl == null) {
        // Try to create a new local block reader for the block and
        // put it to the block reader map.
        DNAddrPair retval = chooseDataNode(seekContext);
        DatanodeInfo chosenNode = retval.info;
        InetSocketAddress targetAddr = retval.addr;
        if (NetUtils.isLocalAddressWithCaching(targetAddr.getAddress())) {
          BlockReaderLocalBase newBrl = BlockReaderLocalBase.newBlockReader(
            dfsClient.conf, src, dfsClient.namespaceId, seekContext.getTargetBlock()
                .getBlock(), chosenNode, startTime, realLen, dfsClient.metrics,
            verifyChecksum, this.clearOsBuffer, true);
          newBrl.setReadLocal(true);
          newBrl.setFsStats(dfsClient.stats);

          // Put the new block reader to the map if another thread didn't put
          // another one for the same block in it. Otherwise, reuse the one
          // created by another thread and close the current one.
          //
          brl = localBlockReaders.putIfAbsent(seekContext.getTargetBlock()
              .getStartOffset(), newBrl);
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
          nread = brl.read(position
            - seekContext.getTargetBlock().getStartOffset(), buffer, offset,
            realLen);
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Exception when issuing local read", ioe);
          // We are conservative here so for any exception, we close the
          // local block reader and remove it from the reader list to isolate
          // the failure of one reader from future reads.
          localBlockReaders.remove(seekContext.getTargetBlock().getStartOffset(),
              brl);
          brl.close();
          throw ioe;
        }
        if (nread != realLen) {
          throw new IOException("truncated return from reader.read(): "
              + "excpected " + length + ", got " + nread);
        }
      }
      if (seekContext.getTargetBlock() != blockRange.get(0)) {
        blockRange.set(0, seekContext.getTargetBlock());
      }
      return (brl != null);
    } catch (InterruptedIOException iie) {
      throw iie;
    } catch (IOException ioe) {
      DFSClient.LOG.warn("Local Read short circuit failed", ioe);
      return false;
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return read(position, buffer, offset, length, options);
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
  public int read(long position, byte[] buffer, int offset, int length,
      ReadOptions options)
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
    
    DFSReadProfilingData pData = DFSClient.getDFSReadProfilingData();

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
        if (dfsClient.allowParallelReads && dfsClient.parallelReadsThreadPool != null) {
          fetchBlockByteRangeSpeculative(blk, targetStart,
                              targetStart + bytesToRead - 1, buffer, offset,
                              options);
        } else {
          if (pData != null) {
            cliData = new FSClientReadProfilingData();
            pData.addDFSClientReadProfilingData(cliData);
            cliData.startRead();
          }
          
          fetchBlockByteRange(blk, targetStart,
                              targetStart + bytesToRead - 1, buffer, offset,
                              options);
          
          if (pData != null) {
            cliData.endRead();
          }
        }
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
      } else {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("seek() out of TCP buffer " + " block "
              + currentBlock + " current pos: " + pos + " target pos: "
              + targetPos);
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
    
    long timeNow = System.currentTimeMillis();
    if (timeShowNoAvailableByte != NOT_WAITING_FOR_MORE_BYTES
        && timeNow - timeShowNoAvailableByte > timeoutGetAvailFromDatanode) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG
            .debug("To calculate available(), refetch metedata for file " + src);
      }
      // To avoid the minor case that:
      // (1) we stick to a datanode with a replica with older generation stamp
      // (2) after reopening the stream because of failure the file length is
      // (temporarily) smaller than current pos
      // after available value stays to be zero for a while, we re-fetch all the
      // metadata from the name node.
      //
      // The default value of timeoutGetAvailFromDatanode is set to be
      // relatively long to minimize namenode traffic in the case that
      // the the file is kept open with no new data for a long time. This
      // time out is the time bound of update delay of available in corner
      // cases.
      openInfo();
      // Force not to reuse the current block reader
      blockEnd = -1;
      // Reset the refetch metadata timer
      timeShowNoAvailableByte = NOT_WAITING_FOR_MORE_BYTES;
    }

    // In the case that file length shrinks after connection failure,
    // this call might always return -1 without updating the length.
    // In this case, we also reply on get available time out mechanism to
    // eventually make sure the stream is reopened and the client can read
    // data from it.
    read(DFSClient.emptyByteArray, 0, 0);
    
    long availableBytes = getFileLength() - pos;

    if (availableBytes > 0) {
      timeShowNoAvailableByte = NOT_WAITING_FOR_MORE_BYTES;
    } else if (timeShowNoAvailableByte == NOT_WAITING_FOR_MORE_BYTES) {
      timeShowNoAvailableByte = timeNow;
    }

    // Since the client might reopen the file because of exceptions
    // and refetch file length from another data node and gets a file
    // length that is smaller than current position. In this case, instead
    // of returning a negative value, return 0.
    return Math.max(0,
        (int) (Math.min(availableBytes, (long) Integer.MAX_VALUE)));
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
  
  /**
   * Pick the best node from which to stream the data.
   * Entries in <i>nodes</i> are already in the priority order
   */
  static private DatanodeInfo bestNode(DatanodeInfo nodes[],
                               ConcurrentHashMap<DatanodeInfo, Long> deadNodes,
                               Collection<DatanodeInfo> unfavoredNodes,
                               Collection<DatanodeInfo> ignoredNodes)
                                throws IOException {
    InjectionHandler.processEventIO(InjectionEvent.DFSCLIENT_BEFORE_BEST_NODE,
        nodes, deadNodes);
    
    if (nodes != null) {
      // First try to find a node which is not included in favored ndoe list
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])
            && (unfavoredNodes == null || !unfavoredNodes.contains(nodes[i]))
            && (ignoredNodes == null || !ignoredNodes.contains(nodes[i]))) {
            return nodes[i];
        }
      }

      // All live nodes are un-favored. Ignore the unfavor setting
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])
            && (ignoredNodes == null || !ignoredNodes.contains(nodes[i]))) {
            return nodes[i];
        }
      }
    }
    return null;
  }
  
  static private String getBestNodeErrorString(DatanodeInfo nodes[],
      ConcurrentHashMap<DatanodeInfo, Long> deadNodes,
      Collection<DatanodeInfo> unfavoredNodes,
      Collection<DatanodeInfo> ignoredNodes) {
    StringBuilder errMsgr = new StringBuilder(
        "No live nodes contain current block ");
    errMsgr.append("Block locations:");
    for (DatanodeInfo datanode : nodes) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    errMsgr.append(" Dead nodes: ");
    for (DatanodeInfo datanode : deadNodes.keySet()) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    if (unfavoredNodes != null) {
      errMsgr.append(" Unfavored nodes: ");
      for (DatanodeInfo datanode : unfavoredNodes) {
        errMsgr.append(" ");
        errMsgr.append(datanode.toString());
      }
    }
    if (ignoredNodes != null) {
      errMsgr.append(" Ignored nodes: ");
      for (DatanodeInfo datanode : ignoredNodes) {
        errMsgr.append(" ");
        errMsgr.append(datanode.toString());
      }
    }
    return errMsgr.toString();
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

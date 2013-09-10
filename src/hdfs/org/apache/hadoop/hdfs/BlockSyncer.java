package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockRecord;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecoveryTimeoutException;
import org.apache.hadoop.hdfs.server.datanode.SyncBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockAlreadyCommittedException;
import org.apache.hadoop.hdfs.server.protocol.BlockSynchronizationProtocol;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;


public class BlockSyncer implements SyncBlock {
  final private int namespaceId;
  final private BlockSynchronizationProtocol bsp;
  final private Log LOG;

  public BlockSyncer(int namespaceId, BlockSynchronizationProtocol bsp, Log lOG) {
    this.namespaceId = namespaceId;
    this.bsp = bsp;
    this.LOG = lOG;
  }

  /** Block synchronization */
  @Override
  public LocatedBlock syncBlock(
      Block block,
      List<BlockRecord> syncList,
    boolean closeFile, List<InterDatanodeProtocol> datanodeProxies,
    long deadline
  ) 
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      bsp.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      return null;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    throwIfAfterTime(deadline);
    long generationstamp = -1;
    try {
      generationstamp = bsp.nextGenerationStamp(block, closeFile);
    } catch (RemoteException e) {
      if (e.unwrapRemoteException() instanceof BlockAlreadyCommittedException) {
        throw new BlockAlreadyCommittedException(e);
      } else {
        throw e;
      }
    }
    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for(BlockRecord r : syncList) {
      try {
        throwIfAfterTime(deadline);
        LOG.info("Updating block " + r + " to " + newblock);
        r.datanode.updateBlock(namespaceId, r.info.getBlock(), newblock, closeFile);
        successList.add(r.id);
      } catch (BlockRecoveryTimeoutException e) {
        throw e;
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    LOG.info("Updated blocks on syncList for block " + block + " to " + newblock);

    DataNode.stopAllProxies(datanodeProxies);

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      throwIfAfterTime(deadline);
      bsp.commitBlockSynchronization(block,
          newblock.getGenerationStamp(), newblock.getNumBytes(), closeFile, false,
          nlist);
      DatanodeInfo[] info = new DatanodeInfo[nlist.length];
      for (int i = 0; i < nlist.length; i++) {
        info[i] = new DatanodeInfo(nlist[i]);
      }
      return new LocatedBlock(newblock, info); // success
    }

    //failed
    StringBuilder b = new StringBuilder();
    for(BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException("Cannot recover " + block + ", none of these "
        + syncList.size() + " datanodes success {" + b + "\n}");
  }
  
  static public void throwIfAfterTime(long timeoutTime) throws IOException {
    if (timeoutTime > 0 && System.currentTimeMillis() > timeoutTime) {
      throw new BlockRecoveryTimeoutException("The client have timed out.");
    }
  }
}

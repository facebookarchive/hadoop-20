package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockRecord;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecoveryTimeoutException;
import org.apache.hadoop.hdfs.server.datanode.SyncBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;


public class BlockRecoveryCoordinator {
  final private Log LOG;
  final private Configuration conf;
  final private int socketTimeout;
  final private DataNode dn;
  final private SyncBlock blockSyncer;
  final private DatanodeRegistration dnr;

  public BlockRecoveryCoordinator(Log lOG, Configuration conf,
      int socketTimeout, DataNode dn, SyncBlock blockSyncer,
      DatanodeRegistration dnr) {
    LOG = lOG;
    this.conf = conf;
    this.socketTimeout = socketTimeout;
    this.dn = dn;
    this.blockSyncer = blockSyncer;
    this.dnr = dnr;
  }
  
  /**
   * Recover a block
   * 
   * @param keepLength
   *          if true, will only recover replicas that have the same length as
   *          the block passed in. Otherwise, will calculate the minimum length
   *          of the replicas and truncate the rest to that length.
   **/
  public LocatedBlock recoverBlock(int namespaceId, Block block,
      boolean keepLength, DatanodeID[] datanodeids, boolean closeFile,
      long deadline) throws IOException {
    int errorCount = 0;

    // Number of "replicasBeingWritten" in 0.21 parlance - these are replicas
    // on DNs that are still alive from when the write was happening
    int rbwCount = 0;
    // Number of "replicasWaitingRecovery" in 0.21 parlance - these replicas
    // have survived a DN restart, and thus might be truncated (eg if the
    // DN died because of a machine power failure, and when the ext3 journal
    // replayed, it truncated the file
    int rwrCount = 0;

    List<BlockRecord> blockRecords = new ArrayList<BlockRecord>();
    List<InterDatanodeProtocol> datanodeProxies = new ArrayList<InterDatanodeProtocol>();
    // check generation stamps
    for (DatanodeID id : datanodeids) {
      try {
        InterDatanodeProtocol datanode;
        if (dnr!= null && dnr.equals(id)) {
          LOG.info("Skipping IDNPP creation for local id " + id
              + " when recovering " + block);
          datanode = dn;
        } else {
          LOG.info("Creating IDNPP for non-local id " + id + " (dnReg="
              + dnr + ") when recovering "
              + block);
          datanode = DataNode.createInterDataNodeProtocolProxy(id, conf,
              socketTimeout);
          datanodeProxies.add(datanode);
        }
        BlockSyncer.throwIfAfterTime(deadline);
        BlockRecoveryInfo info = datanode
            .startBlockRecovery(namespaceId, block);
        if (info == null) {
          LOG.info("No block metadata found for block " + block
              + " on datanode " + id);
          continue;
        }
        if (info.getBlock().getGenerationStamp() < block.getGenerationStamp()) {
          LOG.info("Only old generation stamp "
              + info.getBlock().getGenerationStamp() + " found on datanode "
              + id + " (needed block=" + block + ")");
          continue;
        }
        blockRecords.add(new BlockRecord(id, datanode, info));

        if (info.wasRecoveredOnStartup()) {
          rwrCount++;
        } else {
          rbwCount++;
        }
      } catch (BlockRecoveryTimeoutException e) {
        throw e;
      } catch (IOException e) {
        ++errorCount;
        InterDatanodeProtocol.LOG.warn(
            "Failed to getBlockMetaDataInfo for block (=" + block
                + ") from datanode (=" + id + ")", e);
      }
    }

    // If we *only* have replicas from post-DN-restart, then we should
    // include them in determining length. Otherwise they might cause us
    // to truncate too short.
    boolean shouldRecoverRwrs = (rbwCount == 0);

    List<BlockRecord> syncList = new ArrayList<BlockRecord>();
    long minlength = Long.MAX_VALUE;

    for (BlockRecord record : blockRecords) {
      BlockRecoveryInfo info = record.info;
      assert (info != null && info.getBlock().getGenerationStamp() >= block
          .getGenerationStamp());
      if (!shouldRecoverRwrs && info.wasRecoveredOnStartup()) {
        LOG.info("Not recovering replica " + record
            + " since it was recovered on "
            + "startup and we have better replicas");
        continue;
      }
      if (keepLength) {
        if (info.getBlock().getNumBytes() == block.getNumBytes()) {
          syncList.add(record);
        }
      } else {
        syncList.add(record);
        if (info.getBlock().getNumBytes() < minlength) {
          minlength = info.getBlock().getNumBytes();
        }
      }
    }

    if (syncList.isEmpty() && errorCount > 0) {
      DataNode.stopAllProxies(datanodeProxies);
      throw new IOException("All datanodes failed: block=" + block
          + ", datanodeids=" + Arrays.asList(datanodeids));
    }
    if (!keepLength) {
      block.setNumBytes(minlength);
    }
    return blockSyncer.syncBlock(block, syncList, closeFile, datanodeProxies,
        deadline);
  }
}
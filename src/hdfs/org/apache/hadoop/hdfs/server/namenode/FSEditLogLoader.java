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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AppendOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ClearNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.HardLinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MergeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.raid.RaidCodec;

import com.google.common.base.Joiner;

public class FSEditLogLoader {
  public static final Log LOG = LogFactory.getLog(FSEditLogLoader.class);
  private final FSNamesystem fsNamesys;
  public static final long TXID_IGNORE = -1;
  private long lastAppliedTxId = -1;

  public FSEditLogLoader(FSNamesystem fsNamesys) {
    this.fsNamesys = fsNamesys;
  }
  
  /**
   * Allocate inode id for legacy edit log, or generate inode id for new edit log,
   * and update lastInodeId in {@link FSDirectory}
   * Also doing some sanity check
   * 
   * @param fsDir {@link FSDirectory}
   * @param inodeIdFromOp inode id read from edit log
   * @param logVersion current layout version
   * @param lastInodeId the latest inode id in the system
   * @return inode id
   * @throws IOException When invalid inode id in layout that supports inode id.
   *                      
   */
  private static long getAndUpdateLastInodeId(FSDirectory fsDir, 
      long inodeIdFromOp, int logVersion) throws IOException {
    long inodeId = inodeIdFromOp;
    if (inodeId == INodeId.GRANDFATHER_INODE_ID) {
      // This id is read from old edit log
      if (LayoutVersion.supports(Feature.ADD_INODE_ID, logVersion)) {
        throw new IOException("The layout version " + logVersion
            + " supports inodeId but gave bogus inodeId");
      }
      inodeId = fsDir.allocateNewInodeId();
    } else {
      // need to reset lastInodeId. fsdir gets lastInodeId firstly from
      // fsimage but editlog captures more recent inodeId allocations
      if (inodeId > fsDir.getLastInodeId()) {
        fsDir.resetLastInodeId(inodeId);
      }
    }
    return inodeId;
  }
  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(EditLogInputStream edits, long lastAppliedTxId) 
  throws IOException {
    long startTime = now();
    this.lastAppliedTxId = lastAppliedTxId;
    int numEdits = loadFSEdits(edits, true);
    FSImage.LOG.info("Edits file " + edits.toString() 
        + " of size: " + edits.length() + ", # of edits: " + numEdits 
        + " loaded in: " + (now()-startTime)/1000 + " seconds.");
    return numEdits;
  }

  int loadFSEdits(EditLogInputStream edits, boolean closeOnExit)
      throws IOException {
    int numEdits = 0;
    int logVersion = edits.getVersion();

    try {
      numEdits = loadEditRecords(logVersion, edits, false);
    } finally {
      if(closeOnExit) {
        edits.close();
      }
    }
    return numEdits;
  }
  
  static void loadEditRecord(int logVersion, 
      EditLogInputStream in,
      long[] recentOpcodeOffsets, 
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts,
      FSNamesystem fsNamesys,
      FSDirectory fsDir,
      int numEdits,
      FSEditLogOp op) throws IOException{
    
    recentOpcodeOffsets[numEdits % recentOpcodeOffsets.length] =
        in.getPosition();
    incrOpCount(op.opCode, opCounts);
    
    switch (op.opCode) {
    case OP_ADD:
    case OP_CLOSE: {
      AddCloseOp addCloseOp = (AddCloseOp)op;

      // versions > 0 support per file replication
      // get name and replication
      final short replication  = fsNamesys.adjustReplication(addCloseOp.replication);

      long blockSize = addCloseOp.blockSize;
      BlockInfo blocks[] = new BlockInfo[addCloseOp.blocks.length];
      for (int i = 0; i < addCloseOp.blocks.length; i++) {
        // We already read&create BlockInfo in FSEditLogOp
        blocks[i] = new BlockInfo(addCloseOp.blocks[i], replication);
        blocks[i].setChecksum(addCloseOp.blocks[i].getChecksum());
      }

      // Older versions of HDFS does not store the block size in inode.
      // If the file has more than one block, use the size of the
      // first block as the blocksize. Otherwise use the default
      // block size.
      if (-8 <= logVersion && blockSize == 0) {
        if (blocks.length > 1) {
          blockSize = blocks[0].getNumBytes();
        } else {
          long first = ((blocks.length == 1)? blocks[0].getNumBytes(): 0);
          blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
        }
      }
      
      PermissionStatus permissions = fsNamesys.getUpgradePermission();
      if (addCloseOp.permissions != null) {
        permissions = addCloseOp.permissions;
      }


      // The open lease transaction re-creates a file if necessary.
      // Delete the file if it already exists.
      if (FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug(op.opCode + ": " + addCloseOp.path +
            " numblocks : " + blocks.length +
            " clientHolder " + addCloseOp.clientName +
            " clientMachine " + addCloseOp.clientMachine);
      }
      
      long inodeId = getAndUpdateLastInodeId(fsDir, addCloseOp.inodeId, logVersion);
      // updateINodeFile calls unprotectedAddFile if node doesn't exist
      INodeFile node = fsDir.updateINodefile(inodeId, addCloseOp.path, permissions,
          blocks, replication, addCloseOp.mtime, addCloseOp.atime, blockSize,
          addCloseOp.clientName, addCloseOp.clientMachine);
      
      if (addCloseOp.opCode == FSEditLogOpCodes.OP_ADD) {
        if (!node.isUnderConstruction()) {
          throw new IOException("INodeFile : " + node
              + " is not under construction");
        }
        INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
        if (!cons.getClientName().equals(addCloseOp.clientName)) {
          fsNamesys.leaseManager.removeLease(cons.getClientName(),
              addCloseOp.path);
          FSNamesystem.LOG.info("Updating client name for : " + addCloseOp.path
              + " from : " + cons.getClientName() + " to : "
              + addCloseOp.clientName);
        }
        cons.setClientName(addCloseOp.clientName);
        cons.setClientMachine(addCloseOp.clientMachine);
        // TODO: There is a chance that by clearing targets, we over count safe
        // mode counts. Here is the case:
        // The OP_ADD is triggered by a commitSynchronizedBlock() (which means
        // only the generation stamp of the last block changed). Before block
        // synchronization, a block is already reported by a data node after
        // data node restarting so we've already incremented safe block count.
        // Here we clear the targets without decrementing the safe block count,
        // so that later the safe block count will be added again.
        // We need to fix it.
        cons.clearTargets();
        fsNamesys.leaseManager.addLease(cons.getClientName(),
                                        addCloseOp.path, 
                                        cons.getModificationTime());
      } else {
        INodeFile newNode = node;
        if (node.isUnderConstruction()) {
          INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) node;
          newNode = pendingFile.convertToInodeFile();
          newNode.setAccessTime(addCloseOp.atime);
          fsNamesys.leaseManager.removeLease(pendingFile.getClientName(),
              addCloseOp.path);
          fsDir.replaceNode(addCloseOp.path, node, newNode);
          pendingFile.clearTargets();
        }
      }
      break;
    }
    case OP_APPEND: {
      AppendOp appendOp = (AppendOp)op;
      INodeFile node = fsDir.getFileINode(appendOp.path);
      
      // node must exist and the file must be closed
      if (node == null || node.isUnderConstruction()) {
        logErrorAndFail("Append - can not append to "
            + (node == null ? "non-existent" : "under construction") + " file");
      }

      // check if all blocks match
      if (node.getBlocks().length != appendOp.blocks.length) {
        logErrorAndFail("Append - the transaction has incorrect block information");
      }
      int index = 0;
      for (Block b1 : node.getBlocks()) {
        Block b2 = appendOp.blocks[index++];
        // blocks should be exactly the same (id, gs, size)
        if (!b1.equals(b2) || b1.getNumBytes() != b2.getNumBytes()) {
          LOG.error("Append: existing block: " + b1 + ", block in edit log: "
              + b2);
          logErrorAndFail("Append: blocks are mismatched");
        }
      }
          
      INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
          node.getId(),
          node.getLocalNameBytes(), 
          node.getReplication(),
          node.getModificationTime(),
          node.getAccessTime(), 
          node.getPreferredBlockSize(), 
          appendOp.blocks,
          node.getPermissionStatus(), 
          appendOp.clientName,
          appendOp.clientMachine, 
          fsNamesys.getNode(appendOp.clientMachine));
      fsNamesys.dir.replaceNode(appendOp.path, null, node, cons, true);
      fsNamesys.leaseManager.addLease(cons.getClientName(),
          appendOp.path, 
          cons.getModificationTime());
      break;
    }
    case OP_SET_REPLICATION: {
      SetReplicationOp setReplicationOp = (SetReplicationOp)op;
      short replication = fsNamesys.adjustReplication(
          setReplicationOp.replication);
      fsDir.unprotectedSetReplication(setReplicationOp.path,
                                      replication, null);
      break;
    }
    case OP_CONCAT_DELETE: {
      ConcatDeleteOp concatDeleteOp = (ConcatDeleteOp)op;
      fsDir.unprotectedConcat(concatDeleteOp.trg, concatDeleteOp.srcs,
          concatDeleteOp.timestamp);
      break;
    }
    case OP_MERGE: {
      MergeOp mergeOp = (MergeOp)op;
      RaidCodec codec = RaidCodec.getCodec(mergeOp.codecId);
      if (codec == null) {
        LOG.error("Codec " + mergeOp.codecId + " doesn't exist");
        logErrorAndFail("Merge: codec doesn't exist");
      }
      INode[] sourceINodes = fsDir.getExistingPathINodes(mergeOp.source);
      INode[] parityINodes = fsDir.getExistingPathINodes(mergeOp.parity);
      fsDir.unprotectedMerge(parityINodes, sourceINodes, mergeOp.parity, mergeOp.source,
          codec, mergeOp.checksums, mergeOp.timestamp);
      break;
    }
    case OP_HARDLINK: {
      HardLinkOp hardLinkOp = (HardLinkOp)op;
      fsDir.unprotectedHardLinkTo(hardLinkOp.src, hardLinkOp.dst, hardLinkOp.timestamp);
      break;
    }
    case OP_RENAME: {
      RenameOp renameOp = (RenameOp)op;
      HdfsFileStatus dinfo = fsDir.getHdfsFileInfo(renameOp.dst);
      fsDir.unprotectedRenameTo(renameOp.src, renameOp.dst,
                                renameOp.timestamp);
      fsNamesys.changeLease(renameOp.src, renameOp.dst, dinfo);
      break;
    }
    case OP_DELETE: {
      DeleteOp deleteOp = (DeleteOp)op;
      fsDir.unprotectedDelete(deleteOp.path, deleteOp.timestamp);
      break;
    }
    case OP_MKDIR: {
      MkdirOp mkdirOp = (MkdirOp)op;
      PermissionStatus permissions = fsNamesys.getUpgradePermission();
      if (mkdirOp.permissions != null) {
        permissions = mkdirOp.permissions;
      }
      
      long inodeId = getAndUpdateLastInodeId(fsDir, mkdirOp.inodeId, logVersion);
      fsDir.unprotectedMkdir(inodeId, mkdirOp.path, permissions,
                             mkdirOp.timestamp);
      break;
    }
    case OP_SET_GENSTAMP: {
      SetGenstampOp setGenstampOp = (SetGenstampOp)op;
      fsNamesys.setGenerationStamp(setGenstampOp.genStamp);
      break;
    }
    case OP_SET_PERMISSIONS: {
      SetPermissionsOp setPermissionsOp = (SetPermissionsOp)op;
      fsDir.unprotectedSetPermission(setPermissionsOp.src,
                                     setPermissionsOp.permissions);
      break;
    }
    case OP_SET_OWNER: {
      SetOwnerOp setOwnerOp = (SetOwnerOp)op;
      fsDir.unprotectedSetOwner(setOwnerOp.src, setOwnerOp.username,
                                setOwnerOp.groupname);
      break;
    }
    case OP_SET_NS_QUOTA: {
      SetNSQuotaOp setNSQuotaOp = (SetNSQuotaOp)op;
      fsDir.unprotectedSetQuota(setNSQuotaOp.src,
                                setNSQuotaOp.nsQuota,
                                FSConstants.QUOTA_DONT_SET);
      break;
    }
    case OP_CLEAR_NS_QUOTA: {
      ClearNSQuotaOp clearNSQuotaOp = (ClearNSQuotaOp)op;
      fsDir.unprotectedSetQuota(clearNSQuotaOp.src,
                                FSConstants.QUOTA_RESET,
                                FSConstants.QUOTA_DONT_SET);
      break;
    }

    case OP_SET_QUOTA:
      SetQuotaOp setQuotaOp = (SetQuotaOp)op;
      fsDir.unprotectedSetQuota(setQuotaOp.src,
                                setQuotaOp.nsQuota,
                                setQuotaOp.dsQuota);
      break;

    case OP_TIMES: {
      TimesOp timesOp = (TimesOp)op;

      fsDir.unprotectedSetTimes(timesOp.path,
                                timesOp.mtime,
                                timesOp.atime, true);
      break;
    }
    case OP_START_LOG_SEGMENT:
    case OP_END_LOG_SEGMENT: {
      // no data in here currently.
      break;
    }
    case OP_DATANODE_ADD:
    case OP_DATANODE_REMOVE:
      break;
    default:
      throw new IOException("Invalid operation read " + op.opCode);
    }
  }
    
  @SuppressWarnings("deprecation")
  int loadEditRecords(int logVersion, EditLogInputStream in, boolean closeOnExit)
      throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    
    EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts =
      new EnumMap<FSEditLogOpCodes, Holder<Integer>>(FSEditLogOpCodes.class);

    fsNamesys.writeLock();
    fsDir.writeLock();

    long recentOpcodeOffsets[] = new long[2];
    Arrays.fill(recentOpcodeOffsets, -1);

    try {
      try {
        FSEditLogOp op;
        FSEditLog.LOG.info("Planning to load: " + numEdits);
         
        while (true) {
          try {
            op = in.readOp();
            if (op == null) {
              break;
            }
          } catch (Throwable e) {
            String errorMessage = "Failed to read txId " + lastAppliedTxId
                + "skipping the bad section in the log";
            checkFail(errorMessage);
            in.resync();
            FSImage.LOG.info("After resync, position is " + in.getPosition());
            continue;
          }
          if (logVersion <= FSConstants.STORED_TXIDS) {
            long diskTxid = op.txid;
            if (diskTxid != (lastAppliedTxId + 1)) {
              String errorMsg = "The transaction id in the edit log : "
                  + diskTxid + " does not match the transaction id inferred"
                  + " from FSIMAGE : " + (lastAppliedTxId + 1);
              checkFail(errorMsg);
            }
          }
          
          try {
            loadEditRecord(logVersion, 
                in, 
                recentOpcodeOffsets, 
                opCounts, 
                fsNamesys,
                fsDir, 
                numEdits, 
                op);
            lastAppliedTxId = op.txid;
            numEdits++;
          } catch (Throwable t) {
            String errorMsg = "Failed to load transaction " + op + ": " + t.getMessage();
            checkFail(errorMsg);
            // assume the transaction was loaded to avoid another error
            lastAppliedTxId = op.txid;
          }   
        }
      } finally {
        if(closeOnExit)
          in.close();
      }
    } catch (Throwable t) {
      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      String errorMessage = getErrorMessage(recentOpcodeOffsets, in.getPosition());
      FSImage.LOG.error(errorMessage);
      throw new IOException(errorMessage, t);
    } finally {
      fsDir.writeUnlock();
      fsNamesys.writeUnlock();
    }
    dumpOpCounts(opCounts);
    return numEdits;
  }
  
  /**
   * When encountering an error while loading the transaction, we can
   * skip the problematic transaction and continue, first prompting the user.
   * This will only be possible when NN is started with appropriate option.
   */
  private void checkFail(String errorMsg) throws IOException {
    if (fsNamesys.failOnTxIdMismatch()) {
      FSEditLog.LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
    MetaRecoveryContext.editLogLoaderPrompt(errorMsg);
  }
  
  public static String getErrorMessage(long[] recentOpcodeOffsets, long position) {
    StringBuilder sb = new StringBuilder();
    sb.append("Error replaying edit log at offset " + position);
    if (recentOpcodeOffsets[0] != -1) {
      Arrays.sort(recentOpcodeOffsets);
      sb.append("\nRecent opcode offsets:");
      for (long offset : recentOpcodeOffsets) {
        if (offset != -1) {
          sb.append(' ').append(offset);
        }
      }
    }
    return sb.toString();
  }
  
  public long getLastAppliedTxId() {
    return lastAppliedTxId;
  }

  static void dumpOpCounts(
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
    if (!FSImage.LOG.isDebugEnabled())
      return;
    StringBuilder sb = new StringBuilder();
    sb.append("Summary of operations loaded from edit log:\n  ");
    Joiner.on("\n  ").withKeyValueSeparator("=").appendTo(sb, opCounts);
    FSImage.LOG.debug(sb.toString());
  }

  private static void incrOpCount(FSEditLogOpCodes opCode,
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
    Holder<Integer> holder = opCounts.get(opCode);
    if (holder == null) {
      holder = new Holder<Integer>(1);
      opCounts.put(opCode, holder);
    } else {
      holder.held++;
    }
  }
  
  /**
   * Return the number of valid transactions in the stream. If the stream is
   * truncated during the header, returns a value indicating that there are
   * 0 valid transactions. This reads through the stream but does not close
   * it.
   * @throws IOException if the stream cannot be read due to an IO error (eg
   *                     if the log does not exist)
   */
  public static EditLogValidation validateEditLog(EditLogInputStream in) {
    long lastPos = 0;
    long firstTxId = HdfsConstants.INVALID_TXID;
    long lastTxId = HdfsConstants.INVALID_TXID;
    long numValid = 0;
    try {
      FSEditLogOp op = null;
      while (true) {
        lastPos = in.getPosition();
        try {
          if ((op = in.readOp()) == null) {
            break;
          }
        } catch (Throwable t) {
          FSImage.LOG.warn("Caught exception after reading " + numValid +
              " ops from " + in + " while determining its valid length." +
              "Position was " + lastPos, t);
          in.resync();
          FSImage.LOG.info("After resync, position is " + in.getPosition());
          continue;
        }
        if (firstTxId == HdfsConstants.INVALID_TXID) {
          firstTxId = op.getTransactionId();
        }
        if (lastTxId == HdfsConstants.INVALID_TXID
            || op.txid > lastTxId) {
          lastTxId = op.getTransactionId();
        } else {
          FSImage.LOG.error("Out of order txid found. Found " + op.txid
                            + ", expected " + (lastTxId + 1));
        }
        numValid++;
      }
    } catch (Throwable t) {
      // Catch Throwable and not just IOE, since bad edits may generate
      // NumberFormatExceptions, AssertionErrors, OutOfMemoryErrors, etc.
      FSImage.LOG.debug("Caught exception after reading " + numValid +
          " ops from " + in + " while determining its valid length.", t);
    }
    return new EditLogValidation(lastPos, firstTxId, lastTxId, false);
  }
  
  static void logErrorAndFail(String errorMsg) throws IOException {
    LOG.error(errorMsg);
    throw new IOException(errorMsg);
  }
  
  public static class EditLogValidation {
    private long validLength;
    private long startTxId;
    private long endTxId;
    private boolean hasCorruptHeader;
     
    public EditLogValidation(long validLength, long startTxId, long endTxId,
        boolean hasCorruptHeader) {
      this.validLength = validLength;
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.hasCorruptHeader = hasCorruptHeader;
    }
    
    long getValidLength() { return validLength; }
    
    long getStartTxId() { return startTxId; }
    
    public long getEndTxId() { return endTxId; }
    
    public long getNumTransactions() {
      if (endTxId == HdfsConstants.INVALID_TXID
          || startTxId == HdfsConstants.INVALID_TXID) {
        return 0;
      }
      return (endTxId - startTxId) + 1;
    }
    
    boolean hasCorruptHeader() { return hasCorruptHeader; }
  }
  
  /**
   * Stream wrapper that keeps track of the current stream position.
   */
  public static class PositionTrackingInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;
    
    public PositionTrackingInputStream(InputStream is) {
      super(is);
    }

    public PositionTrackingInputStream(InputStream is, long position) {
      super(is);
      curPos = position;
    }

    @Override
    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) curPos++;
      return ret;
    }

    @Override
    public int read(byte[] data) throws IOException {
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    @Override
    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    @Override
    public void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }

    @Override
    public long skip(long amt) throws IOException {
      long ret = super.skip(amt);
      curPos += ret;
      return ret;
    }
  }
}

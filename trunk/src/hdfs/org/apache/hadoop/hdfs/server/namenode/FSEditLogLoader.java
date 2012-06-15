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

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ClearNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;

import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.google.common.base.Joiner;

public class FSEditLogLoader {
  private final FSNamesystem fsNamesys;
  public static final long TXID_IGNORE = -1;
  private long currentTxId;

  public FSEditLogLoader(FSNamesystem fsNamesys) {
    this.fsNamesys = fsNamesys;
  }
  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(EditLogInputStream edits, long expectedStartingTxId)
  throws IOException {
    long startTime = now();
    currentTxId = expectedStartingTxId;
    int numEdits = loadFSEdits(edits, true);
    FSImage.LOG.info("Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (now()-startTime)/1000 + " seconds.");
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
    
    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
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
        blocks[i] = new BlockInfo(addCloseOp.blocks[i], replication);
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

      INodeFile node = fsDir.updateINodefile(addCloseOp.path, permissions,
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
        }
      }
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

      fsDir.unprotectedMkdir(mkdirOp.path, permissions,
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
        while ((op = in.readOp()) != null) {
          if (logVersion <= FSConstants.STORED_TXIDS) {
            long diskTxid = op.txid;
            if (diskTxid != currentTxId) {
              if (fsNamesys.failOnTxIdMismatch()) {
                throw new IOException("The transaction id in the edit log : "
                    + diskTxid + " does not match the transaction id inferred"
                    + " from FSIMAGE : " + currentTxId);
              } else {
                FSNamesystem.LOG.error("The transaction id in the edit log : "
                    + diskTxid + " does not match the transaction id inferred"
                    + " from FSIMAGE : " + currentTxId +
                    ", continuing with transaction id : " + diskTxid);
                currentTxId = diskTxid;
              }
            }
          }
          
          loadEditRecord(logVersion, 
              in, 
              recentOpcodeOffsets, 
              opCounts, 
              fsNamesys,
              fsDir, 
              numEdits, 
              op);
          currentTxId++;
          numEdits++;
        }
      } finally {
        if(closeOnExit)
          in.close();
      }
    } catch (Throwable t) {
      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      StringBuilder sb = new StringBuilder();
      sb.append("Error replaying edit log at offset " + in.getPosition());
      if (recentOpcodeOffsets[0] != -1) {
        Arrays.sort(recentOpcodeOffsets);
        sb.append("\nRecent opcode offsets:");
        for (long offset : recentOpcodeOffsets) {
          if (offset != -1) {
            sb.append(' ').append(offset);
          }
        }
      }
      String errorMessage = sb.toString();
      FSImage.LOG.error(errorMessage);
      throw new IOException(errorMessage, t);
    } finally {
      fsDir.writeUnlock();
      fsNamesys.writeUnlock();
    }
    if (FSImage.LOG.isDebugEnabled()) {
      dumpOpCounts(opCounts);
    }
    return numEdits;
  }
  
  public long getCurrentTxId() {
    return currentTxId;
  }


  private static void dumpOpCounts(
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
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
  static EditLogValidation validateEditLog(EditLogInputStream in) {
    long lastPos = 0;
    long firstTxId = HdfsConstants.INVALID_TXID;
    long lastTxId = HdfsConstants.INVALID_TXID;
    long numValid = 0;
    try {
      FSEditLogOp op = null;
      while (true) {
        lastPos = in.getPosition();
        if ((op = in.readOp()) == null) {
          break;
        }
        if (firstTxId == HdfsConstants.INVALID_TXID) {
          firstTxId = op.txid;
        }
        if (lastTxId == HdfsConstants.INVALID_TXID
            || op.txid == lastTxId + 1) {
          lastTxId = op.txid;
        } else {
          FSImage.LOG.error("Out of order txid found. Found " + op.txid 
                            + ", expected " + (lastTxId + 1));
          break;
        }
        numValid++;
      }
    } catch (Throwable t) {
      // Catch Throwable and not just IOE, since bad edits may generate
      // NumberFormatExceptions, AssertionErrors, OutOfMemoryErrors, etc.
      FSImage.LOG.debug("Caught exception after reading " + numValid +
          " ops from " + in + " while determining its valid length.", t);
    }
    return new EditLogValidation(lastPos, firstTxId, lastTxId);
  }
  
  static class EditLogValidation {
    private long validLength;
    private long startTxId;
    private long endTxId;
     
    EditLogValidation(long validLength, 
                      long startTxId, long endTxId) {
      this.validLength = validLength;
      this.startTxId = startTxId;
      this.endTxId = endTxId;
    }
    
    long getValidLength() { return validLength; }
    
    long getStartTxId() { return startTxId; }
    
    long getEndTxId() { return endTxId; }
    
    long getNumTransactions() { 
      if (endTxId == HdfsConstants.INVALID_TXID
          || startTxId == HdfsConstants.INVALID_TXID) {
        return 0;
      }
      return (endTxId - startTxId) + 1;
    }
  }
  
  /**
   * Stream wrapper that keeps track of the current stream position.
   */
  static class PositionTrackingInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;

    public PositionTrackingInputStream(InputStream is) {
      super(is);
    }
    
    public PositionTrackingInputStream(InputStream is, long position) {
      super(is);
      curPos = position;
    }

    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) curPos++;
      return ret;
    }

    public int read(byte[] data) throws IOException {
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

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
  }
}

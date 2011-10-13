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

import java.io.IOException;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.lang.Thread;
import java.util.Date;
import java.util.Collection;
import java.text.SimpleDateFormat;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.common.Storage;

/**
 * This class reads transaction logs from the primary's shared device
 * and feeds it to the standby NameNode.
 */

public class Ingest implements Runnable {

  public static final Log LOG = AvatarNode.LOG;
  static final SimpleDateFormat DATE_FORM =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Standby standby;
  private Configuration confg;  // configuration of local namenode
  private File ingestFile;
  volatile private boolean running = true;
  volatile private boolean catchingUp = true;
  volatile private long catchUpLag;
  volatile private boolean lastScan = false; // is this the last scan?
  volatile private boolean reopen = false; // Close and open the file again
  private CheckpointSignature lastSignature;
  volatile private boolean success = false;  // not successfully ingested yet
  RandomAccessFile rp;  // the file to read transactions from.
  FileInputStream fp;
  FileChannel fc ;
  long currentPosition; // current offset in the transaction log
  final FSNamesystem fsNamesys;

  Ingest(Standby standby, FSNamesystem ns, Configuration conf, File edits) 
  throws IOException {
    this.fsNamesys = ns;
    this.standby = standby;
    this.confg = conf;
    this.ingestFile = edits;
    catchUpLag = conf.getLong("avatar.catchup.lag", 2 * 1024 * 1024L);
  }

  public void run() {
    if (standby.checkpointInProgress) {
      CheckpointSignature sig = standby.getLastRollSignature();
      if (sig == null) {
        // This should never happen, since if checkpoint is in progress
        // and the Ingest thread is getting recreated there should be a sig
        throw new RuntimeException("Ingest: Weird thing has happened. The " +
        		"checkpoint was in progress, but there is no signature of the " +
        		"fsedits available");
      }
      if (sig.editsTime != ingestFile.lastModified()) {
        // This means that the namenode checkpointed itself in the meanwhile
        throw new RuntimeException("Ingest: The primary node has checkpointed" +
        		" so we cannot proceed. Restarting Avatar is the only option");
      }
      /**
       * If the checkpoint is in progress it means that the edits file was 
       * already read and we cannot read it again from the start. 
       * Just successfuly return.
       */
      success = true;
      return;
    }
    while (running) {
      try {
        // keep ingesting transactions from remote edits log.
        loadFSEdits(ingestFile);
      } catch (Exception e) {
        LOG.warn("Ingest: Exception while processing transactions (" + 
                 running + ") " + StringUtils.stringifyException(e));
        throw new RuntimeException("Ingest: failure", e);
      } finally {
        LOG.warn("Ingest: Processing transactions has a hiccup. " + running);
      }
    }
    success = true;      // successful ingest.
  }

  /**
   * Immediately Stop the ingestion of transaction logs
   */
  void stop() {
    running = false;
  }

  /**
   * Indicate that this is the last pass over the transaction log
   */
  void quiesce() {
    lastScan = true;
  }
  
  public boolean catchingUp() {
    return catchingUp;
  }

  /**
   * Indicate that this is the last pass over the transaction log
   * Verify that file modification time of the edits log matches
   * that of the signature.
   */
  synchronized void quiesce(CheckpointSignature sig) {
    lastSignature = sig;
    lastScan = true;
  }

  private synchronized CheckpointSignature getLastCheckpointSignature() {
    return this.lastSignature;
  }

  /**
   * Indicate whether ingest was successful or not.
   * Returns true on success, else false.
   */
  boolean getIngestStatus() {
    return success;
  }

  /**
   * Returns the distance in bytes between the current position inside of the
   * edits log and the length of the edits log
   */ 
  public long getLagBytes() {
    try {
      return this.fc == null ? -1 :
        (this.fc.size() - this.fc.position());
    } catch (IOException ex) {
      LOG.error("Error getting the lag", ex);
      return -1;
    }
  }

  /**
   * Load an edit log, and continue applying the changes to the in-memory 
   * structure. This is where we ingest transactions into the standby.
   */
  private int loadFSEdits(File edits) throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    int logVersion = 0;
    String clientName = null;
    String clientMachine = null;
    String path = null;
    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRename = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpOther = 0;
    long startTime = FSNamesystem.now();

    LOG.info("Ingest: Consuming transactions from file " + edits +
             " of size " + edits.length());
    rp = new RandomAccessFile(edits, "r");
    fp = new FileInputStream(rp.getFD()); // open for reads
    fc = rp.getChannel();

    DataInputStream in = new DataInputStream(fp);
    try {
      // Read log file version. Could be missing. 
      in.mark(4);
      // If edits log is greater than 2G, available method will return negative
      // numbers, so we avoid having to call available
      boolean available = true;
      try {
        logVersion = in.readByte();
      } catch (EOFException e) {
        available = false;
      }
      if (available) {
        fc.position(0);          // reset
        in = new DataInputStream(fp);
        logVersion = in.readInt();
        if (logVersion != FSConstants.LAYOUT_VERSION) // future version
          throw new IOException(
                          "Ingest: Unexpected version of the file system log file: "
                          + logVersion + ". Current version = " 
                          + FSConstants.LAYOUT_VERSION + ".");
      }
      assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                            "Unsupported version " + logVersion;
      currentPosition = fc.position();
      numEdits = ingestFSEdits(edits, in, logVersion); // continue to ingest 
    } finally {
      LOG.info("Ingest: Closing transactions file " + edits);
      // At this time we are done reading the transaction log
      // We need to sync to have on disk status the same as in memory
      fsDir.fsImage.getEditLog().logSync();
      fp.close();
    }
    LOG.info("Ingest: Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Ingest: numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete + " numOpRename = " + numOpRename 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpTimes = " + numOpTimes
          + " numOpOther = " + numOpOther);
    }

    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }

  /**
   * Continue to ingest transaction logs until the currentState is 
   * no longer INGEST. If lastScan is set to true, then we process 
   * till the end of the file and return.
   */
  int ingestFSEdits(File fname, DataInputStream in, 
                    int logVersion) throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    String clientName = null;
    String clientMachine = null;
    String path = null;
    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRename = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpConcatDelete = 0, numOpOther = 0;
    long startTime = FSNamesystem.now();
    boolean error = false;
    boolean quitAfterScan = false;

    boolean supportChecksum = false;
    Checksum checksum = FSEditLog.getChecksumForRead();

    DataInputStream rawIn = in;
    if (logVersion < -26) { // support fsedits checksum
      supportChecksum = true;
      in = new DataInputStream(new CheckedInputStream(fp, checksum));
    }

    while (running && !quitAfterScan) {

      // if the application requested that we make a final pass over 
      // the transaction log, then we remember it here. We close and
      // reopen the file to ensure that we can see all the data in the
      // file, one reason being that NFS has open-to-close cache
      // coherancy and the edit log could be stored in NFS.
      //
      if (reopen || lastScan) {
        if (lastScan) {
          LOG.info("Ingest: Starting last scan of transaction log " + fname);
          quitAfterScan = true;
        }
        fp.close();
        rp = new RandomAccessFile(fname, "r");
        fp = new FileInputStream(rp.getFD()); // open for reads
        fc = rp.getChannel();

        // discard older buffers and start a fresh one.
        fc.position(currentPosition);
        catchingUp = (fc.size() - fc.position() > catchUpLag);
        in = rawIn = new DataInputStream(fp);
        if (supportChecksum) {
          in = new DataInputStream(new CheckedInputStream(fp, checksum));
        }
        reopen = false;
      }

      //
      // Verify that signature of file matches. This is imporatant in the
      // case when the Primary NN was configured to write transactions to 
      // to devices (local and NFS) and the Primary had encountered errors
      // to the NFS device and has continued writing transactions to its
      // device only. In this case, the rollEditLog() RPC would return the
      // modtime of the edits file of the Primary's local device and will
      // not match with the timestamp of our local log from where we are
      // ingesting.
      //
      CheckpointSignature signature = getLastCheckpointSignature();
      if (signature != null) {
        long localtime = fname.lastModified();
        if (localtime == signature.editsTime) {
          LOG.debug("Ingest: Matched modification time of edits log. ");
        } else if (localtime < signature.editsTime) {
          LOG.info("Ingest: Timestamp of transaction log on local machine is " +
                   localtime +
                   " and on remote namenode is " + signature.editsTime);
          String msg = "Ingest: Timestamp of transaction log on local machine is " + 
                       DATE_FORM.format(new Date(localtime)) +
                       " and on remote namenode is " +
                       DATE_FORM.format(new Date(signature.editsTime));
          LOG.info(msg);
          throw new IOException(msg);
        } else {
          LOG.info("Ingest: Timestamp of transaction log on local machine is " +
                   localtime +
                   " and on remote namenode is " + signature.editsTime);
          String msg = "Ingest: Timestamp of transaction log on localmachine is " + 
                       DATE_FORM.format(new Date(localtime)) +
                       " and on remote namenode is " +
                       DATE_FORM.format(new Date(signature.editsTime)) +
                       ". But this can never happen.";
          LOG.info(msg);
          throw new IOException(msg);
        }
      }

      //
      // Process all existing transactions till end of file
      //
      while (running) {
        currentPosition = fc.position(); // record the current file offset.
        checksum.reset();
        fsNamesys.writeLock();
        try {
          long timestamp = 0;
          long mtime = 0;
          long atime = 0;
          long blockSize = 0;
          byte opcode = -1;
          error = false;
          try {
            opcode = in.readByte();
            if (opcode == OP_INVALID) {
              FSNamesystem.LOG.debug("Ingest: Invalid opcode, reached end of log " +
                                     "Number of transactions found " + 
                                     numEdits);
              break; // No more transactions.
            }
          } catch (EOFException e) {
            break; // No more transactions.
          }
          switch (opcode) {
          case OP_ADD:
          case OP_CLOSE: {
          // versions > 0 support per file replication
          // get name and replication
          int length = in.readInt();
          if (-7 == logVersion && length != 3||
              -17 < logVersion && logVersion < -7 && length != 4 ||
              logVersion <= -17 && length != 5) {
              throw new IOException("Ingest: Incorrect data format."  +
                                    " logVersion is " + logVersion +
                                    " but writables.length is " +
                                    length + ". ");
          }
          path = FSImage.readString(in);
          short replication = readShort(in);
          mtime = readLong(in);
          if (logVersion <= -17) {
            atime = readLong(in);
          }
          if (logVersion < -7) {
            blockSize = readLong(in);
          }
          // get blocks
          Block blocks[] = null;
          if (logVersion <= -14) {
            blocks = readBlocks(in);
          } else {
            BlockTwo oldblk = new BlockTwo();
            int num = in.readInt();
            blocks = new Block[num];
            for (int i = 0; i < num; i++) {
              oldblk.readFields(in);
              blocks[i] = new Block(oldblk.blkid, oldblk.len, 
                                    Block.GRANDFATHER_GENERATION_STAMP);
            }
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
          if (logVersion <= -11) {
            permissions = PermissionStatus.read(in);
          }

          // clientname, clientMachine and block locations of last block.
          if (opcode == OP_ADD && logVersion <= -12) {
            clientName = FSImage.readString(in);
            clientMachine = FSImage.readString(in);
            if (-13 <= logVersion) {
              readDatanodeDescriptorArray(in);
            }
          } else {
            clientName = "";
            clientMachine = "";
          }

          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          // The open lease transaction re-creates a file if necessary.
          // Delete the file if it already exists.
          if (FSNamesystem.LOG.isDebugEnabled()) {
            FSNamesystem.LOG.debug(opcode + ": " + path + 
                                   " numblocks : " + blocks.length +
                                   " clientHolder " +  clientName +
                                   " clientMachine " + clientMachine);
          }

          fsDir.unprotectedDelete(path, mtime);

          // add to the file tree
          INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                                                    path, permissions,
                                                    blocks, replication, 
                                                    mtime, atime, blockSize);
          if (opcode == OP_ADD) {
            numOpAdd++;
            //
            // Replace current node with a INodeUnderConstruction.
            // Recreate in-memory lease record.
            //
            INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                      node.getLocalNameBytes(),
                                      node.getReplication(), 
                                      node.getModificationTime(),
                                      node.getPreferredBlockSize(),
                                      node.getBlocks(),
                                      node.getPermissionStatus(),
                                      clientName, 
                                      clientMachine, 
                                      null);
            fsDir.replaceNode(path, node, cons);
            fsNamesys.leaseManager.addLease(cons.clientName, path);
            fsDir.fsImage.getEditLog().logOpenFile(path, cons);
            } else {
              fsDir.fsImage.getEditLog().logCloseFile(path, node);
            }
            break;
          } 
          case OP_SET_REPLICATION: {
          numOpSetRepl++;
          path = FSImage.readString(in);
          short replication = readShort(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedSetReplication(path, replication, null);
          fsDir.fsImage.getEditLog().logSetReplication(path, replication);
          break;
          } 

          case OP_CONCAT_DELETE: {
          numOpConcatDelete++;
          int length = in.readInt();
          if (length < 3) { // trg, srcs.., timestam
            throw new IOException("Incorrect data format. "
                                  + "Concat operation.");
          }
          String trg = FSImage.readString(in);
          int srcSize = length - 1 - 1; //trg and timestamp
          String [] srcs = new String [srcSize];
          for(int i=0; i<srcSize;i++) {
            srcs[i]= FSImage.readString(in);
          }
          timestamp = readLong(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedConcat(trg, srcs, timestamp);
          fsDir.fsImage.getEditLog().logConcat(trg, srcs, timestamp);
          break;
          }

          case OP_RENAME: {
          numOpRename++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Ingest: Incorrect data format. " 
                                  + "Mkdir operation.");
          }
          String s = FSImage.readString(in);
          String d = FSImage.readString(in);
          timestamp = readLong(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          HdfsFileStatus dinfo = fsDir.getHdfsFileInfo(d);
          fsDir.unprotectedRenameTo(s, d, timestamp);
          fsNamesys.changeLease(s, d, dinfo);
          fsDir.fsImage.getEditLog().logRename(s, d, timestamp);
          break;
          }
          case OP_DELETE: {
          numOpDelete++;
          int length = in.readInt();
          if (length != 2) {
            throw new IOException("Ingest: Incorrect data format. " 
                                  + "delete operation.");
          }
          path = FSImage.readString(in);
          timestamp = readLong(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedDelete(path, timestamp);
          fsDir.fsImage.getEditLog().logDelete(path, timestamp);
          break;
          }
          case OP_MKDIR: {
          numOpMkDir++;
          PermissionStatus permissions = fsNamesys.getUpgradePermission();
          int length = in.readInt();
          if (-17 < logVersion && length != 2 ||
              logVersion <= -17 && length != 3) {
            throw new IOException("Ingest: Incorrect data format. " 
                                  + "Mkdir operation.");
          }
          path = FSImage.readString(in);
          timestamp = readLong(in);

          // The disk format stores atimes for directories as well.
          // However, currently this is not being updated/used because of
          // performance reasons.
          if (logVersion <= -17) {
            atime = readLong(in);
          }

          if (logVersion <= -11) {
            permissions = PermissionStatus.read(in);
          }
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          INode inode = fsDir.unprotectedMkdir(path, permissions, timestamp);
          fsDir.fsImage.getEditLog().logMkDir(path, inode);
          break;
          }
          case OP_SET_GENSTAMP: {
          numOpSetGenStamp++;
          long lw = in.readLong();
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsNamesys.setGenerationStamp(lw);
          fsDir.fsImage.getEditLog().logGenerationStamp(lw);
          break;
          } 
          case OP_DATANODE_ADD: {
          numOpOther++;
          FSImage.DatanodeImage nodeimage = new FSImage.DatanodeImage();
          nodeimage.readFields(in);
          //Datnodes are not persistent any more.
          break;
          }
          case OP_DATANODE_REMOVE: {
          numOpOther++;
          DatanodeID nodeID = new DatanodeID();
          nodeID.readFields(in);
          //Datanodes are not persistent any more.
          break;
          }
          case OP_SET_PERMISSIONS: {
          numOpSetPerm++;
          if (logVersion > -11)
            throw new IOException("Ingest: Unexpected opcode " + opcode
                                  + " for version " + logVersion);
          path = FSImage.readString(in);
          FsPermission permission = FsPermission.read(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedSetPermission(path, permission);
          fsDir.fsImage.getEditLog().logSetPermissions(path, permission);
          break;
          }
          case OP_SET_OWNER: {
          numOpSetOwner++;
          if (logVersion > -11)
            throw new IOException("Ingest: Unexpected opcode " + opcode
                                  + " for version " + logVersion);
          path = FSImage.readString(in);
          String username = FSImage.readString_EmptyAsNull(in);
          String groupname = FSImage.readString_EmptyAsNull(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedSetOwner(path, username, groupname);
          fsDir.fsImage.getEditLog().logSetOwner(path, username, groupname);
          break;
          }
          case OP_SET_NS_QUOTA: {
          if (logVersion > -16) {
            throw new IOException("Ingest: Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          path = FSImage.readString(in);
          long nsQuota = readLongWritable(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          INodeDirectory dir = fsDir.unprotectedSetQuota(path, 
                                    nsQuota, 
                                    FSConstants.QUOTA_DONT_SET);
          fsDir.fsImage.getEditLog().logSetQuota(path, dir.getNsQuota(), 
              dir.getDsQuota());
          break;
          }
          case OP_CLEAR_NS_QUOTA: {
          if (logVersion > -16) {
            throw new IOException("Ingest: Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          path = FSImage.readString(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          INodeDirectory dir = fsDir.unprotectedSetQuota(path,
                                    FSConstants.QUOTA_RESET,
                                    FSConstants.QUOTA_DONT_SET);
          fsDir.fsImage.getEditLog().logSetQuota(path, dir.getNsQuota(), 
              dir.getDsQuota());
          break;
          }

          case OP_SET_QUOTA:
            String src = FSImage.readString(in);
            long nsQuota = readLongWritable(in);
            long dsQuota = readLongWritable(in);
            FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
            INodeDirectory dir = fsDir.unprotectedSetQuota(src,
                                    nsQuota, dsQuota);
          fsDir.fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(), 
              dir.getDsQuota());
                                      
          break;

          case OP_TIMES: {
          numOpTimes++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Ingest: Incorrect data format. " 
                                  + "times operation.");
          }
          path = FSImage.readString(in);
          mtime = readLong(in);
          atime = readLong(in);
          FSEditLog.validateChecksum(supportChecksum, rawIn, checksum, numEdits);
          fsDir.unprotectedSetTimes(path, mtime, atime, true);
          fsDir.fsImage.getEditLog().logTimes(path, mtime, atime);
          break;
          }
          default: {
          throw new IOException("Ingest: Never seen opcode " + opcode);
          }
          }
          if (checksum.getValue() != FSEditLog.getChecksumForWrite().getValue()) {
            throw new IOException(
                "Ingest: mismatched r/w checksums for transaction #" + numEdits);
          }
          numEdits++;
          LOG.info("Ingest: Processed transaction from " + fname + " opcode " + opcode +
                   " file offset " + currentPosition);
        }
        catch (ChecksumException cex) {
          LOG.info("Checksum error reading the transaction #" + numEdits +
                   " reopening the file");
          reopen = true;
        }
        catch (IOException e) {
          LOG.info("Encountered error reading transaction", e);
          error = true; // if we haven't reached eof, then error.
          break;
        } finally {
          fsDir.fsImage.getEditLog().logSyncIfNeeded();
          fsNamesys.writeUnlock();
        }
      }
      
   
      // if we failed to read the entire transaction from disk, 
      // then roll back to the offset where there was a last good 
      // read, sleep for sometime for new transaction to
      // appear in the file and then continue;
      //
      if (error || running) {

        // discard older buffers and start a fresh one.
        fc.position(currentPosition);
        catchingUp = (fc.size() - fc.position() > catchUpLag);
        in = rawIn = new DataInputStream(fp);
        if (supportChecksum) {
          in = new DataInputStream(new CheckedInputStream(fp, checksum));
        }

        if (error) {
          LOG.info("Ingest: Incomplete transaction record at offset " + 
                   fc.position() +
                   " but the file is of size " + fc.size() + 
                   ". Continuing....");
        }

        if (running && !lastScan) {
          try {
            Thread.sleep(1000); // sleep for a second
          } catch (InterruptedException e) {
            // break out of waiting if we receive an interrupt.
          }
        }
      }
    }
    
    if (error) {
      // This was the last scan of the file but we could not read a full
      // transaction from disk. If we proceed this will corrupt the image
      throw new IOException("Failed to read the edits log. " + 
            "Incomplete transaction at " + currentPosition);
    }
    LOG.info("Ingest: Edits file " + fname.getName() +
      " numedits " + numEdits +
      " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");

    // If the last Scan was completed, then stop the Ingest thread.
    if (lastScan && quitAfterScan) {
      LOG.info("Ingest: lastScan completed.");
      running = false;
    }
    return numEdits; // total transactions consumed
  }

  // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /** Read an integer from an input stream */
  private static long readLongWritable(DataInputStream in) throws IOException {
    synchronized (longWritable) {
      longWritable.readFields(in);
      return longWritable.get();
    }
  }

  /**
   * A class to read in blocks stored in the old format. The only two
   * fields in the block were blockid and length.
   */
  static class BlockTwo implements Writable {
    long blkid;
    long len;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (BlockTwo.class,
         new WritableFactory() {
           public Writable newInstance() { return new BlockTwo(); }
         });
    }


    BlockTwo() {
      blkid = 0;
      len = 0;
    }

    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }

  //
  // These methods are copied from FsEdits/FsImage because they are defined as
  // private scope in those files.
  //
  static private DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in
      ) throws IOException {
    DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFieldsFromFSEditLog(in);
    }
    return locations;
  }

  static private short readShort(DataInputStream in) throws IOException {
    try {
      return Short.parseShort(FSImage.readString(in));
    } catch (NumberFormatException ex) {
      throw new IOException(ex);
    }
  }

  static private long readLong(DataInputStream in) throws IOException {
    try {
      return Long.parseLong(FSImage.readString(in));
    } catch (NumberFormatException ex) {
      throw new IOException(ex);
    }
  }

  static private Block[] readBlocks(DataInputStream in) throws IOException {
    int numBlocks = in.readInt();
    Block[] blocks = new Block[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }
    return blocks;
  }

  private static final byte OP_INVALID = -1;
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME = 1;  // rename
  private static final byte OP_DELETE = 2;  // delete
  private static final byte OP_MKDIR = 3;   // create directory
  private static final byte OP_SET_REPLICATION = 4; // set replication
  //the following two are used only for backward compatibility :
  @Deprecated private static final byte OP_DATANODE_ADD = 5;
  @Deprecated private static final byte OP_DATANODE_REMOVE = 6;
  private static final byte OP_SET_PERMISSIONS = 7;
  private static final byte OP_SET_OWNER = 8;
  private static final byte OP_CLOSE = 9;    // close after write
  private static final byte OP_SET_GENSTAMP = 10;    // store genstamp
  /* The following two are not used any more. Should be removed once
   * LAST_UPGRADABLE_LAYOUT_VERSION is -17 or newer. */
  private static final byte OP_SET_NS_QUOTA = 11; // set namespace quota
  private static final byte OP_CLEAR_NS_QUOTA = 12; // clear namespace quota
  private static final byte OP_TIMES = 13; // sets mod & access time on a file
  private static final byte OP_SET_QUOTA = 14; // sets name and disk quotas.
  private static final byte OP_CONCAT_DELETE = 16; // concat files.
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.zookeeper.Op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating random edit log sequences. Inspired by
 * the logic used in {@link TestEditLogBenchmark}
 * </p>
 * Current log ops used: OP_ADD, OP_CLOSE, OP_DELETE,
 * OP_SET_REPLICATION, OP_SET_GENSTAMP, OP_MKDIR,
 * OP_RENAME, OP_SET_OWNER, OP_SET_QUOTA, OP_TIMES,
 * OP_SET_PERMISSIONS, OP_CONCAT_DELETE
  */
public class LogOpGenerator {

  private static final Log LOG = LogFactory.getLog(LogOpGenerator.class);

  private static final int OPS_PER_FILE = 12;

  private final int numFiles;
  private final int blocksPerFile;
  private final ArrayList<FSEditLogOp> possibleOps;
  private final int numOpsPossible;
  private final Random random;
  private INodeId inodeId;

  /**
   * Instantiates pseudo-random edit log sequence generator.
   * @param numFiles Total number of distinct files
   * @param blocksPerFile Total number of distinct blocks per file
   */
  public LogOpGenerator(int numFiles, int blocksPerFile) {
    this.numFiles = numFiles;
    this.blocksPerFile = blocksPerFile;
    numOpsPossible = numFiles * OPS_PER_FILE;
    possibleOps = Lists.newArrayListWithExpectedSize(numOpsPossible);
    random = new Random();
    inodeId = new INodeId();
    init();
  }

  /**
   * Prefills the list of possible operations.
   */
  private void init() {
    LOG.info("--- Generating " + numOpsPossible + " log operations! ---");
    Random rng = new Random();
    for (int i = 0; i < numFiles; i++) {
      PermissionStatus p = new PermissionStatus("hadoop",
          "hadoop", new FsPermission((short)0777));
      INodeFileUnderConstruction inode = new INodeFileUnderConstruction(inodeId.nextValue(),
          p, (short) 3, 64, 0, "", "", null);
      for (int b = 0; b < blocksPerFile; b++) {
        Block block = new Block(b);
        BlockInfo bi = new BlockInfo(block, 3);
        bi.setChecksum(rng.nextInt(Integer.MAX_VALUE) + 1);
        try {
          inode.storage.addBlock(bi);
        } catch (IOException ioe) {
          LOG.error("Cannot add block", ioe);
        }
      }
      FsPermission perm = new FsPermission((short) 0);
      String name = "/filename-" + i;
      possibleOps.addAll(Arrays.asList(newOpenFile(name, inode),
          newCloseFile(name, inode),
          newDelete(name, 0),
          newSetReplication(name, (short)3),
          newGenerationStamp(i),
          newMkDir(name, inode),
          newRename(name, name, i),
          newSetOwner(name, "hadoop", "hadop"),
          newSetQuota(name, 1, 1),
          newTimes(name, 0 , 0),
          newSetPermissions(name, perm),
          newConcat(name, new String[] { name, name, name}, i),
          newMerge(name, name, "xor", new int[]{1, 1, 1}, i)));
    }
    LOG.info("--- Created " + numOpsPossible + " log operations! ---");
  }


  /**
   * Generate contiguous (monotonically increasing) transactions chosen at
   * random.
   * @param firstTxId First transaction id to include
   * @param lastTxId Last transaction id to include
   * @return Log segment of log operations with transaction ids firstTxId to
   *         lastTxId (inclusive) chosen using a pseudo-random generator
   */
  public List<FSEditLogOp> generateContiguousSegment(int firstTxId,
      int lastTxId) {
    List<FSEditLogOp> contiguousOps =
        Lists.newArrayListWithExpectedSize(lastTxId - firstTxId + 1);
    for (int i = firstTxId; i <= lastTxId; i++) {
      FSEditLogOp randomOp = possibleOps.get(random.nextInt(numOpsPossible));
      randomOp.setTransactionId(i);
      contiguousOps.add(randomOp);
    }
    return contiguousOps;
  }

  public static FSEditLogOp newOpenFile(String path,
      INodeFileUnderConstruction newNode) {
    FSEditLogOp.AddOp op = FSEditLogOp.AddOp.getUniqueInstance();
    op.set(newNode.getId(),
        path,
        newNode.getReplication(),
        newNode.getModificationTime(),
        newNode.getAccessTime(),
        newNode.getPreferredBlockSize(),
        newNode.getBlocks(),
        newNode.getPermissionStatus(),
        newNode.getClientName(),
        newNode.getClientMachine());
    return op;
  }

  public static FSEditLogOp newCloseFile(String path, INodeFile newNode) {
    FSEditLogOp.CloseOp op = FSEditLogOp.CloseOp.getUniqueInstance();
    op.set(newNode.getId(),
        path,
        newNode.getReplication(),
        newNode.getModificationTime(),
        newNode.getAccessTime(),
        newNode.getPreferredBlockSize(),
        newNode.getBlocks(),
        newNode.getPermissionStatus(),
        null,
        null);
    return op;
  }

  public static FSEditLogOp newDelete(String src, long timestamp){
    FSEditLogOp.DeleteOp op = FSEditLogOp.DeleteOp.getUniqueInstance();
    op.set(src, timestamp);
    return op;
  }

  public static FSEditLogOp newSetReplication(String src, short replication) {
    FSEditLogOp.SetReplicationOp op =
        FSEditLogOp.SetReplicationOp.getUniqueInstance();
    op.set(src, replication);
    return op;
  }

  public static FSEditLogOp newGenerationStamp(long genstamp) {
    FSEditLogOp.SetGenstampOp op =
        FSEditLogOp.SetGenstampOp.getUniqueInstance();
    op.set(genstamp);
    return op;
  }

  // Helper methods for generating various edit log operations

  public static FSEditLogOp newMkDir(String path, INode newNode) {
    FSEditLogOp.MkdirOp op = FSEditLogOp.MkdirOp.getUniqueInstance();
    op.set(newNode.getId(), path, newNode.getModificationTime(),
        newNode.getPermissionStatus());
    return op;
  }
  
  public static FSEditLogOp getMkDirInstance(String path) {
    FSEditLogOp.MkdirOp op = FSEditLogOp.MkdirOp.getUniqueInstance();
    op.set(INodeId.GRANDFATHER_INODE_ID, path, 0, new PermissionStatus("hadoop", "hadoop", new FsPermission(
        (short) 0777)));
    return op;
  }

  public static FSEditLogOp newRename(String src, String dst, long timestamp) {
    FSEditLogOp.RenameOp op = FSEditLogOp.RenameOp.getUniqueInstance();
    op.set(src, dst, timestamp);
    return op;
  }

  public static FSEditLogOp newSetOwner(String src, String username, String groupname) {
    FSEditLogOp.SetOwnerOp op = FSEditLogOp.SetOwnerOp.getUniqueInstance();
    op.set(src, username, groupname);
    return op;
  }

  public static FSEditLogOp newSetQuota(String src, long nsQuota, long dsQuota) {
    FSEditLogOp.SetQuotaOp op = FSEditLogOp.SetQuotaOp.getUniqueInstance();
    op.set(src, nsQuota, dsQuota);
    return op;
  }

  public static FSEditLogOp newTimes(String src, long mtime, long atime) {
    FSEditLogOp.TimesOp op = FSEditLogOp.TimesOp.getUniqueInstance();
    op.set(src, mtime, atime);
    return op;
  }

  public static FSEditLogOp newSetPermissions(String src, FsPermission permissions) {
    FSEditLogOp.SetPermissionsOp op =
        FSEditLogOp.SetPermissionsOp.getUniqueInstance();
    op.set(src, permissions);
    return op;
  }

  public static FSEditLogOp newConcat(String trg, String [] srcs, long timestamp) {
    FSEditLogOp.ConcatDeleteOp op =
        FSEditLogOp.ConcatDeleteOp.getUniqueInstance();
    op.set(trg, srcs, timestamp);
    return op;
  }
  
  public static FSEditLogOp newMerge(String parity, String source, String codecId, 
      int[] checksums, long timestamp) {
    FSEditLogOp.MergeOp op = 
        FSEditLogOp.MergeOp.getUniqueInstance();
    op.set(parity, source, codecId, checksums, timestamp);
    return op;
  }
}

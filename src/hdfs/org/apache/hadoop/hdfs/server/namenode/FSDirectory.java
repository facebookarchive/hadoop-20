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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 *************************************************/
class FSDirectory implements FSConstants, Closeable {

  final INodeDirectoryWithQuota rootDir;
  FSImage fsImage;  
  private boolean ready = false;
  // Metrics record
  private MetricsRecord directoryMetrics = null;
  private final int lsLimit;  // max list limit
  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  // lock to protect BlockMap.
  private ReentrantReadWriteLock bLock;
  private Condition cond;
  private boolean hasRwLock;
  // Whether or not to sync writes to the edit log for persisting blocks.
  private boolean syncPersistBlockEdits = false;

  // utility methods to acquire and release read lock and write lock
  // if hasRwLock is false, then readLocks morph into writeLocks.
  void readLock() {
    if (hasRwLock) {
      this.bLock.readLock().lock();
    } else {
      writeLock();
    }
  }

  void readUnlock() {
    if (hasRwLock) {
      this.bLock.readLock().unlock();
    } else {
      writeUnlock();
    }
  }

  void writeLock() {
    this.bLock.writeLock().lock();
  }

  void writeUnlock() {
    this.bLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.bLock.isWriteLockedByCurrentThread();
  }

  /** Access an existing dfs name directory. */
  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this(new FSImage(conf), ns, conf);
    if (conf.getBoolean("dfs.name.dir.restore", false)) {
      NameNode.LOG.info("set FSImage.restoreFailedStorage");
      fsImage.setRestoreFailedStorage(true);
    }
  }

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
        ns.createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE, -1);
    this.fsImage = fsImage;
    this.fsImage.setFSNamesystem(ns);
    int configuredLimit = conf.getInt(
        "dfs.ls.limit", 1000);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : 1000;
    int threshold = conf.getInt(
        "dfs.namenode.name.cache.threshold",
        10);
    NameNode.LOG.info("Caching file names occuring more than " + threshold
        + " times ");
    nameCache = new NameCache<ByteArray>(threshold);
    initialize(conf);
  }

  private FSNamesystem getFSNamesystem() {
    return fsImage.getFSNamesystem();
  }
    
  private void initialize(Configuration conf) {
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    directoryMetrics = MetricsUtil.createRecord(metricsContext, "FSDirectory");
    directoryMetrics.setTag("sessionId", conf.get("session.id"));
    this.bLock = new ReentrantReadWriteLock(); // non-fair
    this.cond = bLock.writeLock().newCondition();
    this.hasRwLock = getFSNamesystem().hasRwLock;
    this.syncPersistBlockEdits = conf.getBoolean(
        "dfs.persist.blocks.sync.edits", false);
  }

  void loadFSImage(Collection<File> dataDirs,
                   Collection<File> editsDirs,
                   StartupOption startOpt) throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      fsImage.setStorageDirectories(dataDirs, editsDirs);
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      if (fsImage.recoverTransitionRead(dataDirs, editsDirs, startOpt)) {
        fsImage.saveNamespace(true);
      }
      FSEditLog editLog = fsImage.getEditLog();
      assert editLog != null : "editLog must be initialized";
      if (!editLog.isOpen())
        editLog.open();
      fsImage.setCheckpointDirectories(null, null);
    } catch(IOException e) {
      fsImage.close();
      throw e;
    }
    writeLock();
    try {
      this.ready = true;
      this.nameCache.initialized();
      cond.signalAll();
    } finally {
      writeUnlock();
    }
  }

  private void incrDeletedFileCount(int count) {
    directoryMetrics.incrMetric("files_deleted", count);
    directoryMetrics.update();
  }
    
  /**
   * Shutdown the filestore
   */
  public void close() throws IOException {
    fsImage.close();
  }

  /**
   * Block until the object is ready to be used.
   */
  void waitForReady() {
    if (!ready) {
      writeLock();
      try {
        while (!ready) {
          try {
            cond.await(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ie) {
          }
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   * Add the given filename to the fs.
   */
  INodeFileUnderConstruction addFile(String path,
                String[] names,
                byte[][] components,
                INode[] inodes,
                PermissionStatus permissions,
                short replication,
                long preferredBlockSize,
                String clientName,
                String clientMachine,
                DatanodeDescriptor clientNode,
                long generationStamp) 
                throws IOException {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = FSNamesystem.now();
    if (inodes[inodes.length-2] == null) { // non-existent directory
      if (!mkdirs(names[names.length-1],
          names, components, inodes, inodes.length-1,
          permissions, true, modTime)) {
        return null;
      }
    }
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                                 permissions,replication,
                                 preferredBlockSize, modTime, clientName, 
                                 clientMachine, clientNode);
    newNode.setLocalName(components[inodes.length-1]);
    writeLock();
    try {
      newNode = addChild(inodes, inodes.length-1, newNode, -1, false);
    } finally {
      writeUnlock();
    }
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                                   +"failed to add "+path
                                   +" to the file system");
      return null;
    }
    // add create file record to log, record new generation stamp
    fsImage.getEditLog().logOpenFile(path, newNode);

    NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                  +path+" is added to the file system");
    return newNode;
  }

  /**
   */
  INode unprotectedAddFile( String path, 
                            PermissionStatus permissions,
                            Block[] blocks, 
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize) {
    INode newNode;
    long diskspace = -1; // unknown
    if (blocks == null)
      newNode = new INodeDirectory(permissions, modificationTime);
    else {
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, atime, preferredBlockSize);
      diskspace = ((INodeFile)newNode).diskspaceConsumed(blocks);
    }
    writeLock();
    try {
      try {
        newNode = addNode(path, newNode, diskspace, false);
        if(newNode != null && blocks != null) {
          int nrBlocks = blocks.length;
          // Add file->block mapping
          INodeFile newF = (INodeFile)newNode;
          for (int i = 0; i < nrBlocks; i++) {
            newF.setBlock(i, getFSNamesystem().blocksMap.addINode(blocks[i], newF));
          }
        }
      } catch (IOException e) {
        return null;
      }
      return newNode;
    } finally {
      writeUnlock();
    }
  }

  INodeDirectory addToParent( byte[] src,
                              INodeDirectory parentINode,
                              INode newNode,
                              boolean propagateModTime) {
    // NOTE: This does not update space counts for parents
    // add new node to the parent
    INodeDirectory newParent = null;
    writeLock();
    try {
      try {
        newParent = rootDir.addToParent(src, newNode, parentINode,
                                            false, propagateModTime);
        cacheName(newNode);
      } catch (FileNotFoundException e) {
        return null;
      }
      if(newParent == null)
        return null;
      if(!newNode.isDirectory()) {
        // Add block->file mapping
        INodeFile newF = (INodeFile)newNode;
        BlockInfo[] blocks = newF.getBlocks();
        for (int i = 0; i < blocks.length; i++) {
          newF.setBlock(i, getFSNamesystem().blocksMap.addINode(blocks[i], newF));
        }
      }
    } finally {
      writeUnlock();
    }
    return newParent;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  Block addBlock(String path, INode[] inodes, Block block) throws IOException {
    waitForReady();

    writeLock();
    try {
      INodeFile fileNode = (INodeFile) inodes[inodes.length-1];

      // check quota limits and updated space consumed
      updateCount(inodes, inodes.length-1, 0, 
          fileNode.getPreferredBlockSize()*fileNode.getReplication(), true);
      
      // associate the new list of blocks with this file
      BlockInfo blockInfo = getFSNamesystem().blocksMap.addINode(block, fileNode);
      fileNode.addBlock(blockInfo);

      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    + path + " with " + block
                                    + " block is added to the in-memory "
                                    + "file system");
    } finally {
      writeUnlock();
    }
    return block;
  }

  /**
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFileUnderConstruction file) 
                     throws IOException {
    waitForReady();

    writeLock();
    try {
      fsImage.getEditLog().logOpenFile(path, file);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
    } finally {
      writeUnlock();
    }
    if (this.syncPersistBlockEdits) {
      fsImage.getEditLog().logSync();
    }
  }

  /**
   * Close file.
   */
  void closeFile(String path, INodeFile file) throws IOException {
    waitForReady();
    writeLock();
    try {
      long now = FSNamesystem.now();
      // file is closed
      file.setModificationTimeForce(now);
      fsImage.getEditLog().logCloseFile(path, file);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove the block from a file.
   */
  boolean removeBlock(String path, INodeFileUnderConstruction fileNode, 
                      Block block) throws IOException {
    waitForReady();

    writeLock();
    try {
      // modify file-> block and blocksMap
      fileNode.removeBlock(block);
      getFSNamesystem().blocksMap.removeBlock(block);
      // Remove the block from corruptReplicasMap
      getFSNamesystem().corruptReplicas.removeFromCorruptReplicasMap(block);

      // write modified block locations to log
      fsImage.getEditLog().logOpenFile(path, fileNode);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
                                    +path+" with "+block
                                    +" block is removed from the file system");
    } finally {
      writeUnlock();
    }
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long)
   */
  boolean renameTo(String src, String srcLocalName, byte[] srcComponent,
      INode[] srcInodes, String dst, INode[] dstInodes,
      byte[] lastComponent) throws QuotaExceededException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                                  +src+" to "+dst);
    }
    waitForReady();
    long now = FSNamesystem.now();
    if (!unprotectedRenameTo(src, srcLocalName, srcComponent, srcInodes,
        dst, dstInodes, lastComponent, now))
      return false;
    fsImage.getEditLog().logRename(src, dst, now);
    return true;
  }

  /** Change a path name
   * 
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp) 
  throws QuotaExceededException {
    return unprotectedRenameTo(src, null, null, null, dst, null, null, timestamp);
  }

  private boolean unprotectedRenameTo(String src, String srcLocalName, byte[] srcComponent, INode[] srcInodes,
      String dst, INode[] dstInodes, byte[] dstComponent, long timestamp)
    throws QuotaExceededException {
    writeLock();
    try {
      if (srcInodes == null) {
        String[] srcNames = INode.getPathNames(src);
        srcLocalName = srcNames[srcNames.length-1];
        byte[][] srcComponents = INode.getPathComponents(srcNames);
        srcComponent = srcComponents[srcComponents.length-1];
        srcInodes = new INode[srcComponents.length];
        rootDir.getExistingPathINodes(srcComponents, srcInodes);
      }

      // check the validation of the source
      if (srcInodes[srcInodes.length-1] == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because source does not exist");
        return false;
      } 
      if (srcInodes.length == 1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ " because source is the root");
        return false;
      }
      if (dstInodes == null) {
        byte[][] dstComponents = INode.getPathComponents(dst);
        dstComponent = dstComponents[dstComponents.length-1];
        dstInodes = new INode[dstComponents.length];
        rootDir.getExistingPathINodes(dstComponents, dstInodes);
      }
      INode dstNode = dstInodes[dstInodes.length-1];
      if (dstNode != null && dstNode.isDirectory()) {
        dst += Path.SEPARATOR + srcLocalName;
        dstInodes = Arrays.copyOf(dstInodes, dstInodes.length+1);
        dstComponent = srcComponent;
        dstInodes[dstInodes.length-1] = ((INodeDirectory)dstNode).
                               getChildINode(dstComponent);
      }
      
      // check the validity of the destination
      if (dst.equals(src)) {
        return true;
      }
      // dst cannot be directory or a file under src
      if (dst.startsWith(src) && 
          dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because destination starts with src");
        return false;
      }

      if (dstInodes[dstInodes.length-1] != null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ 
                                     " because destination exists");
        return false;
      }
      if (dstInodes[dstInodes.length-2] == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ 
            " because destination's parent does not exist");
        return false;
      }
      
      // Ensure dst has quota to accommodate rename
      verifyQuotaForRename(srcInodes,dstInodes);
      
      INode dstChild = null;
      INode srcChild = null;
      String srcChildName = null;
        try {
        // remove src
        srcChild = removeChild(srcInodes, srcInodes.length-1);
        if (srcChild == null) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
              + "failed to rename " + src + " to " + dst
              + " because the source can not be removed");
          return false;
      }
        srcChildName = srcChild.getLocalName();
        srcChild.setLocalName(dstComponent);
        
        // add src to the destination
        dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
            srcChild, -1, false);
      if (dstChild != null) {
          srcChild = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " + src
                    + " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length-2].setModificationTime(timestamp);
        dstInodes[dstInodes.length-2].setModificationTime(timestamp);

        // update dstInodes
        dstInodes[dstInodes.length-1] = dstChild;
        srcInodes[srcInodes.length-1] = null;
        return true;
        }
      } finally {
        if (dstChild == null && srcChild != null) {
          // put it back
          srcChild.setLocalName(srcChildName);
          addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild, -1,
              false);
        }
      }
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst);
      return false;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set file replication
   * 
   * @param src file name
   * @param replication new replication
   * @param oldReplication old replication - output parameter
   * @return array of file blocks
   * @throws IOException
   */
  Block[] setReplication(String src, 
                         short replication,
                         int[] oldReplication
                         ) throws IOException {
    waitForReady();
    Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    if (fileBlocks != null)  // log replication change
      fsImage.getEditLog().logSetReplication(src, replication);
    return fileBlocks;
  }

  Block[] unprotectedSetReplication( String src, 
                                     short replication,
                                     int[] oldReplication
                                     ) throws IOException {
    if (oldReplication == null)
      oldReplication = new int[1];
    oldReplication[0] = -1;
    Block[] fileBlocks = null;
    writeLock();
    try {
      INode[] inodes = rootDir.getExistingPathINodes(src);
      INode inode = inodes[inodes.length - 1];
      if (inode == null)
        return null;
      if (inode.isDirectory())
        return null;
      INodeFile fileNode = (INodeFile)inode;
      oldReplication[0] = fileNode.getReplication();

      // check disk quota
      long dsDelta = (replication - oldReplication[0]) *
           (fileNode.diskspaceConsumed()/oldReplication[0]);
      updateCount(inodes, inodes.length-1, 0, dsDelta, true);

      fileNode.setReplication(replication);
      fileBlocks = fileNode.getBlocks();
    } finally {
      writeUnlock();
    }
    return fileBlocks;
  }

  /**
   * Get the blocksize of a file
   * @param filename the filename
   * @return the number of bytes 
   * @throws IOException if it is a directory or does not exist.
   */
  long getPreferredBlockSize(String filename) throws IOException {
    readLock();
    try {
      INode fileNode = rootDir.getNode(filename);
      if (fileNode == null) {
        throw new IOException("Unknown file: " + filename);
      }
      if (fileNode.isDirectory()) {
        throw new IOException("Getting block size of a directory: " + 
                              filename);
      }
      return ((INodeFile)fileNode).getPreferredBlockSize();
    } finally {
      readUnlock();
    }
  }
  
  /**
   * This is a method required in addition
   * to getFileInode
   * It returns the INode regardless of its type
   * getFileInode only returns the inode if it is
   * of a file type
   */  
  INode getInode(String src) {
    src = normalizePath(src);
    readLock();
    try {
      INode inode = rootDir.getNode(src);
      return inode;
    } finally {
      readUnlock();
    }
  }

  boolean exists(String src) {
    src = normalizePath(src);
    readLock();
    try {
      INode inode = rootDir.getNode(src);
      if (inode == null) {
         return false;
      }
      return inode.isDirectory()? true: ((INodeFile)inode).getBlocks() != null;
    } finally {
      readUnlock();
    }
  }

  void setPermission(String src, FsPermission permission
      ) throws IOException {
    unprotectedSetPermission(src, permission);
    fsImage.getEditLog().logSetPermissions(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions) throws FileNotFoundException {
    writeLock();
    try {
        INode inode = rootDir.getNode(src);
        if(inode == null)
            throw new FileNotFoundException("File does not exist: " + src);
        inode.setPermission(permissions);
    } finally {
      writeUnlock();
    }
  }

  void setOwner(String src, String username, String groupname
      ) throws IOException {
    unprotectedSetOwner(src, username, groupname);
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname) throws FileNotFoundException {
    writeLock();
    try {
      INode inode = rootDir.getNode(src);
      if(inode == null)
          throw new FileNotFoundException("File does not exist: " + src);
      if (username != null) {
        inode.setUser(username);
      }
      if (groupname != null) {
        inode.setGroup(groupname);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   */
  public void concatInternal(String target, String [] srcs) {
    // actual move
    waitForReady();

    long now = FSNamesystem.now();
    unprotectedConcat(target, srcs, now);
    fsImage.getEditLog().logConcat(target, srcs, now);
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param target target file to move the blocks to
   * @param srcs list of file to move the blocks from
   * Must be public because also called from EditLogs
   * NOTE: - it does not update quota (not needed for concat)
   */
  public void unprotectedConcat(String target, String [] srcs, long now) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
    }
    // do the move

    INode [] trgINodes =  getExistingPathINodes(target);
    INodeFile trgInode = (INodeFile) trgINodes[trgINodes.length-1];
    INodeDirectory trgParent = (INodeDirectory)trgINodes[trgINodes.length-2];

    INodeFile [] allSrcInodes = new INodeFile[srcs.length];
    int i = 0;
    int totalBlocks = 0;

    writeLock();
    try {
      for(String src : srcs) {
        INodeFile srcInode = getFileINode(src);
        allSrcInodes[i++] = srcInode;
        totalBlocks += srcInode.blocks.length;
      }
      trgInode.appendBlocks(allSrcInodes, totalBlocks); // copy the blocks

      // since we are in the same dir - we can use same parent to remove files
      int count = 0;
      for(INodeFile nodeToRemove: allSrcInodes) {
        if(nodeToRemove == null) continue;

        nodeToRemove.blocks = null;
        trgParent.removeChild(nodeToRemove);
        count++;
      }
      trgInode.setModificationTime(now);
      trgParent.setModificationTime(now);
      // update quota on the parent directory ('count' files removed, 0 space)
      unprotectedUpdateCount(trgINodes, trgINodes.length-1, - count, 0);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove the file from management, return blocks
   */
  INode delete(String src, INode[] inodes, List<Block> collectedBlocks) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "+src);
    }
    waitForReady();
    long now = FSNamesystem.now();
    INode deletedNode = unprotectedDelete(src, inodes, collectedBlocks, now);
    if (deletedNode != null) {
      fsImage.getEditLog().logDelete(src, now);
    }
    return deletedNode;
  }
  
  /** Return if an inode is empty or not **/
  boolean isDirEmpty(INode targetNode) {
	   boolean dirNotEmpty = true;
    if (!(targetNode != null && targetNode.isDirectory())) {
      return true;
    }
    readLock();
    try {
      assert targetNode != null : "should be taken care in isDir() above";
      if (((INodeDirectory)targetNode).getChildren().size() != 0) {
        dirNotEmpty = false;
      }
    } finally {
      readUnlock();
    }
    return dirNotEmpty;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param src a string representation of a path to an inode
   * @param modificationTime the time the inode is removed
   * @return the deleted target inode, null if deletion failed
   */ 
  INode unprotectedDelete(String src, long modificationTime) {
    return unprotectedDelete(src, this.getExistingPathINodes(src), null, modificationTime);
  }

  INode unprotectedDelete(String src, INode[] inodes, long modificationTime) {
    return unprotectedDelete(src, inodes, null, modificationTime);
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * The blocks will be put in toBeDeletedBlocks to be removed later
   * @param src a string representation of a path to an inode
   * @param inodes all the inodes on the given path
   * @param toBeDeletedBlocks the place holder for the blocks to be removed
   * @param modificationTime the time the inode is removed
   * @return the deleted target inode, null if deletion failed
   */ 
  INode unprotectedDelete(String src, INode inodes[], List<Block> toBeDeletedBlocks,
                          long modificationTime) {
    src = normalizePath(src);

    writeLock();
    try {
      INode targetNode = inodes[inodes.length-1];

      if (targetNode == null) { // non-existent src
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            +"failed to remove "+src+" because it does not exist");
        return null;
      } else if (inodes.length == 1) { // src is the root
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
            "failed to remove " + src +
            " because the root is not allowed to be deleted");
        return null;
      } else {
        try {
          // Remove the node from the namespace
          removeChild(inodes, inodes.length-1);
          // set the parent's modification time
          inodes[inodes.length-2].setModificationTime(modificationTime);
          // GC all the blocks underneath the node.
          if (toBeDeletedBlocks == null) {
            ArrayList<Block> v = new ArrayList<Block>();
            int filesRemoved =
              targetNode.collectSubtreeBlocksAndClear(v);
            incrDeletedFileCount(filesRemoved);
            // remove the blocks right away if toBeDeletedBlocks is null
            getFSNamesystem().removePathAndBlocks(src, v);
          } else {
            int filesRemoved =
              targetNode.collectSubtreeBlocksAndClear(toBeDeletedBlocks);
            incrDeletedFileCount(filesRemoved);
            // pass null for the blocks because the blocks will be deleted later
            getFSNamesystem().removePathAndBlocks(src, null);
          }
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                +src+" is removed");
          }
          return targetNode;
        } catch (IOException e) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
              "failed to remove " + src + " because " + e.getMessage());
          return null;
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Replaces the specified inode with the specified one.
   */
  void replaceNode(String path, INodeFile oldnode, INodeFile newnode) 
                                                   throws IOException {
    replaceNode(path, null, oldnode, newnode, true);
  }
  
  /**
   * @see #replaceNode(String, INodeFile, INodeFile)
   */
  void replaceNode(String path, INode[] inodes, INodeFile oldnode, INodeFile newnode,
                           boolean updateDiskspace) throws IOException {    
    writeLock();
    try {
      long dsOld = oldnode.diskspaceConsumed();
      
      //
      // Remove the node from the namespace 
      //
      if (!oldnode.removeNode()) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.replaceNode: " +
                                     "failed to remove " + path);
        throw new IOException("FSDirectory.replaceNode: " +
                              "failed to remove " + path);
      } 
      
      /* Currently oldnode and newnode are assumed to contain the same
       * blocks. Otherwise, blocks need to be removed from the blocksMap.
       */
      if (inodes == null) {
        inodes = rootDir.getExistingPathINodes(path);
      }
      INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
      newnode.setLocalName(oldnode.getLocalNameBytes());
      parent.addChild(newnode, false);
      inodes[inodes.length-1] = newnode;

      //check if disk space needs to be updated.
      long dsNew = 0;
      if (updateDiskspace && (dsNew = newnode.diskspaceConsumed()) != dsOld) {
        try {
          updateSpaceConsumed(path, null, 0, dsNew-dsOld);
        } catch (QuotaExceededException e) {
          // undo
          replaceNode(path, inodes, newnode, oldnode, false);
          throw e;
        }
      }
      
      int index = 0;
      for (Block b : newnode.getBlocks()) {
        BlockInfo info = getFSNamesystem().blocksMap.addINode(b, newnode);
        newnode.setBlock(index, info); // inode refers to the block in BlocksMap
        index++;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Get a listing of files given path 'src'
   *
   * This function is admittedly very inefficient right now.  We'll
   * make it better later.
   */
  FileStatus[] getListing(String src) {
    String srcs = normalizePath(src);

    readLock();
    try {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null)
        return null;
      if (!targetNode.isDirectory()) {
        return new FileStatus[]{createFileStatus(
        		src, targetNode)};
      }
      List<INode> contents = ((INodeDirectory)targetNode).getChildren();
      if(! srcs.endsWith(Path.SEPARATOR))
    	  srcs += Path.SEPARATOR;
      FileStatus listing[] = new FileStatus[contents.size()];
      int i = 0;
      for (INode cur : contents) {
        listing[i] = createFileStatus(srcs+cur.getLocalName(), cur);
        i++;
      }
      return listing;
    } finally {
      readUnlock();
    }
  }

  /**
   * Get a listing of files given path 'src'
   *
   * This function is admittedly very inefficient right now.  We'll
   * make it better later.
   */
  HdfsFileStatus[] getHdfsListing(String src) {
    String srcs = normalizePath(src);

    readLock();
    try {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null)
        return null;
      if (!targetNode.isDirectory()) {
        return new HdfsFileStatus[]{createHdfsFileStatus(
        		HdfsFileStatus.EMPTY_NAME, targetNode)};
      }
      List<INode> contents = ((INodeDirectory)targetNode).getChildren();
      HdfsFileStatus listing[] = new HdfsFileStatus[contents.size()];
      int i = 0;
      for (INode cur : contents) {
        listing[i] = createHdfsFileStatus(cur.name, cur);
        i++;
      }
      return listing;
    } finally {
      readUnlock();
    }
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src the directory name
   * @param targetNode the inode representing the given path
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  DirectoryListing getPartialListing(String src, INode targetNode,
      byte[] startAfter,
      boolean needLocation) throws IOException {
    readLock();
    try {
      if (targetNode == null)
        return null;

      if (!targetNode.isDirectory()) {
        HdfsFileStatus[] partialListing = new HdfsFileStatus[]{
            createHdfsFileStatus(
                HdfsFileStatus.EMPTY_NAME, targetNode)};
        if (needLocation) {
          return new LocatedDirectoryListing(partialListing,
              new LocatedBlocks[] {createLocatedBlocks(targetNode)}, 0);
        } else {
          return new DirectoryListing(partialListing, 0);
        }
      }
      INodeDirectory dirInode = (INodeDirectory)targetNode;
      List<INode> contents = dirInode.getChildren();
      // find the first child whose name is greater than startAfter
      int startChild = dirInode.nextChild(startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      LocatedBlocks [] blockLocations = new LocatedBlocks[numOfListing];
      for (int i=0; i<numOfListing; i++) {
        INode cur = contents.get(startChild+i);
        listing[i] = createHdfsFileStatus(cur.name, cur);
        if (needLocation) {
          blockLocations[i] = createLocatedBlocks(cur);
        }
      }
      if (needLocation) {
        return new LocatedDirectoryListing(
            listing, blockLocations, totalNumChildren-startChild-numOfListing);
      } else {
        return new DirectoryListing(
            listing, totalNumChildren-startChild-numOfListing);
      }
  } finally {
    readUnlock();
  }
  }
  
  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  FileStatus getFileInfo(String src, INode targetNode) {
    String srcs = normalizePath(src);
    readLock();
    try {
      if (targetNode == null) {
        return null;
      }
      else {
        return createFileStatus(srcs, targetNode);
      }
    } finally {
      readUnlock();
    }
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getHdfsFileInfo(String src) {
    String srcs = normalizePath(src);
    readLock();
    try {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        return null;
      }
      else {
        return createHdfsFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
      }
    } finally {
      readUnlock();
    }
  }

  static HdfsFileStatus getHdfsFileInfo(INode node) {
    return createHdfsFileStatus(HdfsFileStatus.EMPTY_NAME, node);
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) {
    waitForReady();
    readLock();
    try {
      INode targetNode = rootDir.getNode(src);
      if (targetNode == null)
        return null;
      if(targetNode.isDirectory())
        return null;
      return ((INodeFile)targetNode).getBlocks();
    } finally {
      readUnlock();
    }
  }

  /**
   * Get {@link INode} associated with the file.
   */
  INodeFile getFileINode(String src) {
    readLock();
    try {
      INode inode = rootDir.getNode(src);
      if (inode == null || inode.isDirectory())
        return null;
      return (INodeFile)inode;
    } finally {
      readUnlock();
    }
  }

  /**
   * Retrieve the existing INodes along the given path.
   * 
   * @param path the path to explore
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see INodeDirectory#getExistingPathINodes(byte[][], INode[])
   */
  INode[] getExistingPathINodes(String path) {
    readLock();
    try {
      return rootDir.getExistingPathINodes(path);
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get the parent node of path.
   *
   * @param path the path to explore
   * @return its parent node
   */
  INodeDirectory getParent(byte[][] path) throws FileNotFoundException {
    readLock();
    try {
      return rootDir.getParent(path);
    } finally {
      readUnlock();
    }
  }

  /** 
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src) {
    String srcs = normalizePath(src);
    readLock();
    try {
      if (srcs.startsWith("/") && 
          !srcs.endsWith("/") && 
          rootDir.getNode(srcs) == null) {
        return true;
      } else {
        return false;
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) {
    readLock();
    try {
      INode node = rootDir.getNode(normalizePath(src));
      return isDir(node);
    } finally {
      readUnlock();
    }
  }

  static boolean isDir(INode inode) {
    return inode != null && inode.isDirectory();
  }

  /** Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   * 
   * @param path path for the file.
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFound if path does not exist.
   */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
                                         throws QuotaExceededException,
                                                FileNotFoundException {
    updateSpaceConsumed(path, null, nsDelta, dsDelta);
  }
  
  /** Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   * 
   * @param path path for the file.
   * @param inodes inode array representation of the path
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFound if path does not exist.
   */
  void updateSpaceConsumed(String path, INode[] inodes, long nsDelta, long dsDelta)
  throws QuotaExceededException,
         FileNotFoundException {
    writeLock();
    try {
      if (inodes == null) {
        inodes = rootDir.getExistingPathINodes(path);
      }
      int len = inodes.length;
      if (inodes[len - 1] == null) {
        throw new FileNotFoundException(path + 
                                        " does not exist under rootDir.");
      }
      updateCount(inodes, len-1, nsDelta, dsDelta, true);
    } finally {
      writeUnlock();
    }
  }
  
  /** update count of each inode with quota
   * 
   * @param inodes an array of inodes on a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private void updateCount(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta, boolean checkQuota)
                           throws QuotaExceededException {
    if (!ready) {
      //still intializing. do not check or update quotas.
      return;
    }
    if (numOfINodes>inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
      }
    for(int i = 0; i < numOfINodes; i++) {
          if (inodes[i].isQuotaSet()) { // a directory with quota
            INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
        node.updateNumItemsInTree(nsDelta, dsDelta);
          }
        }
      }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INode[], int, long, long, boolean)}
   */ 
  private void updateCountNoQuotaCheck(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta) {
    try {
      updateCount(inodes, numOfINodes, nsDelta, dsDelta, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.updateCountNoQuotaCheck - unexpected ", e);
    }
  }

  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   * @param inodes
   * @param numOfINodes
   * @param nsDelta
   * @param dsDelta
   */
   private void unprotectedUpdateCount(INode[] inodes, int numOfINodes,
                                      long nsDelta, long dsDelta) {
    for(int i=0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i];
        node.unprotectedUpdateNumItemsInTree(nsDelta, dsDelta);
      }
    }
  }

  
  /** Return the name of the path represented by inodes at [0, pos] */
  private static String getFullPathName(INode[] inodes, int pos) {
    StringBuilder fullPathName = new StringBuilder();
    for (int i=1; i<=pos; i++) {
      fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
    }
    return fullPathName.toString();
  }

  /** Return the full path name of the specified inode */
  static String getFullPathName(INode inode) {
    // calculate the depth of this inode from root
    int depth = 0;
    for (INode i = inode; i != null; i = i.parent) {
      depth++;
    }
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      inodes[depth-i-1] = inode;
      inode = inode.parent;
    }
    return getFullPathName(inodes, depth-1);
  }
  
  /**
   * Create a directory 
   * If ancestor directories do not exist, automatically create them.

   * @param src string representation of the path to the directory
   * @param permissions the permission of the directory
   * @param inheritPermission if the permission of the directory should inherit
   *                          from its parent or not. The automatically created
   *                          ones always inherit its permission from its parent
   * @param now creation time
   * @return true if the operation succeeds false otherwise
   * @throws FileNotFoundException if an ancestor or itself is a file
   * @throws QuotaExceededException if directory creation violates
   *                                any quota limit
   */
  boolean mkdirs(String src,PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileNotFoundException, QuotaExceededException {
      src = normalizePath(src);
      String[] names = INode.getPathNames(src);
      byte[][] components = INode.getPathComponents(names);
      return mkdirs(src, names, components, null,
          names.length, permissions, inheritPermission, now);
  }

  /**
   * Create a directory
   * If ancestor directories do not exist, automatically create them.

   * @param src string representation of the path to the directory
   * @param names the string array representation of src
   * @param componenets the bytes array representation of src
   * @param inodes the inodes array representation of src
   * @param pos the position at which the directory to be created
   * @param permissions the permission of the directory
   * @param inheritPermission if the permission of the directory should inherit
   *                          from its parent or not. The automatically created
   *                          ones always inherit its permission from its parent
   * @param now creation time
   * @return true if the operation succeeds false otherwise
   * @throws FileNotFoundException if an ancestor or itself is a file
   * @throws QuotaExceededException if directory creation violates
   *                                any quota limit
   */
  boolean mkdirs(String src, String[] names, byte[][] components,
      INode[] inodes, int pos, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileNotFoundException, QuotaExceededException {
    writeLock();
    try {
      if (inodes == null) {
        inodes = new INode[pos];
        rootDir.getExistingPathINodes(components, inodes);
      }

      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      for(; i < inodes.length && inodes[i] != null; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        if (!inodes[i].isDirectory()) {
          throw new FileNotFoundException("Parent path is not a directory: "
              + pathbuilder);
        }
      }

      // create directories beginning from the first null index
      for(; i < pos; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        String cur = pathbuilder.toString();
        unprotectedMkdir(inodes, i, components[i], permissions,
            inheritPermission || i != components.length-1, now);
        if (inodes[i] == null) {
          return false;
        }
        // Directory creation also count towards FilesCreated
        // to match count of files_deleted metric. 
        if (getFSNamesystem() != null)
          NameNode.getNameNodeMetrics().numFilesCreated.inc();
        fsImage.getEditLog().logMkDir(cur, inodes[i]);
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.mkdirs: created directory " + cur);
      }
    } finally {
      writeUnlock();
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(String src, PermissionStatus permissions,
                          long timestamp) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];
    writeLock();
    try {
      rootDir.getExistingPathINodes(components, inodes);
      unprotectedMkdir(inodes, inodes.length-1, components[inodes.length-1],
          permissions, false, timestamp);
      return inodes[inodes.length-1];
    } finally {
      writeUnlock();
    }
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(INode[] inodes, int pos,
      byte[] name, PermissionStatus permission, boolean inheritPermission,
      long timestamp) throws QuotaExceededException {
    inodes[pos] = addChild(inodes, pos, 
        new INodeDirectory(name, permission, timestamp),
        -1, inheritPermission );
  }
  
  /** Add a node child to the namespace. The full path name of the node is src.
   * childDiskspace should be -1, if unknown. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addNode(String src, T child, 
        long childDiskspace, boolean inheritPermission) 
  throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    byte[] path = components[components.length - 1];
    child.setLocalName(path);
    cacheName(child);
    INode[] inodes = new INode[components.length];
    writeLock();
    try {
      rootDir.getExistingPathINodes(components, inodes);
      return addChild(inodes, inodes.length-1, child, childDiskspace,
                      inheritPermission);
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and diskspace to a given position.
   *  
   * @param inodes INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param nsDelta needed namespace
   * @param dsDelta needed diskspace
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta,
      INode commonAncestor) throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    if (pos>inodes.length) {
      pos = inodes.length;
    }
    int i = pos - 1;
    try {
      // check existing components in the path  
      for(; i >= 0; i--) {
        if (commonAncestor == inodes[i]) {
          // Moving an existing node. Stop checking for quota when common
          // ancestor is reached
          return;
        }
        if (inodes[i].isQuotaSet()) { // a directory with quota
          INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
          node.verifyQuota(nsDelta, dsDelta);
        }
      }
    } catch (QuotaExceededException e) {
      e.setPathName(getFullPathName(inodes, i));
      throw e;
    }
  }
  
  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   * 
   * @param srcInodes directory from where node is being moved.
   * @param dstInodes directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[]dstInodes)
      throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode srcInode = srcInodes[srcInodes.length - 1];
    INode commonAncestor = null;
    for(int i =0;srcInodes[i] == dstInodes[i]; i++) {
      commonAncestor = srcInodes[i];
    }
    INode.DirCounts counts = new INode.DirCounts();
    srcInode.spaceConsumedInTree(counts);
    verifyQuota(dstInodes, dstInodes.length - 1, counts.getNsCount(),
            counts.getDsCount(), commonAncestor);
  }
  
  /** Add a node child to the inodes at index pos. 
   * Its ancestors are stored at [0, pos-1]. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission,
      boolean checkQuota) throws QuotaExceededException {
    INode.DirCounts counts = new INode.DirCounts();
    child.spaceConsumedInTree(counts);
    if (childDiskspace < 0) {
      childDiskspace = counts.getDsCount();
    }
    updateCount(pathComponents, pos, counts.getNsCount(), childDiskspace,
        checkQuota);
    T addedNode = ((INodeDirectory)pathComponents[pos-1]).addChild(
        child, inheritPermission);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -counts.getNsCount(), 
          -childDiskspace, true);
    }
    return addedNode;
  }
  
  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission)
      throws QuotaExceededException {
    return addChild(pathComponents, pos, child, childDiskspace,
        inheritPermission, true);
  }
  
  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int pos, T child, long childDiskspace, boolean inheritPermission) {
    T inode = null;
    try {
      inode = addChild(pathComponents, pos, child, childDiskspace,
          inheritPermission, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e); 
    }
    return inode;
  }
  
  /** Remove an inode at index pos from the namespace.
   * Its ancestors are stored at [0, pos-1].
   * Count of each ancestor with quota is also updated.
   * Return the removed node; null if the removal fails.
   */
  private INode removeChild(INode[] pathComponents, int pos) {
    INode removedNode = 
      ((INodeDirectory)pathComponents[pos-1]).removeChild(pathComponents[pos]);
    if (removedNode != null) {
      INode.DirCounts counts = new INode.DirCounts();
      removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(pathComponents, pos,
                  -counts.getNsCount(), -counts.getDsCount());
    }
    return removedNode;
  }
  
  /**
   */
  String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  ContentSummary getContentSummary(String src) throws IOException {
    String srcs = normalizePath(src);
    readLock();
    try {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        throw new FileNotFoundException("File does not exist: " + srcs);
      }
      else {
        return targetNode.computeContentSummary();
      }
    } finally {
      readUnlock();
    }
  }

  /** Update the count of each directory with quota in the namespace
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   * 
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  void updateCountForINodeWithQuota() {
    updateCountForINodeWithQuota(rootDir, new INode.DirCounts(), 
                                 new ArrayList<INode>(50));
  }
  
  /** 
   * Update the count of the directory if it has a quota and return the count
   * 
   * This does not throw a QuotaExceededException. This is just an update
   * of of existing state and throwing QuotaExceededException does not help
   * with fixing the state, if there is a problem.
   * 
   * @param dir the root of the tree that represents the directory
   * @param counters counters for name space and disk space
   * @param nodesInPath INodes for the each of components in the path.
   * @return the size of the tree
   */
  private static void updateCountForINodeWithQuota(INodeDirectory dir, 
                                               INode.DirCounts counts,
                                               ArrayList<INode> nodesInPath) {
    long parentNamespace = counts.nsCount;
    long parentDiskspace = counts.dsCount;
    
    counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
    counts.dsCount = 0L;
    
    /* We don't need nodesInPath if we could use 'parent' field in 
     * INode. using 'parent' is not currently recommended. */
    nodesInPath.add(dir);

    for (INode child : dir.getChildren()) {
      if (child.isDirectory()) {
        updateCountForINodeWithQuota((INodeDirectory)child, 
                                     counts, nodesInPath);
      } else { // reduce recursive calls
        counts.nsCount += 1;
        counts.dsCount += ((INodeFile)child).diskspaceConsumed();
      }
    }
      
    if (dir.isQuotaSet()) {
      ((INodeDirectoryWithQuota)dir).setSpaceConsumed(counts.nsCount,
                                                      counts.dsCount);

      // check if quota is violated for some reason.
      if ((dir.getNsQuota() >= 0 && counts.nsCount > dir.getNsQuota()) ||
          (dir.getDsQuota() >= 0 && counts.dsCount > dir.getDsQuota())) {

        // can only happen because of a software bug. the bug should be fixed.
        StringBuilder path = new StringBuilder(512);
        for (INode n : nodesInPath) {
          path.append('/');
          path.append(n.getLocalName());
        }
        
        NameNode.LOG.warn("Quota violation in image for " + path + 
                          " (Namespace quota : " + dir.getNsQuota() +
                          " consumed : " + counts.nsCount + ")" +
                          " (Diskspace quota : " + dir.getDsQuota() +
                          " consumed : " + counts.dsCount + ").");
      }            
    }
      
    // pop 
    nodesInPath.remove(nodesInPath.size()-1);
    
    counts.nsCount += parentNamespace;
    counts.dsCount += parentDiskspace;
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   * @returns INodeDirectory if any of the quotas have changed. null other wise.
   * @throws FileNotFoundException if the path does not exist or is a file
   * @throws QuotaExceededException if the directory tree size is 
   *                                greater than the given quota
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota) 
                       throws FileNotFoundException, QuotaExceededException {
    // sanity check
    if ((nsQuota < 0 && nsQuota != FSConstants.QUOTA_DONT_SET && 
         nsQuota < FSConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != FSConstants.QUOTA_DONT_SET && 
          dsQuota < FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "dsQuota : " + nsQuota + " and " +
                                         dsQuota);
    }
    
    String srcs = normalizePath(src);
    INode[] inodes = rootDir.getExistingPathINodes(src);
    INode targetNode = inodes[inodes.length-1];
    if (targetNode == null) {
      throw new FileNotFoundException("Directory does not exist: " + srcs);
    } else if (!targetNode.isDirectory()) {
      throw new FileNotFoundException("Cannot set quota on a file: " + srcs);  
    } else { // a directory inode
      INodeDirectory dirNode = (INodeDirectory)targetNode;
      long oldNsQuota = dirNode.getNsQuota();
      long oldDsQuota = dirNode.getDsQuota();
      if (nsQuota == FSConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == FSConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }        

      if (dirNode instanceof INodeDirectoryWithQuota) { 
        // a directory with quota; so set the quota to the new value
        ((INodeDirectoryWithQuota)dirNode).setQuota(nsQuota, dsQuota);
      } else {
        // a non-quota directory; so replace it with a directory with quota
        INodeDirectoryWithQuota newNode = 
          new INodeDirectoryWithQuota(nsQuota, dsQuota, dirNode);
        // non-root directory node; parent != null
        INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
        dirNode = newNode;
        parent.replaceChild(newNode);
      }
      return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota) 
                throws FileNotFoundException, QuotaExceededException {
    writeLock();
    try {
      INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
      if (dir != null) {
        fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(), 
                                         dir.getDsQuota());
      }
    } finally {
      writeUnlock();
    }
  }
  
  long totalInodes() {
    readLock();
    try {
      return rootDir.numItemsInTree();
    } finally {
      readUnlock();
    }
  }

  /**
   * Sets the access time on the file. Logs it in the transaction log
   */
  void setTimes(String src, INodeFile inode, long mtime, long atime, boolean force) 
                                                        throws IOException {
    if (unprotectedSetTimes(src, inode, mtime, atime, force)) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force) 
                              throws IOException {
    INodeFile inode = getFileINode(src);
    return unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(String src, INodeFile inode, long mtime,
                                      long atime, boolean force) throws IOException {
    boolean status = false;
    if (mtime != -1) {
      inode.setModificationTimeForce(mtime);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime);
        status = true;
      }
    } 
    return status;
  }
  
  /**
   * Create HdfsFileStatus by file INode 
   */
   static FileStatus createFileStatus(String path, INode node) {
    // length is zero for directories
    return new FileStatus(node.isDirectory() ? 0 : node.computeContentSummary().getLength(), 
        node.isDirectory(), 
        node.isDirectory() ? 0 : ((INodeFile)node).getReplication(), 
        node.isDirectory() ? 0 : ((INodeFile)node).getPreferredBlockSize(),
        node.getModificationTime(),
        node.getAccessTime(),
        node.getFsPermission(),
        node.getUserName(),
        node.getGroupName(),
        new Path(path));
  }

  /**
   * Create HdfsFileStatus by file INode 
   */
   private static HdfsFileStatus createHdfsFileStatus(byte[] path, INode node) {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     if (node instanceof INodeFile) {
       INodeFile fileNode = (INodeFile)node;
       size = fileNode.computeContentSummary().getLength();
       replication = fileNode.getReplication();
       blocksize = fileNode.getPreferredBlockSize();
     }
     else
     if (node.isDirectory()) {
       INodeDirectory dirNode = (INodeDirectory)node;
       //length is used to represent the number of children for directories.
       size = dirNode.getChildren().size();
     }
     return new HdfsFileStatus(
         size,
         node.isDirectory(),
         replication,
         blocksize,
         node.getModificationTime(),
         node.getAccessTime(),
         node.getFsPermission(),
         node.getUserName(),
         node.getGroupName(),
         path);
   }

   /** a default LocatedBlocks object, its content should not be changed */
   private final static LocatedBlocks EMPTY_BLOCK_LOCS = new LocatedBlocks();
   /**
    * Create FileStatus with location info by file INode
    */
   private LocatedBlocks createLocatedBlocks(INode node) throws IOException {
     LocatedBlocks loc = null;
     if (node instanceof INodeFile) {
       loc = getFSNamesystem().getBlockLocationsInternal(
           (INodeFile)node, 0L, Long.MAX_VALUE, Integer.MAX_VALUE);
     }
     if (loc==null) {
       loc = EMPTY_BLOCK_LOCS;
     }
     return loc;
   }

   /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode) {
    // Name is cached only for files
    if (inode.isDirectory()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
}

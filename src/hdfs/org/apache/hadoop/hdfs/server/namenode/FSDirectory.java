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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FileStatusExtended;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.hdfs.util.LightWeightGSet;
import org.apache.hadoop.raid.RaidCodec;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 *
 *************************************************/
public class FSDirectory implements FSConstants, Closeable {

  final INodeDirectoryWithQuota rootDir;
  FSImage fsImage;
  private boolean ready = false;
  private final int lsLimit;  // max list limit
  
  static int BLOCK_DELETION_NO_LIMIT = 0;

  private static Random random = new Random(FSNamesystem.now());

  /**
   * Caches frequently used file names used in {@link INode} to reuse
   * byte[] objects and reduce heap usage.
   */
  private final FSDirectoryNameCache nameCache;

  // lock to protect BlockMap.
  private ReentrantReadWriteLock bLock;
  private Condition cond;
  private boolean hasRwLock;
  
  /** Keeps track of the lastest hard link ID;*/
  private volatile long latestHardLinkID = 0;

  /** {@link INodeId} to allocate unique id for each iNode */
  private INodeId inodeId;
  
  /** Map between inode id to inode, not thread safe, operations need lock */
  private final INodeMap inodeMap;
  
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
  FSDirectory(FSNamesystem ns, Configuration conf, 
      Collection<URI> dataDirs,
      Collection<URI> editsDirs,
      Map<URI, NNStorageLocation> locationMap) throws IOException {
    this(new FSImage(conf, dataDirs, editsDirs, locationMap), ns, conf);
    fsImage.setFSNamesystem(ns);
  }

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    inodeId = new INodeId();
    rootDir = new INodeDirectoryWithQuota(INodeId.ROOT_INODE_ID, INodeDirectory.ROOT_NAME,
        ns.createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE, Long.MAX_VALUE);
    inodeMap = initInodeMap(rootDir);
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
    nameCache = new FSDirectoryNameCache(threshold);
    initialize();
  }
  
  private INodeMap initInodeMap(INodeDirectory rootDir) {
    // Compute the map capacity by allocating 1.1% of total memory
    int capacity = LightWeightGSet.computeCapacity(1.1, "INodeMap");
    INodeMap map = new INodeMap(capacity);
    map.put(rootDir);
    return map;
  }

  private long getAndIncrementLastHardLinkID() {
    return this.latestHardLinkID++;
  }
  
  /**
   * Reset the lastHardLinkID if the hardLinkID is larger than the original lastHardLinkID.
   * 
   * This function is not thread safe.
   * @param hardLinkID
   */
  void resetLastHardLinkIDIfLarge(long hardLinkID) {
    // latestHardLinkID denotes the next hardlinkId we want to issue.
    if (this.latestHardLinkID < hardLinkID + 1) {
      this.latestHardLinkID = hardLinkID + 1;
    }
  }
  
  /**
   * Set the last allocated inode id when fsimage or editlog is loaded. 
   */
  public void resetLastInodeId(long newValue) throws IOException {
    try {
      inodeId.skipTo(newValue);
    } catch(IllegalStateException ise) {
      throw new IOException(ise);
    }
  }

  /** Should only be used for tests to reset to any value */
  void resetLastInodeIdWithoutChecking(long newValue) {
    inodeId.setCurrentValue(newValue);
  }
  
  /** @return the last inode ID. */
  public long getLastInodeId() {
    return inodeId.getCurrentValue();
  }

  /** Allocate a new inode ID. */
  public long allocateNewInodeId() {
    return inodeId.nextValue();
  }
  
  private FSNamesystem getFSNamesystem() {
    return fsImage.getFSNamesystem();
  }

  private void initialize() {
    this.bLock = new ReentrantReadWriteLock(); // non-fair
    this.cond = bLock.writeLock().newCondition();
    this.hasRwLock = getFSNamesystem().hasRwLock;
  }

  void loadFSImage(StartupOption startOpt, Configuration conf) 
      throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      boolean saveNamespace =
          fsImage.recoverTransitionRead(startOpt);
      if (saveNamespace) {
        fsImage.saveNamespace();
      }
      if (conf.getBoolean("dfs.namenode.openlog", true)) {
        fsImage.openEditLog();
      }
    } catch (IOException e) {
      NameNode.LOG.fatal("Exception when loading the image,", e);
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
                long generationStamp,
                long accessTime)
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
    } else if (!inodes[inodes.length-2].isDirectory()) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
          + "failed to add " + path + " as its parent is not a directory");
      throw new FileNotFoundException("Parent path is not a directory: " + path);
    }
    
    if (accessTime == -1){
    	accessTime = modTime;
    }
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                                 allocateNewInodeId(),
                                 permissions,replication,
                                 preferredBlockSize, modTime, accessTime, clientName, 
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

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    +path+" is added to the file system");
    }
    
    return newNode;
  }

  /**
   * Creates a {@link INodeFileUnderConstruction} and adds an empty file with no
   * blocks to the FileSystem. No locks are taken while doing updates, the
   * caller is expected to use the write lock.
   */
  private INodeFileUnderConstruction unprotectedAddFile(long inodeId, String path,
      PermissionStatus permissions, short replication, long modificationTime,
      long atime, long preferredBlockSize, String clientName,
      String clientMachine) {
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
        inodeId, permissions, replication, preferredBlockSize, modificationTime,
        clientName, clientMachine, null);
    try {
      newNode = addNode(path, newNode, 0, false);
    } catch (IOException e) {
      return null;
    }
    return newNode;
  }

  @Deprecated
  INode unprotectedAddFile(long inodeId,
                            String path,
                            PermissionStatus permissions,
                            Block[] blocks,
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize) {
    INode newNode;
    long diskspace = -1; // unknown
    if (blocks == null)
      newNode = new INodeDirectory(inodeId, permissions, modificationTime);
    else {
      newNode = new INodeFile(inodeId, permissions, blocks.length, replication,
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
            newF.setBlock(i, getFSNamesystem().blocksMap.addINode(blocks[i],
                newF, newF.getReplication()));
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

  /**
   * Add node to parent node when loading the image.
   */
  INodeDirectory addToParent( byte[] src,
                              INodeDirectory parentINode,
                              INode newNode,
                              boolean propagateModTime,
                              int childIndex) {
    // NOTE: This does not update space counts for parents
    // add new node to the parent
    INodeDirectory newParent = null;
    writeLock();
    try {
      try {
        newParent = rootDir.addToParent(src, newNode, parentINode,
                                            false, propagateModTime, childIndex);
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
          newF.setBlock(i, getFSNamesystem().blocksMap.addINodeForLoading(blocks[i], newF));
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
      INode.enforceRegularStorageINode(fileNode, 
          "addBlock only works for regular file, not " + path);
      
      // check quota limits and updated space consumed
      updateCount(inodes, inodes.length-1, 0,
          fileNode.getPreferredBlockSize()*fileNode.getReplication(), true);

      // associate the new list of blocks with this file
      BlockInfo blockInfo = 
          getFSNamesystem().blocksMap.addINode(block, fileNode,
              fileNode.getReplication());
      fileNode.getStorage().addBlock(blockInfo);

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                      + path + " with " + block
                                      + " block is added to the in-memory "
                                      + "file system");
      }
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
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                                      +path+" with "+ file.getBlocks().length
                                      +" blocks is persisted to the file system");
      }
    } finally {
      writeUnlock();
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
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
                                      +path+" with "+block
                                      +" block is removed from the file system");
      }
    } finally {
      writeUnlock();
    }
    return true;
  }

  /**
   * Retrieves a the hardlink id for a given file.
   *
   * @param src
   *          the file to lookup
   * @return the hardlink id
   * @throws IOException
   *           if the specified file is not a valid hardlink file
   */
  public long getHardLinkId(String src) throws IOException {
    byte[][] components = INode.getPathComponents(src);
    readLock();
    try {
      INodeFile node = this.getFileINode(components);
      if ((!exists(node)) || (!(node instanceof INodeHardLinkFile))) {
        throw new IOException(src + " is not a valid hardlink file");
      }
      return ((INodeHardLinkFile) node).getHardLinkID();
    } finally {
      readUnlock();
    }
  }

  /**
   * @see #unprotectedHardLinkTo(String, String)
   */
  boolean hardLinkTo(String src, String[] srcNames, byte[][] srcComponents, INode[] srcInodes,
      String dst, String[] dstNames, byte[][] dstComponents, INode[] dstInodes)
      throws QuotaExceededException, FileNotFoundException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.hardLinkTo: src: " + src
          + " dst: " + dst);
    }
    waitForReady();
    long now = FSNamesystem.now();
    if (!unprotectedHardLinkTo(src, srcNames, srcComponents, srcInodes, dst,
        dstNames, dstComponents, dstInodes, now)) {
      return false;
    }
    fsImage.getEditLog().logHardLink(src, dst, now);
    return true;
  }

  /** hard link the dst path to the src path
   *
   * @param src source path
   * @param dst destination path
   * @param timestamp The modification timestamp for the dst's parent directory
   * @return true if the hardLink succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   * @throws FileNotFoundException
   */
  boolean unprotectedHardLinkTo(String src, String dst, long timestamp)
  throws QuotaExceededException, FileNotFoundException {
    return unprotectedHardLinkTo(src, null, null, null, dst, null, null, null, timestamp);
  }

  private boolean unprotectedHardLinkTo(String src, String[] srcNames, byte[][] srcComponents,
      INode[] srcINodes, String dst, String[] dstNames, byte[][] dstComponents,
      INode[] dstINodes, long timestamp)
    throws QuotaExceededException, FileNotFoundException {
    writeLock();
    try {
      // Get the src file inodes if necessary
      if (srcINodes == null) {
        srcNames = INode.getPathNames(src);
        srcComponents = INode.getPathComponents(srcNames);
        srcINodes = new INode[srcComponents.length];
        rootDir.getExistingPathINodes(srcComponents, srcINodes);
      }
      INode srcINode = srcINodes[srcINodes.length - 1];

      // Check the validation of the src
      if (srcINode == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hardlink " + dst + " to " + src
            + " because source does not exist.");
        return false;
      }

      if (srcINode.isDirectory()) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hardlink " + dst + " to " + src + " because source is a directory.");
        return false;
      }
      
      INodeFile srcINodeFile = (INodeFile) srcINode;
      if (srcINodeFile.getBlocks() == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hardlink " + dst + " to " + src
            + " because the source file is NULL blocks");
        return false;
      }

      if (srcINodeFile.isUnderConstruction()) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hardlink " + dst + " to " + src
            + " because the source file is still under construction.");
        return false;
      }
      
      if (srcINodeFile.getStorageType() != StorageType.REGULAR_STORAGE) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hardlink " + dst + " to " + src
            + " because the source file is not regular file.");
        return false;
      }

      // Get the dst file inodes if necessary
      if (dstINodes == null) {
        dstNames = INode.getPathNames(dst);
        dstComponents = INode.getPathComponents(dst);
        dstINodes = new INode[dstComponents.length];
        rootDir.getExistingPathINodes(dstComponents, dstINodes);
      }
      byte[] dstComponent = dstComponents[dstComponents.length-1];
      INode dstINode = dstINodes[dstINodes.length - 1];

      // Check the validation of the dst
      if (dstINode != null) {
        // if the src and dst has already linked together, return true directly.
        if (dstINode instanceof INodeHardLinkFile &&
            srcINodeFile instanceof INodeHardLinkFile &&
            ((INodeHardLinkFile)dstINode).getHardLinkID() ==
            ((INodeHardLinkFile)srcINodeFile).getHardLinkID()) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + "succeeded to hardlink " + dst + " to " + src + " because the hard link has" +
              		"already exists.");
          return true;
        } else {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + "failed to hardlink " + dst + " to " + src + " because destination file exists.");
          return false;
        }
      }

      // Create the intermediate directories if they don't exist.
      if (dstINodes[dstINodes.length - 2] == null) {
        if (!mkdirs(dst, dstNames, dstComponents, dstINodes, dstINodes.length - 1,
            new PermissionStatus(FSNamesystem.getCurrentUGI().getUserName(),
                null, FsPermission.getDefault()),
            true, FSNamesystem.now())) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + "failed to hardlink " + dst + " to " + src
              + " because cannot create destination's parent dir" );
          return false;
        }
      }
      // Ensure the destination has quota for hard linked file
      int nonCommonAncestorPos = verifyQuotaForHardLink(srcINodes, dstINodes);

      INodeHardLinkFile srcLinkedFile = null;
      if (srcINodeFile instanceof INodeHardLinkFile) {
        // The source file is already a hard linked file
        srcLinkedFile = (INodeHardLinkFile) srcINodeFile;
        if (srcLinkedFile.getHardLinkFileInfo().getReferenceCnt() == Integer.MAX_VALUE) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + "failed to hardlink " + dst + " to " + src
              + " because src file has the " + Integer.MAX_VALUE + " reference cnts" );
          return false;
        }
      } else {
        // The source file is a regular file
        try {
          // Convert the source file from INodeFile to INodeHardLinkFile
          srcLinkedFile = new INodeHardLinkFile(srcINodeFile, this.getAndIncrementLastHardLinkID());
        } catch (IOException e) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + "failed to hardlink " + dst + " to " + src
              + " because source is empty.");
          return false;
        }
        
        // all blocks should point to the new INodeHardLinkFile
        for (BlockInfo bi : srcINodeFile.getBlocks()) {
          bi.setINode(srcLinkedFile);
        }

        try {
          // Replace the new INodeHardLinkFile in its parent directory
          ((INodeDirectory)srcINodes[srcINodes.length - 2]).replaceChild(srcLinkedFile);
        } catch (IllegalArgumentException e) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
              + " failed to hardlink " + dst + " to " + src
              + " because no such src file is found in its parent direcoty.");
          return false;
        }
        
        // Increment the reference cnt for the src file
        srcLinkedFile.incReferenceCnt();
      }

      // Create the destination INodeHardLinkFile
      INodeHardLinkFile dstLinkedFile = new INodeHardLinkFile(srcLinkedFile);
      dstLinkedFile.setLocalName(dstComponent);

      // Add the dstLinkedFile to the destination directory
      dstLinkedFile = addChildNoQuotaCheck(dstINodes, nonCommonAncestorPos, dstINodes.length - 1,
          dstLinkedFile, -1, false);
      if (dstLinkedFile == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "failed to hard link " + dst + " to " + src
            + " because cannot add the dst file to its parent directory.");
        return false;
      }
      
      // Increase the reference cnt for the dst 
      dstLinkedFile.incReferenceCnt();
      dstINodes[dstINodes.length - 1] = dstLinkedFile;
      srcINodes[srcINodes.length - 1] = srcLinkedFile;
      dstINodes[dstINodes.length -2].setModificationTime(timestamp);
      
      assert(dstLinkedFile.getReferenceCnt() == srcLinkedFile.getReferenceCnt());

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedHardLinkTo: "
            + "succeeded to hardlink " + dst + " to " + src
            +" and the reference cnt is " + srcLinkedFile.getReferenceCnt());
        }
      return true;
    } finally {
      writeUnlock();
    }
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
  
  /**
   * Convert a file into a raid file format 
   */
  public void raidFile(INode sourceINodes[], String source, 
      RaidCodec codec, short expectedSourceRepl, Block[] parityBlocks)
          throws IOException {
    waitForReady();
    long now = FSNamesystem.now();
    unprotectedRaidFile(sourceINodes, source, codec, expectedSourceRepl,
        parityBlocks, now);
    fsImage.getEditLog().logRaidFile(source, codec.id, expectedSourceRepl,
        now);
    
  }
  
  public void unprotectedRaidFile(INode sINodes[], String source, 
      RaidCodec codec, short expectedSourceRepl, Block[] rawParityBlocks,
      long now) throws IOException {
    INodeFile sINode = (INodeFile)sINodes[sINodes.length - 1];
    BlockInfo[] parityBlocks = new BlockInfo[rawParityBlocks.length];
    StringBuilder sb = new StringBuilder();
    writeLock();
    try {
      // check quota limits and updated space consumed
      updateCount(sINodes, sINodes.length-1, 0,
          sINode.getPreferredBlockSize()*codec.parityReplication*rawParityBlocks.length,
          true);
      for (int i = 0; i < rawParityBlocks.length; i++) {
        // associate the new list of blocks with this file
        parityBlocks[i] = getFSNamesystem().blocksMap.addINode(
            rawParityBlocks[i], sINode, codec.parityReplication);
        sb.append(" " + parityBlocks[i]);
      }
      
      // Merge parity blocks and source blocks
      INodeRaidStorage newStorage = 
          sINode.getStorage().convertToRaidStorage(parityBlocks, codec, null,
              getFSNamesystem().blocksMap, sINode.getReplication(), sINode);
      sINode.storage = newStorage;
      sINode.setModificationTime(now);
      if (getFSNamesystem().isAccessTimeSupported()) {
        sINode.setAccessTime(now);
      }
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                      + source + " with blocks [" + sb.toString() 
                                      + "] are added to the in-memory "
                                      + "file system");
      }
    } finally {
      writeUnlock();
    }
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
      verifyQuotaForRename(srcInodes, dstInodes);

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
        dstChild = addChildNoQuotaCheck(dstInodes, 0, dstInodes.length - 1,
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
          addChildNoQuotaCheck(srcInodes, 0, srcInodes.length - 1, srcChild, -1,
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
  BlockInfo[] setReplication(String src,
                         short replication,
                         int[] oldReplication
                         ) throws IOException {
    waitForReady();
    BlockInfo[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    if (fileBlocks != null)  // log replication change
      fsImage.getEditLog().logSetReplication(src, replication);
    return fileBlocks;
  }

  BlockInfo[] unprotectedSetReplication( String src,
                                     short replication,
                                     int[] oldReplication
                                     ) throws IOException {
    if (oldReplication == null)
      oldReplication = new int[1];
    oldReplication[0] = -1;
    BlockInfo[] fileBlocks = null;
    writeLock();
    try {
      INode[] inodes = rootDir.getExistingPathINodes(src);
      INode inode = inodes[inodes.length - 1];
      if (inode == null)
        return null;
      if (inode.isDirectory())
        return null;
      INodeFile fileNode = (INodeFile)inode;
      if (fileNode.getStorageType() == StorageType.RAID_STORAGE) {
        short minRepl = 
            ((INodeRaidStorage)fileNode.getStorage()).getCodec().minSourceReplication; 
        if (minRepl > replication) {
          throw new IOException("Couldn't set replication smaller than " 
            + minRepl); 
        }
      }
      oldReplication[0] = fileNode.getReplication();
      long dsDelta = (replication - oldReplication[0]) * 
                     (fileNode.diskspaceConsumed()/oldReplication[0]);
      
      if (fileNode instanceof INodeHardLinkFile) {
        // check the disk quota for the hard link file
        INodeHardLinkFile hardLinkedFile = (INodeHardLinkFile)fileNode;
        INode[] anncestorINodeArray = hardLinkedFile.getAncestorSet().toArray(new INode[0]);
        updateCount(anncestorINodeArray, anncestorINodeArray.length-1, 0, dsDelta, true);
      } else {
        // check disk quota for the regular file
        updateCount(inodes, inodes.length-1, 0, dsDelta, true);
      }

      fileNode.setReplication(replication);
      if (fileNode.getStorageType() == StorageType.RAID_STORAGE) {
        // For raided file, only increase source blocks
        fileBlocks = ((INodeRaidStorage)fileNode.getStorage()).getSourceBlocks(); 
      } else {
        fileBlocks = fileNode.getBlocks();
      }
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
    byte[][] components = INodeDirectory.getPathComponents(filename);
    readLock();
    try {
      INode fileNode = rootDir.getNode(components);
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
   * Updates the in memory inode for the file with the new information and
   * returns a reference to the INodeFile. This method in most cases would
   * return a {@link INodeFileUnderConstruction}, the only case where it should
   * return a {@link INodeFile} would be when it tries to update an already
   * closed file.
   *
   * @return reference to the {@link INodeFile}
   * @throws IOException
   */
  protected INodeFile updateINodefile(long inodeId, String path,
      PermissionStatus permissions, Block[] blocks, short replication,
      long mtime, long atime, long blockSize, String clientName,
      String clientMachine) throws IOException {
    writeLock();
    try {
      INode node = getINode(path);
      if (!exists(node)) {
        return this.unprotectedAddFile(inodeId, path, permissions, replication, mtime,
            atime, blockSize, clientName, clientMachine);
      }

      if (node.isDirectory()) {
        throw new IOException(path + " is a directory");
      }

      INodeFile file = (INodeFile) node;

      BlockInfo[] oldblocks = file.getBlocks();
      if (oldblocks == null) {
        throw new IOException("blocks for file are null : " + path);
      }

      // Update the inode with new information.
      BlockInfo[] blockInfo = new BlockInfo[blocks.length];
      for (int i = 0; i < blocks.length; i++) {
        // Need to use update inode here, because when we add a block to the
        // NameNode, it is persisted to the edit log with size 0, now when we
        // persist another block to the edit log then the previous block
        // length has been calculated already and we write the new block
        // length to the edit log. But during ingest, the old block length of
        // 0 has already been stored and it is reused in BlocksMap#addINode()
        // instead of overwriting the new value.
        BlockInfo oldblock = (i < oldblocks.length) ? oldblocks[i] : null;
        blockInfo[i] = getFSNamesystem().blocksMap.updateINode(oldblock,
            blocks[i], file, file.getReplication(), false);
      }

      int remaining = oldblocks.length - blocks.length;
      if (remaining > 0) {
        if (remaining > 1)
          throw new IOException("Edit log indicates more than one block was" +
              " abandoned");
        // The last block is no longer part of the file, mostly was abandoned.
        getFSNamesystem().blocksMap.removeBlock(
            oldblocks[oldblocks.length - 1]);
      }

      file.updateFile(permissions, blockInfo, replication,
          mtime, atime, blockSize);
      return file;
    } finally {
      writeUnlock();
    }
  }

  /**
   * This is a method required in addition
   * to getFileInode
   * It returns the INode regardless of its type
   * getFileInode only returns the inode if it is
   * of a file type
   */
  INode getINode(String src) {
    src = normalizePath(src);
    byte[][] components = INodeDirectory.getPathComponents(src);
    readLock();
    try {
      INode inode = rootDir.getNode(components);
      return inode;
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get the inode from inodeMap based on its inode id.
   * @param id The given id
   * @return The inode associated with the given id
   */
  INode getINode(long id) {
    readLock();
    try {
      INode inode = inodeMap.get(id);
      return inode;
    } finally {
      readUnlock();
    }
  }
  
  /**
   * See {@link ClientProtocol#getHardLinkedFiles(String)}.
   */
  public String[] getHardLinkedFiles(String src) throws IOException {
    byte[][] components = INode.getPathComponents(src);
    List<byte [][]> results = null;
    readLock();
    try {
      INodeFile inode = getFileINode(components);
      if (!exists(inode)) {
        throw new IOException(src + " does not exist");
      }
      if (inode instanceof INodeHardLinkFile) {
        HardLinkFileInfo info = ((INodeHardLinkFile) inode)
          .getHardLinkFileInfo();
        // Only get the list of names as byte arrays under the lock.
        results = new ArrayList<byte[][]>(info.getReferenceCnt());
        for (int i = 0; i < info.getReferenceCnt(); i++) {
          try {
            results.add(getINodeByteArray(info.getHardLinkedFile(i)));
          } catch (IOException ioe) {
            NameNode.LOG.info("Hardlink may have been deleted", ioe);
          }
        }
      } else {
        return new String[] {};
      }
    } finally {
      readUnlock();
    }

    // Convert byte arrays to strings outside the lock since this is expensive.
    String[] files = new String[results.size()-1];
    int size = 0;
    for (int i = 0; i < results.size(); i++) {
      String file = getFullPathName(results.get(i));
      if (!file.equals(src)) {
        if (size >= files.length) {
          throw new IOException(src + " is not part of the list of hardlinked "
              + "files! This is a serious bug!");
        }
        files[size++] = file;
      }
    }
    return files;
  }

  private boolean exists(INode inode) {
    if (inode == null) {
      return false;
    }
    return inode.isDirectory() ? true : ((INodeFile)inode).getBlocks() != null;
  }

  boolean exists(String src) {
    src = normalizePath(src);
    byte[][] components = INodeDirectory.getPathComponents(src);
    readLock();
    try {
      INode inode = rootDir.getNode(components);
      return exists(inode);
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
    byte[][] components = INodeDirectory.getPathComponents(src);
    writeLock();
    try {
        INode inode = rootDir.getNode(components);
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
    byte[][] components = INodeDirectory.getPathComponents(src);
    writeLock();
    try {
      INode inode = rootDir.getNode(components);
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
  public void concatInternal(String target, String [] srcs) 
    throws IOException {
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
  public void unprotectedConcat(String target, String [] srcs, long now) 
    throws IOException {
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
        totalBlocks += srcInode.getBlocks().length;
      }
      trgInode.getStorage().appendBlocks(allSrcInodes, totalBlocks, trgInode); 
      // copy the blocks

      // since we are in the same dir - we can use same parent to remove files
      int count = 0;
      for(INodeFile nodeToRemove: allSrcInodes) {
        if(nodeToRemove == null) continue;

        nodeToRemove.storage = null;
        trgParent.removeChild(nodeToRemove);
        inodeMap.remove(nodeToRemove);
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
   * Merge all the blocks in the parity file into source file.
   * Source file will be converted into INodeRaidStorage format to include both
   * parity blocks and source blocks
   */
  public void mergeInternal(INode parityINodes[], INode sourceINodes[],
      String parity, String source, RaidCodec codec, int[] checksums)
          throws IOException {
    waitForReady();
    long now = FSNamesystem.now();
    unprotectedMerge(parityINodes, sourceINodes, parity, source, codec,
        checksums, now);
    fsImage.getEditLog().logMerge(parity, source, codec.id, checksums, now);
  }
  
  public void unprotectedMerge(INode parityINodes[], INode sourceINodes[],
      String parity, String source, RaidCodec codec, int[] checksums, 
      long now) throws IOException {
    INodeFile sINode = (INodeFile)sourceINodes[sourceINodes.length - 1];
    INodeFile pINode = (INodeFile)parityINodes[parityINodes.length - 1];
    INodeDirectory pParentINode = 
        (INodeDirectory)parityINodes[parityINodes.length - 2];
    writeLock();
    try {
      INodeRaidStorage newStorage = sINode.getStorage().convertToRaidStorage(
          pINode.getBlocks(), codec, checksums, getFSNamesystem().blocksMap, 
          sINode.getReplication(), sINode);
      INode.DirCounts oldCounts = new INode.DirCounts();
      sINode.spaceConsumedInTree(oldCounts);
      INode.DirCounts newCounts = new INode.DirCounts();
      sINode.storage = newStorage;
      sINode.spaceConsumedInTree(newCounts);
      
      // check quota
      int startPos = sINode.getStartPosForQuoteUpdate();
      updateCount(sourceINodes, startPos, sourceINodes.length - 1, 0, 
          newCounts.getDsCount() - oldCounts.getDsCount(), true);

      // Remove parity
      INode child = removeChild(parityINodes, parityINodes.length-1);
      if (child != pINode) {
        throw new IOException("parity inode is null or invalid");
      }
      pINode.storage = null;
      pINode.parent = null;
      inodeMap.remove(pINode);
      sINode.setModificationTime(now);
      pParentINode.setModificationTime(now);
      if (getFSNamesystem().isAccessTimeSupported()) {
        sINode.setAccessTime(now);
        pParentINode.setAccessTime(now);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove the file from management, return up to blocksLimit number of blocks
   */
  INode delete(String src, INode[] inodes, List<BlockInfo> collectedBlocks,
      int blocksLimit) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "+src);
    }
    waitForReady();
    long now = FSNamesystem.now();
    INode deletedNode = unprotectedDelete(src, inodes, collectedBlocks,
        blocksLimit, now);
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
    return unprotectedDelete(src, this.getExistingPathINodes(src), null,
        BLOCK_DELETION_NO_LIMIT, modificationTime);
  }

  @Deprecated
  INode unprotectedDelete(String src, INode[] inodes, long modificationTime) {
    return unprotectedDelete(src, inodes, null,
        BLOCK_DELETION_NO_LIMIT, modificationTime);
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * Up to blocksLimit blocks will be put in toBeDeletedBlocks to be removed later
   * @param src a string representation of a path to an inode
   * @param inodes all the inodes on the given path
   * @param toBeDeletedBlocks the place holder for the blocks to be removed
   * @param blocksLimit up limit number of blocks to be returned
   * @param modificationTime the time the inode is removed
   * @return the deleted target inode, null if deletion failed
   */
  INode unprotectedDelete(String src, INode inodes[], List<BlockInfo> toBeDeletedBlocks,
                          int blocksLimit, long modificationTime) {
    src = normalizePath(src);

    writeLock();
    try {
      INode targetNode = inodes[inodes.length-1];

      if (targetNode == null) { // non-existent src
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
              +"failed to remove "+src+" because it does not exist");
        }
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
            toBeDeletedBlocks = new ArrayList<BlockInfo>();
            blocksLimit = BLOCK_DELETION_NO_LIMIT;
          }
          List<INode> removedINodes = new ArrayList<INode>();
          int filesRemoved = targetNode.collectSubtreeBlocksAndClear(
              toBeDeletedBlocks, blocksLimit, removedINodes);
          FSNamesystem.incrDeletedFileCount(getFSNamesystem(), filesRemoved);
          // Delete collected blocks immediately;
          // Remaining blocks need to be collected and deleted later on
          getFSNamesystem().removePathAndBlocks(src, toBeDeletedBlocks);
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                +src+" is removed");
          }
          targetNode.parent = null;  //mark the node as deleted
          removeFromInodeMap(removedINodes);
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
   * Add inode to inodeMap
   * Not thread safe, needs lock on operations
   * @param inode inode to be added
   */
  void addToInodeMap(INode inode) {
    inodeMap.put(inode);
  }
  
  /** For testing */
  public GSet<INode, INode> getInodeMap() {
    return inodeMap;
  }
  
  /**
   * Remove inodes from inodeMap
   * Not thread safe, needs lock on operations
   * @param removedINodes inodes to be removed
   */
  void removeFromInodeMap(List<INode> inodes) {
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode != null) {
          inodeMap.remove(inode);
        }
      }
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
        BlockInfo info = 
            getFSNamesystem().blocksMap.addINode(b, newnode, 
                newnode.getReplication());
        newnode.setBlock(index, info); // inode refers to the block in BlocksMap
        index++;
      }
      
      inodeMap.remove(oldnode);
      inodeMap.put(newnode);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Retrieves a list of random files with some information.
   *
   * @param percent
   *          the percent of files to return
   * @return the list of files
   */
  public List<FileStatusExtended> getRandomFileStats(double percent) {
    readLock();
    try {
      List<FileStatusExtended> stats = new LinkedList<FileStatusExtended>();
      for (INodeFile file : getRandomFiles(percent)) {
        try {
          String path = file.getFullPathName();
          FileStatus stat = createFileStatus(path, file);
          Lease lease = this.getFSNamesystem().leaseManager.getLeaseByPath(path);
          String holder = (lease == null) ? null : lease.getHolder();
          long hardlinkId = (file instanceof INodeHardLinkFile) ? ((INodeHardLinkFile) file)
              .getHardLinkID() : -1;
              stats.add(new FileStatusExtended(stat, file.getBlocks(), holder,
                  hardlinkId));
        } catch (IOException ioe) {
          // the file has already been deleted; ingore it
        }
      }
      return stats;
    } finally {
      readUnlock();
    }
  }

  private List<INodeFile> getRandomFiles(double percent) {
    List<INodeFile> files = new LinkedList<INodeFile>();
    // This is an expensive operation (proportional to maxFiles) under a lock,
    // but we don't expect to call this function with a very large maxFiles
    // argument.
    getRandomFiles(rootDir, files, percent);
    return files;
  }

  private boolean chooseNode(double prob) {
    return (random.nextDouble() < prob);
  }

  private void getRandomFiles(INode node, List<INodeFile> files,
      double prob) {

    if (!node.isDirectory() && chooseNode(prob)) {
      files.add((INodeFile)node);
    }

    if (node.isDirectory()) {
      for (INode child : ((INodeDirectory) node).getChildren()) {
        getRandomFiles(child, files, prob);
      }
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
    byte[][] components = INodeDirectory.getPathComponents(srcs);
    readLock();
    try {
      INode targetNode = rootDir.getNode(components);
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
    
    // prepare components outside of readLock
    String srcs = normalizePath(src);
    byte[][] components = INodeDirectory.getPathComponents(srcs);

    readLock();
    try {
      INode targetNode = rootDir.getNode(components);
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

  /**
   * Get the extended file info for a specific file.
   *
   * @param src
   *          The string representation of the path to the file
   * @param targetNode
   *          the INode for the corresponding file
   * @param leaseHolder
   *          the lease holder for the file
   * @return object containing information regarding the file or null if file
   *         not found
   * @throws IOException
   *           if permission to access file is denied by the system
   */
  FileStatusExtended getFileInfoExtended(String src, INode targetNode,
      String leaseHolder)
      throws IOException {
    readLock();
    try {
      if (targetNode == null) {
        return null;
      }
      FileStatus stat = createFileStatus(src, targetNode);
      long hardlinkId = (targetNode instanceof INodeHardLinkFile) ? ((INodeHardLinkFile) targetNode)
          .getHardLinkID() : -1;
      return new FileStatusExtended(stat, ((INodeFile) targetNode).getBlocks(),
          leaseHolder, hardlinkId);
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
    byte[][] components = INodeDirectory.getPathComponents(srcs);
    readLock();
    try {
      INode targetNode = rootDir.getNode(components);
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
    byte[][] components = INodeDirectory.getPathComponents(src);
    readLock();
    try {
      INode targetNode = rootDir.getNode(components);
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
    byte[][] components = INodeDirectory.getPathComponents(src);
    readLock();
    try {
      INode inode = rootDir.getNode(components);
      if (inode == null || inode.isDirectory())
        return null;
      return (INodeFile)inode;
    } finally {
      readUnlock();
    }
  }

  /**
   * Get {@link INode} associated with the file.
   * Given name components.
   */
  INodeFile getFileINode(byte[][] components) {
    readLock();
    try {
      INode inode = rootDir.getNode(components);
      if (inode == null || inode.isDirectory())
        return null;
      return (INodeFile)inode;
    } finally {
      readUnlock();
    }
  }

  /*
   * Search the given block from the file and return the block index
   */
  int getBlockIndex(INodeFile inode, Block blk, String file) throws IOException {
    if (inode == null) {
      throw new IOException("inode for file " + file + " is null");
    }
    readLock();
    try {
      return inode.getBlockIndex(blk, file);
    } finally {
      readUnlock();
    }
  }

  /*
   * get file size by collecting all blocks' lengths
   */
  long getFileSize(INodeFile inode) { 
    readLock();
    try {
      return inode.getFileSize(); 
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get {@link INode} associated with the file/directory.
   * Given name components.
   */
  INode getINode(byte[][] components) {
    readLock();
    try {
      INode inode = rootDir.getNode(components);
      if (inode == null)
        return null;
      return (INode)inode;
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
  public INode[] getExistingPathINodes(String path) {
    byte[][] components = INode.getPathComponents(path);
    INode[] inodes = new INode[components.length];

    readLock();
    try {
      rootDir.getExistingPathINodes(components, inodes);
      return inodes;
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
    byte[][] components = INodeDirectory.getPathComponents(srcs);
    readLock();
    try {
      if (srcs.startsWith("/") &&
          !srcs.endsWith("/") &&
          rootDir.getNode(components) == null) {
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
    byte[][] components = INodeDirectory.getPathComponents(normalizePath(src));
    readLock();
    try {
      INode node = rootDir.getNode(components);
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

  /** 
   * Update count of each inode with quota in the inodes array from the position of 0 to
   * the position of numOfINodes
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
    this.updateCount(inodes, 0, numOfINodes, nsDelta, dsDelta, checkQuota);
  }

  /** 
   * Update NS quota of each inode with quota in the inodes array from the position 0 to
   * the position of endPos.
   * 
   * And update DS quota of each inode with quota in the inodes array from the position of 
   * dsUpdateStartPos to the position of endPos.
   * 
   * Currently only HardLink operations use a dsUpdateStartPos that is non-zero 
   * and hence we have the special handling of DS quota, 
   * but for NS quota we always assume 0.
   *
   * @param inodes an array of inodes on a path
   * @param dsUpdateStartPos The start position to update the DS quota
   * @param endPos the endPos of inodes to update the both NS and DS quota
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private void updateCount(INode[] inodes, int dsUpdateStartPos, int endPos,
                           long nsDelta, long dsDelta, boolean checkQuota)
  throws QuotaExceededException {
    if (!ready) {
      // still intializing. do not check or update quotas.
      return;
    }
    if (endPos > inodes.length) {
      endPos = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, 0, dsUpdateStartPos, endPos, nsDelta, dsDelta);
    }
    for (int i = 0; i < endPos; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node = (INodeDirectoryWithQuota) inodes[i];
        if (i >= dsUpdateStartPos) {
          node.updateNumItemsInTree(nsDelta, dsDelta);
        } else {
          node.updateNumItemsInTree(nsDelta, 0);
        }
        
      }
    }
  }

  /**
   * Update quota of each inode in the inodes array from the position of startPos to
   * the position of endPos. But it won't throw out the QuotaExceededException.
   */
  private void updateCountNoQuotaCheck(INode[] inodes, int startPos, int endPos,
                           long nsDelta, long dsDelta) {
    try {
      updateCount(inodes, startPos, endPos, nsDelta, dsDelta, false);
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
        node.updateNumItemsInTree(nsDelta, dsDelta);
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

  /** Return the name of the path represented by the byte array*/
  static String getFullPathName(byte[][] names) {
    StringBuilder fullPathName = new StringBuilder();
    for (int i = 1; i < names.length; i++) {
      byte[] name = names[i];
      fullPathName.append(Path.SEPARATOR_CHAR)
        .append(DFSUtil.bytes2String(name));
    }
    return fullPathName.toString();
  }

  /** Return the inode array representing the given inode's full path name
   * 
   * @param inode an inode
   * @return the node array representation of the given inode's full path
   * @throws IOException if the inode is invalid
   */
  static INode[] getINodeArray(INode inode) throws IOException {
    // calculate the depth of this inode from root
    int depth = getPathDepth(inode);
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      inodes[depth-i-1] = inode;
      inode = inode.parent;
    }
    return inodes;
  }
  
  /**
   * Get the depth of node inode from root
   * @param inode an inode
   * @return depth
   * @throws IOException if the given inode is invalid
   */
  static private int getPathDepth(INode inode) throws IOException {
    // calculate the depth of this inode from root
    int depth = 1;
    INode node; // node on the path to the root
    for (node = inode; node.parent != null; node = node.parent) {
      depth++;
    }

    // parent should be root
    if (node.isRoot()) {
      return depth;
    }
    
    // invalid inode
    throw new IOException("Invalid inode: " + inode.getLocalName());
  }

  /** Return the byte array representing the given inode's full path name 
   * 
   * @param inode an inode
   * @return the byte array representation of the full path of the inode
   * @throws IOException if the inode is invalid
   */
  static byte[][] getINodeByteArray(INode inode) throws IOException {
    // calculate the depth of this inode from root
    int depth = getPathDepth(inode);
    
    byte[][] names = new byte[depth][];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      names[depth-i-1] = inode.getLocalNameBytes();
      inode = inode.parent;
    }
    return names;
  }

  /** Return the full path name of the specified inode
   * 
   * @param inode
   * @return its full path name
   * @throws IOException if the inode is invalid
   */
  static String getFullPathName(INode inode) throws IOException {
    INode[] inodes = getINodeArray(inode);
    return getFullPathName(inodes, inodes.length-1);
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
        unprotectedMkdir(allocateNewInodeId(), inodes, i, components[i], permissions,
            inheritPermission || i != components.length-1, now);
        if (inodes[i] == null) {
          return false;
        }
        // Directory creation also count towards FilesCreated
        // to match count of files_deleted metric.
        if (getFSNamesystem() != null)
          NameNode.getNameNodeMetrics().numFilesCreated.inc();
        fsImage.getEditLog().logMkDir(cur, inodes[i]);
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.mkdirs: created directory " + cur);
        }
      }
    } finally {
      writeUnlock();
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(long inodeId, String src, PermissionStatus permissions,
                          long timestamp) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];
    writeLock();
    try {
      rootDir.getExistingPathINodes(components, inodes);
      unprotectedMkdir(inodeId, inodes, inodes.length-1, components[inodes.length-1],
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
  private void unprotectedMkdir(long inodeId, INode[] inodes, int pos,
      byte[] name, PermissionStatus permission, boolean inheritPermission,
      long timestamp) throws QuotaExceededException {
    inodes[pos] = addChild(inodes, pos,
        new INodeDirectory(inodeId, name, permission, timestamp),
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
   * This functiuon assumes that the nsQuotaStartPos is less or equal than the dsQuotaStartPos
   *
   * @param inodes INodes corresponding to a path
   * @param dsQuotaStartPos the start position where the NS quota needs to be updated
   * @param dsQuotaStartPos the start position where the DS quota needs to be updated
   * @param endPos the end position where the NS and DS quota need to be updated
   * @param nsDelta needed namespace
   * @param dsDelta needed diskspace
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuota(INode[] inodes, int nsQuotaStartPos, int dsQuotaStartPos,
      int endPos, long nsDelta, long dsDelta)
  throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    if (endPos >inodes.length) {
      endPos = inodes.length;
    }
    int i = endPos - 1;
    Assert.assertTrue("nsQuotaStartPos shall be less or equal than the dsQuotaStartPos",
        (nsQuotaStartPos <= dsQuotaStartPos));
    try {
      // check existing components in the path
      for(; i >= nsQuotaStartPos; i--) {
        if (inodes[i].isQuotaSet()) { // a directory with quota
          INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i];
          if (i >= dsQuotaStartPos) {
            // Verify both nsQuota and dsQuota
            node.verifyQuota(nsDelta, dsDelta);
          } else {
            // Verify the nsQuota only
            node.verifyQuota(nsDelta, 0);
          }
        }
      }
    } catch (QuotaExceededException e) {
      e.setPathName(getFullPathName(inodes, i));
      throw e;
    }
  }

  /**
   * Verify quota for the hardlink operation where srcInodes[srcInodes.length-1] copies to
   * dstInodes[dstInodes.length-1]
   *
   * @param srcInodes directory from where node is being moved.
   * @param dstInodes directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private int verifyQuotaForHardLink(INode[] srcInodes, INode[] dstInodes)
  throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return 0;
    }
    Set<INode> ancestorSet = null;  
    INode srcInode = srcInodes[srcInodes.length - 1]; 
    int minLength = Math.min(srcInodes.length, dstInodes.length); 
    int nonCommonAncestor;
    
    // Get the counts from the src file
    INode.DirCounts counts = new INode.DirCounts(); 
    srcInode.spaceConsumedInTree(counts); 
      
    if (srcInode instanceof INodeHardLinkFile) {
      // handle the hardlink file 
      INodeHardLinkFile srcHardLinkFile = (INodeHardLinkFile) srcInode; 
      ancestorSet = srcHardLinkFile.getAncestorSet(); 
      for (nonCommonAncestor = 0; nonCommonAncestor < minLength; nonCommonAncestor++) { 
        if (!ancestorSet.contains(dstInodes[nonCommonAncestor])) {  
          break;
        } 
      }
    } else {
      // handle the regular file  
      for (nonCommonAncestor = 0; nonCommonAncestor < minLength; nonCommonAncestor++) { 
        if (srcInodes[nonCommonAncestor] != dstInodes[nonCommonAncestor]) { 
          break;  
        } 
      } 
    }
    // Verify the NS quota from the beginning and verify the DS quota from the 1st nonCommonAncestor
    verifyQuota(dstInodes, 0, nonCommonAncestor, dstInodes.length - 1,  
        counts.getNsCount(), counts.getDsCount());
    
    return nonCommonAncestor;
  }
  
  /**
   * Verify quota for the rename operation where srcInodes[srcInodes.length-1] copies to
   * dstInodes[dstInodes.length-1]
   *
   * @param srcInodes directory from where node is being moved.
   * @param dstInodes directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes)
  throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode srcInode = srcInodes[srcInodes.length - 1];
    int minLength = Math.min(srcInodes.length, dstInodes.length);
    int nonCommonAncestor;
    INode.DirCounts counts = new INode.DirCounts();
    srcInode.spaceConsumedInTree(counts);
    // handle the regular file
    for (nonCommonAncestor = 0; nonCommonAncestor < minLength; nonCommonAncestor++) {
      if (srcInodes[nonCommonAncestor] != dstInodes[nonCommonAncestor]) {
        break;
      }
    }
    // Verify the NS and DS quota from the 1st nonCommonAncestor
    verifyQuota(dstInodes, nonCommonAncestor, nonCommonAncestor, dstInodes.length - 1,
        counts.getNsCount(), counts.getDsCount());
  }

  /** Add a node child to the inodes at index pos.
   * Its ancestors are stored at [startPos, endPos-1].
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addChild(INode[] pathComponents, int startPos, int endPos,
      T child, long childDiskspace, boolean inheritPermission,
      boolean checkQuota) throws QuotaExceededException {
    INode.DirCounts counts = new INode.DirCounts();
    child.spaceConsumedInTree(counts);
    if (childDiskspace < 0) {
      childDiskspace = counts.getDsCount();
    }
    updateCount(pathComponents, startPos, endPos, counts.getNsCount(), childDiskspace,
        checkQuota);
    T addedNode = null;
    try {
      addedNode = ((INodeDirectory) pathComponents[endPos - 1]).addChild(child,
          inheritPermission);
    } finally {
      if (addedNode == null) {
        updateCount(pathComponents, startPos, endPos, -counts.getNsCount(),
            -childDiskspace, true);
      } else {
        inodeMap.put(addedNode);
      }
    }
    return addedNode;
  }

  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission)
      throws QuotaExceededException {
    return addChild(pathComponents, 0, pos, child, childDiskspace,
        inheritPermission, true);
  }

  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int startPos, int endPos, T child, long childDiskspace, boolean inheritPermission) {
    T inode = null;
    try {
      inode = addChild(pathComponents, startPos, endPos, child, childDiskspace,
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
  private INode removeChild(INode[] pathComponents, int endPos) {
    INode removedNode = pathComponents[endPos];
    int startPos = removedNode.getStartPosForQuoteUpdate();
    removedNode =
      ((INodeDirectory)pathComponents[endPos-1]).removeChild(pathComponents[endPos]);
    if (removedNode != null) {
      INode.DirCounts counts = new INode.DirCounts();
      removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(pathComponents, startPos, endPos,
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
    byte[][] components = INodeDirectory.getPathComponents(srcs);
    readLock();
    try {
      INode targetNode = rootDir.getNode(components);
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
        
        // replace oldNode by newNode with the same id
        inodeMap.put(newNode);
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

  long totalDiskSpace() {
    readLock();
    try {
      return rootDir.diskspaceConsumed();
    } finally {
      readUnlock();
    }
  }

  /**
   * Sets the access time on the file/directory. Logs it in the transaction log
   */
  void setTimes(String src, INode inode, long mtime, long atime, boolean force)
                                                        throws IOException {
    if (unprotectedSetTimes(src, inode, mtime, atime, force)) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force)
                              throws IOException {
    INode inode = getINode(src);
    return unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(String src, INode inode, long mtime,
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
   * Create FileStatus by file INode
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
       size = fileNode.getFileSize();
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
    nameCache.cacheName(inode);
  }
  
  void imageLoaded() throws IOException {
    nameCache.imageLoaded();
  }
  
  public int getInodeMapSize() {
    return inodeMap.size();
  }
}

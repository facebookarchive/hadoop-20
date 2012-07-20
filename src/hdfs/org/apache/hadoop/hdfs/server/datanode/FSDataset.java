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
package org.apache.hadoop.hdfs.server.datanode;

import java.nio.channels.FileChannel;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.DU.NamespaceSliceDU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockFlags;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
public class FSDataset implements FSConstants, FSDatasetInterface {
  
  static String[] getFileNames(File[] files) {
    String[] fileNames = new String[files.length];
    for (int i = 0; i < files.length; i++) {
      fileNames[i] = files[i].getName();
    }
    return fileNames;
  }

  /**
   * A NamespaceSlice represents a portion of a namespace stored on a volume.  
   * Taken together, all BNamespaceSlices sharing a namespaceID across a 
   * cluster represent a single namespace.
   */
  class NamespaceSlice {
    private final int namespaceId;
    private final FSVolume volume; // volume to which this namespaceSlice belongs to
    private final FSDir dataDir; // StorageDirectory/current/nsid/current
    private final File detachDir; // directory store Finalized replica
    private final File rbwDir ; // directory store RBW replica
    private final File tmpDir; // directory store Temporary replica
    private final NamespaceSliceDU dfsUsage;

    /**
     * 
     * @param namespaceId
     * @param volume {@link FSVolume} to which this NamespaceSlice belongs to
     * @param nsDir directory corresponding to the NameSpaceSlice
     * @param conf
     * @throws IOException
     */
    NamespaceSlice(int namespaceId, FSVolume volume, File nsDir, Configuration conf, boolean supportAppends)
        throws IOException {
      this.namespaceId = namespaceId;
      this.volume = volume;
      File nsDirCur = new File(nsDir, DataStorage.STORAGE_DIR_CURRENT); 
      File dataDirFile = new File(nsDirCur, DataStorage.STORAGE_DIR_FINALIZED);
      this.dataDir = new FSDir(namespaceId, dataDirFile, volume);
            
      this.detachDir = new File(nsDir, "detach");
      if (detachDir.exists()) {
        recoverDetachedBlocks(dataDirFile, detachDir);
      }

      // Files that were being written when the datanode was last shutdown
      // are now moved back to the data directory. It is possible that
      // in the future, we might want to do some sort of datanode-local
      // recovery for these blocks. For example, crc validation.
      //
      this.tmpDir = new File(nsDir, "tmp");
      if (tmpDir.exists()) {
        FileUtil.fullyDelete(tmpDir);
      }
      
      this.rbwDir = new File(nsDirCur, DataStorage.STORAGE_DIR_RBW);
      // Files that were being written when the datanode was last shutdown
      // should not be deleted.
      if (rbwDir.exists()) {
        if (supportAppends) {
          recoverBlocksBeingWritten(rbwDir);
        } else {
          // rename tmpDir to prepare delete
          File toDeleteDir = new File(tmpDir.getParent(),
          DELETE_FILE_EXT + tmpDir.getName());
          if (tmpDir.renameTo(toDeleteDir)) {
            // asyncly delete the renamed directory
            asyncDiskService.deleteAsyncFile(volume, toDeleteDir);
          } else {
            // rename failed, let's synchronously delete the directory
            FileUtil.fullyDelete(tmpDir);
            DataNode.LOG.warn("Deleted " + tmpDir.getPath());
          }
        }
      }
      
      if (!rbwDir.mkdirs()) {
        if (!rbwDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + rbwDir.toString());
        }
      }
      if (!tmpDir.mkdirs()) {
        if (!tmpDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + tmpDir.toString());
        }
      }
      if (!detachDir.mkdirs()) {
        if (!detachDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + detachDir.toString());
        }
      }
      this.dfsUsage = volume.dfsUsage.addNamespace(namespaceId, nsDir, conf);
    }
    
    void getBlockInfo(LightWeightHashSet<Block> blocks){
      dataDir.getBlockInfo(blocks);
    }
    
    /**
     * Recover detached files on datanode restart. If a detached block
     * does not exist in the original directory, then it is moved to the
     * original directory.
     */
    private void recoverDetachedBlocks(File dataDir, File dir) 
                                           throws IOException {
      File contents[] = dir.listFiles();

      if (contents == null) {
        return;
      }
      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isFile()) {
          throw new IOException ("Found " + contents[i] + " in " + dir +
                                 " but it is not a file.");
        }

        //
        // If the original block file still exists, then no recovery
        // is needed.
        //
        File blk = new File(dataDir, contents[i].getName());
        if (!blk.exists()) {
          if (!contents[i].renameTo(blk)) {
            throw new IOException("Unable to recover detached file " +
                                  contents[i]);
          }
          continue;
        }
        if (!contents[i].delete()) {
            throw new IOException("Unable to cleanup detached file " +
                                  contents[i]);
        }
      }
    }
    
    void getBlocksBeingWrittenInfo(LightWeightHashSet<Block> blockSet) { 
      if (rbwDir == null) {
        return;
      }
 
      File[] blockFiles = rbwDir.listFiles();
      if (blockFiles == null) {
        return;
      }
      String[] blockFileNames = getFileNames(blockFiles);  
      for (int i = 0; i < blockFiles.length; i++) {
        if (!blockFiles[i].isDirectory()) {
        // get each block in the rbwDir direcotry
          if (Block.isBlockFilename(blockFileNames[i])) {
            long genStamp = FSDataset.getGenerationStampFromFile(
                blockFileNames, blockFileNames[i]);
            Block block = 
              new Block(blockFiles[i], blockFiles[i].length(), genStamp);
            
            // add this block to block set
            blockSet.add(block);
            if (DataNode.LOG.isDebugEnabled()) {
              DataNode.LOG.debug("recoverBlocksBeingWritten for block " + block);
            }
          }
        }
      }
    }
    
    /**
     * Recover blocks that were being written when the datanode
     * was earlier shut down. These blocks get re-inserted into
     * ongoingCreates. Also, send a blockreceived message to the NN
     * for each of these blocks because these are not part of a 
     * block report.
     */
    private void recoverBlocksBeingWritten(File bbw) throws IOException {
      FSDir fsd = new FSDir(namespaceId, bbw, this.volume);
      LightWeightHashSet<BlockAndFile> blockSet = new LightWeightHashSet<BlockAndFile>();
      fsd.getBlockAndFileInfo(blockSet);
      for (BlockAndFile b : blockSet) {
        File f = b.pathfile;  // full path name of block file
        lock.writeLock().lock();
        try {
          volumeMap.add(namespaceId, b.block, new DatanodeBlockInfo(volume, f,
              DatanodeBlockInfo.UNFINALIZED));
          volumeMap.addOngoingCreates(namespaceId, b.block, ActiveFile.createStartupRecoveryFile(f));
        } finally {
          lock.writeLock().unlock();
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("recoverBlocksBeingWritten for block " + b.block + "namespaceId: "+namespaceId);
        }
      }
    } 

    File getDirectory() {
      return dataDir.getDirectory().getParentFile();
    }
    
    File getCurrentDir() {
      return dataDir.getDirectory();
    }
    
    File getRbwDir() {
      return rbwDir;
    }
    
    void decDfsUsed(long value) {
      dfsUsage.decDfsUsed(value);
    }
    
    long getDfsUsed() throws IOException {
      return dfsUsage.getUsed();
    }
    
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return FSDataset.createTmpFile(b, f);
    }
    
    File createDetachFile(Block b) throws IOException {
      File f = new File(detachDir, b.getBlockName());
      return FSDataset.createTmpFile(b, f);
    }
    
    File getTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return f;
    }
    
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(Block b, boolean replicationRequest) throws IOException {
      File f= null;
      if (!replicationRequest) {
        f = new File(rbwDir, b.getBlockName());
      } else {
        f = new File(tmpDir, b.getBlockName());
      }
      return FSDataset.createTmpFile(b, f);
    }
    

    /**
     * RBW files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createRbwFile(Block b) throws IOException {
      File f = new File(rbwDir, b.getBlockName());
      return FSDataset.createTmpFile(b, f);
    }

    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(namespaceId, b, f);
      File metaFile = getMetaFile(blockFile , b);
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      return blockFile;
    }
      
    void checkDirs() throws DiskErrorException {
      dataDir.checkDirTree();
      DiskChecker.checkDir(tmpDir);
      DiskChecker.checkDir(detachDir);
      DiskChecker.checkDir(rbwDir);
    }
      
    void clearPath(File f) {
      dataDir.clearPath(f);
    }
      
    public String toString() {
      return dataDir.getDirectory().getAbsolutePath();
    }
    
    public void shutdown() {
      volume.dfsUsage.removeNamespace(namespaceId);
    }
  }

  /** Find the metadata file for the specified block file.
   * Return the generation stamp from the name of the metafile.
   */
  static long getGenerationStampFromFile(String[] listdir, String blockName) {
    for (int j = 0; j < listdir.length; j++) {
      String path = listdir[j];
      if (!path.startsWith(blockName)) {
        continue;
      }
      String[] vals = StringUtils.split(path, '_');
      if (vals.length != 3) {     // blk, blkid, genstamp.meta
        continue;
      }
      String[] str = StringUtils.split(vals[2], '.');
      if (str.length != 2) {
        continue;
      }
      return Long.parseLong(str[0]);
    }
    DataNode.LOG.warn("Block " + blockName +
                      " does not have a metafile!");
    return Block.GRANDFATHER_GENERATION_STAMP;
  }
  /**
   * A data structure than encapsulates a Block along with the full pathname
   * of the block file
   */
  static class BlockAndFile implements Comparable<BlockAndFile> {
    final Block block;
    final File pathfile;

    BlockAndFile(File fullpathname, Block block) {
      this.pathfile = fullpathname;
      this.block = block;
    }

    public int compareTo(BlockAndFile o)
    {
      return this.block.compareTo(o.block);
    }
  }

  /**
   * A node type that can be built into a tree reflecting the
   * hierarchy of blocks on the local disk.
   */
  class FSDir {
    File dir;
    int numBlocks = 0;
    volatile FSDir childrenDirs[];
    int lastChildIdx = 0;
    
    File getDirectory(){
      return dir;
    }
    
    FSDir[] getChildren() {
      return childrenDirs;
    }
    	
    public FSDir() { 
    }
    
    public FSDir(int namespaceId, File dir) throws IOException{
      this(namespaceId, dir, null);
    }
    
    public FSDir(int namespaceId, File dir, FSVolume volume) throws IOException {
      this.dir = dir;
      this.childrenDirs = null;      
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = dir.listFiles();
        String[] filesNames = getFileNames(files);
        int numChildren = 0;
        for (int i = 0; i < files.length; i++) {
          File file = files[i];
          String fileName = filesNames[i];
          if (isPendingDeleteFilename(fileName)){ 
            // Should not cause throwing an exception.    
            // Obsolete files are not included in the block report.   
            asyncDiskService.deleteAsyncFile(volume, file);   
          } else if (file.isDirectory()) {
            numChildren++;
          } else if (Block.isBlockFilename(fileName)) {
            numBlocks++;
            if (volume != null) {   
              long blkSize = file.length();
              long genStamp = FSDataset.getGenerationStampFromFile(filesNames, fileName);    
                volumeMap.add(namespaceId, new Block(file, blkSize, genStamp),     
                  new DatanodeBlockInfo(volume, file, blkSize));   
            }
          }
        }
        if (numChildren > 0) {
          FSDir[] newChildren = new FSDir[numChildren];
          int curdir = 0;
          for (int idx = 0; idx < files.length; idx++) {
            String fileName = files[idx].getName();
            if (files[idx].isDirectory() && !isPendingDeleteFilename(fileName)) {
              newChildren[curdir] = new FSDir(namespaceId, files[idx], volume);
              curdir++;
            }
          }
          childrenDirs = newChildren;
        }
      }
    }
        
    public File addBlock(int namespaceId, Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(namespaceId, b, src, false, false);          
      return (file != null) ? file : addBlock(namespaceId, b, src, true, true);
    }

    private File addBlock(int namespaceId, Block b, File src, boolean createOk, 
                          boolean resetIdx) throws IOException {
      if (numBlocks < maxBlocksPerDir) {
        File dest = new File(dir, b.getBlockName());
        File metaData = getMetaFile( src, b );
        File newmeta = getMetaFile(dest, b);
        if ( ! metaData.renameTo( newmeta ) ||
            ! src.renameTo( dest ) ) {
          throw new IOException( "could not move files for " + b +
                                 " from tmp to " + 
                                 dest.getAbsolutePath() );
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("addBlock: Moved " + metaData + " to " + newmeta);
          DataNode.LOG.debug("addBlock: Moved " + src + " to " + dest);
        }

        numBlocks += 1;
        return dest;
      }
       
      FSDir[] children = this.getChildren();
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = random.nextInt(children.length);              
      }
            
      if (lastChildIdx >= 0 && children != null) {
        //Check if any child-tree has room for a block.
        for (int i=0; i < children.length; i++) {
          int idx = (lastChildIdx + i)%children.length;
          File file = children[idx].addBlock(namespaceId, b, src, false, resetIdx);
          if (file != null) {
            lastChildIdx = idx;
            return file; 
          }
        }
        lastChildIdx = -1;
      }
            
      if (!createOk) {
        return null;
      }
            
      if (children == null || children.length == 0) {
        // make sure children is immutable once initialized.
        FSDir[] newChildren = new FSDir[maxBlocksPerDir];
        for (int idx = 0; idx < maxBlocksPerDir; idx++) {
          newChildren[idx] = new FSDir(namespaceId, new File(dir,
              DataStorage.BLOCK_SUBDIR_PREFIX + idx));
        }
        childrenDirs = children = newChildren;
      }
            
      //now pick a child randomly for creating a new set of subdirs.
      lastChildIdx = random.nextInt(children.length);
      return children[ lastChildIdx ].addBlock(namespaceId, b, src, true, false); 
    }

    /**
     * Populate the given blockSet with any child blocks
     * found at this node.
     */
    public void getBlockInfo(LightWeightHashSet<Block> blockSet) {
      FSDir[] children = this.getChildren();
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      String[] blockFilesNames = getFileNames(blockFiles);
      
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFilesNames[i])) {
          long genStamp = FSDataset.getGenerationStampFromFile(blockFilesNames,
              blockFilesNames[i]);
          blockSet.add(new Block(blockFiles[i], blockFiles[i].length(), genStamp));
        }
      }
    }

    /**
     * Populate the given blockSet with any child blocks
     * found at this node. With each block, return the full path
     * of the block file.
     */
    void getBlockAndFileInfo(LightWeightHashSet<BlockAndFile> blockSet) {
      FSDir[] children = this.getChildren();
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockAndFileInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      String[] blockFilesNames = getFileNames(blockFiles);      
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFilesNames[i])) {
          long genStamp = FSDataset.getGenerationStampFromFile(blockFilesNames,
              blockFilesNames[i]);
          Block block = new Block(blockFiles[i], blockFiles[i].length(), genStamp);
          blockSet.add(new BlockAndFile(blockFiles[i].getAbsoluteFile(), block));
        }
      }
    }

    /**
     * check if a data directory is healthy
     * @throws DiskErrorException
     */
    public void checkDirTree() throws DiskErrorException {
      DiskChecker.checkDir(dir);

      FSDir[] children = this.getChildren();
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].checkDirTree();
        }
      }
    }

    void clearPath(File f) {
      String root = dir.getAbsolutePath();
      String dir = f.getAbsolutePath();
      if (dir.startsWith(root)) {
        String[] dirNames = dir.substring(root.length()).
          split(File.separator + "subdir");
        if (clearPath(f, dirNames, 1))
          return;
      }
      clearPath(f, null, -1);
    }

    /*
     * dirNames is an array of string integers derived from
     * usual directory structure data/subdirN/subdirXY/subdirM ...
     * If dirName array is non-null, we only check the child at
     * the children[dirNames[idx]]. This avoids iterating over
     * children in common case. If directory structure changes
     * in later versions, we need to revisit this.
     */
    private boolean clearPath(File f, String[] dirNames, int idx) {
      if ((dirNames == null || idx == dirNames.length) &&
          dir.compareTo(f) == 0) {
        numBlocks--;
        return true;
      }

      FSDir[] children = this.getChildren();
      if (dirNames != null) {
        //guess the child index from the directory name
        if (idx > (dirNames.length - 1) || children == null) {
          return false;
        }
        int childIdx;
        try {
          childIdx = Integer.parseInt(dirNames[idx]);
        } catch (NumberFormatException ignored) {
          // layout changed? we could print a warning.
          return false;
        }
        return (childIdx >= 0 && childIdx < children.length) ?
          children[childIdx].clearPath(f, dirNames, idx+1) : false;
      }

      //guesses failed. back to blind iteration.
      if (children != null) {
        for(int i=0; i < children.length; i++) {
          if (children[i].clearPath(f, null, -1)){
            return true;
          }
        }
      }
      return false;
    }
    
    public String toString() {
      FSDir[] children = this.getChildren();
      return "FSDir{" +
        "dir=" + dir +
        ", children=" + (children == null ? null : Arrays.asList(children)) +
        "}";
    }
  }
  
  /**
   * A map from namespace ID to NamespaceSlice object
   * 
   * Only three operations are supported: add a namespace, remove a namespace
   * and get a snapshot of the list of the namespace map, which is an immutable
   * object.
   * 
   * No extra locking is allowed in this object
   */
  class NamespaceMap {
    /**
     * Any object referred here needs to be immutable. Every time this map is
     * updated, a new map is created and the reference here is changed to the
     * new map.
     */
    private Map<Integer, NamespaceSlice> namespaceMap = new HashMap<Integer, NamespaceSlice>();;

    /**
     * It is the only method a caller is supposed to access namespaceMap. This
     * method will return a immutable map. It is a snapshot.
     * 
     * @return
     */
    private synchronized Map<Integer, NamespaceSlice> getNamespaceMapSnapshot() {
      return namespaceMap;
    }

    public synchronized void addNamespace(int namespaceId, NamespaceSlice ns)
        throws IOException {
      // add a new name-space by copying all the entries to a new map.
      Map<Integer, NamespaceSlice> newMap = new HashMap<Integer, NamespaceSlice>(
          namespaceMap);
      newMap.put(namespaceId, ns);
      namespaceMap = newMap;
    }

    public synchronized void removeNamespace(int namespaceId) {
      Map<Integer, NamespaceSlice> newMap = new HashMap<Integer, NamespaceSlice>(
          namespaceMap);
      newMap.remove(namespaceId);
      namespaceMap = newMap;
    }
  }
  
  class FSVolume {
    private final NamespaceMap namespaceMap;
    private final File currentDir;    // <StorageDirectory>/current
    private final DF usage;           
    private final long reserved;
    private final FSDataset dataset;
    private DU dfsUsage;
    
    FSVolume(FSDataset dataset, File currentDir, Configuration conf) throws IOException {
      this.currentDir = currentDir; 
      File parent = currentDir.getParentFile();
      this.usage = new DF(parent, conf);
      this.reserved = usage.getReserved();
      this.dataset = dataset;
      this.namespaceMap = new NamespaceMap();
      this.dfsUsage = new DU(currentDir, conf);
      this.dfsUsage.start();
    }
    
    /**
     * It is the only method a caller is supposed to access namespaceMap.
     * This method will return a immutable map. It is a snapshot.
     * @return
     */
    private Map<Integer, NamespaceSlice> getNamespaceMapSnapshot() {
      return namespaceMap.getNamespaceMapSnapshot();
    }

    NamespaceSlice getNamespaceSlice(int namespaceId){
      return getNamespaceMapSnapshot().get(namespaceId);
    }
    
    /** Return storage directory corresponding to the volume */
    public File getDir() {
      return currentDir.getParentFile();
    }
    
    public File getCurrentDir() {
      return currentDir;
    }
    
    public File getRbwDir(int namespaceId) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.getRbwDir();
    }
    
    void decDfsUsed(int namespaceId, long value) {
      // this lock is put in FSVolume since it is called only ReplicaFileDeleteWork
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      if (ns != null) {
        ns.decDfsUsed(value);
      }
    }
    
    long getDfsUsed() throws IOException {
      long dfsUsed = 0;
      for (NamespaceSlice ns : getNamespaceMapSnapshot().values()) {
        dfsUsed += ns.getDfsUsed();
      }
      return dfsUsed;
    }
    
    long getNSUsed(int namespaceId) throws IOException {
      return getNamespaceMapSnapshot().get(namespaceId).getDfsUsed();
    }
        
    long getCapacity() throws IOException {
      if (reserved > usage.getCapacity()) {
        return 0;
      }

      return usage.getCapacity()-reserved;
    }
      
    long getAvailable() throws IOException {
      long remaining = getCapacity()-getDfsUsed();
      long available = usage.getAvailable();
      if (remaining>available) {
        remaining = available;
      }
      return (remaining > 0) ? remaining : 0;
    }
    
    long getReserved() {
      return this.reserved;
    }
      
    String getMount() throws IOException {
      return usage.getMount();
    }

    String getFileSystem() throws IOException {
      return usage.getFilesystem();
    }
     
    File addBlock(int namespaceId, Block b, File f) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.addBlock(b, f);
    }
      
    void checkDirs() throws DiskErrorException {
      for (NamespaceSlice ns : getNamespaceMapSnapshot().values()) {
        ns.checkDirs();
      }
    }
    
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(int namespaceId, Block b) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.createTmpFile(b);
    }
    
    File getTmpFile(int namespaceId, Block b) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.getTmpFile(b);
    }
    
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(int namespaceId, Block b, boolean replicationRequest) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.createTmpFile(b, replicationRequest);
    }
    
    /**
     * Files used for copy-on-write. They need recovery when datanode
     * restarts.
     */
    File createDetachFile(int namespaceId, Block b, String filename) throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      return ns.createDetachFile(b);
    }
    
    public void addNamespace(int namespaceId, String nsDir, Configuration conf, boolean supportAppends)
        throws IOException {
      File nsdir = new File(currentDir, nsDir);
      NamespaceSlice ns = new NamespaceSlice(namespaceId, this, nsdir, conf, supportAppends);
      namespaceMap.addNamespace(namespaceId, ns);
    }
    
    void getBlocksBeingWrittenInfo(int namespaceId, LightWeightHashSet<Block> blockSet) {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      if (ns == null) {
        return;
      }
      ns.getBlocksBeingWrittenInfo(blockSet);
      return;
    }

    public void shutdownNamespace(int namespaceId) {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      if (ns != null) {
        this.namespaceMap.removeNamespace(namespaceId);
        ns.shutdown();
      }
    }

    void getBlockInfo(int namespaceId, LightWeightHashSet<Block> blockSet)
        throws IOException {
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      ns.getBlockInfo(blockSet);
      return;
    }
    
    public void shutdown() {
      for (NamespaceSlice ns : getNamespaceMapSnapshot().values()) {
        ns.shutdown();
      }
      dfsUsage.shutdown();
    }
    
    void clearPath(int namespaceId, File f) throws IOException{
      NamespaceSlice ns = getNamespaceSlice(namespaceId);
      ns.clearPath(f);
      return;
    }
    
    public String toString() {
      return currentDir.getAbsolutePath();
    }
  }

  /**
   * This class maintain a list of FSVolume objects.
   * Only three operations are supported: add volumes, remove volumes,
   * and get a snapshot of the list of the volumes, which is an immutable
   * object.
   */
  static class FSVolumeList {
    volatile FSVolume[] fsVolumes = null;
    
    public FSVolumeList(FSVolume[] volumes) {
      fsVolumes = volumes;
    }
    
    public synchronized void addVolumes(FSVolume[] volArray) {
      if (volArray == null || volArray.length == 0) {
        return;
      }

      int size = fsVolumes.length + volArray.length;
      FSVolume fsvs[] = new FSVolume[size];
      int idx = 0;
      for (; idx < fsVolumes.length; idx++) {
        fsvs[idx] = fsVolumes[idx];
      }
      for (; idx < size; idx++) {
        fsvs[idx] = volArray[idx - fsVolumes.length];
      }
      fsVolumes = fsvs;
    }
    
    public synchronized void removeVolumes(List<FSVolume> removed_vols) {
      // repair array - copy non null elements
      int removed_size = (removed_vols == null) ? 0 : removed_vols.size();
      if (removed_size > 0) {
        FSVolume fsvs[] = new FSVolume[fsVolumes.length - removed_size];
        for (int idx = 0, idy = 0; idx < fsVolumes.length; idx++) {
          if (!removed_vols.contains(fsVolumes[idx])) {
            fsvs[idy] = fsVolumes[idx];
            idy++;
          }
        }
        fsVolumes = fsvs; // replace array of volumes
      }
    }
    
    public FSVolume[] getVolumeListSnapshot() {
      return fsVolumes;
    }
  }

  static class FSVolumeSet {
    final FSVolumeList volumeList;
    int curVolume = 0;

    ExecutorService scannersExecutor;
    boolean supportAppends;

    private FSVolumeSet(FSVolume[] volumes, int threads, boolean supportAppends) {
      this.volumeList = new FSVolumeList(volumes);
      this.supportAppends = supportAppends;
      if (threads > 1) {
        scannersExecutor = Executors.newFixedThreadPool(threads);
      }
    }
    
    public boolean isValidDir(File currentDir) {
      FSVolume[] volumes = this.getVolumes();
      for (int idx = 0; idx < volumes.length; idx++) {
        if (volumes[idx].getCurrentDir().equals(currentDir)) {
          return true;
        }
      }
      return false;
    }
    
    protected void addVolumes(FSVolume[] volArray) {
      volumeList.addVolumes(volArray);
    }
    
    protected int numberOfVolumes() {
      return getVolumes().length;
    }

    public FSVolume[] getVolumes() {
      return volumeList.getVolumeListSnapshot();
    }
      
    private FSVolume getNextVolume(long blockSize) throws IOException {
      FSVolume[] volumes = this.getVolumes();

      if(volumes.length < 1) {
        throw new DiskOutOfSpaceException("No more available volumes");
      }
      
      // since volumes could've been removed because of the failure
      // make sure we are not out of bounds
      if (curVolume >= volumes.length) {
        curVolume = 0;
      }

      int startVolume = curVolume;

      while (true) {
        FSVolume volume = volumes[curVolume];
        curVolume = (curVolume + 1) % volumes.length;
        if (volume.getAvailable() > blockSize) {
          return volume;
        }
        if (curVolume == startVolume) {
          throw new DiskOutOfSpaceException(
              "Insufficient space for an additional block");
        }
      }
    }
      
    private long getDfsUsed() throws IOException {
      long dfsUsed = 0L;
      FSVolume[] volumes = this.getVolumes();

      for (int idx = 0; idx < volumes.length; idx++) {
        dfsUsed += volumes[idx].getDfsUsed();
      }
      return dfsUsed;
    }

    private long getNSUsed(int namespaceId) throws IOException {
      long dfsUsed = 0L;
      FSVolume[] volumes = this.getVolumes();

      for (int idx = 0; idx < volumes.length; idx++) {
        dfsUsed += volumes[idx].getNSUsed(namespaceId);
      }
      return dfsUsed;
    }
    
    private long getCapacity() throws IOException {
      long capacity = 0L;
      FSVolume[] volumes = this.getVolumes();

      for (int idx = 0; idx < volumes.length; idx++) {
        capacity += volumes[idx].getCapacity();
      }
      return capacity;
    }
      
    private long getRemaining() throws IOException {
      long remaining = 0L;
      FSVolume[] volumes = this.getVolumes();

      for (int idx = 0; idx < volumes.length; idx++) {
        remaining += volumes[idx].getAvailable();
      }
      return remaining;
    }
    
    private void getBlocksBeingWrittenInfo(int namespaceId,
        LightWeightHashSet<Block> blockSet) throws IOException {
      long startTime = System.currentTimeMillis(); 
      FSVolume[] volumes = this.getVolumes();

      if (scannersExecutor != null) {
        synchronized(scannersExecutor) {
          List<Future<LightWeightHashSet<Block>>> builders =
              new ArrayList<Future<LightWeightHashSet<Block>>>();
          for (int idx = 0; idx < volumes.length; idx++) {
            builders.add(scannersExecutor
                .submit(new BlocksBeingWrittenInfoBuilder(volumes[idx],
                    namespaceId)));
          }
          for (Future<LightWeightHashSet<Block>> future : builders) {
            try {
              blockSet.addAll(future.get());
            } catch (ExecutionException ex) {
              DataNode.LOG.error(
                  "Error generating block being written info from volumes ",
                  ex.getCause());
              throw new IOException(ex);
            } catch (InterruptedException iex) {
              DataNode.LOG.error(
                  "Error waiting for generating block being written info", iex);
              throw new IOException(iex);
            }
          }
        }
      } else {
        for (int idx = 0; idx < volumes.length; idx++) {
          volumes[idx].getBlocksBeingWrittenInfo(namespaceId, blockSet);
        }
      }
      long scanTime = (System.currentTimeMillis() - startTime)/1000;
      DataNode.LOG.info("Finished generating blocks being written report for " +
      volumes.length + " volumes in " + scanTime + " seconds");
    }
      
    private void getBlockInfo(int namespaceId, LightWeightHashSet<Block> blockSet) {
      long startTime = System.currentTimeMillis();
      FSVolume[] volumes = this.getVolumes();

      if (scannersExecutor != null) {
        synchronized (scannersExecutor) {
          List<Future<LightWeightHashSet<Block>>> builders =
              new ArrayList<Future<LightWeightHashSet<Block>>>();
          for (int idx = 0; idx < volumes.length; idx++) {
            builders.add(scannersExecutor.submit(new BlockInfoBuilder(
                volumes[idx], namespaceId)));
          }
          for (Future<LightWeightHashSet<Block>> future : builders) {
            try {
              blockSet.addAll(future.get());
            } catch (ExecutionException ex) {
              DataNode.LOG.error("Error scanning volumes ", ex.getCause());
            } catch (InterruptedException iex) {
              DataNode.LOG.error("Error waiting for scan", iex);
            }
          }
        }
      } else {
        for (int idx = 0; idx < volumes.length; idx++) {
          try{  
            volumes[idx].getBlockInfo(namespaceId, blockSet);
          } catch (IOException e) {
            DataNode.LOG.error("Error scanning volumes ", e.getCause());
          }
        }
      }
      long scanTime = (System.currentTimeMillis() - startTime)/1000;
      DataNode.LOG.info("Finished generating block report for " +
          volumes.length + " volumes in " + scanTime + " seconds");
    }
      
    /**
     * goes over all the volumes and checkDir eachone of them
     * if one throws DiskErrorException - removes from the list of active 
     * volumes. 
     * @return list of all the removed volumes
     */
    private List<FSVolume> checkDirs() {      
      List<FSVolume> removed_vols = null;

      FSVolume[] fsVolumes = this.getVolumes();
      for (int idx = 0; idx < fsVolumes.length; idx++) {
        FSVolume fsv = fsVolumes[idx];
        try {
          fsv.checkDirs();
        } catch (DiskErrorException e) {
          DataNode.LOG.warn("Removing failed volume " + fsv + ": ", e);
          if (removed_vols == null) {
            removed_vols = new ArrayList<FSVolume>();
            removed_vols.add(fsVolumes[idx]);
          }
        }
      }

      if (removed_vols != null && removed_vols.size() > 0) {
        volumeList.removeVolumes(removed_vols);
        DataNode.LOG.info("Completed FSVolumeSet.checkDirs. Removed="
            + removed_vols.size() + "volumes. List of current volumes: "
            + toString());
      }

      return removed_vols;
    }
    
    private void addNamespace(int namespaceId, String nsDir, Configuration conf)
        throws IOException {
      FSVolume[] volumes = this.getVolumes();

      for (FSVolume v : volumes) {
        v.addNamespace(namespaceId, nsDir, conf, supportAppends);
      }
    }
    
    private void addNamespace(FSVolume[] vs, int namespaceId, String nsDir, Configuration conf)
        throws IOException {    
      for (FSVolume v : vs) {
        v.addNamespace(namespaceId, nsDir, conf, supportAppends);
      }
    }

    private void removeNamespace(int namespaceId) {
      FSVolume[] volumes = this.getVolumes();

      for (FSVolume v : volumes) {
        v.shutdownNamespace(namespaceId);
      }
    }
    
    public String toString() {
      StringBuffer sb = new StringBuffer();
      FSVolume[] volumes = this.getVolumes();

      for (int idx = 0; idx < volumes.length; idx++) {
        sb.append(volumes[idx].toString());
        if (idx != volumes.length - 1) { sb.append(","); }
      }
      return sb.toString();
    }
  }
  
  private static class BlockInfoBuilder implements Callable<LightWeightHashSet<Block>> {
    FSVolume volume;
    int namespaceId;

    public BlockInfoBuilder(FSVolume volume, int namespaceId) {
      this.volume = volume;
      this.namespaceId = namespaceId;
    }

    @Override
    public LightWeightHashSet<Block> call() throws Exception {
      LightWeightHashSet<Block> result = new LightWeightHashSet<Block>();
      volume.getBlockInfo(namespaceId, result);
      return result;
    }
  }

  private static class BlocksBeingWrittenInfoBuilder implements
      Callable<LightWeightHashSet<Block>> {
    FSVolume volume;
    int namespaceId;

    public BlocksBeingWrittenInfoBuilder(FSVolume volume, int namespaceId) {
      this.volume = volume;
      this.namespaceId = namespaceId;
    }

    @Override
    public LightWeightHashSet<Block> call() throws Exception {
      LightWeightHashSet<Block> result = new LightWeightHashSet<Block>();
      volume.getBlocksBeingWrittenInfo(namespaceId, result);
      return result;
    }
  }
  //////////////////////////////////////////////////////
  //
  // FSDataSet
  //
  //////////////////////////////////////////////////////

  //Find better place?
  public static final String METADATA_EXTENSION = ".meta";
  public static final short METADATA_VERSION = 1;
  public static final String DELETE_FILE_EXT = "toDelete.";

  static class ActiveFile implements ReplicaBeingWritten, Cloneable {
    static final long UNKNOWN_SIZE = -1;
    
    final File file;
    final List<Thread> threads = new ArrayList<Thread>(2);
    private volatile long bytesAcked;
    private volatile long bytesOnDisk;
    /**
     * Set to true if this file was recovered during datanode startup.
     * This may indicate that the file has been truncated (eg during
     * underlying filesystem journal replay)
     */
    final boolean wasRecoveredOnStartup;

    ActiveFile(File f, List<Thread> list) throws IOException {
      this(f, list, UNKNOWN_SIZE);
    }

    ActiveFile(File f, List<Thread> list, long expectedSize) throws IOException {
      this(f, false, expectedSize);
      if (list != null) {
        threads.addAll(list);
      }
      threads.add(Thread.currentThread());
    }

    /**
     * Create an ActiveFile from a file on disk during DataNode startup.
     * This factory method is just to make it clear when the purpose
     * of this constructor is.
     * @throws IOException 
     */
    public static ActiveFile createStartupRecoveryFile(File f)
        throws IOException {
      return new ActiveFile(f, true, UNKNOWN_SIZE);
    }

    private ActiveFile(File f, boolean recovery, long expectedSize)
        throws IOException {
      file = f;
      long fileLength = f.length();
      if (expectedSize != UNKNOWN_SIZE && fileLength != expectedSize) {
        throw new IOException("File " + f + " on disk size " + fileLength
            + " doesn't match expected size " + expectedSize);
      }
     bytesAcked = bytesOnDisk = fileLength;
      wasRecoveredOnStartup = recovery;
    }

    public long getBytesAcked() {
      return bytesAcked;
    }

    public void setBytesAcked(long value) {
      bytesAcked = value;
    }

    public long getBytesOnDisk() {
      return bytesOnDisk;
    }

    public void setBytesOnDisk(long value) {
      bytesOnDisk = value;
    }

    public String toString() {
      return getClass().getSimpleName() + "(file=" + file
          + ", threads=" + threads + ")";
    }
    
    public ActiveFile getClone() throws CloneNotSupportedException {
      return (ActiveFile) super.clone();
    }
  }
  
  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + METADATA_EXTENSION;
  }

  static File getMetaFile(File f , Block b) {
    return new File(getMetaFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }
  protected File getMetaFile(int namespaceId, Block b) throws IOException {
    return getMetaFile(getBlockFile(namespaceId, b), b);
  }

  /** Find the corresponding meta data file from a given block file */
  public static File findMetaFile(final File blockFile) throws IOException {
    return findMetaFile(blockFile, false);
  }

  private static File findMetaFile(final File blockFile, boolean missingOk)
    throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      if (missingOk) {
        return null;
      } else {
        throw new IOException("Meta file not found, blockFile=" + blockFile);
      }
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: "
          + Arrays.asList(matches));
    }
    return matches[0];
  }
  
  /**
   * Check if a file is scheduled for deletion
   * name should be obtained by File.getName()
   */
  static boolean isPendingDeleteFilename(String name) {
    return name.startsWith(DELETE_FILE_EXT);
  }

  /** Find the corresponding meta data file from a given block file */
  static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    String metaname = metaFile.getName();
    String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw (IOException)new IOException("blockFile=" + blockFile
          + ", metaFile=" + metaFile).initCause(nfe);
    }
  }

  /** Return the block file for the given ID */ 
  public File findBlockFile(int namespaceId, long blockId) {
    lock.readLock().lock();
    try {
      final Block eb = new Block(blockId);
      File blockfile = null;
      ActiveFile activefile = volumeMap.getOngoingCreates(namespaceId, eb);
      if (activefile != null) {
        blockfile = activefile.file;
      }
      if (blockfile == null) {
        blockfile = getFile(namespaceId, eb);
      }
      if (blockfile == null) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("volumeMap=" + volumeMap);
        }
      }
      return blockfile;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Block getStoredBlock(int namespaceId, long blkid) throws IOException {
    return getStoredBlock(namespaceId, blkid, false);
  }
  /** {@inheritDoc} */
  public Block getStoredBlock(int namespaceId, long blkid,
      boolean useOnDiskLength) throws IOException {
    lock.readLock().lock();
    try {
      File blockfile = findBlockFile(namespaceId, blkid);
      if (blockfile == null) {
        return null;
      }
      File metafile = findMetaFile(blockfile, true);
      if (metafile == null) {
        return null;
      }
      Block block = new Block(blkid);
      if (useOnDiskLength) {
        block.setNumBytes(getOnDiskLength(namespaceId, block));
      } else {
        block.setNumBytes(getVisibleLength(namespaceId, block));
      }
      block.setGenerationStamp(parseGenerationStamp(blockfile, metafile));
      return block;
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean metaFileExists(int namespaceId, Block b) throws IOException {
    return getMetaFile(namespaceId, b).exists();
  }
  
  public long getMetaDataLength(int namespaceId, Block b) throws IOException {
    File checksumFile = getMetaFile(namespaceId, b);
    return checksumFile.length();
  }

  public MetaDataInputStream getMetaDataInputStream(int namespace, Block b)
      throws IOException {
    File checksumFile = getMetaFile(namespace, b);
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }

  FSVolumeSet volumes;
  private DataNode datanode;
  private Configuration conf;
  private int maxBlocksPerDir = 0;
  private boolean initialized = false;
  
  VolumeMap volumeMap; 
  static  Random random = new Random();
  FSDatasetAsyncDiskService asyncDiskService;
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private boolean shouldHardLinkBlockCopy;
  private int validVolsRequired;
  
  //this constructor is used to create PersistedSimulatedFSDataset
  public FSDataset() { 
  }
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  public FSDataset(DataNode datanode, Configuration conf, int numNamespaces){
    this.datanode = datanode;
    this.conf = conf;
    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    volumeMap = new VolumeMap(numNamespaces);
  }
  
  @Override
  public void initialize(DataStorage storage) throws IOException{
    lock.writeLock().lock();
    try{
      if(initialized){
        return;
      }
      
      // The number of volumes required for operation is the total number 
      // of volumes configured minus the number of failed volumes we can
      // tolerate.
      final int volFailuresTolerated =
        conf.getInt("dfs.datanode.failed.volumes.tolerated", 0);
      String[] dataDirs = conf.getStrings("dfs.data.dir");
      int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
      this.validVolsRequired = volsConfigured - volFailuresTolerated;
      if (validVolsRequired < 1 || validVolsRequired > storage.getNumStorageDirs()) {
        throw new DiskErrorException("Too many failed volumes - "
                  + "current valid volumes: " + storage.getNumStorageDirs() 
                  + ", volumes configured: " + volsConfigured 
                  + ", volume failures tolerated: " + volFailuresTolerated );
      }
      File[] roots = new File[storage.getNumStorageDirs()];
      for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
        roots[idx] = storage.getStorageDir(idx).getCurrentDir();
      }
      asyncDiskService = new FSDatasetAsyncDiskService(roots, conf);   
      FSVolume[] volArray = new FSVolume[storage.getNumStorageDirs()];
      for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
        volArray[idx] = new FSVolume(this, storage.getStorageDir(idx).getCurrentDir(),
            conf);
        DataNode.LOG.info("FSDataset added volume - "
            + storage.getStorageDir(idx).getCurrentDir());
      }
      int threads = conf.getInt("dfs.datanode.blockscanner.threads", 1);
      volumes = new FSVolumeSet(volArray, threads, datanode.isSupportAppends());
      registerMBean(storage.getStorageID());
      initialized = true;
    } finally {
      lock.writeLock().unlock();
    }
    shouldHardLinkBlockCopy = conf.getBoolean("dfs.datanode.blkcopy.hardlink",
        true);
  }

  private class VolumeThread extends Thread {
    private Configuration conf;
    private FSVolume volume;
    private boolean hasError = false;
    private Map<Integer, String> namespaceIdDir;
    private boolean supportAppends;

    private VolumeThread(FSVolume volume,
        Configuration conf,
        Map<Integer, String> namespaceIdDir, boolean supportAppends) {
      this.namespaceIdDir = namespaceIdDir;
      this.volume = volume;
      this.conf = conf;
      this.supportAppends = supportAppends;

    }

    public void run() {
      DataNode.LOG.info("Start building volume: " + volume);
      try {
        for (Integer namespaceId : namespaceIdDir.keySet()) {
          volume.addNamespace(namespaceId, namespaceIdDir.get(namespaceId),
              conf, supportAppends);
        }       
      } catch (IOException ioe) {
        DataNode.LOG.error("Error building volume : " + volume, ioe);
        hasError = true;
      }
      DataNode.LOG.info("Finish building volume for " + volume);
    }
  }
  
  private void createVolumes(FSVolumeSet volumes, DataStorage storage,
      Configuration conf, VolumeMap volumeMap,
      Map<Integer, String> namespaceIdDir) throws IOException {
    FSVolume[] myVolumes = volumes.getVolumes();

    ArrayList<VolumeThread> scanners = new ArrayList<VolumeThread>(
        myVolumes.length);
    
    for(FSVolume volume : myVolumes){
      scanners.add(new VolumeThread(volume, conf,
          namespaceIdDir, volumes.supportAppends));
    }
    
    for(VolumeThread vt : scanners){
      vt.start();
    }
    boolean hasError = false;
    for (VolumeThread vt : scanners) {
      try {
        vt.join();
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      if (!hasError && vt.hasError) {
        hasError = true;
      } 

    }
    if (hasError) {
      throw new IOException("Error creating volumes");
    }
  }


  /**
   * Return the total space used by dfs datanode
   */
  public long getDfsUsed() throws IOException {
    return volumes.getDfsUsed();
  }
  
  /**
   * Return the total space used by one namespace in dfs datanode
   */
  public long getNSUsed(int namespaceId) throws IOException {
    return volumes.getNSUsed(namespaceId);
  }
  
  /**
   * Return true - if there are still valid volumes 
   * on the DataNode
   */
  public boolean hasEnoughResource(){
    return volumes.numberOfVolumes() >= this.validVolsRequired;
  }

  /**
   * Return total capacity, used and unused
   */
  public long getCapacity() throws IOException {
    return volumes.getCapacity();
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  public long getRemaining() throws IOException {
    return volumes.getRemaining();
  }

  /**
   * Find the block's on-disk length
   */
  public long getFinalizedBlockLength(int namespaceId, Block b) throws IOException {
    DatanodeBlockInfo info = volumeMap.get(namespaceId, b);
    if (info == null) {
      throw new IOException("Can't find block " + b + " in volumeMap");
    }
    return info.getFinalizedSize();
  }

  @Override
  public long getOnDiskLength(int namespaceId, Block b) throws IOException {
    ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, b);

    if (activeFile != null) {
      return activeFile.getBytesOnDisk();
    } else {
      return getFinalizedBlockLength(namespaceId, b);
    }
  }

  @Override
  public long getVisibleLength(int namespaceId, Block b) throws IOException {
    ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, b);

    if (activeFile != null) {
      return activeFile.getBytesAcked();
    } else {
      return getFinalizedBlockLength(namespaceId, b);
    }
  }

  @Override
  public ReplicaBeingWritten getReplicaBeingWritten(
      int namespaceId, Block b) throws IOException {
    lock.readLock().lock();
    try {
      return volumeMap.getOngoingCreates(namespaceId, b);
    } finally {
      lock.readLock().unlock();
    }
  }  

  /**
   * Get File name for a given block.
   */
  public File getBlockFile(int namespaceId, Block b) throws IOException {
    File f = validateBlockFile(namespaceId, b);
    if (f == null) {
      if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
        InterDatanodeProtocol.LOG
            .debug("b=" + b + ", volumeMap=" + volumeMap);
      }
      throw new IOException("Block " + b + ", namespace= " + namespaceId
          + " is not valid.");
    }
    return f;
  }
  
  public InputStream getBlockInputStream(int namespaceId, Block b) throws IOException {
    return new FileInputStream(getBlockFile(namespaceId, b));
  }

  public InputStream getBlockInputStream(int namespaceId, Block b, long seekOffset) throws IOException {
    File blockFile = getBlockFile(namespaceId, b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (seekOffset > 0) {
      blockInFile.seek(seekOffset);
    }
    return new FileInputStream(blockInFile.getFD());
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  public BlockInputStreams getTmpInputStreams(int namespaceId, Block b, 
                          long blkOffset, long ckoff) throws IOException {
    lock.readLock().lock();
    try {
      DatanodeBlockInfo info = volumeMap.get(namespaceId, b);
      if (info == null) {
        throw new IOException("Block " + b + " does not exist in volumeMap.");
      }
      FSVolume v = info.getVolume();
      File blockFile = info.getFile();
      if (blockFile == null) {
        blockFile = v.getTmpFile(namespaceId, b);
      }
      RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
      if (blkOffset > 0) {
        blockInFile.seek(blkOffset);
      }
      File metaFile = getMetaFile(blockFile, b);
      RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
      if (ckoff > 0) {
        metaInFile.seek(ckoff);
      }
      return new BlockInputStreams(new FileInputStream(blockInFile.getFD()),
          new FileInputStream(metaInFile.getFD()));
    } finally {
      lock.readLock().unlock();
    }
  }

  private BlockWriteStreams createBlockWriteStreams( File f , File metafile) throws IOException {
      return new BlockWriteStreams(new FileOutputStream(new RandomAccessFile( f , "rw" ).getFD()),
          new FileOutputStream( new RandomAccessFile( metafile , "rw" ).getFD() ));

  }

  /**
   * Make a copy of the block if this block is linked to an existing
   * snapshot. This ensures that modifying this block does not modify
   * data in any existing snapshots.
   * @param block Block
   * @param numLinks Detach if the number of links exceed this value
   * @throws IOException
   * @return - true if the specified block was detached
   */
  public boolean detachBlock(int namespaceId, Block block, int numLinks) throws IOException {
    DatanodeBlockInfo info = null;

    lock.readLock().lock();
    try {
      info = volumeMap.get(namespaceId, block);
    } finally {
      lock.readLock().unlock();
    }
    
    return info.detachBlock(namespaceId, block, numLinks);
  }

  /** {@inheritDoc} */
  public void updateBlock(int namespaceId, Block oldblock, Block newblock) throws IOException {
    if (oldblock.getBlockId() != newblock.getBlockId()) {
      throw new IOException("Cannot update oldblock (=" + oldblock
          + ") to newblock (=" + newblock + ").");
    }

    // Protect against a straggler updateblock call moving a block backwards
    // in time.
    boolean isValidUpdate =
      (newblock.getGenerationStamp() > oldblock.getGenerationStamp()) ||
      (newblock.getGenerationStamp() == oldblock.getGenerationStamp() &&
       newblock.getNumBytes() == oldblock.getNumBytes());

    if (!isValidUpdate) {
      throw new IOException(
        "Cannot update oldblock=" + oldblock +
        " to newblock=" + newblock + " since generation stamps must " +
        "increase, or else length must not change.");
    }

    for(;;) {
      final List<Thread> threads = tryUpdateBlock(namespaceId, oldblock, newblock);
      if (threads == null) {
        DataNode.LOG.info("Updated Block: namespaceid: " + namespaceId + " oldBlock: "
            + oldblock + " newBlock: " + newblock);
        return;
      }

      DataNode.LOG.info("Waiting other threads to update block: namespaceid: "
          + namespaceId + " oldBlock: " + oldblock + " newBlock: " + newblock);
      interruptAndJoinThreads(threads);
    }
  }

  /**
   * Try to interrupt all of the given threads, and join on them.
   * If interrupted, returns false, indicating some threads may
   * still be running.
   */
  private boolean interruptAndJoinThreads(List<Thread> threads) {
    // interrupt and wait for all ongoing create threads
    for(Thread t : threads) {
      t.interrupt();
    }
    for(Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        DataNode.LOG.warn("interruptOngoingCreates: t=" + t, e);
        return false;
      }
    }
    return true;
  }


  /**
   * Return a list of active writer threads for the given block.
   * @return null if there are no such threads or the file is
   * not being created
   */
  private ArrayList<Thread> getActiveThreads(int namespaceId, Block block) {
    lock.writeLock().lock();
    try {
      //check ongoing create threads
      final ActiveFile activefile = volumeMap.getOngoingCreates(namespaceId, block);
      if (activefile != null && !activefile.threads.isEmpty()) {
        //remove dead threads
        for(Iterator<Thread> i = activefile.threads.iterator(); i.hasNext(); ) {
          final Thread t = i.next();
          if (!t.isAlive()) {
            i.remove();
          }
        }
  
        //return living threads
        if (!activefile.threads.isEmpty()) {
          return new ArrayList<Thread>(activefile.threads);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
    return null;
  }
  
  /**
   * Try to update an old block to a new block.
   * If there are ongoing create threads running for the old block,
   * the threads will be returned without updating the block.
   *
   * @return ongoing create threads if there is any. Otherwise, return null.
   */
  private List<Thread> tryUpdateBlock(int namespaceId, 
      Block oldblock, Block newblock) throws IOException {
    lock.writeLock().lock();
    try {
      //check ongoing create threads
      ArrayList<Thread> activeThreads = getActiveThreads(namespaceId, oldblock);
      if (activeThreads != null) {
        return activeThreads;
      }

      if (volumeMap.get(namespaceId, oldblock) == null) {
        throw new IOException("Block " + oldblock
            + " doesn't exist or has been recovered to a new generation ");
      }

      //No ongoing create threads is alive.  Update block.
      File blockFile = findBlockFile(namespaceId, oldblock.getBlockId());
      if (blockFile == null) {
        throw new IOException("Block " + oldblock + " does not exist.");
      }
  
      File oldMetaFile = findMetaFile(blockFile);
      long oldgs = parseGenerationStamp(blockFile, oldMetaFile);
      
    // First validate the update

      //update generation stamp
      if (oldgs > newblock.getGenerationStamp()) {
        throw new IOException("Cannot update block (id=" + newblock.getBlockId()
            + ") generation stamp from " + oldgs
            + " to " + newblock.getGenerationStamp());
      }
      
      //update length
      if (newblock.getNumBytes() > oldblock.getNumBytes()) {
        throw new IOException("Cannot update block file (=" + blockFile
            + ") length from " + oldblock.getNumBytes() + " to " + newblock.getNumBytes());
      }

      // Although we've waited for the active threads all dead before updating
      // the map so there should be no data race there, we still create new
      // ActiveFile object to make sure in case another thread holds it,
      // it won't cause any problem for us.
      //
      try {
        volumeMap.copyOngoingCreates(namespaceId, oldblock);
      } catch (CloneNotSupportedException e) {
        // It should never happen.
        throw new IOException("Cannot clone ActiveFile object", e);
      }

      // Now perform the update

      // rename meta file to a tmp file
      File tmpMetaFile = new File(oldMetaFile.getParent(),
          oldMetaFile.getName() + "_tmp" + newblock.getGenerationStamp());
      if (!oldMetaFile.renameTo(tmpMetaFile)) {
        throw new IOException("Cannot rename block meta file to " + tmpMetaFile);
      }

      long oldFileLength = blockFile.length();
      if (newblock.getNumBytes() < oldFileLength) {
        truncateBlock(blockFile, tmpMetaFile, oldFileLength,
            newblock.getNumBytes());
      ActiveFile file = volumeMap.getOngoingCreates(namespaceId, oldblock);
      if (file != null) {
        file.setBytesAcked(newblock.getNumBytes());
        file.setBytesOnDisk(newblock.getNumBytes());
      } else {
        // This should never happen unless called from unit tests.
        this.getDatanodeBlockInfo(namespaceId, oldblock).syncInMemorySize();
      }
      }
  
      //rename the tmp file to the new meta file (with new generation stamp)
      File newMetaFile = getMetaFile(blockFile, newblock);
      if (!tmpMetaFile.renameTo(newMetaFile)) {
        throw new IOException("Cannot rename tmp meta file to " + newMetaFile);
      }
  
      if(volumeMap.getOngoingCreates(namespaceId, oldblock) != null){
        ActiveFile af = volumeMap.removeOngoingCreates(namespaceId, oldblock);
        volumeMap.addOngoingCreates(namespaceId, newblock, af);
      }
      volumeMap.update(namespaceId, oldblock, newblock);
  
      // paranoia! verify that the contents of the stored block 
      // matches the block file on disk.
      validateBlockMetadata(namespaceId, newblock);
      return null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  static void truncateBlock(File blockFile, File metaFile,
      long oldBlockFileLen, long newlen) throws IOException {
    if (newlen == oldBlockFileLen) {
      return;
    }
    if (newlen > oldBlockFileLen) {
      throw new IOException("Cannot truncate block to from oldlen (="
          + oldBlockFileLen + ") to newlen (=" + newlen + ")");
    }

    if (newlen == 0) {
      // Special case for truncating to 0 length, since there's no previous
      // chunk.
      RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
      try {
        //truncate blockFile
        blockRAF.setLength(newlen);
      } finally {
        blockRAF.close();
      }
      //update metaFile
      RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
      try {
        metaRAF.setLength(BlockMetadataHeader.getHeaderSize());
      } finally {
        metaRAF.close();
      }
      return;
    }
    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long newChunkCount = (newlen - 1)/bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + newChunkCount*checksumsize;
    long lastchunkoffset = (newChunkCount - 1)*bpc;
    int lastchunksize = (int)(newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile
      blockRAF.setLength(newlen);

      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  private final static String DISK_ERROR = "Possible disk error on file creation: ";
  /** Get the cause of an I/O exception if caused by a possible disk error
   * @param ioe an I/O exception
   * @return cause if the I/O exception is caused by a possible disk error;
   *         null otherwise.
   */
  static IOException getCauseIfDiskError(IOException ioe) {
    if (ioe.getMessage()!=null && ioe.getMessage().startsWith(DISK_ERROR)) {
      return (IOException)ioe.getCause();
    } else {
      return null;
    }
  }

  /**
   * Start writing to a block file
   * If isRecovery is true and the block pre-exists, then we kill all
      volumeMap.put(b, v);
      volumeMap.put(b, v);
   * other threads that might be writing to this block, and then reopen the file.
   * If replicationRequest is true, then this operation is part of a block
   * replication request.
   */
  public BlockWriteStreams writeToBlock(int namespaceId, Block b, boolean isRecovery,
                           boolean replicationRequest) throws IOException {
    //
    // Make sure the block isn't a valid one - we're still creating it!
    //
    if (isValidBlock(namespaceId, b, false)) {
      if (!isRecovery) {
        throw new BlockAlreadyExistsException("Block " + b + " is valid, and cannot be written to.");
      }
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client 
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.
      detachBlock(namespaceId, b, 1);
    }
    long blockSize = b.getNumBytes();

    //
    // Serialize access to /tmp, and check if file already there.
    //
    File f = null;
    List<Thread> threads = null;
    long expectedFileSize = ActiveFile.UNKNOWN_SIZE;
    lock.writeLock().lock();
    try {
      //
      // Is it already in the create process?
      //
      ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, b);
      if (activeFile != null) {
        f = activeFile.file;
        threads = activeFile.threads;
        expectedFileSize = activeFile.getBytesOnDisk();

        if (!isRecovery) {
          throw new BlockAlreadyExistsException("Block " + b +
                                  " has already been started (though not completed), and thus cannot be created.");
        } else {
          for (Thread thread:threads) {
            thread.interrupt();
          }
        }
        volumeMap.removeOngoingCreates(namespaceId, b);
      }
      FSVolume v = null;
      if (!isRecovery) {
        v = volumes.getNextVolume(blockSize);
        // create temporary file to hold block in the designated volume
        f = createTmpFile(namespaceId, v, b, replicationRequest);
      } else if (f != null) {
        DataNode.LOG.info("Reopen already-open Block for append " + b);
        // create or reuse temporary file to hold block in the designated volume
        v = volumeMap.get(namespaceId, b).getVolume();
        volumeMap.add(namespaceId, b, new DatanodeBlockInfo(v, f,
            DatanodeBlockInfo.UNFINALIZED));
      } else {
        // reopening block for appending to it.
        DataNode.LOG.info("Reopen Block for append " + b);
        v = volumeMap.get(namespaceId, b).getVolume();
        f = createTmpFile(namespaceId, v, b, replicationRequest);
        File blkfile = getBlockFile(namespaceId, b);
        File oldmeta = getMetaFile(namespaceId, b);
        File newmeta = getMetaFile(f, b);

        // rename meta file to tmp directory
        DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
        if (!oldmeta.renameTo(newmeta)) {
          throw new IOException("Block " + b + " reopen failed. " +
                                " Unable to move meta file  " + oldmeta +
                                " to tmp dir " + newmeta);
        }

        // rename block file to tmp directory
        DataNode.LOG.debug("Renaming " + blkfile + " to " + f);
        if (!blkfile.renameTo(f)) {
          if (!f.delete()) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to remove file " + f);
          }
          if (!blkfile.renameTo(f)) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to move block file " + blkfile +
                                  " to tmp dir " + f);
          }
        }
      }
      if (f == null) {
        DataNode.LOG.warn("Block " + b + " reopen failed " +
                          " Unable to locate tmp file.");
        throw new IOException("Block " + b + " reopen failed " +
                              " Unable to locate tmp file.");
      }
      // If this is a replication request, then this is not a permanent
      // block yet, it could get removed if the datanode restarts. If this
      // is a write or append request, then it is a valid block.
      if (replicationRequest) {
        volumeMap.add(namespaceId, b, new DatanodeBlockInfo(v));
      } else {
        volumeMap.add(namespaceId, b, new DatanodeBlockInfo(v, f, -1));
      }
      volumeMap.addOngoingCreates(namespaceId, b, new ActiveFile(f, threads,
          expectedFileSize));
      
    } finally {
      lock.writeLock().unlock();
    }

    try {
      if (threads != null) {
        for (Thread thread:threads) {
          thread.join();
        }
      }
    } catch (InterruptedException e) {
      throw new IOException("Recovery waiting for thread interrupted.");
    }

    //
    // Finally, allow a writer to the block file
    // REMIND - mjc - make this a filter stream that enforces a max
    // block size, so clients can't go crazy
    //
    File metafile = getMetaFile(f, b);
    DataNode.LOG.debug("writeTo blockfile is " + f + " of size " + f.length());
    DataNode.LOG.debug("writeTo metafile is " + metafile + " of size " + metafile.length());
    return createBlockWriteStreams( f , metafile);
  }

  /**
   * Retrieves the offset in the block to which the
   * the next write will write data to.
   */
  public long getChannelPosition(int namespaceId, Block b, BlockWriteStreams streams) 
                                 throws IOException {
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    return file.getChannel().position();
  }

  /**
   * Sets the offset in the block to which the
   * the next write will write data to.
   */
  public void setChannelPosition(int namespaceId, Block b, BlockWriteStreams streams, 
                                 long dataOffset, long ckOffset) 
                                 throws IOException {
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    if (file.getChannel().size() < dataOffset) {
      String  msg = "Trying to change block file offset of block " + b +
                    " file " + volumeMap.get(namespaceId, b).getVolume().getTmpFile(namespaceId, b) +
                    " to " + dataOffset +
                    " but actual size of file is " +
                    file.getChannel().size();
      throw new IOException(msg);
    }
    if (dataOffset > file.getChannel().size()) {
      throw new IOException("Set position over the end of the data file.");
    }
    file.getChannel().position(dataOffset);
    file = (FileOutputStream) streams.checksumOut;
    if (ckOffset > file.getChannel().size()) {
      throw new IOException("Set position over the end of the checksum file.");
    }
    file.getChannel().position(ckOffset);
  }

  File createTmpFile(int namespaceId, FSVolume vol, Block blk,
                        boolean replicationRequest) throws IOException {
    lock.writeLock().lock();
    try {
      if ( vol == null ) {
        vol = volumeMap.get(namespaceId, blk).getVolume();
        if ( vol == null ) {
          throw new IOException("Could not find volume for block " + blk);
        }
      }
      return vol.createTmpFile(namespaceId, blk, replicationRequest);
    } finally {
      lock.writeLock().unlock();
    }
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  @Override // FSDatasetInterface
  public void finalizeBlock(int namespaceId, Block b) throws IOException {
    finalizeBlockInternal(namespaceId, b, true);
  }

  @Override
  public void finalizeBlockIfNeeded(int namespaceId, Block b) throws IOException {
    finalizeBlockInternal(namespaceId, b, true);
  }
  
  /**
   * Complete the block write!
   */
  public void finalizeBlockInternal(int namespaceId, Block b, boolean reFinalizeOk)
    throws IOException {
    lock.writeLock().lock();
    DatanodeBlockInfo replicaInfo = volumeMap.get(namespaceId, b);
    try {
      ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, b);
      if (activeFile == null) {
        if (reFinalizeOk) {
          return;
        } else {
          throw new IOException("Block " + b + " is already finalized.");
        }
      }
      File f = activeFile.file;
      if (f == null || !f.exists()) {
        throw new IOException("No temporary file " + f + " for block " + b);
      }
      FSVolume v = replicaInfo.getVolume();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f + 
                              " for block " + b);
      }
          
      File dest = null;
      dest = v.addBlock(namespaceId, b, f);
      volumeMap.add(namespaceId, b,
          new DatanodeBlockInfo(v, dest, activeFile.getBytesOnDisk()));
      volumeMap.removeOngoingCreates(namespaceId, b);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean isBlockFinalizedInternal(int namespaceId, Block b,
      boolean validate) {
    DatanodeBlockInfo blockInfo = volumeMap.get(namespaceId, b);
    
    // We skip the check for validate case to avoid redundant codes
    // but keep old codes' behavior. Though it looks like a bug, but we
    // would fix it in a separate patch.
    // 
    if (!validate && blockInfo == null) {
      return false; // block is not finalized
    }
    FSVolume v = blockInfo.getVolume();
    if (v == null) {
      DataNode.LOG.warn("No volume for block " + b);
      return false; // block is not finalized
    }
    ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, b);
    if (activeFile != null) {
      if (validate) {
        File f = activeFile.file;
        if (f == null || !f.exists()) {
          // we should never get into this position.
          DataNode.LOG.warn("No temporary file " + f + " for block " + b);
        }
      }
      return false; // block is not finalized
    }
    return true; // block is finalized
  }
  
  /**
   * is this block finalized? Returns true if the block is already
   * finalized, otherwise returns false.
   */
  public boolean isBlockFinalized(int namespaceId, Block b) {
    return isBlockFinalizedInternal(namespaceId, b, false);
  }

  /**
   * is this block finalized? Returns true if the block is already
   * finalized, otherwise returns false.
   */
  private boolean isBlockFinalizedWithLock(int namespaceId, Block b) {
    lock.readLock().lock();
    try {
      return isBlockFinalizedInternal(namespaceId, b, true);
    } finally {
      lock.readLock().unlock();
    }
  }
  
  /**
   * Remove the temporary block file (if any)
   */
  public void unfinalizeBlock(int namespaceId, Block b) throws IOException {
    lock.writeLock().lock();
    try {
      // remove the block from in-memory data structure
      ActiveFile activefile = volumeMap.removeOngoingCreates(namespaceId, b);
      if (activefile == null) {
        return;
      }
      volumeMap.remove(namespaceId, b);
      
      // delete the on-disk temp file
      if (delBlockFromDisk(activefile.file, getMetaFile(activefile.file, b), b)) {
        DataNode.LOG.warn("Block " + b + " unfinalized and removed. " );
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove a block from disk
   * @param blockFile block file
   * @param metaFile block meta file
   * @param b a block
   * @return true if on-disk files are deleted; false otherwise
   */
  private boolean delBlockFromDisk(File blockFile, File metaFile, Block b) {
    if (blockFile == null) {
      DataNode.LOG.warn("No file exists for block: " + b);
      return true;
    }

    if (!blockFile.delete()) {
      DataNode.LOG.warn("Not able to delete the block file: " + blockFile);
      return false;
    } else { // remove the meta file
      if (metaFile != null && !metaFile.delete()) {
        DataNode.LOG.warn(
            "Not able to delete the meta block file: " + metaFile);
        return false;
      }
    }
    return true;
  }
  
  /**
  * Return a table of blocks being written data
   * @throws IOException 
  */
  public Block[] getBlocksBeingWrittenReport(int namespaceId) throws IOException {
    LightWeightHashSet<Block> blockSet = new LightWeightHashSet<Block>();
    volumes.getBlocksBeingWrittenInfo(namespaceId, blockSet);
    Block blockTable[] = new Block[blockSet.size()];
    int i = 0;
      for (Iterator<Block> it = blockSet.iterator(); it.hasNext(); i++) {
    blockTable[i] = it.next();
    }
    return blockTable;
  }
  
  /**
   * Return a table of block data for given namespace
   */
  public Block[] getBlockReport(int namespaceId) {
    // getBlockReport doesn't grant the global lock as we believe it is
    // OK to get some inconsistent partial results. The inconsistent
    // information will finally be fixed by the next incremental
    LightWeightHashSet<Block> blockSet = new LightWeightHashSet<Block>();
    volumes.getBlockInfo(namespaceId, blockSet);
    Block blockTable[] = new Block[blockSet.size()];
    int i = 0;
    for (Iterator<Block> it = blockSet.iterator(); it.hasNext(); i++) {
      blockTable[i] = it.next();
    }
    return blockTable;
  }

  /**
   * Check whether the given block is a valid one.
   */
  public boolean isValidBlock(int namespaceId, Block b, boolean checkSize)
      throws IOException {
    File f = null;
    ;
    try {
      f = getValidateBlockFile(namespaceId, b, checkSize);
    } catch (IOException e) {
      DataNode.LOG.warn("Block " + b + " is not valid:", e);
    }

    return ((f != null) ? isBlockFinalizedWithLock(namespaceId, b) : false);
  }
  
  public boolean isValidVolume(File currentDir) throws IOException {
    return volumes.isValidDir(currentDir);
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File validateBlockFile(int namespaceId, Block b) throws IOException {
    return getValidateBlockFile(namespaceId, b, false);
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File getValidateBlockFile(int namespaceId, Block b, boolean checkSize)
      throws IOException {
    //Should we check for metadata file too?
    DatanodeBlockInfo blockInfo = getDatanodeBlockInfo(namespaceId, b);
    File f = null;
    if (blockInfo != null) {
      if (checkSize) {
        blockInfo.verifyFinalizedSize();
      }
      f = blockInfo.getFile();
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskError();
    }
    
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("b=" + b + ", f=" + ((f == null) ? "null"
          : f));
    }
    return null;
  }

  /** {@inheritDoc} */
  public void validateBlockMetadata(int namespaceId, Block b) throws IOException {
    DatanodeBlockInfo info;
    lock.readLock().lock();
    try {
      info = volumeMap.get(namespaceId, b);
    } finally {
      lock.readLock().unlock();
    }
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    FSVolume v = info.getVolume();
    File tmp = v.getTmpFile(namespaceId, b);
    File f = info.getFile();
    long fileSize;
    if (f == null) {
      f = tmp;
      if (f == null) {
        throw new IOException("Block " + b + " does not exist on disk.");
      }
      if (!f.exists()) {
        throw new IOException("Block " + b + 
                              " block file " + f +
                              " does not exist on disk.");
      }
      fileSize = f.length();
    } else {
      if (info.isFinalized()) {
        info.verifyFinalizedSize();
        fileSize = info.getFinalizedSize();
      } else {
        fileSize = f.length();
      }
    }
    if (b.getNumBytes() > fileSize) {
      throw new IOException("Block " + b + 
                            " length is " + b.getNumBytes()  +
                            " does not match block file length " +
                            f.length());
    }
    File meta = getMetaFile(f, b);
    if (meta == null) {
      throw new IOException("Block " + b + 
                            " metafile does not exist.");
    }
    if (!meta.exists()) {
      throw new IOException("Block " + b + 
                            " metafile " + meta +
                            " does not exist on disk.");
    }
    long metaFileSize = meta.length();
    if (metaFileSize == 0 && fileSize > 0) {
      throw new IOException("Block " + b + " metafile " + meta + " is empty.");
    }
    long stamp = parseGenerationStamp(f, meta);
    if (stamp != b.getGenerationStamp()) {
      throw new IOException("Block " + b + 
                            " genstamp is " + b.getGenerationStamp()  +
                            " does not match meta file stamp " +
                            stamp);
    }
    if (metaFileSize == 0) {
      // no need to check metadata size for 0 size file
      return;
    }
    // verify that checksum file has an integral number of checkum values.
    DataChecksum dcs = BlockMetadataHeader.readHeader(meta).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    long actual = metaFileSize - BlockMetadataHeader.getHeaderSize();
    long numChunksInMeta = actual/checksumsize;
    if (actual % checksumsize != 0) {
      throw new IOException("Block " + b +
                            " has a checksum file of size " + metaFileSize +
                            " but it does not align with checksum size of " +
                            checksumsize);
    }
    int bpc = dcs.getBytesPerChecksum();
    long minDataSize = (numChunksInMeta - 1) * bpc;
    long maxDataSize = numChunksInMeta * bpc;
    if (fileSize > maxDataSize || fileSize <= minDataSize) {
      throw new IOException("Block " + b +
                            " is of size " + f.length() +
                            " but has " + (numChunksInMeta + 1) +
                            " checksums and each checksum size is " +
                            checksumsize + " bytes.");
    }
    // We could crc-check the entire block here, but it will be a costly 
    // operation. Instead we rely on the above check (file length mismatch)
    // to detect corrupt blocks.
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  public void invalidate(int namespaceId, Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      lock.writeLock().lock();
      try {
        DatanodeBlockInfo dinfo = volumeMap.get(namespaceId, invalidBlks[i]);
        if (dinfo == null) {
          // It is possible that after block reports, Datanodes receive
          // duplicate invalidate requests from name-node. We just skip
          // the block. In the end of the function, we don't throw an exception,
          // since no need for a disk check.
          //
          DataNode.LOG.info("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          continue;
        }
        f = dinfo.getFile();
        v = dinfo.getVolume();
        if (f == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Block not found in blockMap." +
                            ((v == null) ? " " : " Block found in volumeMap."));
          error = true;
          continue;
        }
        if (v == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". No volume for this block." +
                            " Block found in blockMap. " + f + ".");
          error = true;
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Parent not found for file " + f + ".");
          error = true;
          continue;
        }
        //TODO ???
        v.clearPath(namespaceId, parent);
        volumeMap.remove(namespaceId, invalidBlks[i]);
      } finally {
        lock.writeLock().unlock();
      }
      File metaFile = getMetaFile( f, invalidBlks[i]);

      //rename the files to be deleted
      //for safety we add prefix instead of suffix,
      //so the valid block files still start with "blk_"
      File blockFileRenamed = new File(f.getParent()
		  + File.separator + DELETE_FILE_EXT + f.getName());
      File metaFileRenamed = new File(metaFile.getParent()
		  + File.separator + DELETE_FILE_EXT + metaFile.getName());

      if((!f.renameTo(blockFileRenamed)) ||
		  (!metaFile.renameTo(metaFileRenamed))) {
	  DataNode.LOG.warn("Unexpected error trying to delete block "
                  + invalidBlks[i] +
                  ". Cannot rename files for deletion.");
	  error = true;
	  continue;
      }
      if(invalidBlks[i].getNumBytes() != BlockFlags.NO_ACK){
        datanode.notifyNamenodeDeletedBlock(namespaceId, invalidBlks[i]);
      }
      // Delete the block asynchronously to make sure we can do it fast enough
      asyncDiskService.deleteAsync(v, blockFileRenamed, metaFileRenamed,
		  invalidBlks[i].toString(), namespaceId);
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }

  /**
   * Turn the block identifier into a filename.
   */
  public File getFile(int namespaceId, Block b) {
    lock.readLock().lock();
    try {
      DatanodeBlockInfo info = volumeMap.get(namespaceId, b);
      if (info != null) {
        return info.getFile();
      }
      return null;
    } finally {
      lock.readLock().unlock();
    }
  }

  public DatanodeBlockInfo getDatanodeBlockInfo(int namespaceId, Block b) {
    return volumeMap.get(namespaceId, b);
  }

  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  public void checkDataDir() throws DiskErrorException {
    long total_blocks=0, removed_blocks=0;
    List<FSVolume> failed_vols = null;

    lock.readLock().lock();
    try {
      failed_vols = volumes.checkDirs();
    } finally {
      lock.readLock().unlock();
    }

    //if there no failed volumes return
    if(failed_vols == null) 
      return;
    
    // else 
    // remove related blocks
    long mlsec = System.currentTimeMillis();
    lock.writeLock().lock();
    try {
      volumeMap.removeUnhealthyVolumes(failed_vols);
    } finally {
      lock.writeLock().unlock();
    }
    mlsec = System.currentTimeMillis() - mlsec;
    DataNode.LOG.warn(">>>>>>>>>>>>Removed " + removed_blocks + " out of " + total_blocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for(FSVolume fv : failed_vols) {
      sb.append(fv.toString() + ";");
    }

    throw  new DiskErrorException("DataNode failed volumes:" + sb);
  
  }
  
  public void addVolumes(Configuration conf, int namespaceId, String nsDir,
      Collection<StorageDirectory> dirs) throws Exception {
    if (dirs == null || dirs.isEmpty()) {
      return;
    }
    FSVolume[] volArray = new FSVolume[dirs.size()];
    File[] dirArray = new File[dirs.size()];
    int idx = 0;
    for (Iterator<StorageDirectory> iter = dirs.iterator() ; iter.hasNext(); idx++) {
      dirArray[idx] = iter.next().getCurrentDir();
      volArray[idx] = new FSVolume(this, dirArray[idx], conf);
    }

    lock.writeLock().lock();
    try {
      volumes.addVolumes(volArray);
      for (FSVolume vol : volArray) {
        vol.addNamespace(namespaceId, nsDir, conf, datanode.isSupportAppends());
      }
    } finally {
      lock.writeLock().unlock();
    }

    asyncDiskService.insertDisk(dirArray, conf);
  }


  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  ObjectName mbeanName;
  ObjectName versionBeanName;
  Random rand = new Random();
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in
    // package naming for mbeans and their impl.
    StandardMBean bean;
    String storageName;
    if (storageId == null || storageId.equals("")) {// Temp fix for the uninitialized storage
      storageName = "UndefinedStorageId" + rand.nextInt();
    } else {
      storageName = storageId;
    }
    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean("DataNode", "FSDatasetState-" + storageName, bean);
      versionBeanName = VersionInfo.registerJMX("DataNode");
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }

    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
    if (versionBeanName != null) {
      MBeanUtil.unregisterMBean(versionBeanName);
    }
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }

    if(volumes != null) {
      lock.writeLock().lock();
      try {
        if (volumes.scannersExecutor != null) {
          volumes.scannersExecutor.shutdown();
        }

        for (FSVolume volume : volumes.getVolumes()) {
          if(volume != null) {
            volume.shutdown();
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }
  
  public void addNamespace(int namespaceId, String nsDir,
      Configuration conf) throws IOException {
    DataNode.LOG.info("Adding namespace " + namespaceId);
    lock.writeLock().lock();
    try{
      volumeMap.initNamespace(namespaceId);
      volumes.addNamespace(namespaceId, nsDir, conf);
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  public void removeNamespace(int namespaceId){
    DataNode.LOG.info("Removing namespace " + namespaceId);
    lock.writeLock().lock();
    try{
      if (volumeMap != null) {
        volumeMap.removeNamespace(namespaceId);
      }
      if (volumes != null) {
        volumes.removeNamespace(namespaceId);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String getStorageInfo() {
    return toString();
  }

  @Override
  public BlockRecoveryInfo startBlockRecovery(int namespaceId, long blockId)
      throws IOException {
    Block stored = getStoredBlock(namespaceId, blockId, true);

    if (stored == null) {
      return null;
    }

    // It's important that this loop not be synchronized - otherwise
    // this will deadlock against the thread it's joining against!
    while (true) {
      DataNode.LOG.debug(
          "Interrupting active writer threads for block " + stored);
      List<Thread> activeThreads = getActiveThreads(namespaceId, stored);
      if (activeThreads == null) break;
      if (interruptAndJoinThreads(activeThreads))
        break;
    }

    lock.readLock().lock();
    try {
      // now that writers are stopped, re-fetch the block's meta info
      stored = getStoredBlock(namespaceId, blockId, true);

      if (stored == null) {
        return null;
      }

      ActiveFile activeFile = volumeMap.getOngoingCreates(namespaceId, stored);
      boolean isRecovery = (activeFile != null) && activeFile.wasRecoveredOnStartup;


      BlockRecoveryInfo info = new BlockRecoveryInfo(stored, isRecovery);
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("getBlockMetaDataInfo successful block=" + stored +
                  " length " + stored.getNumBytes() +
                  " genstamp " + stored.getGenerationStamp());
      }

      // paranoia! verify that the contents of the stored block
      // matches the block file on disk.
      validateBlockMetadata(namespaceId, stored);
      return info;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Copies a file as fast as possible. Tries to do a hardlink instead of a copy
   * if the hardlink parameter is specified.
   *
   * @param src
   *          the source file for copying
   * @param dst
   *          the destination file for copying
   * @param hardlink
   *          whether or not to attempt a hardlink
   * @throws IOException
   */
  public void copyFile(File src, File dst, boolean hardlink) throws IOException {

    if (src == null || dst == null) {
      throw new IOException("src/dst file is null");
    }

    try {
      if (hardlink && shouldHardLinkBlockCopy) {
        // Remove destination before hard linking, since this file might already
        // exist and a hardlink would fail as a result.
        if (dst.exists()) {
          if(!dst.delete()) {
            throw new IOException("Deletion of file : " + dst + " failed");
          }
        }
        HardLink.createHardLink(src, dst);
        DataNode.LOG.info("Hard Link Created from : " + src + " to " + dst);
        return;
      }
    } catch (IOException e) {
      DataNode.LOG.warn("Hard link failed from : " + src + " to " + dst
          + " continuing with regular file copy");
    }

    FileChannel input = null;
    FileChannel output = null;
    try {
      // This improves copying performance a lot, it uses native buffers
      // for copying.
      input = new FileInputStream(src).getChannel();
      output = new FileOutputStream(dst).getChannel();
      if (input == null || output == null)  {
        throw new IOException("Could not create file channels for src : " + src
            + " dst : " + dst);
      }
      long bytesLeft = input.size();
      long position = 0;
      while (bytesLeft > 0) {
        long bytesWritten = output.transferFrom(input, position, bytesLeft);
        bytesLeft -= bytesWritten;
        position += bytesWritten;
      }
      if (datanode.syncOnClose) {
        output.force(true);
      }
    } finally {
      if (input != null) {
        input.close();
      }
      if (output != null) {
        output.close();
      }
    }
  }

  /**
   * Adds a file to the ongoingCreates datastructure to indicate we are creating
   * a file.
   * 
   * @param dstNamespaceId
   *          namespace id for dstBlock
   * @param dstBlock
   *          the block that we are going to create
   * @param dstVol
   *          the volume on which the file is to be created.
   * @return the temporary file for the block
   * @throws IOException
   */
  private File addToOngoingCreates(int dstNamespaceId, Block dstBlock,
      FSVolume dstVol) throws IOException {
    List<Thread> threads = null;
    // We do not want to create a BBW, hence treat this as a replication
    // request.
    File dstBlockFile = createTmpFile(dstNamespaceId, dstVol, dstBlock, true);
    volumeMap.addOngoingCreates(dstNamespaceId, dstBlock,
        new ActiveFile(dstBlockFile, threads));
    return dstBlockFile;
  }

  /**
   * Find a volume on the datanode for the destination block to be placed on.
   * It tries to place the destination block on the same volume as the source
   * block since hardlinks can be performed only between two files on the same
   * disk
   * 
   * @param srcFileSystem
   *          the file system for srcBlockFile
   * @param srcNamespaceId
   *          the namespace id for srcBlock
   * @param srcBlock
   *          the source block which needs to be hardlinked
   * @param srcBlockFile
   *          the block file for srcBlock
   * @return the FSVolume on which we should put the dstBlock, null if we can't
   *         find such a volume.
   * @throws IOException
   */
  private FSVolume findVolumeForHardLink(String srcFileSystem,
      int srcNamespaceId, Block srcBlock, File srcBlockFile)
    throws IOException {
    FSVolume dstVol = null;
    if (srcBlockFile == null || !srcBlockFile.exists()) {
      throw new IOException("File " + srcBlockFile
          + " is not valid or does not have"
          + " a valid block file");
    }

    // The source file might not necessarily be a part of the FSVolumeSet of
    // this datanode, it could be part of a FSVolumeSet of another datanode on
    // the same host.
    DatanodeBlockInfo blockInfo = volumeMap.get(srcNamespaceId, srcBlock);
    if (blockInfo != null) {
      dstVol = blockInfo.getVolume();
    } else {
      for(FSVolume volume : volumes.getVolumes()) {
        String volFileSystem = volume.getFileSystem();
        if (volFileSystem.equals(srcFileSystem)) {
          dstVol = volume;
          break;
        }
      }
    }
    return dstVol;
  }

  /**
   * Finds a volume for the dstBlock and adds the new block to the FSDataset
   * data structures to indicate we are going to start writing to the block.
   *
   * @param srcFileSystem
   *          the file system for srcBlockFile
   * @param srcBlockFile
   *          the block file for the srcBlock
   * @param srcNamespaceId
   *          the namespace id for source block
   * @param srcBlock
   *          the source block that needs to be copied over
   * @param dstNamespaceId
   *          the namespace id for destination block
   * @param dstBlock
   *          the new destination block that needs to be created for copying
   * @return returns whether or not a hardlink is possible, if hardlink was not
   *         requested this is always false.
   * @throws IOException
   */
  private boolean copyBlockLocalAdd(String srcFileSystem, File srcBlockFile,
      int srcNamespaceId, Block srcBlock, int dstNamespaceId, Block dstBlock)
      throws IOException {
    boolean hardlink = true;
    File dstBlockFile = null;
    lock.writeLock().lock();
    try {
      if (isValidBlock(dstNamespaceId, dstBlock, false) ||
          volumeMap.getOngoingCreates(dstNamespaceId, dstBlock) != null) {
        throw new BlockAlreadyExistsException("Block " + dstBlock
            + " already exists");
      }

      if (srcBlockFile == null || !srcBlockFile.exists()) {
        throw new IOException("Block " + srcBlock.getBlockName()
            + " is not valid or does not have a valid block file");
      }

      FSVolume dstVol = null;
      if (shouldHardLinkBlockCopy) {
        dstVol = findVolumeForHardLink(
            srcFileSystem, srcNamespaceId, srcBlock, srcBlockFile);
      }

      // Could not find a volume for a hard link, fall back to regular file
      // copy.
      if (dstVol == null) {
        dstVol = volumes.getNextVolume(srcBlock.getNumBytes());
        hardlink = false;
      }

      dstBlockFile = addToOngoingCreates(dstNamespaceId, dstBlock, dstVol);
      volumeMap.add(dstNamespaceId, dstBlock, new DatanodeBlockInfo(dstVol,
          dstBlockFile, DatanodeBlockInfo.UNFINALIZED));
    } finally {
      lock.writeLock().unlock();
    }

    if (dstBlockFile == null) {
      throw new IOException("Could not allocate block file for : " +
          dstBlock.getBlockName());
    }
    return hardlink;
  }

  /**
   * Finalize the block in FSDataset.
   * 
   * @param dstNamespaceId
   *          the namespace id for dstBlock
   * @param dstBlock
   *          the block that needs to be finalized
   * @param dstBlockFile
   *          the block file for the block that has to be finalized
   * @throws IOException
   */
  private void copyBlockLocalFinalize(int dstNamespaceId,
      Block dstBlock, File dstBlockFile)
    throws IOException {
    long blkSize = dstBlockFile.length();
    lock.writeLock().lock();
    try {
      DatanodeBlockInfo info = volumeMap.get(dstNamespaceId, dstBlock);
      if (info == null) {
        throw new IOException("Could not find information for " + dstBlock);
      }
      FSVolume dstVol = info.getVolume();
      // Finalize block on disk.
      File dest = dstVol.addBlock(dstNamespaceId, dstBlock, dstBlockFile);
      volumeMap.add(dstNamespaceId, dstBlock, new DatanodeBlockInfo(dstVol,
          dest, blkSize));
      volumeMap.removeOngoingCreates(dstNamespaceId, dstBlock);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void copyBlockLocal(String srcFileSystem, File srcBlockFile,
      int srcNamespaceId, Block srcBlock, int dstNamespaceId, Block dstBlock)
      throws IOException {
    File dstBlockFile = null;
    try {
      boolean hardlink = copyBlockLocalAdd(srcFileSystem, srcBlockFile,
          srcNamespaceId, srcBlock, dstNamespaceId, dstBlock);

      dstBlockFile = volumeMap.get(dstNamespaceId, dstBlock).getFile();
      // Create metafile.
      File metaFileSrc = getMetaFile(srcBlockFile, srcBlock);
      File metaFileDst = getMetaFile(dstBlockFile, dstBlock);

      // Copy files.
      copyFile(srcBlockFile, dstBlockFile, hardlink);
      copyFile(metaFileSrc, metaFileDst, hardlink);

      // Finalize block
      copyBlockLocalFinalize(dstNamespaceId, dstBlock, dstBlockFile);
    } catch (BlockAlreadyExistsException be) {
      throw be;
    } catch (IOException e) {
      unfinalizeBlock(dstNamespaceId, dstBlock);
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getFileSystemForBlock(int namespaceId, Block block) throws IOException {
    if (!isValidBlock(namespaceId, block, false)) {
      throw new IOException("Invalid block");
    }
    return volumeMap.get(namespaceId, block).getVolume().getFileSystem();
  }

  static File createTmpFile(Block b, File f) throws IOException {
    if (f.exists()) {
      throw new IOException("Unexpected problem in creating temporary file for "+
                            b + ".  File " + f + " should not be present, but is.");
    }
    // Create the zero-length temp file
    //
    boolean fileCreated = false;
    try {
      fileCreated = f.createNewFile();
    } catch (IOException ioe) {
      throw (IOException)new IOException(DISK_ERROR +f).initCause(ioe);
    }
    if (!fileCreated) {
      throw new IOException("Unexpected problem in creating temporary file for "+
                            b + ".  File " + f + " should be creatable, but is already present.");
    }
    return f;
  }

  @Override
  public long size(int namespaceId) {
    try {
      return volumeMap.size(namespaceId);
    } catch (Exception e) {
      return -1;
    }
  }

}

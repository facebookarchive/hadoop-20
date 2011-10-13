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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.*;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
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


  /** Find the metadata file for the specified block file.
   * Return the generation stamp from the name of the metafile.
   */
  static long getGenerationStampFromFile(File[] listdir, 
      File blockFile) {
    String blockName = blockFile.getName();
    for (int j = 0; j < listdir.length; j++) {
      String path = listdir[j].getName();
      if (!path.startsWith(blockName)) {
        continue;
      }
      String[] vals = path.split("_");
      if (vals.length != 3) {     // blk, blkid, genstamp.meta
        continue;
      }
      String[] str = vals[2].split("\\.");
      if (str.length != 2) {
        continue;
      }
      return Long.parseLong(str[0]);
    }
    DataNode.LOG.warn("Block " + blockFile + 
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
    FSDir children[];
    int lastChildIdx = 0;
    /**
     */
    public FSDir(File dir) throws IOException {
      this(dir, null);
    }
    	
    public FSDir() { 
    }
    
    public FSDir(File dir, FSVolume volume) 
      throws IOException {
      this.dir = dir;
      this.children = null;
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = dir.listFiles();
        int numChildren = 0;
        for (File file : files) {
          if (isPendingDeleteFilename(file)){
            // Should not cause throwing an exception.
            // Obsolete files are not included in the block report.
            asyncDiskService.deleteAsyncFile(volume, file);
          } else if (file.isDirectory()) {
            numChildren++;
          } else if (Block.isBlockFilename(file)) {
            numBlocks++;
            if (volume != null) {
              long genStamp = FSDataset.getGenerationStampFromFile(files, file);
              synchronized (volumeMap) {
                volumeMap.put(new Block(file, file.length(), genStamp), 
                    new DatanodeBlockInfo(volume, file));
              }
            }
          }
        }
        if (numChildren > 0) {
          children = new FSDir[numChildren];
          int curdir = 0;
          for (int idx = 0; idx < files.length; idx++) {
            if (files[idx].isDirectory() && !isPendingDeleteFilename(files[idx])) {
              children[curdir] = new FSDir(files[idx], volume);
              curdir++;
            }
          }
        }
      }
    }
        
    public File addBlock(Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(b, src, false, false);          
      return (file != null) ? file : addBlock(b, src, true, true);
    }

    private File addBlock(Block b, File src, boolean createOk, 
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
            
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = random.nextInt(children.length);              
      }
            
      if (lastChildIdx >= 0 && children != null) {
        //Check if any child-tree has room for a block.
        for (int i=0; i < children.length; i++) {
          int idx = (lastChildIdx + i)%children.length;
          File file = children[idx].addBlock(b, src, false, resetIdx);
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
        children = new FSDir[maxBlocksPerDir];
        for (int idx = 0; idx < maxBlocksPerDir; idx++) {
          children[idx] = new FSDir(new File(dir, DataStorage.BLOCK_SUBDIR_PREFIX+idx));
        }
      }
            
      //now pick a child randomly for creating a new set of subdirs.
      lastChildIdx = random.nextInt(children.length);
      return children[ lastChildIdx ].addBlock(b, src, true, false); 
    }

    /**
     * Populate the given blockSet with any child blocks
     * found at this node.
     */
    public void getBlockInfo(LightWeightHashSet<Block> blockSet) {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = 
            FSDataset.getGenerationStampFromFile(blockFiles, blockFiles[i]);
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
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockAndFileInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = FSDataset.getGenerationStampFromFile(blockFiles, blockFiles[i]);
          Block block = new Block(blockFiles[i], blockFiles[i].length(), genStamp);
          blockSet.add(new BlockAndFile(blockFiles[i].getAbsoluteFile(), block));
        }
      }
    }    

    /**
     * check if a data diretory is healthy
     * @throws DiskErrorException
     */
    public void checkDirTree() throws DiskErrorException {
      DiskChecker.checkDir(dir);
            
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
      return "FSDir{" +
        "dir=" + dir +
        ", children=" + (children == null ? null : Arrays.asList(children)) +
        "}";
    }
  }

  class FSVolume {
    protected File currentDir;
    protected FSDir dataDir;
    protected File tmpDir;
    protected File blocksBeingWritten;     // clients write here
    protected File detachDir; // copy on write for blocks in snapshot
    protected DF usage;
    protected DU dfsUsage;
    protected long reserved;
    
    FSVolume() { 
    }

    FSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
      this.currentDir = currentDir;
      this.dataDir = new FSDir(currentDir, this);
      boolean supportAppends = datanode.isSupportAppends();
      File parent = currentDir.getParentFile();

      this.detachDir = new File(parent, "detach");
      if (detachDir.exists()) {
        recoverDetachedBlocks(currentDir, detachDir);
      }

      // remove all blocks from "tmp" directory. These were either created
      // by pre-append clients (0.18.x) or are part of replication request.
      // They can be safely removed.
      this.tmpDir = new File(parent, "tmp");
      if (tmpDir.exists()) {
        FileUtil.fullyDelete(tmpDir);
      }
      
      // Files that were being written when the datanode was last shutdown
      // should not be deleted.
      blocksBeingWritten = new File(parent, "blocksBeingWritten");
      if (blocksBeingWritten.exists()) {
        if (supportAppends) {
          recoverBlocksBeingWritten(blocksBeingWritten);
        } else {
          // rename tmpDir to prepare delete
          File toDeleteDir = new File(tmpDir.getParent(),
          DELETE_FILE_EXT + tmpDir.getName());
          if (tmpDir.renameTo(toDeleteDir)) {
            // asyncly delete the renamed directory
            asyncDiskService.deleteAsyncFile(this, toDeleteDir);
          } else {
            // rename failed, let's synchronously delete the directory
            DataNode.LOG.warn("Fail to asynchronosly delete " +
            tmpDir.getPath() + ". Trying synchronosly deletion.");
            FileUtil.fullyDelete(tmpDir);
            DataNode.LOG.warn("Deleted " + tmpDir.getPath());
          }
        }
      }
      
      if (!blocksBeingWritten.mkdirs()) {
        if (!blocksBeingWritten.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + blocksBeingWritten.toString());
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
      this.usage = new DF(parent, conf);
      this.dfsUsage = new DU(parent, conf);
      this.dfsUsage.start();
    }


	File getCurrentDir() {
      return currentDir;
    }
    
    void decDfsUsed(long value) {
      // The caller to this method (BlockFileDeleteTask.run()) does
      // not have locked FSDataset.this yet.
      dfsUsage.decDfsUsed(value);
    }
    
    long getDfsUsed() throws IOException {
      return dfsUsage.getUsed();
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
      
    String getMount() throws IOException {
      return usage.getMount();
    }
      
    File getDir() {
      return dataDir.dir;
    }
    
    /**
     * Temporary files. They get moved to the real block directory either when
     * the block is finalized or the datanode restarts.
     */
    File createTmpFile(Block b, boolean replicationRequest) throws IOException {
      File f= null;
      if (!replicationRequest) {
        f = new File(blocksBeingWritten, b.getBlockName());
      } else {
        f = new File(tmpDir, b.getBlockName());
      }
      return createTmpFile(b, f);
    }

    /**
     * Returns the name of the temporary file for this block.
     */
    File getTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return f;
    }

    /**
     * Files used for copy-on-write. They need recovery when datanode
     * restarts.
     */
    File createDetachFile(Block b, String filename) throws IOException {
      File f = new File(detachDir, filename);
      return createTmpFile(b, f);
    }

    File createTmpFile(Block b, File f) throws IOException {
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
        DataNode.LOG.warn("createTmpFile failed for file " + f + " Block " + b);
        throw (IOException)new IOException(DISK_ERROR +f).initCause(ioe);
      }
      if (!fileCreated) {
        throw new IOException("Unexpected problem in creating temporary file for "+
                              b + ".  File " + f + " should be creatable, but is already present.");
      }
      return f;
    }
      
    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(b, f);
      File metaFile = getMetaFile( blockFile , b);
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      return blockFile;
    }
      
    void checkDirs() throws DiskErrorException {
      dataDir.checkDirTree();
      DiskChecker.checkDir(tmpDir);
      DiskChecker.checkDir(blocksBeingWritten);
    }
    
    void getBlocksBeingWrittenInfo(LightWeightHashSet<Block> blockSet) {
      if (blocksBeingWritten == null) {
        return;
      }
 
      File[] blockFiles = blocksBeingWritten.listFiles();
      if (blockFiles == null) {
        return;
      }
         
      for (int i = 0; i < blockFiles.length; i++) {
        if (!blockFiles[i].isDirectory()) {
        // get each block in the blocksBeingWritten direcotry
          if (Block.isBlockFilename(blockFiles[i])) {
            long genStamp = 
              FSDataset.getGenerationStampFromFile(blockFiles, blockFiles[i]);
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
        
    void getBlockInfo(LightWeightHashSet<Block> blockSet) {
      dataDir.getBlockInfo(blockSet);
    }
    
    void clearPath(File f) {
      dataDir.clearPath(f);
    }
      
    public String toString() {
      if (dataDir != null && dataDir.dir != null) {
        return dataDir.dir.getAbsolutePath();
      } else {
        return null;
      }
    }

    /**
     * Recover blocks that were being written when the datanode
     * was earlier shut down. These blocks get re-inserted into
     * ongoingCreates. Also, send a blockreceived message to the NN
     * for each of these blocks because these are not part of a 
     * block report.
     */
    protected void recoverBlocksBeingWritten(File bbw) throws IOException {
      FSDir fsd = new FSDir(bbw);
      LightWeightHashSet<BlockAndFile> blockSet = new LightWeightHashSet<BlockAndFile>();
      fsd.getBlockAndFileInfo(blockSet);
      for (BlockAndFile b : blockSet) {
        File f = b.pathfile;  // full path name of block file
        lock.writeLock().lock();
        try {
          volumeMap.put(b.block, new DatanodeBlockInfo(this, f));
          ongoingCreates.put(b.block, ActiveFile.createStartupRecoveryFile(f));
        } finally {
          lock.writeLock().unlock();
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("recoverBlocksBeingWritten for block " + b.block);
        }
      }
    } 
    
    /**
     * Recover detached files on datanode restart. If a detached block
     * does not exist in the original directory, then it is moved to the
     * original directory.
     */
    protected void recoverDetachedBlocks(File dataDir, File dir) 
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
  }
    
  static class FSVolumeSet {
    FSVolume[] volumes = null;
    int curVolume = 0;
    ExecutorService scannersExecutor;

    FSVolumeSet(FSVolume[] volumes, int threads) {
      this.volumes = volumes;
      if (threads > 1) {
        scannersExecutor = Executors.newFixedThreadPool(threads);
      }
    }
    
    protected int numberOfVolumes() {
      return volumes.length;
    }
      
    synchronized FSVolume getNextVolume(long blockSize) throws IOException {
      
      if(volumes.length < 1) {
        throw new DiskOutOfSpaceException("No more available volumes");
      }
      
      // since volumes could've been removed because of the failure
      // make sure we are not out of bounds
      if(curVolume >= volumes.length) {
        curVolume = 0;
      }
      
      int startVolume = curVolume;
      
      while (true) {
        FSVolume volume = volumes[curVolume];
        curVolume = (curVolume + 1) % volumes.length;
        if (volume.getAvailable() > blockSize) { return volume; }
        if (curVolume == startVolume) {
          throw new DiskOutOfSpaceException("Insufficient space for an additional block");
        }
      }
    }
      
    long getDfsUsed() throws IOException {
      long dfsUsed = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        dfsUsed += volumes[idx].getDfsUsed();
      }
      return dfsUsed;
    }

    synchronized long getCapacity() throws IOException {
      long capacity = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        capacity += volumes[idx].getCapacity();
      }
      return capacity;
    }
      
    synchronized long getRemaining() throws IOException {
      long remaining = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        remaining += volumes[idx].getAvailable();
      }
      return remaining;
    }
    
    synchronized void getBlocksBeingWrittenInfo(LightWeightHashSet<Block> blockSet) {
      long startTime = System.currentTimeMillis(); 

      for (int idx = 0; idx < volumes.length; idx++) {
        volumes[idx].getBlocksBeingWrittenInfo(blockSet);
      }
      long scanTime = (System.currentTimeMillis() - startTime)/1000;
      DataNode.LOG.info("Finished generating blocks being written report for " +
      volumes.length + " volumes in " + scanTime + " seconds");
    }
      
    synchronized void getBlockInfo(LightWeightHashSet<Block> blockSet) {
      long startTime = System.currentTimeMillis();
      if (scannersExecutor != null) {
        List<Future<LightWeightHashSet<Block>>> builders =
          new ArrayList<Future<LightWeightHashSet<Block>>>();
        for (int idx = 0; idx < volumes.length; idx++) {
          builders.add(scannersExecutor.submit(
              new BlockInfoBuilder(volumes[idx])));
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
      } else {
        for (int idx = 0; idx < volumes.length; idx++) {
          volumes[idx].getBlockInfo(blockSet);
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
    synchronized List<FSVolume> checkDirs() {
      
      ArrayList<FSVolume> removed_vols = null;  
      
      for (int idx = 0; idx < volumes.length; idx++) {
        FSVolume fsv = volumes[idx];
        try {
          fsv.checkDirs();
        } catch (DiskErrorException e) {
          DataNode.LOG.warn("Removing failed volume " + fsv + ": ",e);
          if(removed_vols == null) {
            removed_vols = new ArrayList<FSVolume>(1);
          }
          removed_vols.add(volumes[idx]);
          volumes[idx] = null; //remove the volume
        }
      }
      
      // repair array - copy non null elements
      int removed_size = (removed_vols==null)? 0 : removed_vols.size();
      if(removed_size > 0) {
        FSVolume fsvs[] = new FSVolume [volumes.length-removed_size];
        for(int idx=0,idy=0; idx<volumes.length; idx++) {
          if(volumes[idx] != null) {
            fsvs[idy] = volumes[idx];
            idy++;
          }
        }
        volumes = fsvs; // replace array of volumes
      }
      DataNode.LOG.info("Completed FSVolumeSet.checkDirs. Removed=" + removed_size + 
          "volumes. List of current volumes: " +   toString());
      
      return removed_vols;
    }
      
    public String toString() {
      StringBuffer sb = new StringBuffer();
      for (int idx = 0; idx < volumes.length; idx++) {
        sb.append(volumes[idx].toString());
        if (idx != volumes.length - 1) { sb.append(","); }
      }
      return sb.toString();
    }
  }
  
  private static class BlockInfoBuilder implements Callable<LightWeightHashSet<Block>> {
    FSVolume volume;

    public BlockInfoBuilder(FSVolume volume) {
      this.volume = volume;
    }

    @Override
    public LightWeightHashSet<Block> call() throws Exception {
      LightWeightHashSet<Block> result = new LightWeightHashSet<Block>();
      volume.getBlockInfo(result);
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

  static class ActiveFile {
    final File file;
    final List<Thread> threads = new ArrayList<Thread>(2);
    private volatile long visibleLength;
    /**
     * Set to true if this file was recovered during datanode startup.
     * This may indicate that the file has been truncated (eg during
     * underlying filesystem journal replay)
     */
    final boolean wasRecoveredOnStartup;

    ActiveFile(File f, List<Thread> list) {
      this(f, false);
      if (list != null) {
        threads.addAll(list);
      }
      threads.add(Thread.currentThread());
    }

    /**
     * Create an ActiveFile from a file on disk during DataNode startup.
     * This factory method is just to make it clear when the purpose
     * of this constructor is.
     */
    public static ActiveFile createStartupRecoveryFile(File f) {
      return new ActiveFile(f, true);
    }

    private ActiveFile(File f, boolean recovery) {
      file = f;
      visibleLength = f.length();
      wasRecoveredOnStartup = recovery;
    }

    public long getVisibleLength() {
      return visibleLength;
    }

    public void setVisibleLength(long value) {
      visibleLength = value;
    }

    public String toString() {
      return getClass().getSimpleName() + "(file=" + file
          + ", threads=" + threads + ")";
    }
  }
  
  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + METADATA_EXTENSION;
  }
  
  static File getMetaFile(File f , Block b) {
    return new File(getMetaFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }
  protected File getMetaFile(Block b) throws IOException {
    return getMetaFile(getBlockFile(b), b);
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
   */
  static boolean isPendingDeleteFilename(File f) {
    String name = f.getName();
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
  public File findBlockFile(long blockId) {
    lock.readLock().lock();
    try {
      final Block b = new Block(blockId);
      File blockfile = null;
      ActiveFile activefile = ongoingCreates.get(b);
      if (activefile != null) {
        blockfile = activefile.file;
      }
      if (blockfile == null) {
        blockfile = getFile(b);
      }
      if (blockfile == null) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("ongoingCreates=" + ongoingCreates);
          DataNode.LOG.debug("volumeMap=" + volumeMap);
        }
      }
      return blockfile;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** {@inheritDoc} */
  public Block getStoredBlock(long blkid) throws IOException {
    lock.readLock().lock();
    try {
      File blockfile = findBlockFile(blkid);
      if (blockfile == null) {
        return null;
      }
      File metafile = findMetaFile(blockfile, true);
      if (metafile == null) {
        return null;
      }
      Block block = new Block(blkid);
      return new Block(blkid, getVisibleLength(block),
          parseGenerationStamp(blockfile, metafile));
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean metaFileExists(Block b) throws IOException {
    return getMetaFile(b).exists();
  }
  
  public long getMetaDataLength(Block b) throws IOException {
    File checksumFile = getMetaFile( b );
    return checksumFile.length();
  }

  public MetaDataInputStream getMetaDataInputStream(Block b)
      throws IOException {
    File checksumFile = getMetaFile( b );
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }

  FSVolumeSet volumes;
  protected DataNode datanode;
  protected HashMap<Block,ActiveFile> ongoingCreates = new HashMap<Block,ActiveFile>();
  protected int maxBlocksPerDir = 0;
  HashMap<Block,DatanodeBlockInfo> volumeMap = new HashMap<Block, DatanodeBlockInfo>();;
  static  Random random = new Random();
  FSDatasetAsyncDiskService asyncDiskService;
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  
  //this constructor is used to create PersistedSimulatedFSDataset
  public FSDataset() { 
  }
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  public FSDataset(DataNode datanode, DataStorage storage, Configuration conf)
      throws IOException {
    this.datanode = datanode;
    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    volumeMap = new HashMap<Block, DatanodeBlockInfo>();
    File[] roots = new File[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      roots[idx] = storage.getStorageDir(idx).getCurrentDir();
    }
    asyncDiskService = new FSDatasetAsyncDiskService(roots, conf);
    FSVolume[] volArray = createVolumes(storage, conf);
    int threads = conf.getInt("dfs.datanode.blockscanner.threads", 1);
    volumes = new FSVolumeSet(volArray, threads);
    registerMBean(storage.getStorageID());
  }

  private class VolumeThread extends Thread {
    private File volumeDir;
    private Configuration conf;
    private FSVolume volume;
    private boolean hasError = false;
    private VolumeThread(File volumeDir, Configuration conf) {
      this.volumeDir = volumeDir;
      this.conf = conf;
    }
    public void run() {
      DataNode.LOG.info("Start building volume for " + volumeDir);
      try {
        volume = new FSVolume(volumeDir, conf);
      } catch (IOException ioe) {
        DataNode.LOG.error("Error building volume at " + volumeDir, ioe);
        hasError = true;
      }
      DataNode.LOG.info("Finish building volume for " + volumeDir);
    }
  }
  
  private FSVolume[] createVolumes(DataStorage storage, Configuration conf)
    throws IOException {
    VolumeThread[] scanners = new VolumeThread[storage.getNumStorageDirs()];
    for (int idx = 0; idx < scanners.length; idx++) {
      scanners[idx] = new VolumeThread(
          storage.getStorageDir(idx).getCurrentDir(), conf);
      scanners[idx].start();
    }
    boolean hasError = false;
    FSVolume[] volumes = new FSVolume[scanners.length];
    for (int idx = 0; idx < scanners.length; idx++ ) {
      try {
        scanners[idx].join();
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      if (!hasError && scanners[idx].hasError) {
        hasError = true;
      } 
      if ( !hasError) {
        volumes[idx] = scanners[idx].volume;
      }
    }
    if (hasError) {
      throw new IOException("Error creating volumes");
    }
    return volumes;
  }


  /**
   * Return the total space used by dfs datanode
   */
  public long getDfsUsed() throws IOException {
    return volumes.getDfsUsed();
  }
  /**
   * Return true - if there are still valid volumes 
   * on the DataNode
   */
  public boolean hasEnoughResource(){
    return volumes.numberOfVolumes() >= MIN_NUM_OF_VALID_VOLUMES;
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
  public long getLength(Block b) throws IOException {
    return getBlockFile(b).length();
  }

  @Override
  public long getVisibleLength(Block b) throws IOException {
    lock.readLock().lock();
    try {
      ActiveFile activeFile = ongoingCreates.get(b);

      if (activeFile != null) {
        return activeFile.getVisibleLength();
      } else {
        return getLength(b);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void setVisibleLength(Block b, long length)
    throws IOException {
    lock.writeLock().lock();
    try {
      ActiveFile activeFile = ongoingCreates.get(b);

      if (activeFile != null) {
        activeFile.setVisibleLength(length);
      } else {
        throw new IOException(
          String.format("block %s is not being written to", b)
        );
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get File name for a given block.
   */
  public File getBlockFile(Block b) throws IOException {
    File f = validateBlockFile(b);
    if (f == null) {
      if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
        InterDatanodeProtocol.LOG
            .debug("b=" + b + ", volumeMap=" + volumeMap);
      }
      throw new IOException("Block " + b + " is not valid.");
    }
    return f;
  }
  
  public InputStream getBlockInputStream(Block b) throws IOException {
    return new FileInputStream(getBlockFile(b));
  }

  public InputStream getBlockInputStream(Block b, long seekOffset) throws IOException {
    File blockFile = getBlockFile(b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (seekOffset > 0) {
      blockInFile.seek(seekOffset);
    }
    return new FileInputStream(blockInFile.getFD());
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  public BlockInputStreams getTmpInputStreams(Block b, 
                          long blkOffset, long ckoff) throws IOException {
    lock.readLock().lock();
    try {
      DatanodeBlockInfo info = volumeMap.get(b);
      if (info == null) {
        throw new IOException("Block " + b + " does not exist in volumeMap.");
      }
      FSVolume v = info.getVolume();
      File blockFile = info.getFile();
      if (blockFile == null) {
        blockFile = v.getTmpFile(b);
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
  public boolean detachBlock(Block block, int numLinks) throws IOException {
    DatanodeBlockInfo info = null;

    lock.readLock().lock();
    try {
      info = volumeMap.get(block);
    } finally {
      lock.readLock().unlock();
    }
    
    return info.detachBlock(block, numLinks);
  }

  static protected <T> void updateBlockMap(Map<Block, T> blockmap,
      Block oldblock, Block newblock) throws IOException {
    if (blockmap.containsKey(oldblock)) {
      T value = blockmap.remove(oldblock);
      blockmap.put(newblock, value);
    }
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock) throws IOException {
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
      final List<Thread> threads = tryUpdateBlock(oldblock, newblock);
      if (threads == null) {
        return;
      }

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
  private ArrayList<Thread> getActiveThreads(Block block) {
    lock.writeLock().lock();
    try {
      //check ongoing create threads
      final ActiveFile activefile = ongoingCreates.get(block);
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
  private List<Thread> tryUpdateBlock(
      Block oldblock, Block newblock) throws IOException {
    lock.writeLock().lock();
    try {
      //check ongoing create threads
      ArrayList<Thread> activeThreads = getActiveThreads(oldblock);
      if (activeThreads != null) {
        return activeThreads;
      }

      //No ongoing create threads is alive.  Update block.
      File blockFile = findBlockFile(oldblock.getBlockId());
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

    // Now perform the update

    //rename meta file to a tmp file
    File tmpMetaFile = new File(oldMetaFile.getParent(),
        oldMetaFile.getName()+"_tmp" + newblock.getGenerationStamp());
    if (!oldMetaFile.renameTo(tmpMetaFile)){
      throw new IOException("Cannot rename block meta file to " + tmpMetaFile);
    }

      if (newblock.getNumBytes() < oldblock.getNumBytes()) {
        truncateBlock(blockFile, tmpMetaFile, oldblock.getNumBytes(), newblock.getNumBytes());
      ActiveFile file = ongoingCreates.get(oldblock);
      if (file != null) {
        file.setVisibleLength(newblock.getNumBytes());
      }
      }
  
      //rename the tmp file to the new meta file (with new generation stamp)
      File newMetaFile = getMetaFile(blockFile, newblock);
      if (!tmpMetaFile.renameTo(newMetaFile)) {
        throw new IOException("Cannot rename tmp meta file to " + newMetaFile);
      }
  
      updateBlockMap(ongoingCreates, oldblock, newblock);
      updateBlockMap(volumeMap, oldblock, newblock);
  
      // paranoia! verify that the contents of the stored block 
      // matches the block file on disk.
      validateBlockMetadata(newblock);
      return null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  static void truncateBlock(File blockFile, File metaFile,
      long oldlen, long newlen) throws IOException {
    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
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
  public BlockWriteStreams writeToBlock(Block b, boolean isRecovery,
                           boolean replicationRequest) throws IOException {
    //
    // Make sure the block isn't a valid one - we're still creating it!
    //
    if (isValidBlock(b)) {
      if (!isRecovery) {
        throw new BlockAlreadyExistsException("Block " + b + " is valid, and cannot be written to.");
      }
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client 
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.
      detachBlock(b, 1);
    }
    long blockSize = b.getNumBytes();

    //
    // Serialize access to /tmp, and check if file already there.
    //
    File f = null;
    List<Thread> threads = null;
    lock.writeLock().lock();
    try {
      //
      // Is it already in the create process?
      //
      ActiveFile activeFile = ongoingCreates.get(b);
      if (activeFile != null) {
        f = activeFile.file;
        threads = activeFile.threads;
        
        if (!isRecovery) {
          throw new BlockAlreadyExistsException("Block " + b +
                                  " has already been started (though not completed), and thus cannot be created.");
        } else {
          for (Thread thread:threads) {
            thread.interrupt();
          }
        }
        ongoingCreates.remove(b);
      }
      FSVolume v = null;
      if (!isRecovery) {
        v = volumes.getNextVolume(blockSize);
        // create temporary file to hold block in the designated volume
        f = createTmpFile(v, b, replicationRequest);
      } else if (f != null) {
        DataNode.LOG.info("Reopen already-open Block for append " + b);
        // create or reuse temporary file to hold block in the designated volume
        v = volumeMap.get(b).getVolume();
        volumeMap.put(b, new DatanodeBlockInfo(v, f));
      } else {
        // reopening block for appending to it.
        DataNode.LOG.info("Reopen Block for append " + b);
        v = volumeMap.get(b).getVolume();
        f = createTmpFile(v, b, replicationRequest);
        File blkfile = getBlockFile(b);
        File oldmeta = getMetaFile(b);
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
        volumeMap.put(b, new DatanodeBlockInfo(v));
      } else {
        volumeMap.put(b, new DatanodeBlockInfo(v, f));
      }
      ongoingCreates.put(b, new ActiveFile(f, threads));
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
  public long getChannelPosition(Block b, BlockWriteStreams streams) 
                                 throws IOException {
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    return file.getChannel().position();
  }

  /**
   * Sets the offset in the block to which the
   * the next write will write data to.
   */
  public void setChannelPosition(Block b, BlockWriteStreams streams, 
                                 long dataOffset, long ckOffset) 
                                 throws IOException {
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    if (file.getChannel().size() < dataOffset) {
      String msg = "Trying to change block file offset of block " + b +
                     " file " + volumeMap.get(b).getVolume().getTmpFile(b) +
                     " to " + dataOffset +
                     " but actual size of file is " +
                     file.getChannel().size();
      throw new IOException(msg);
    }
    file.getChannel().position(dataOffset);
    file = (FileOutputStream) streams.checksumOut;
    file.getChannel().position(ckOffset);
  }

  File createTmpFile( FSVolume vol, Block blk,
                        boolean replicationRequest) throws IOException {
    lock.writeLock().lock();
    try {
      if ( vol == null ) {
        vol = volumeMap.get( blk ).getVolume();
        if ( vol == null ) {
          throw new IOException("Could not find volume for block " + blk);
        }
      }
      return vol.createTmpFile(blk, replicationRequest);
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


  @Override
  public void finalizeBlock(Block b) throws IOException {
    finalizeBlockInternal(b, false);
  }

  @Override
  public void finalizeBlockIfNeeded(Block b) throws IOException {
    finalizeBlockInternal(b, true);
  }

  /**
   * Complete the block write!
   */
  private void finalizeBlockInternal(Block b, boolean reFinalizeOk)
    throws IOException {
    lock.writeLock().lock();
    try {
      ActiveFile activeFile = ongoingCreates.get(b);
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
      FSVolume v = volumeMap.get(b).getVolume();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f + 
                              " for block " + b);
      }
          
      File dest = null;
      dest = v.addBlock(b, f);
      volumeMap.put(b, new DatanodeBlockInfo(v, dest));
      ongoingCreates.remove(b);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * is this block finalized? Returns true if the block is already
   * finalized, otherwise returns false.
   */
  private boolean isFinalized(Block b) {
    lock.readLock().lock();
    try {
      FSVolume v = volumeMap.get(b).getVolume();
      if (v == null) {
        DataNode.LOG.warn("No volume for block " + b);
        return false;             // block is not finalized
      }
      ActiveFile activeFile = ongoingCreates.get(b);
      if (activeFile == null) {
        return true;            // block is already finalized
      }
      File f = activeFile.file;
      if (f == null || !f.exists()) {
        // we shud never get into this position.
        DataNode.LOG.warn("No temporary file " + f + " for block " + b);
      }
      return false;             // block is not finalized
    } finally {
      lock.readLock().unlock();
    }
  }
  
  /**
   * Remove the temporary block file (if any)
   */
  public void unfinalizeBlock(Block b) throws IOException {
    lock.writeLock().lock();
    try {
      // remove the block from in-memory data structure
      ActiveFile activefile = ongoingCreates.remove(b);
      if (activefile == null) {
        return;
      }
      volumeMap.remove(b);
      
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
  */
  public Block[] getBlocksBeingWrittenReport() {
    LightWeightHashSet<Block> blockSet = new LightWeightHashSet<Block>();
    volumes.getBlocksBeingWrittenInfo(blockSet);
    Block blockTable[] = new Block[blockSet.size()];
    int i = 0;
      for (Iterator<Block> it = blockSet.iterator(); it.hasNext(); i++) {
    blockTable[i] = it.next();
    }
    return blockTable;
  }
  
  /**
   * Return a table of block data
   */
  public Block[] getBlockReport() {
    LightWeightHashSet<Block> blockSet = new LightWeightHashSet<Block>();
    volumes.getBlockInfo(blockSet);
    Block blockTable[] = new Block[blockSet.size()];
    blockSet.pollToArray(blockTable);
    return blockTable;
  }

  /**
   * Check whether the given block is a valid one.
   */
  public boolean isValidBlock(Block b) {
    File f = null;;
    try {
      f = validateBlockFile(b);
    } catch(IOException e) {
      DataNode.LOG.warn("Block " + b + " is not valid:",e);
    }

    return ((f != null) ? isFinalized(b) : false);
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File validateBlockFile(Block b) throws IOException {
    //Should we check for metadata file too?
    File f = getFile(b);
    
    if(f != null ) {
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
      DataNode datanode = DataNode.getDataNode();
      datanode.checkDiskError();
    }
    
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("b=" + b + ", f=" + f);
    }
    return null;
  }

  /** {@inheritDoc} */
  public void validateBlockMetadata(Block b) throws IOException {
    DatanodeBlockInfo info = volumeMap.get(b);
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    FSVolume v = info.getVolume();
    File tmp = v.getTmpFile(b);
    File f = info.getFile();
    if (f == null) {
      f = tmp;
    }
    if (f == null) {
      throw new IOException("Block " + b + " does not exist on disk.");
    }
    if (!f.exists()) {
      throw new IOException("Block " + b + 
                            " block file " + f +
                            " does not exist on disk.");
    }
    if (b.getNumBytes() != f.length()) {
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
    if (meta.length() == 0) {
      throw new IOException("Block " + b + " metafile " + meta + " is empty.");
    }
    long stamp = parseGenerationStamp(f, meta);
    if (stamp != b.getGenerationStamp()) {
      throw new IOException("Block " + b + 
                            " genstamp is " + b.getGenerationStamp()  +
                            " does not match meta file stamp " +
                            stamp);
    }
    // verify that checksum file has an integral number of checkum values.
    DataChecksum dcs = BlockMetadataHeader.readHeader(meta).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    long actual = meta.length() - BlockMetadataHeader.getHeaderSize();
    long numChunksInMeta = actual/checksumsize;
    if (actual % checksumsize != 0) {
      throw new IOException("Block " + b +
                            " has a checksum file of size " + meta.length() +
                            " but it does not align with checksum size of " +
                            checksumsize);
    }
    int bpc = dcs.getBytesPerChecksum();
    long minDataSize = (numChunksInMeta - 1) * bpc;
    long maxDataSize = numChunksInMeta * bpc;
    if (f.length() > maxDataSize || f.length() <= minDataSize) {
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
  public void invalidate(Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      lock.writeLock().lock();
      try {
        DatanodeBlockInfo dinfo = volumeMap.get(invalidBlks[i]);
        if (dinfo == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          error = true;
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
        v.clearPath(parent);
        volumeMap.remove(invalidBlks[i]);
      } finally {
        lock.writeLock().unlock();
      }
      File metaFile = getMetaFile( f, invalidBlks[i] );
      long dfsBytes = f.length() + metaFile.length();
      

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
      if(invalidBlks[i].getNumBytes() != BlockCommand.NO_ACK){
        datanode.notifyNamenodeDeletedBlock(invalidBlks[i]);
      }
      // Delete the block asynchronously to make sure we can do it fast enough
      asyncDiskService.deleteAsync(v, blockFileRenamed, metaFileRenamed,
		  dfsBytes, invalidBlks[i].toString());
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }

  /**
   * Turn the block identifier into a filename.
   */
  public File getFile(Block b) {
    lock.readLock().lock();
    try {
      DatanodeBlockInfo info = volumeMap.get(b);
      if (info != null) {
        return info.getFile();
      }
      return null;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  public void checkDataDir() throws DiskErrorException {
    long total_blocks=0, removed_blocks=0;
    List<FSVolume> failed_vols =  volumes.checkDirs();
    
    //if there no failed volumes return
    if(failed_vols == null) 
      return;
    
    // else 
    // remove related blocks
    long mlsec = System.currentTimeMillis();
    lock.writeLock().lock();
    try {
      Iterator<Block> ib = volumeMap.keySet().iterator();
      while(ib.hasNext()) {
        Block b = ib.next();
        total_blocks ++;
        // check if the volume block belongs to still valid
        FSVolume vol = volumeMap.get(b).getVolume();
        for(FSVolume fv: failed_vols) {
          if(vol == fv) {
            DataNode.LOG.warn("removing block " + b.getBlockId() + " from vol " 
                + vol.dataDir.dir.getAbsolutePath());
            ib.remove();
            removed_blocks++;
            break;
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
    mlsec = System.currentTimeMillis() - mlsec;
    DataNode.LOG.warn(">>>>>>>>>>>>Removed " + removed_blocks + " out of " + total_blocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for(FSVolume fv : failed_vols) {
      sb.append(fv.dataDir.dir.getAbsolutePath() + ";");
    }

    throw  new DiskErrorException("DataNode failed volumes:" + sb);
  
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
      if (volumes.scannersExecutor != null) {
        volumes.scannersExecutor.shutdown();
      }
      for (FSVolume volume : volumes.volumes) {
        if(volume != null) {
          volume.dfsUsage.shutdown();
        }
      }
    }
  }

  public String getStorageInfo() {
    return toString();
  }

  @Override
  public BlockRecoveryInfo startBlockRecovery(long blockId)
      throws IOException {
    Block stored = getStoredBlock(blockId);

    if (stored == null) {
      return null;
    }

    // It's important that this loop not be synchronized - otherwise
    // this will deadlock against the thread it's joining against!
    while (true) {
      DataNode.LOG.debug(
          "Interrupting active writer threads for block " + stored);
      List<Thread> activeThreads = getActiveThreads(stored);
      if (activeThreads == null) break;
      if (interruptAndJoinThreads(activeThreads))
        break;
    }

    lock.readLock().lock();
    try {
      ActiveFile activeFile = ongoingCreates.get(stored);
      boolean isRecovery = (activeFile != null) && activeFile.wasRecoveredOnStartup;


      BlockRecoveryInfo info = new BlockRecoveryInfo(
          stored, isRecovery);
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("getBlockMetaDataInfo successful block=" + stored +
                  " length " + stored.getNumBytes() +
                  " genstamp " + stored.getGenerationStamp());
      }

      // paranoia! verify that the contents of the stored block
      // matches the block file on disk.
      validateBlockMetadata(stored);
      return info;
    } finally {
      lock.readLock().unlock();
    }
  }
}

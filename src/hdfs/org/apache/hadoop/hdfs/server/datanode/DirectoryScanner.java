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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetDelta.BlockOperation;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in
 * {@link FSDataset}
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  
  public static final String DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY 
    = "dfs.datanode.directoryscan.interval";
  public static final int DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT = 3600;
  public static final String DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY 
    = "dfs.datanode.directoryscan.threads";
  public static final int DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT = 1;
  
  private static final Log LOG = LogFactory.getLog(DirectoryScanner.class);

  private final DataNode datanode;
  private final FSDataset dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService threadPoolExecutor;
  private final long scanPeriodMsecs;
  private volatile boolean shouldRun = false;
  private final FSDatasetDelta delta;
  

  final Map<Integer, LinkedList<ScanDifference>> diffsPerNamespace = 
      new HashMap<Integer, LinkedList<ScanDifference>>();
  final Map<Integer, Stats> statsPerNamespace = new HashMap<Integer, Stats>();

  /** Stats tracked for reporting and testing, per namespace */
  static class Stats {
    final int namespaceId;
    // This specifies the total amount of ScanInfo objects created
    // during scan. Each scanInfo object represents a block,
    // but it can lack metafile or blockfile. 
    long totalBlocks = 0;
    long missingMetaFile = 0;
    long missingBlockFile = 0;
    long missingMemoryBlocks = 0;
    
    /** block is mismatched when its metafile is missing
     *  or has wrong generation stamp,
     *  or block file length is different than expected */
    long mismatchBlocks = 0;
    
    public Stats(int namespaceId) {
      this.namespaceId = namespaceId;
    }
    
    public String toString() {
      return "Namespace " + namespaceId + " Total blocks: " + totalBlocks
          + ", missing metadata files:" + missingMetaFile
          + ", missing block files:" + missingBlockFile
          + ", missing blocks in memory:" + missingMemoryBlocks
          + ", mismatched blocks:" + mismatchBlocks;
    }
  }
  
  
  
  static class ScanInfoListPerNamespace {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, LinkedList<DiskScanInfo>> map;
    
    void put(Integer nsid, LinkedList<DiskScanInfo> value) {
      map.put(nsid, value);
    }
    
    ScanInfoListPerNamespace() {
      map = new HashMap<Integer, LinkedList<DiskScanInfo>>();
    }
    
    ScanInfoListPerNamespace(int sz) {
      map = new HashMap<Integer, LinkedList<DiskScanInfo>>(sz);
    }
    
    public void clear() {
      map.clear();
    }
    
    /**
     * Merges "that" ScanInfoPerNamespace into this one
     * @param that
     */
    public void addAll(ScanInfoListPerNamespace that) {
      if (that == null) return;
      
      for (Entry<Integer, LinkedList<DiskScanInfo>> entry : that.map.entrySet()) {
        Integer namespaceId = entry.getKey();
        LinkedList<DiskScanInfo> list = entry.getValue();
        
        if (this.map.containsKey(namespaceId)) {
          //merge that per-namespace linked list with this one
          this.map.get(namespaceId).addAll(list);
        } else {
          //add that new namespace and its linked list to this
          this.map.put(namespaceId, list);
        }
      }
    }
    
    /**
     * Convert all the LinkedList values in this ScanInfoPerNamespace map
     * into sorted arrays, and return a new map of these arrays per namespace
     * @return a map of ScanInfo arrays per namespace
     */
    public Map<Integer, DiskScanInfo[]> toSortedArrays() {
      Map<Integer, DiskScanInfo[]> result = 
        new HashMap<Integer, DiskScanInfo[]>(this.map.size());
      
      for (Entry<Integer, LinkedList<DiskScanInfo>> entry : this.map.entrySet()) {
        Integer namespaceId = entry.getKey();
        LinkedList<DiskScanInfo> list = entry.getValue();
        
        // convert list to array
        DiskScanInfo[] record = list.toArray(new DiskScanInfo[list.size()]);
        // Sort array based on blockId
        Arrays.sort(record);
        result.put(namespaceId, record);            
      }
      return result;
    }
  }
  
  static class FileInfo {
    final File file;
    final String fileName;
    final long blockId;
    final long getStamp;
    
    FileInfo(File file, String fileName, long blockId) {
      this(file, fileName, blockId, Block.GRANDFATHER_GENERATION_STAMP);
    }
    
    FileInfo(File file, String fileName, long blockId, long genStamp) {
      this.file = file; 
      this.fileName = fileName;
      this.blockId = blockId;
      this.getStamp = genStamp;
    }
  }

  /**
   * Tracks the files and other information related to a block on the disk
   * Missing file is indicated by setting the corresponding member
   * to null.
   */
  final static class DiskScanInfo implements Comparable<DiskScanInfo> {
    
    static final int INLINE_CHECKSUM_LAYOUT = 1;
    static final int SEPARATE_FILES_LAYOUT = 2;
    
    private final long blockId;
    private final File metaFile;
    private final File blockFile;
    private final FSVolume volume;
    private final int layout;
    final long fileLength;
    final long genStamp;
    
    DiskScanInfo(int layout, long blockId, File blockFile, File metaFile,
        FSVolume vol, long fileLength, long genStamp) {
      this.blockId = blockId;
      this.metaFile = metaFile;
      this.blockFile = blockFile;
      this.volume = vol;
      this.fileLength = fileLength;
      this.genStamp = genStamp;
      this.layout = layout;
    }
    
    static DiskScanInfo getSeparateFilesLayoutScanInfo(long blockId,
        FileInfo blockFileInfo, FileInfo metaFileInfo, FSVolume vol) {
      File metaFile = null;
      long genStamp = Block.GRANDFATHER_GENERATION_STAMP;
      if (metaFileInfo != null) {
        metaFile = metaFileInfo.file;
        genStamp = metaFileInfo.getStamp;

      }
      File blockFile = null;
      long fileLength = 0;
      if (blockFileInfo != null) {
        blockFile = blockFileInfo.file;
        fileLength = blockFile.length();
      }
      return new DiskScanInfo(SEPARATE_FILES_LAYOUT, blockId, blockFile,
          metaFile, vol, fileLength, genStamp);
    }

    static DiskScanInfo getInlineFilesLayoutScanInfo(long blockId,
        FileInfo singleFileInfo, FSVolume vol) {
      String[] groundSeparated = StringUtils
          .split(singleFileInfo.fileName, '_');
      if (groundSeparated.length != 6) {
        throw new IllegalStateException("FileName \"" + singleFileInfo.fileName
            + "\" doesn't " + "reflect new layout format!");
      }
      int checksumType = Integer.parseInt(groundSeparated[4]);
      int bytesPerChecksum = Integer.parseInt(groundSeparated[5]);
      long fileLength = BlockInlineChecksumReader.getBlockSizeFromFileLength(
          singleFileInfo.file.length(), checksumType, bytesPerChecksum);
      return new DiskScanInfo(INLINE_CHECKSUM_LAYOUT, blockId,
          singleFileInfo.file, singleFileInfo.file, vol, fileLength,
          singleFileInfo.getStamp);
    }

    /**
     * Returns layout of this scanned block files
     * 
     * @return INLINE_CHECKSUM_LAYOUT or SEPARATE_FILES_LAYOUT
     */
    int getLayout() {
      return layout;
    }
    
    File getMetaFile() {
      return metaFile;
    }

    File getBlockFile() {
      return blockFile;
    }

    long getBlockId() {
      return blockId;
    }

    FSVolume getVolume() {
      return volume;
    }
    
    public long getGenStamp() {
      return this.genStamp;
    }
    
    public long getLength() {
      return this.fileLength;
    }

    @Override // Comparable
    public int compareTo(DiskScanInfo b) {
      if (blockId < b.blockId) {
        return -1;
      } else if (blockId == b.blockId) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  static class ScanDifference {
    /** 
     * This means the memory block is missing, but its files are found on disk 
     * */
    static final int MEMORY_BLOCK_MISSING = 1;
    /** 
     * The block exists in memory, but none of its files exist on disk 
     * */
    static final int DISK_FILES_MISSING = 2;
    /** 
     * This state means that block exists in mem and at least one file exists on disk, 
     * but they are not synced to each other, e.g. getStamp or length is different 
     * */
    static final int OUT_OF_SYNC = 3;
    
    private final DiskScanInfo diskScanInfo;
    private final long blockId;
    private final int state;
    
    private ScanDifference(long blockId, DiskScanInfo diskScanInfo, int state) {
      this.blockId = blockId;
      this.diskScanInfo = diskScanInfo;
      this.state = state;
    }
    
    static ScanDifference createDiffForMemoryBlockMissing(DiskScanInfo diskInfo) {
      return new ScanDifference(diskInfo.getBlockId(), diskInfo, MEMORY_BLOCK_MISSING);
      
    }

    static ScanDifference createDiffForDiskFilesMissing(long blockId) {
      return new ScanDifference(blockId, null, DISK_FILES_MISSING);
    }
    
    static ScanDifference createDiffOutOfSync(DiskScanInfo diskInfo) {
      return new ScanDifference(diskInfo.getBlockId(), diskInfo, OUT_OF_SYNC);
    }
    
    int getState() {
      return this.state;
    }
    
    long getBlockId() {
      return blockId;
    }

    File getMetaFile() {
      return diskScanInfo.getMetaFile();
    }

    File getBlockFile() {
      return diskScanInfo.getBlockFile();
    }

    FSVolume getVolume() {
      return diskScanInfo.getVolume();
    }
    
    public long getGenStamp() {
      return diskScanInfo.getGenStamp();
    }
    
    public long getLength() {
      return diskScanInfo.getLength();
    }
    
    public boolean isInlineChecksum() {
      return diskScanInfo.getLayout() == DiskScanInfo.INLINE_CHECKSUM_LAYOUT;
    }

  }
  
  DirectoryScanner(DataNode dn, FSDataset dataset, Configuration conf) {
    this.datanode = dn;
    this.dataset = dataset;
    int interval = conf.getInt(DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT);
    scanPeriodMsecs = interval * 1000L; // msec
    int numThreads = conf.getInt(DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
        DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool = Executors.newFixedThreadPool(numThreads,
        new Daemon.DaemonFactory());
    threadPoolExecutor = new ScheduledThreadPoolExecutor(1,
        new Daemon.DaemonFactory());
    this.delta = new FSDatasetDelta();

    this.dataset.setDatasetDelta(delta);
  }

  void start() {
    shouldRun = true;
    long offset = DFSUtil.getRandom()
        .nextInt((int) (scanPeriodMsecs/1000L)) * 1000L; //msec
    long firstScanTime = System.currentTimeMillis() + offset;
    LOG.info("Periodic Directory Tree Verification scan starting at " 
        + firstScanTime + " with interval " + scanPeriodMsecs);
    threadPoolExecutor.scheduleAtFixedRate(this, offset, scanPeriodMsecs, 
                                     TimeUnit.MILLISECONDS);
  }
  
  // for unit test
  boolean getRunStatus() {
    return shouldRun;
  }

  private void resetDiffsAndStats() {
    diffsPerNamespace.clear();
    statsPerNamespace.clear();
  }

  /**
   * Main program loop for DirectoryScanner
   * Runs "reconcile()" periodically under the masterThread.
   */
  @Override
  public void run() {
    try {
      InjectionHandler.processEvent(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);
      if (!shouldRun) {
        //shutdown has been activated
        LOG.warn("this cycle terminating immediately because 'shouldRun' has been deactivated");
        return;
      }

      Integer[] namespaceIds = datanode.getAllNamespaces();
      for(Integer nsid : namespaceIds) {
        UpgradeManagerDatanode um = 
          datanode.getUpgradeManager(nsid);
        if (um != null && !um.isUpgradeCompleted()) {
          //If distributed upgrades underway, exit and wait for next cycle.
          LOG.warn("this cycle terminating immediately because Distributed Upgrade is in process");
          return; 
        }
      }
      
      //We're are okay to run - do it
      delta.resetDelta();
      delta.startRecordingDelta();
      checkDifferenceAndReconcile();      
      
    } catch (Exception e) {
      //Log and continue - allows Executor to run again next cycle
      LOG.error("Exception during DirectoryScanner execution - will continue next cycle", e);
    } catch (Error er) {
      //Non-recoverable error - re-throw after logging the problem
      LOG.error("System Error during DirectoryScanner execution - permanently terminating periodic scanner", er);
      throw er;
    } finally {
      delta.stopRecordingDelta();
      InjectionHandler.processEvent(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
    }
  }
  
  void shutdown() {
    if (!shouldRun) {
      LOG.warn("DirectoryScanner: shutdown has been called, but periodic scanner not started");
    } else {
      LOG.warn("DirectoryScanner: shutdown has been called");      
    }
    shouldRun = false;
    if (threadPoolExecutor != null) threadPoolExecutor.shutdown();
    if (reportCompileThreadPool != null) reportCompileThreadPool.shutdown();
  }

  /** Get lists of blocks on the disk sorted by blockId, per namespace */
  private Map<Integer, DiskScanInfo[]> getDiskReportPerNamespace() {
    if (dataset.volumes == null) {
      LOG.warn("Dataset volumes are not initialized yet");
      return new HashMap<Integer, DiskScanInfo[]>();
    }
    // First get list of data directories
    FSVolume[] volumes = dataset.volumes.getVolumes();
    ScanInfoListPerNamespace[] volumeReports = 
        new ScanInfoListPerNamespace[volumes.length];
    
    Map<Integer, Future<ScanInfoListPerNamespace>> volumeCompilers =
      new HashMap<Integer, Future<ScanInfoListPerNamespace>>();
    for (int i = 0; i < volumes.length; i++) {
      // check volume is valid
      if (dataset.volumes.isValid(volumes[i])) {
        // and run compiler for it
        ReportCompiler reportCompiler =
          new ReportCompiler(volumes[i], datanode);
        Future<ScanInfoListPerNamespace> result = 
          reportCompileThreadPool.submit(reportCompiler);
        volumeCompilers.put(i, result);
      }
    }
    
    for(Entry<Integer, Future<ScanInfoListPerNamespace>> e : volumeCompilers.entrySet()) {
      try {
        int volume = e.getKey();
        volumeReports[volume] = e.getValue().get();
      } catch (Exception ex) {
        LOG.error("Error compiling report", ex);
        // Propagate ex to DataBlockScanner to deal with
        throw new RuntimeException(ex);
      }
    }

    // Compile consolidated report for all the volumes
    ScanInfoListPerNamespace list = new ScanInfoListPerNamespace();
    for (int i = 0; i < volumes.length; i++) {
      if (dataset.volumes.isValid(volumes[i])) { // volume is still valid
        list.addAll(volumeReports[i]);
      }
    }

    return list.toSortedArrays();
  }
  
  
  /**
   * Scan for the differences between disk and in-memory blocks
   * Scan only the "finalized blocks" lists of both disk and memory.
   */
  /**
   * Scan for the differences between disk and in-memory blocks
   * Scan only the "finalized blocks" lists of both disk and memory.
   */
  void checkDifference() {
    resetDiffsAndStats();
    InjectionHandler.processEvent(InjectionEvent.DIRECTORY_SCANNER_BEFORE_FILE_SCAN);
    Map<Integer, DiskScanInfo[]> diskReportPerNamespace = getDiskReportPerNamespace();
    InjectionHandler.processEvent(InjectionEvent.DIRECTORY_SCANNER_AFTER_FILE_SCAN);
    try {
      for (Entry<Integer, DiskScanInfo[]> entry : diskReportPerNamespace.entrySet()) {

        Integer namespaceId = entry.getKey();
        DiskScanInfo[] namespaceReport = entry.getValue();

        Stats statsRecord = new Stats(namespaceId);
        statsPerNamespace.put(namespaceId, statsRecord);
        LinkedList<ScanDifference> diffRecords = new LinkedList<ScanDifference>();
        diffsPerNamespace.put(namespaceId, diffRecords);

        statsRecord.totalBlocks = namespaceReport.length;
        Block[] memReport = dataset.getBlockReport(namespaceId);
        Arrays.sort(memReport); // Sort based on blockId

        int d = 0; // index for blockpoolReport
        int m = 0; // index for memReprot
        while (m < memReport.length && d < namespaceReport.length) {
          Block memBlock = memReport[Math.min(m, memReport.length - 1)];
          DiskScanInfo scanInfo = namespaceReport[Math.min(
              d, namespaceReport.length - 1)];
          if (scanInfo.getBlockId() < memBlock.getBlockId()) {
            // Block is missing in memory
            // If this block was removed during scan, then do not add it to diff
            if (delta.get(namespaceId, scanInfo.getBlockId()) != BlockOperation.REMOVE) {
              // Otherwise this is the case for reconciliation
              statsRecord.missingMemoryBlocks++;
              statsRecord.missingMetaFile +=
                  scanInfo.getMetaFile() == null ? 1 : 0;
              statsRecord.missingBlockFile +=
                  scanInfo.getBlockFile() == null ? 1 : 0;
              diffRecords.add(ScanDifference.createDiffForMemoryBlockMissing(scanInfo));
            }
            d++;
            continue;
          }
          if (scanInfo.getBlockId() > memBlock.getBlockId()) {
            // Block is missing on the disk
            // If this block was added during scan, then do not add it to diff
            if (delta.get(namespaceId, memBlock) != BlockOperation.ADD) {
              statsRecord.missingBlockFile++;
              statsRecord.missingMetaFile++;
              diffRecords.add(ScanDifference.createDiffForDiskFilesMissing(memBlock.getBlockId()));
            }
            m++;
            continue;
          }
          // Block file and/or metadata file exists on the disk
          // Block exists in memory
          if (scanInfo.getBlockFile() == null) {
            // Block metadata file exits and block file is missing
            // this is not the case for reconciliation if the block was added or removed
            // during scanning process
            BlockOperation deltaOp = delta.get(namespaceId, memBlock);
            if (deltaOp != BlockOperation.ADD && deltaOp != BlockOperation.REMOVE) {
              statsRecord.missingBlockFile += scanInfo.getBlockFile() == null ? 1 : 0;
              diffRecords.add(ScanDifference.createDiffOutOfSync(scanInfo));
            }
          } else if (scanInfo.getGenStamp() != memBlock.getGenerationStamp()
              || scanInfo.getLength() != memBlock.getNumBytes()) {
            // Block metadata file is missing or has wrong generation stamp,
            // or block file length is different than expected. It could happen
            // in both add, remove and update operations
            if (delta.get(namespaceId, memBlock) == null) {
              statsRecord.mismatchBlocks++;
              statsRecord.missingMetaFile += scanInfo.getMetaFile() == null ? 1 : 0;
              diffRecords.add(ScanDifference.createDiffOutOfSync(scanInfo));
            }
          }
          d++;
          m++;
        }
        while (m < memReport.length) {
          if (delta.get(namespaceId, memReport[m].getBlockId()) != BlockOperation.ADD) {
            statsRecord.missingBlockFile++;
            statsRecord.missingMetaFile++;
            diffRecords
              .add(ScanDifference.createDiffForDiskFilesMissing(memReport[m].getBlockId()));
          }
          m++;
        }
        while (d < namespaceReport.length) {
          DiskScanInfo info = namespaceReport[d];
          if (delta.get(namespaceId, info.getBlockId()) != BlockOperation.REMOVE) {
            statsRecord.missingMemoryBlocks++;
            statsRecord.missingMetaFile += info.getMetaFile() == null ? 1 : 0;
            statsRecord.missingBlockFile += info.getBlockFile() == null ? 1 : 0;
            diffRecords.add(ScanDifference.createDiffForMemoryBlockMissing(info));
          }
          d++;
        }
      } //end for
    } catch (IOException e) {
      LOG.warn("Scanning failed beause of IOException", e);
    }
    InjectionHandler.processEvent(InjectionEvent.DIRECTORY_SCANNER_AFTER_DIFF);
    StringBuilder sb = new StringBuilder();
    for (Entry<Integer, Stats> entry : statsPerNamespace.entrySet()) {
      sb.append("Namespace ID: " + entry.getKey() + " results: " + entry.getValue().toString());
    }
    LOG.info(sb.toString());
  }

  
  /**
   * Reconcile differences between disk and in-memory blocks
   */
  void checkDifferenceAndReconcile() {
    resetDiffsAndStats();
    checkDifference();
    
    // now reconcile the differences
    for (Entry<Integer, LinkedList<ScanDifference>> entry : diffsPerNamespace.entrySet()) {
      Integer namespaceId = entry.getKey();
      LinkedList<ScanDifference> diff = entry.getValue();
      for (ScanDifference info : diff) {
        try {
          dataset.checkAndUpdate(namespaceId, delta, info);
        } catch (IOException e) {
          LOG.warn("Cannot reconcile block " + info.toString(), e);
        }
      }
    }
  }


  private static class ReportCompiler 
  implements Callable<ScanInfoListPerNamespace> {
    private FSVolume volume;
    private DataNode datanode;

    public ReportCompiler(FSVolume volume, DataNode datanode) {
      this.volume = volume;
      this.datanode = datanode;
    }

    @Override
    public ScanInfoListPerNamespace call() throws Exception {
      Integer[] namespaceIds = datanode.getAllNamespaces();
      ScanInfoListPerNamespace result = 
          new ScanInfoListPerNamespace(namespaceIds.length);
      for (Integer nsid : namespaceIds) {
        LinkedList<DiskScanInfo> report = new LinkedList<DiskScanInfo>();
        File nsFinalizedDir = volume.getNamespaceSlice(nsid).getCurrentDir();
        result.put(nsid, compileReport(volume, nsFinalizedDir, report));
      }
      return result;
    }
    
    /** Compile list {@link DiskScanInfo} for the blocks in the directory <dir> */
    private LinkedList<DiskScanInfo> compileReport(FSVolume vol, File dir,
        LinkedList<DiskScanInfo> report) {
      File[] files;
      try {
        files = FileUtil.listFiles(dir);
      } catch (IOException ioe) {
        LOG.warn("Exception occured while compiling report: ", ioe);
        // Ignore this directory and proceed.
        return report;
      }
      
      Map<Long, FileInfo> metaFilesForId = new HashMap<Long, FileInfo>();
      Map<Long, FileInfo> blockFileForId = new HashMap<Long, FileInfo>();
      
      Map<Long, FileInfo> inlineFileForId = new HashMap<Long, FileInfo>();
      
      Set<Long> blockIds = new HashSet<Long>();
      
      for(int i = 0; i < files.length; i++) {
        File file = files[i];
        String fileName = file.getName();
        
        if (Block.isInlineChecksumBlockFilename(fileName)) {
          String[] lines = StringUtils.split(fileName, '_');
          long blockId = Long.parseLong(lines[1]);
          long genStamp = Long.parseLong(lines[2]);
          inlineFileForId.put(blockId, new FileInfo(file, fileName, blockId, genStamp));
          blockIds.add(blockId);
          continue;
        }
        
        if (Block.isSeparateChecksumBlockFilename(fileName)
            || Block.isInlineChecksumBlockFilename(fileName)) {
          long blockId = Block.filename2id(fileName);
          blockFileForId.put(blockId, new FileInfo(file, fileName, blockId));
          blockIds.add(blockId);
          continue;
        }
        
        if (BlockWithChecksumFileReader.isMetaFilename(fileName)) {
          long[] parsedMetaFile = BlockWithChecksumFileReader
              .parseMetafileName(fileName);
          long blockId = parsedMetaFile[0];
          long getStamp = parsedMetaFile[1];
          metaFilesForId.put(blockId, new FileInfo(file, fileName, blockId,
              getStamp));
          blockIds.add(blockId);
          continue;
        }

        if (files[i].isDirectory()) {
          compileReport(vol, files[i], report);
          continue;
        } 
        
      }
      
      for (long blockId : blockIds) {
        if (inlineFileForId.containsKey(blockId)) {
          report.add(DiskScanInfo.getInlineFilesLayoutScanInfo(blockId,
              inlineFileForId.get(blockId), vol));
        } else {
          report.add(DiskScanInfo.getSeparateFilesLayoutScanInfo(blockId,
              blockFileForId.get(blockId), metaFilesForId.get(blockId), vol));
        }
      }
      
      return report;
    }
  }
}
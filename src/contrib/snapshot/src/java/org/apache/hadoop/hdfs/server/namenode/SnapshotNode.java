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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSLocatedBlocks;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImage.CheckpointStates;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.*;
import org.apache.hadoop.hdfs.server.namenode.WaitingRoom.*;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.SnapshotProtocol;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**********************************************************
 * The SnapshotNode is responsible for taking periodic 
 * snapshots of the HDFS. The current design only allows
 * one SnapshotNode per cluster.
 *
 * The SnapshotNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic snapshot and then goes back to sleep.
 * The SnapshotNode uses the Namesystem's jetty server to 
 * retreive files.
 *
 **********************************************************/
public class SnapshotNode implements SnapshotProtocol {

  public static final Log LOG =
      LogFactory.getLog(SnapshotNode.class);

  public static final String CURRENT_DIR = "/current";
  public static String SSNAME = "dfs_snapshot_"; // prefix of ss files

  private Configuration conf; // conf

  private String fileServer; // jetty image server namenode listens on
  private FileSystem dfs; // file system

  private String tempDir; // temp dir to download files from namenode
  private String ssDir; // path to store snapshots in

  private Daemon purgeThread; //waiting room purger thread

  private ExecutorService leaseUpdateThreadPool;
  private int maxLeaseUpdateThreads;

  private Server server; // RPC Server
  private InetSocketAddress serverAddress = null; // RPC server address

  private NamenodeProtocol namenode;
  private InetSocketAddress nameNodeAddr;

  public SnapshotNode(Configuration conf) {
    try {
      this.conf = conf;
      init();
    } catch (IOException e) {
      LOG.error("Failed to start SnapshotNode");
      shutdown();
    }
  }

  /**
   * Initialize SnapshotNode
   * @throws IOException
   */
  private void init() throws IOException {
    ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");
    tempDir = conf.get("fs.snapshot.tempdir", "/tmp/snapshot");

    fileServer = getImageServer();
    dfs = FileSystem.get(conf);

    Path ssPath = new Path(ssDir);
    if (!dfs.exists(ssPath)) {
      dfs.mkdirs(ssPath);
    }

    maxLeaseUpdateThreads = conf.getInt("fs.snapshot.leaseupdatethreads", 100);

    // Waiting room purge thread
    purgeThread = new Daemon((new WaitingRoom(conf)).getPurger());
    purgeThread.start();

    // Get namenode rpc connection
    nameNodeAddr = NameNode.getAddress(conf);
    namenode = (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
                               NamenodeProtocol.versionID, nameNodeAddr, conf);

    // Snapshot RPC Server
    InetSocketAddress socAddr = SnapshotNode.getAddress(conf);
    int handlerCount = conf.getInt("fs.snapshot.handler.count", 10);
    server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                           handlerCount, false, conf);
    // The rpc-server port can be ephemeral... ensure we have the correct info
    serverAddress = server.getListenerAddress();
    LOG.info("SnapshotNode up at: " + serverAddress);

    server.start(); // start rpc server
  }

  private static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String nodeport = conf.get("fs.snapshot.server.address");
    if (nodeport == null) {
      nodeport = "localhost:" + 60000; // DEFAULT PORT
    }
    return getAddress(nodeport);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    if (protocol.equals(SnapshotProtocol.class.getName())) {
      return SnapshotProtocol.versionID;
    }

    throw new IOException("Unknown protocol to snapshot node: " + protocol);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, 
                                         int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, 
                                                  clientMethodsHash);
  }

  void prepareDownloadDirs() throws IOException {
    // Check if temp dir exists
    File temp = new File(tempDir);
    if (!temp.exists()) temp.mkdirs();
    if(!temp.isDirectory()) throw new IOException("Temp Dir: " +
                               tempDir + " is not a directory.");

    // Check if current dir in temp exists
    temp = new File(tempDir + CURRENT_DIR);
    if (!temp.exists()) temp.mkdir();
    if(!temp.isDirectory()) throw new IOException("Current in Temp Dir: " +
                           tempDir + CURRENT_DIR + " is not a directory.");

    // Delete all previously downloaded files
    for (File f: temp.listFiles()) {
      f.delete();
    }
  }

  /**
   * Shutdown snapshot node and attached daemons
   */
  public void shutdown() {
    if (purgeThread != null) {
      WaitingRoomPurger purger = (WaitingRoomPurger) purgeThread.getRunnable();
      purger.shutdown();
    }

    RPC.stopProxy(namenode);
    if (server != null) server.stop();
  }

  /**
   * Shutdown snapshot node and attached daemons
   */
  public void shutdownWaitingRoomPurger() {
    if (purgeThread != null) {
      WaitingRoomPurger purger = (WaitingRoomPurger) purgeThread.getRunnable();
      purger.shutdown();
    }
  }


  // SNAPSHOT PROTOCOL //

  @Override
  public String[] listSnapshots() throws IOException {
    Path ssPath = new Path(ssDir);

    if (!dfs.exists(ssPath)) {
      throw new FileNotFoundException("Snapshot dir doesn't exist");
    }

    FileStatus ssStatus = dfs.getFileStatus(ssPath);
      if (!ssStatus.isDir()) {
        throw new IOException("ssDir " + ssDir  +" is not a directory");
      }

    FileStatus[] files = dfs.listStatus(ssPath);
    List<String> ssIds = new ArrayList<String>();

    // Separate snapshot files
    for (FileStatus ss: files) {
      if (ss.isDir()) continue; // skips dirs
      String name = ss.getPath().getName();
      if (!name.startsWith("dfs_snapshot_")) continue;
      ssIds.add(name.substring(13));
    }

    String[] rtn = new String[ssIds.size()];
    for (int i = 0; i < ssIds.size(); i++) {
      rtn[i] = ssIds.get(i);
    }

    return rtn;
  }

  @Override
  public FileStatus getSnapshotFileStatus(String id) throws IOException {
    Path ss = new Path(ssDir + "/" + SSNAME + id);
    return dfs.getFileStatus(ss);
  }

  @Override
  public boolean deleteSnapshot(String id) throws IOException {
    Path fileToDelete = new Path(ssDir + "/" + SSNAME + id);
    return dfs.delete(fileToDelete, false);
  }

  @Override
  public LocatedBlocksWithMetaInfo[] getLocatedBlocks(String snapshotId,
      String path)
  throws IOException {
    FSImage fsImage = new FSImage();
    FSNamesystem namesystem = new FSNamesystem(fsImage, conf);
    Path ssPath = new Path(ssDir + "/" + SSNAME + snapshotId);
    FSDataInputStream in = dfs.open(ssPath);
    fsImage.loadFSImage(new File(ssPath.toString()), in);
    INode inode = namesystem.dir.getInode(path);

    if (inode == null) {
      throw new IOException("File/dir at " + path + 
                            " does not exist in snapshot " + snapshotId);
    }

    List<LocatedBlocksWithMetaInfo> blocks = new ArrayList<LocatedBlocksWithMetaInfo>();
    getAllLocatedBlocks(inode, blocks); // fill blocks with LocatedBlocks for all files

    LocatedBlocksWithMetaInfo[] blocksArr = new LocatedBlocksWithMetaInfo[blocks
        .size()];
    for (int i = 0; i < blocksArr.length; ++i) {
      blocksArr[i] = blocks.get(i);
    }

    fsImage.close();
    return blocksArr;
  }

  @Override
  public void createSnapshot(String snapshotId, boolean updateLeases) throws IOException {
    // Create new SnapshotStore
    SnapshotStorage ssStore = new SnapshotStorage(conf, Util.stringAsURI(tempDir));

    // Download image & edit files from namenode
    downloadSnapshotFiles(ssStore);

    // Merge image and edit files
    doMerge(ssStore);

    // Update file lengths for leased files (optional)
    if (updateLeases) {
      updateLeasedFiles(ssStore);
    }

    // Save snapshot
    saveSnapshot(ssStore, snapshotId);
    ssStore.close();
  }

  private void getAllLocatedBlocks(INode inode,
      List<LocatedBlocksWithMetaInfo> blocks)
  throws IOException {
    if (inode.isDirectory()) {
      INodeDirectory dir = (INodeDirectory) inode;
      for (INode child: dir.getChildren()) {
        getAllLocatedBlocks(child, blocks);
      }
    } else {
      INodeFile file = (INodeFile) inode;
      BlockInfo[] fileBlocks = file.getBlocks();
      List<LocatedBlock> lb = new ArrayList<LocatedBlock>();
      for (BlockInfo block: fileBlocks) {
        // DatanodeInfo is unavailable, so set as empty for now
        lb.add(new LocatedBlock(block, new DatanodeInfo[0]));
      }

      LocatedBlocks locatedBlocks =  new LocatedBlocks(
                             file.computeContentSummary().getLength(), // flength
                             lb, // blks
                             false); // isUnderConstruction

      // Update DatanodeInfo from NN
      blocks.add(namenode.updateDatanodeInfo(locatedBlocks));
    }
  }

  void saveSnapshot(SnapshotStorage ssStore, String id) throws IOException {
    // Create new snapshot in temp file
    Path tmpPath = new Path("/tmp/" + SSNAME + id);
    FSDataOutputStream out = dfs.create(tmpPath);
    ssStore.saveSnapshot(tmpPath.toString(), out);
    out.close();

    // Rename snapshot
    Path ssPath = new Path(ssDir + "/" + SSNAME + id);
    if (!dfs.rename(tmpPath, ssPath)) {
      throw new IOException("Could not rename temp snapshot file");
    }
  }

  void doMerge(SnapshotStorage ssStore) throws IOException {
    FSNamesystem namesystem = new FSNamesystem(ssStore, conf);
    ssStore.doMerge();
  }

  /**
   * Create a snapshot with id equals to 
   * current system time.
   */
  void createSnapshot() throws IOException {
    createSnapshot(Long.toString(System.currentTimeMillis()), true);
  }

  void createSnapshot(String id) throws IOException {
    createSnapshot(id, true);
  }

  /**
   * Tries to get the most up to date lengths of files under construction.
   */
  void updateLeasedFiles(SnapshotStorage ssStore) throws IOException {
    FSNamesystem fsNamesys = ssStore.getFSNamesystem();
    List<Block> blocksForNN = new ArrayList<Block>();

    leaseUpdateThreadPool = new ThreadPoolExecutor(1, maxLeaseUpdateThreads, 60, 
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>());
    ((ThreadPoolExecutor)leaseUpdateThreadPool).allowCoreThreadTimeOut(true);

    // Try to update lengths for leases from DN
    LightWeightLinkedSet<Lease> sortedLeases = fsNamesys.leaseManager.getSortedLeases();
    Iterator<Lease> itr = sortedLeases.iterator();
    while (itr.hasNext()) {
      Lease lease = itr.next();
      for (String path : lease.getPaths()) {
        // Update file lengths using worker threads to increase throughput
        leaseUpdateThreadPool.execute(
                   new LeaseUpdateWorker(conf, path, fsNamesys, blocksForNN));
      }
    }

    try {
      leaseUpdateThreadPool.shutdown();
      // Wait till update tasks finish successfully (max 20 mins?)
      if (!leaseUpdateThreadPool.awaitTermination(1200, TimeUnit.SECONDS)) {
        throw new IOException("Updating lease files failed");
      }
    } catch (InterruptedException e) {
        throw new IOException("Snapshot creation interrupted while updating leased files");
    }

    // Fetch block lengths for renamed/deleted leases from NN
    long[] blockIds = new long[blocksForNN.size()];

    for (int i = 0; i < blocksForNN.size(); ++i) {
      blockIds[i] = blocksForNN.get(i).getBlockId();
    }

    long[] lengths = namenode.getBlockLengths(blockIds);

    for (int i = 0; i < blocksForNN.size(); ++i) {
      if (lengths[i] == -1) {
        // Couldn't update block length, keep preferred length
        LOG.error("Couldn't update length for block " + blocksForNN.get(i));
      } else {
        blocksForNN.get(i).setNumBytes(lengths[i]);
      }
    }
  }

  /**
   * Download fsimage, edits and edits.new files from the name-node.
   * Files will be downloaded in CURRENT_DIR
   * @throws IOException
   */
  void downloadSnapshotFiles(SnapshotStorage ssStore) throws IOException {
    CheckpointSignature start = namenode.getCheckpointSignature();
    ssStore.storage.setStorageInfo(start);
    CheckpointSignature end = null;
    boolean success;

    do {
      // Clear temp files
      prepareDownloadDirs();

      // get fsimage
      File[] srcNames = ssStore.getImageFiles();
      assert srcNames.length == 1 : "No snapshot temporary dir.";
      TransferFsImage.downloadImageToStorage(fileServer, HdfsConstants.INVALID_TXID, ssStore, true, srcNames);
      LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
               srcNames[0].length() + " bytes.");

      // get edits file
      srcNames = ssStore.getEditsFiles();
      assert srcNames.length == 1 : "No snapshot temporary dir.";
      TransferFsImage.downloadEditsToStorage(fileServer, new RemoteEditLog(), ssStore, false);
      LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
               srcNames[0].length() + " bytes.");

      // get edits.new file (only if in the middle of ckpt)
      try {
        srcNames = ssStore.getEditsNewFiles();
        assert srcNames.length == 1 : "No snapshot temporary dir.";
        TransferFsImage.downloadEditsToStorage(fileServer, new RemoteEditLog(), ssStore, true);
        LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
               srcNames[0].length() + " bytes.");
      } catch (FileNotFoundException e) {
        // do nothing
      }

      end = namenode.getCheckpointSignature();

      // Are the downloaded files consistent?
      success = end.checkpointTime == start.checkpointTime && 
                end.checkpointState != CheckpointStates.UPLOAD_DONE;

      start = end;
    } while (!success);
  }

  /**
   * Returns the jetty image server that the Namenode is listening on.
   * @throws IOException
   */
  private String getImageServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);

    if (!"hdfs".equals(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }

    return NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                             "dfs.info.port", "dfs.http.address");
  }

  static class SnapshotStorage extends FSImage {
    Configuration conf;
    File tempDir;
    DataOutputStream out;

    public SnapshotStorage(Configuration conf, URI tempDir) throws IOException {
      super(tempDir);
      this.conf = conf;
      this.tempDir = new File(tempDir.getPath());
    }

    /**
     * Merge image and edit log (in memory).
     * Files to merge include fsimage, edits, and possibly edits.new
     * @throws IOException
     */
    void doMerge() throws IOException {
      StorageDirectory sdTemp = null;
      Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.IMAGE_AND_EDITS);
      if (it.hasNext()) {
        sdTemp = it.next();
      } else {
        throw new IOException("Could not locate snapshot temp directory.");
      }

      loadFSImage(NNStorage.getStorageFile(sdTemp, NameNodeFile.IMAGE));
      Collection<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>(); 
      EditLogInputStream is = new EditLogFileInputStream(NNStorage.getStorageFile(sdTemp, NameNodeFile.EDITS));
      editStreams.add(is);
      File editsNew = NNStorage.getStorageFile(sdTemp, NameNodeFile.EDITS_NEW);
      if (editsNew.exists()) {
        is = new EditLogFileInputStream(editsNew);
        editStreams.add(is);
      }
      loadEdits(editStreams);
    }

    /** 
     * Writes snapshot to the OutputStream.
     * @param out Stream to write snapshot to
     */
    void saveSnapshot(String dest, DataOutputStream out) throws IOException {
      saveFSImage(dest, out);
    }
  }

  private class LeaseUpdateWorker implements Runnable {
    String path;
    Configuration conf;
    List<Block> blocks;
    FSNamesystem fsNamesys;

    public LeaseUpdateWorker(Configuration conf, String path, 
                             FSNamesystem namesystem, List<Block> blocks) {
      this.path = path;
      this.conf = conf;
      this.blocks = blocks;
      this.fsNamesys = namesystem;
    }
  
    @Override
    public void run() {
      boolean error = false;
      INodeFile node = null;
      DFSClient client = null;

      try {
        client = new DFSClient(conf);

	LOG.info("Trying to update lease for file at " + path);

        // verify that path exists in namespace
        node = fsNamesys.dir.getFileINode(path);
        if (node == null) {
          error = true;
        }
        if (!node.isUnderConstruction()) {
          error = true;
        }
      }
      catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        error = true;
      }

      // Could not find inode in FSNamespace, quit now
      if (error) {
        LOG.error("Couldn't update length for leased file at " + path +
                  " because file not in namespace");
	return;
      }

      BlockInfo[] blks = node.getBlocks();

      // If NN has not leased out any block, return
      if (blks.length == 0) return;

      int index = blks.length - 1; // index of last file block

      LOG.info("Block at index " + index + " being written for file at  " +
               path);

      // Pessimistically update last block length from DataNode. 
      // File could have been renamed, and a new file created in its place.
      try { 
        DFSInputStream stm = client.open(path);
        DFSLocatedBlocks locBlks = stm.fetchLocatedBlocks();

        if (locBlks.locatedBlockCount() >= blks.length) {
          if (blks[index] != null && locBlks.get(index) != null) {
            if (blks[index].getBlockId() == locBlks.get(index).getBlock().getBlockId()) {
              blks[index].setNumBytes(locBlks.get(index).getBlock().getNumBytes());
              return;
            }
          }
        }

        stm.close();
        client.close(); // close dfs client
      }
      catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }

      // If file was renamed/deleted, set block length to preferred size
      // and add it to list of blocks which we should try to update from NN
      LOG.info("Couldn't update block " + blks[index] + " for file " +
               "at " + path + " from DN. Setting length to preferred length " +
               "and queuing block to be checked from NN for updated length.");
      blks[index].setNumBytes(node.getPreferredBlockSize());

      synchronized(blocks) {      
        blocks.add(blks[index]);
      }
    }
  }
}

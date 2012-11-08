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

package org.apache.hadoop.hdfs.server.hightidenode;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.Random;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ProtocolCompatible;
import org.apache.hadoop.hdfs.protocol.HighTideProtocol;
import org.apache.hadoop.hdfs.protocol.PolicyInfo;
import org.apache.hadoop.hdfs.protocol.PolicyInfo.PathInfo;
import org.apache.hadoop.hdfs.server.hightidenode.metrics.HighTideNodeMetrics;

/**
 * This class fixes files by copying data from one of the files in the
 * equivalent set.
 * It periodically fetches the list of corrupt files from the namenode,
 * and fixed missing blocks
 */
public class FileFixer implements Runnable {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.hdfs.hightide.FileFixer");
  private final Configuration conf;

  private volatile boolean running = true;
  private int blockFixInterval = 60*1000; // 1min
  private int numThreads = 100;

  // ThreadPool keep-alive time for threads over core pool size
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;

  // a queue to store corrupted files
  static class PathToPolicy {
    String spath;
    PolicyInfo pinfo;
    PathToPolicy(Path p, PolicyInfo info) {
      this.spath = p.toString();
      this.pinfo = info;
    }
  }

  private Collection<PolicyInfo> all; // list of all policies
  List<PathToPolicy> pathToPolicy;    // find policy based on longest path match

  private PendingReplication filesBeingFixed;  // files that are being fixed


  ThreadPoolExecutor executor;         // threads to fix blocks

  FileFixer(Configuration conf) throws IOException {
    this.conf = conf;
    blockFixInterval = conf.getInt("hightide.blockfix.interval",
                                   blockFixInterval);
    numThreads = conf.getInt("hightide.blockfix.numthreads", numThreads);

    pathToPolicy = new LinkedList<PathToPolicy>();
    executor = new ThreadPoolExecutor( numThreads, numThreads,
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>());

   // start a thread to purge enties from this set automatically
   filesBeingFixed = new PendingReplication(conf.getInt(
                           "dfs.hightide.pending.timeout.sec", -1) * 1000L);
  }

  /**
   * The list of all configured policies.
   */
  void setPolicyInfo(Collection<PolicyInfo> all) throws IOException {
    this.all = all;
    this.pathToPolicy.clear();

    // keep a reverse map from all top-level paths to policies
    for (PolicyInfo pinfo: all) {
      pathToPolicy.add(new PathToPolicy(pinfo.getSrcPath(), pinfo));
      for (PathInfo d:pinfo.getDestPaths()) {
        pathToPolicy.add(new PathToPolicy(d.rpath, pinfo));
      }
    }

    // keep all paths sorted in revere lexicographical order so that 
    // we longest path is first.
    Comparator<PathToPolicy> comp = new Comparator<PathToPolicy>() {
      public int compare(PathToPolicy p1, PathToPolicy p2) {
        return 0 - p1.spath.compareTo(p2.spath);
      }
    };
    Collections.sort(pathToPolicy, comp);
  } 

  /**
   * A singleton thread that finds corrupted files and then schedules
   * blocks to be copied. This thread talks only to NameNodes and does
   * not talk to any datanodes.
   */
  public void run() {
    while (running) {
      try {
        LOG.info("FileFixer continuing to run...");
        doFindFiles();
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      } catch (Error err) {
        LOG.error("Exiting after encountering " +
                    StringUtils.stringifyException(err));
        shutdown();
        throw err;
      }
      try {
        // Sleep before proceeding to fix more files.
        Thread.sleep(blockFixInterval);
      } catch (InterruptedException ie) {
        LOG.error("Encountering InturruptedException " +
                   StringUtils.stringifyException(ie));
      }
    }
  }

  /*
   * Release all resources, shutdown any threads
   */
  void shutdown() {
    running = false;
    filesBeingFixed.stop();
  }

  /*
   * returns the FileSystem of the path. If the FileSystem is down, then
   * log an error and return null
   */
  static FileSystem getFs(Configuration conf, Path p) {
    try {
      return p.getFileSystem(conf);
    } catch (Exception e) {
      // if a single namenode is down, log it and ignore. Continue to
      // fix other namenodes.
      LOG.warn("getFs: Unable to contact filesystem: " + p + " ignoring.... " +
               e);
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Poll namenode(s) to find corrupted files. Enqueue blocks for replication 
   * if needed.
   */
  private void doFindFiles() throws IOException {
    Set<FileSystem> allFs = new HashSet<FileSystem>();
    Set<Path> filesToFix = new HashSet<Path>();      // files that are yet to be fixed

    // collect all unique filesystems in all policies.
    for (PolicyInfo pinfo: all) {
      FileSystem fs = getFs(pinfo.getConf(), pinfo.getSrcPath());
      if (fs != null) {
        allFs.add(fs);
      }
      for (PathInfo d:pinfo.getDestPaths()) {
        fs = getFs(pinfo.getConf(), d.rpath);
        if (fs != null) {
          allFs.add(fs);
        }
      }
    }

    // make a RPC to all relevant namenodes to find corrupt files
    for (FileSystem fs:allFs) {
      if (!running) break;
      List<Path> corruptFiles = null;

      corruptFiles = getCorruptFilesFromNamenode(fs);

      // if we are not already fixing this one, then put it in the list
      // of files that need fixing.
      for (Path p : corruptFiles) {
        if (filesBeingFixed.add(p)) {
           filesToFix.add(p);
        }
      }
    }

    if (!filesToFix.isEmpty()) {
      LOG.info("Found " + filesToFix.size() + " corrupt files.");
    }

    for (Path path: filesToFix) {
      if (!running) break;
      try {
        fixFile(path);
      } catch (IOException ie) {
        LOG.error("Error while processing " + path +
          ": " + StringUtils.stringifyException(ie));
        // For certain kinds of errors, it might be good if we remove
        // this file from filesBeingFixed, so that the file-fix gets
        // attemted in the immediate next iteration. For example, if
        // we get a network Exception, we can retry immediately. On 
        // the other hand, if we get a file length mismatch exception
        // then no amount of retry will fix it, so it is better to 
        // retry less frequently.
      }
    }
  }

  /**
   * Fix a specific file
   */
  private void fixFile(Path badFile) throws IOException {

    PolicyInfo pinfo = null;
    String filename = badFile.toString();

    LOG.info("File  = file to fix:" + badFile);

    // Find the policy that maps this file
    for (PathToPolicy pp: pathToPolicy) {
      if (filename.startsWith(pp.spath)) {
        pinfo = pp.pinfo;
        break;
      }
    }
    if (pinfo == null) {
      throw new IOException("Unable to find matching policy for " +
                            badFile);
    }

    // process the file and fix it. 
    Path src;
    HighTideNode.getMetrics().fixAttempt.inc();
    
    if (filename.startsWith(pinfo.getSrcPath().toString())) {
      // srcPath is corrupted, pick the first destPath as source of truth.
      String[] splits = filename.split(pinfo.getSrcPath().toString());
      src = new Path(pinfo.getDestPaths().get(0).rpath.toString() + splits[1]);
    } else {
      // dest file is corrupted, copy from source to destination
      String[] splits = filename.split(pinfo.getDestPaths().get(0).rpath.toString());
      src = new Path(pinfo.getSrcPath().toString() + splits[1]);
    }
    DistributedFileSystem srcFs = (DistributedFileSystem) src.getFileSystem(pinfo.getConf());
    DistributedFileSystem destFs = (DistributedFileSystem) badFile.getFileSystem(pinfo.getConf());

    FileStatus sstat = srcFs.getFileStatus(src);
    FileStatus dstat = destFs.getFileStatus(badFile);

    // assert that modtime of the two files are same
    if (sstat.getModificationTime() != dstat.getModificationTime()) { 
      String msg = "Unable to fix file " + badFile +
        " because src " + src + " has modification time as " + 
        HighTideNode.dateForm.format(new Date(sstat.getModificationTime())) +
        " but destination " + badFile + " has modification time as " +
        HighTideNode.dateForm.format(new Date(dstat.getModificationTime()));
      LOG.error(msg);
      HighTideNode.getMetrics().fixFailedModTimeMismatch.inc();
      throw new IOException(msg);
    }
    
    // check that blocksize of the two files are same
    if (sstat.getBlockSize() != dstat.getBlockSize()) {
      String msg = "Unable to fix file " + badFile +
        " because src " + src + " has blocksize as " + 
        sstat.getBlockSize() + 
        " but destination " + badFile + " has blocksize as " +
        dstat.getBlockSize();
      LOG.error(msg);
      HighTideNode.getMetrics().fixFailedBlockSizeMismatch.inc();
      throw new IOException(msg);
    }
    
    // check that size of the two files are same
    if (sstat.getLen() != dstat.getLen()) {
      String msg = "Unable to fix file " + badFile +
        " because src " + src + " has size as " + 
        sstat.getLen() + 
        " but destination " + badFile + " has size as " +
        dstat.getLen();
      LOG.error(msg);
      HighTideNode.getMetrics().fixFailedFileLengthMismatch.inc();
      throw new IOException(msg);
    }

    List<LocatedBlock> badBlocks = corruptBlocksInFile(destFs, badFile.toUri().getPath(), dstat);
    List<LocatedBlock> goodBlocks = srcFs.getClient().namenode.getBlockLocations(
                                      src.toUri().getPath(), 0L, sstat.getLen()).getLocatedBlocks();

    // for each of the bad blocks, find the good block
    for (LocatedBlock badBlock: badBlocks) {
      LocatedBlock found = null;
      for (LocatedBlock goodBlock: goodBlocks) {
        if (badBlock.getStartOffset() == goodBlock.getStartOffset()) {
          found = goodBlock;
          break;
        }
      }
      if (found == null || found.getLocations().length == 0) {
        String msg = "Could not find a good block location for badBlock " + badBlock +
                     " in file " + badFile;
        LOG.error(msg);
        HighTideNode.getMetrics().fixFailedNoGoodBlock.inc();
        throw new IOException (msg);
      }

      // execute asynchronously
      WorkItem bp = new WorkItem(badFile, found, badBlock, destFs, conf);
      LOG.info("Queueing up block " + badBlock.getBlock().getBlockName() + 
               " to be fixed from block " + found.getBlock().getBlockName());
      executor.execute(bp);
    }
  }

  /**
   * @return A list of corrupt files as obtained from the namenode
   * If the namenode is down, then return an empty list.
   */
  List<Path> getCorruptFilesFromNamenode(FileSystem fs) throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Only DistributedFileSystem can be handled " +
                            " by HighTide.");
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    List<Path> corruptFiles = new LinkedList<Path>();

    try {
      LOG.info("Checking filesystem: " + dfs.getUri());
      String[] files = 
        DFSUtil.getCorruptFiles(dfs);
      for (String f: files) {
        Path p = new Path(f).makeQualified(fs);
        corruptFiles.add(p);
      }
      return corruptFiles;
    } catch (Exception e) {
      // if a single namenode is down, log it and ignore. Continue to
      // fix other namenodes.
      LOG.warn("getCorruptFilesFromNamenode: Unable to contact filesystem: " + fs.getUri() +
               " ignoring..." + e);
      e.printStackTrace();
      return corruptFiles;
    }
  }

  /**
   * Returns the corrupt blocks in a file.
   **/
  List<LocatedBlock> corruptBlocksInFile(
    DistributedFileSystem fs, String uriPath, FileStatus stat)
  throws IOException {
    List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
    LocatedBlocks locatedBlocks = fs.getClient().namenode.getBlockLocations(
      uriPath, 0, stat.getLen());
    for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
      if (b.isCorrupt() || 
         (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
        LOG.info("Adding bad block for file " + uriPath);
        corrupt.add(b);
      }
    }
    return corrupt;
  }

  /**
   * Setup a session with the specified datanode
   */
  static ClientDatanodeProtocol createClientDatanodeProtocolProxy (
      DatanodeInfo datanodeid, Configuration conf) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
    }
    try {
      return (ClientDatanodeProtocol)RPC.getProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, conf);
    } catch (RPC.VersionMismatch e) {
      long clientVersion = e.getClientVersion();
      long datanodeVersion = e.getServerVersion();
      if (clientVersion > datanodeVersion &&
          !ProtocolCompatible.isCompatibleClientDatanodeProtocol(
              clientVersion, datanodeVersion)) {
        throw new RPC.VersionIncompatible(
            ClientDatanodeProtocol.class.getName(), clientVersion, datanodeVersion);
      }
      return (ClientDatanodeProtocol)e.getProxy();
    }
  }

  // a class to store pairs of blocks.
  static class WorkItem implements Runnable {
    Path badfile;                    // file to be fixed
    LocatedBlock goodBlock;          // existing replica of missing block
    LocatedBlock badBlock;           // missing block
    DistributedFileSystem destFs;    // filesystem of destination
    Configuration conf;
    private static Random rand = new Random();

    WorkItem(Path file, LocatedBlock g, LocatedBlock b, FileSystem fs, Configuration conf) {
      this.goodBlock = g;
      this.badBlock = b;
      this.badfile = file;
      this.destFs = (DistributedFileSystem)fs;
      this.conf = conf;
    }

    @Override
    public void run() {

      String msg = "";
      try {
        // find a random datanode from the destination cluster
        DatanodeInfo[] targets = destFs.getClient().datanodeReport(DatanodeReportType.LIVE);
        DatanodeInfo target = targets[rand.nextInt(targets.length)];

        // find a source datanode from among the datanodes that host this block
        DatanodeInfo srcdn  = goodBlock.getLocations()[rand.nextInt(goodBlock.getLocations().length)];
      
        // The RPC is asynchronous, i.e. the RPC will return immediately even before the
        // physical block copy occurs from the datanode.
        msg = "File " + badfile + ": Copying block " + 
              goodBlock.getBlock().getBlockName() + " from " + srcdn.getName() +
              " to block " + badBlock.getBlock().getBlockName() + 
              " on " + target.getName();
        LOG.info(msg);
        ClientDatanodeProtocol datanode = createClientDatanodeProtocolProxy(srcdn, conf);
        datanode.copyBlock(goodBlock.getBlock(), badBlock.getBlock(), target);
        RPC.stopProxy(datanode);
        HighTideNode.getMetrics().fixSuccessfullyStarted.inc();
      } catch (Throwable e) {
        HighTideNode.getMetrics().fixFailedDatanodeError.inc();
        LOG.error(StringUtils.stringifyException(e) + msg + ". Failed to contact datanode.");
      }
    }
  }
}

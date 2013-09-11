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
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

/**********************************************************
 * A utility that alows users to add files to a "waiting
 * room" instead of deleting them so that any file linked
 * by some snapshot is not deleted permanently. The file
 * system must moveFileToWaitingRoom() rather than 
 * delete(), except if delete() is called by the Waiting
 * RoomPurger.
 *
 * Implements a Runnable Waiting Room Purger which can be 
 * run as a Daemon to lazily remove unnecessary files from 
 * the waiting room.
 **********************************************************/
public class WaitingRoom {

  public static final Log LOG =
      LogFactory.getLog(WaitingRoom.class);

  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private Configuration conf; // conf
  private FileSystem dfs; // file system

  private Path WR; // file waiting room path

  public WaitingRoom(Configuration conf) throws IOException {
    this(conf, FileSystem.get(conf));
  }

  public WaitingRoom(Configuration conf, FileSystem dfs) throws IOException {
    this.conf = conf;
    this.dfs = dfs;
    init();
  }

  /**
   * Initialize WaitingRoom
   * @throws IOException
   */
  private void init() throws IOException {
    WR = new Path(conf.get("fs.snapshot.waitingroom", "/.WR"));
    WR = WR.makeQualified(dfs);
  }

  public Path getWaitingRoomPath() {
    return WR;
  }

  public void setWaitingRoomPath(String path) {
    WR = new Path(path);
    WR = WR.makeQualified(dfs);
  }

  /**
   * Moves a file/dir to the waiting room
   *
   * @param path Path to file/dir
   * @return false if move failed, true otherwise
   */
  public boolean moveToWaitingRoom(Path path) throws IOException {
    // Make path absolute
    if (!path.isAbsolute()) path = new Path(dfs.getWorkingDirectory(), path);

    // Check if path is valid
    if (!dfs.exists(path)) throw new FileNotFoundException(path.toString());

    // Check if file already in waiting area
    String qPath = path.makeQualified(dfs).toString();
    if (qPath.startsWith(WR.toString())) return false;

    // Check if trying to move waiting room or its parent dir to 
    // waiting room
    if (WR.toString().startsWith(qPath)) {
      throw new IOException("Can't delete " + path + 
                        " as it contains the waiting room directory.");
    }

    String fName = path.getName();
    Path baseWRPath = getWaitingRoomPath(path.getParent());

    // Make dir(s) for base
    Stack<Path> parentDirs = new Stack<Path>();
    do {
      parentDirs.push(baseWRPath);
      baseWRPath = baseWRPath.getParent();
    } while (baseWRPath != null);

    while (!parentDirs.empty()) {
      baseWRPath = parentDirs.pop();

      // Create new dir with appended .WRx if already exists.
      for (int i = 0; dfs.exists(baseWRPath) && !dfs.getFileStatus(baseWRPath).isDir(); i++) {
        baseWRPath = new Path(baseWRPath.toString() + ".WR" + i);
      }

      if (!dfs.mkdirs(baseWRPath, PERMISSION)) {
        LOG.warn("Couldn't create base dir path in waiting room for " + baseWRPath);
        return false;
      }
    }

    // Rename file/dir to waiting room. Append .WRx if already exists.
    Path myWRPath = new Path(baseWRPath.toString(), fName);
    for (int i = 0; dfs.exists(myWRPath); i++) {
      myWRPath = new Path(myWRPath.toString() + ".WR" + i);
    }

    if (dfs.rename(path, myWRPath)) return true; // success

    return false;
  }

  private Path getWaitingRoomPath(Path p) {
    return new Path(WR.toString() +  p.toUri().getPath().toString());
  }

  public WaitingRoomPurger getPurger() {
    return new WaitingRoomPurger(conf, dfs);
  }

  public static class WaitingRoomPurger implements Runnable {
    public static final Log LOG =
      LogFactory.getLog(WaitingRoomPurger.class);

    private Configuration conf;
    private FileSystem dfs;

    private String ssDir;
    private String wrDir; 
    private long cleanupPeriod; // in seconds, DEFAULT 24 hours

    private boolean shouldRun;

    private Map<Path, List<Long>> fileMap; // maps file paths to block id list
    private Map<Long, Object> blockRefMap; // contains only unreferenced block ids

    public WaitingRoomPurger(Configuration conf, FileSystem dfs) {
      this.conf = conf;
      this.dfs = dfs;
      this.ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");
      this.wrDir = conf.get("fs.snapshot.waitingroom", "/.WR");
      this.cleanupPeriod = conf.getLong("fs.snapshot.cleanup", 86400);
      this.fileMap = new HashMap<Path, List<Long>>();
      this.blockRefMap = new HashMap<Long, Object>();
    }

    public void shutdown() {
      shouldRun = false;
    }

    private void filterMapsWithSnapshots() throws IOException {
      if (this.fileMap == null) return;

      Path ssPath = new Path(ssDir);

      if (!dfs.exists(ssPath)) return; // ss dir doesn't exist

      FileStatus ssStatus = dfs.getFileStatus(ssPath);
      if (!ssStatus.isDir()) {
        throw new IOException("ssDir " + ssDir + " is not a directory");
      }

      List<FileStatus> snapshots = new ArrayList<FileStatus>();
      FileStatus[] files = dfs.listStatus(ssPath);

      // Separate snapshot files
      for (FileStatus ss: files) {
        if (ss.isDir()) continue; // skips dirs
        String name = ss.getPath().getName();
        if (!name.startsWith("dfs_snapshot_")) continue; // not ss file

        snapshots.add(ss);
      }

      // Filter map with each snapshot
      for (FileStatus ss: snapshots) {
        // Short circuit check
        if (blockRefMap.isEmpty()) return;

        filterMapsWithSnapshot(ss.getPath());
      }
    }

    private void expungeWaitingRoom() throws IOException {
      // Short circuit check
      if (blockRefMap.isEmpty()) return;

      // Find unreferenced files in waiting room and delete them
      outer:
      for (Path path: fileMap.keySet()) {
        List<Long> blockIds = fileMap.get(path);
	int numOfBlocks = blockIds.size();

	// Check if all blocks in file are unreferenced
        for (Long id: blockIds) {
          if (!blockRefMap.containsKey(id)) {
            numOfBlocks--;
          }
        }

        boolean sanityCheck = numOfBlocks == 0 || numOfBlocks == blockIds.size();
        if (!sanityCheck) {
          LOG.warn("File at " + path + " has partially references blocks.");
        }
	
        // If all blocks in file were not referenced, so delete it!
        // NOTE: This will also delete all empty files.
        if (numOfBlocks == blockIds.size()) {
          LOG.info("Deleting file at " + path + " permanently!");
          dfs.delete(path);
        }
      }

      // Delete empty dirs in waiting room
      deleteEmptyDirs();
    }

    private void filterMapsWithSnapshot(Path path) {
      LOG.info("Filtering WaitingRoomPurger maps with snapshot at " + path);

      try {
        FSImage fsImage = new FSImage();
        FSNamesystem namesystem = new FSNamesystem(fsImage, conf);
        FSDataInputStream in = dfs.open(path);

        // Load in snapshot image
        fsImage.loadFSImage(new File(path.toString()), in);

        // Filter block reference map with files in snapshot
        filterMapWithInode(namesystem.dir.rootDir);

        fsImage.close();
        LOG.info("Successfully filtered WaitingRoomPurger maps with snapshot at " + path);
      } 
      catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        LOG.error("Could not load snapshot at " + path + " for filtering");
      }
    }

    private void filterMapWithInode(INode node) {
      // Must NOT filter with files in WaitingRoom already!
      if (node.getFullPathName().startsWith(wrDir)) return;

      LOG.info("Filtering WaitingRoomMaps with inode " + node.getFullPathName());

      if (node.isDirectory()) {
        INodeDirectory dir = (INodeDirectory) node;
        for (INode child: dir.getChildren()) {
          filterMapWithInode(child);
        }
      } else {
        BlockInfo[] blocks = ((INodeFile)node).getBlocks();

        // Mark all blocks of this file as referenced
        for (BlockInfo block: blocks) {
          blockRefMap.remove(block.getBlockId());
        }
      }
    }

    private void deleteEmptyDirs() throws IOException{
      Path wrRoot = new Path(wrDir);

      for (FileStatus fs: dfs.listStatus(wrRoot)) {
        if (fs.isDir()) {
          deleteEmptySubDirs(fs.getPath());
        }
      }
    }

    private void deleteEmptySubDirs(Path dir) throws IOException {
      // Recursively check sub dirs first
      for (FileStatus fs: dfs.listStatus(dir)) {
        if (fs.isDir()) {
          deleteEmptySubDirs(fs.getPath());
        }
      }

      // If dir is empty, delete it
      if (dfs.listStatus(dir).length == 0) {
        dfs.delete(dir);
      }
    }

    /**
     * Loads mapping of all files in WaitingRoom to their block list in
     * to the fileMap. Loads all blocks that are part of files in 
     * WaitingRoom into blockRefMap.
     */
    private void loadMaps() throws IOException {
      LOG.info("Loading WaitingRoomPurger maps.");

      fileMap.clear();
      blockRefMap.clear();

      DFSClient client = new DFSClient(conf);

      Path wrRoot = new Path(wrDir);
      addDirToMaps(wrRoot, client);

      client.close();

      LOG.info("WaitingRoomPurger maps loaded successfully.");
    }

    private void addDirToMaps(Path dir, DFSClient client) throws IOException {
      FileStatus[] children = dfs.listStatus(dir);

      if (children == null) return;

      for (FileStatus child: children) {
        if (!child.isDir()) { // get block ids for file
          Path path = child.getPath(); // paths will be unique
          fileMap.put(path, new ArrayList<Long>());

          DFSInputStream stm = client.open(child.getPath().toUri().getPath());
          LocatedBlocks blocks = stm.fetchLocatedBlocks();
          stm.close();

          for (int i = 0; i < blocks.locatedBlockCount(); i++) {
            Long blockId = blocks.get(i).getBlock().getBlockId();
            fileMap.get(path).add(blockId); // add to file block list
            blockRefMap.put(blockId, null); // mark as unrefereced
          }
        }
        else {
          // If child is a directory, recurse on it
          addDirToMaps(child.getPath(), client);
        }
      }
    }

    void purge() throws IOException {
      loadMaps();
      filterMapsWithSnapshots();
      expungeWaitingRoom();
    }

    @Override
    public void run() {
      shouldRun = true;

      while (shouldRun) {
        try {
          purge();
        }
        catch (IOException e) {
          LOG.error("Exception in WaitingRoomPurger: ");
          LOG.error(StringUtils.stringifyException(e));
          e.printStackTrace();
        }

        try {
          Thread.sleep(cleanupPeriod * 1000);
        }
        catch (InterruptedException e) {
          // do nothing
        }
      }
    }
  }
}
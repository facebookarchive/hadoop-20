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

package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DeleteUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedBlockFileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.PathValidator;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;


/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;
  private boolean clearOsBuffer = false;
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  private PathValidator pathValidator;

  public DistributedFileSystem() {
  }

  /** @deprecated */
  public DistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  /** @deprecated */
  public String getName() { return uri.getAuthority(); }

  public URI getUri() { return uri; }

  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }

    InetSocketAddress namenode = NameNode.getAddress(uri.getAuthority());
    this.dfs = new DFSClient(namenode, null, conf, statistics, getUniqueId(),
        this);
    this.uri = NameNode.getUri(namenode);
    this.workingDir = getHomeDirectory();
    pathValidator = new PathValidator(conf);
  }

  /** Permit paths which explicitly specify the default port. */
  protected void checkPath(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && thisUri.getPort() == -1
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority()))
      return;
    super.checkPath(path);
  }

  /** Normalize paths that explicitly specify the default port. */
  public Path makeQualified(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && thisUri.getPort() == -1
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority())) {
      path = new Path(thisUri.getScheme(), thisUri.getAuthority(),
                      thatUri.getPath());
    }
    return super.makeQualified(path);
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

  public short getDefaultReplication() {
    return dfs.getDefaultReplication();
  }

  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      FileSystem.LogForCollect.info("makeAbsolute: " + f
          + " working directory: " + workingDir);
      return new Path(workingDir, f);
    }
  }

  public void setWorkingDirectory(Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!pathValidator.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  /** {@inheritDoc} */
  public Path getHomeDirectory() {
    return new Path("/user/" + dfs.ugi.getUserName()).makeQualified(this);
  }

  private String getPathName(Path file) {
    checkPath(file);
    String result = makeAbsolute(file).toUri().getPath();
    if (!pathValidator.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
                                         file+" is not a valid DFS filename.");
    }
    return result;
  }
  

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    return dfs.getBlockLocations(getPathName(file.getPath()), start, len);
  }
  
  public LocatedBlocks getLocatedBlocks(Path filePath, long start,
      long len) throws IOException {
    if (filePath == null) {
      return null;
    }
    return dfs.getLocatedBlocks(getPathName(filePath), start, len);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /**
   * Removes data from OS buffers after every read
   */
  @Override
  public void clearOsBuffer(boolean clearOsBuffer) {
    this.clearOsBuffer = clearOsBuffer;
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return open(f, bufferSize, new ReadOptions());
  }

  public FSDataInputStream open(Path f, int bufferSize,
      ReadOptions options) throws IOException {
    return new DFSClient.DFSDataInputStream(
          dfs.open(getPathName(f), bufferSize, verifyChecksum, statistics,
                   clearOsBuffer, options));
  }

  /**
   * Start lease recovery
   *
   * @param f the name of the file to start lease recovery
   * @return if the file is closed or not
   * @throws IOException
   */
  @Deprecated
  public boolean recoverLease(Path f) throws IOException {
    return dfs.recoverLease(getPathName(f), false);
  }

  /**
   * Start lease recovery
   *
   * @param f the name of the file to start lease recovery
   * @param discardLastBlock discard last block if it is not yet hsync-ed.
   * @return if the file is closed or not
   * @throws IOException
   */
  public boolean recoverLease(Path f, boolean discardLastBlock) throws IOException {
    return dfs.recoverLease(getPathName(f), discardLastBlock);
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {

    DFSOutputStream op = (DFSOutputStream)dfs.append(getPathName(f), bufferSize, progress);
    return new FSDataOutputStream(op, statistics, op.getInitialLen());
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite,
    int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {
      return create(f, permission, overwrite, bufferSize,
        replication,
        blockSize,
        getConf().getInt("io.bytes.per.checksum",
            FSConstants.DEFAULT_BYTES_PER_CHECKSUM), progress);

  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      int bytesPerChecksum, Progressable progress) throws IOException {
    return new FSDataOutputStream
    (dfs.create(getPathName(f), permission,
                overwrite, true, replication, blockSize, progress, bufferSize,
                bytesPerChecksum),
     statistics);

  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      int bytesPerChecksum, Progressable progress, InetSocketAddress[] favoredNodes,
      WriteOptions options)
      throws IOException {
    return new FSDataOutputStream(dfs.create(getPathName(f), permission,
        overwrite, true, replication, blockSize, progress, bufferSize,
        bytesPerChecksum, false, false, favoredNodes , options), statistics);
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      int bytesPerChecksum, Progressable progress,
      InetSocketAddress[] favoredNodes) throws IOException {
    return create(f, permission, overwrite, bufferSize, replication, blockSize,
        bytesPerChecksum, progress, favoredNodes, new WriteOptions());
  }
  
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
      short replication, long blockSize, boolean forceSync)
    throws IOException {
    return create(f, FsPermission.getDefault(), overwrite, bufferSize,
                  replication, 
                  blockSize, 
                  getConf().getInt("io.bytes.per.checksum",
                      FSConstants.DEFAULT_BYTES_PER_CHECKSUM),
                  null,
                  forceSync,
                  false);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      int bytesPerChecksum, Progressable progress, boolean forceSync)
    throws IOException {
    return create(f, permission, overwrite, bufferSize,
                  replication, blockSize, bytesPerChecksum,
                  progress, forceSync, false);
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      int bytesPerChecksum, Progressable progress, boolean forceSync,
      boolean doParallelWrites)
  throws IOException {
    return new FSDataOutputStream
    (dfs.create(getPathName(f), permission,
                overwrite, true, replication, blockSize, progress, bufferSize,
                bytesPerChecksum,forceSync, doParallelWrites),
     statistics);

  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   * @see #create(Path, FsPermission, boolean, int, short, long, Progressable)
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize, 
      Progressable progress) throws IOException {

    return new FSDataOutputStream
        (dfs.create(getPathName(f), permission, 
                    overwrite, false, replication, blockSize, progress, bufferSize), 
         statistics);
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress, boolean forceSync, boolean doParallelWrites,
      WriteOptions options)
    throws IOException {
    return new FSDataOutputStream
        (dfs.create(getPathName(f), permission,
                    overwrite, false, replication, blockSize, progress, bufferSize,
                    getConf().getInt("io.bytes.per.checksum",
                      FSConstants.DEFAULT_BYTES_PER_CHECKSUM), forceSync,
                    doParallelWrites, null, options), statistics);
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   * @see #create(Path, FsPermission, boolean, int, short, long, Progressable,
   * boolean)
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress, boolean forceSync, boolean doParallelWrites)
  throws IOException {

    return new FSDataOutputStream
        (dfs.create(getPathName(f), permission, overwrite, false, replication,
		    blockSize, progress, bufferSize,
                    forceSync, doParallelWrites),
         statistics);
  }

  public boolean setReplication(Path src, 
                                short replication
                               ) throws IOException {
    return dfs.setReplication(getPathName(src), replication);
  }

  /**
   * THIS IS DFS only operations, it is not part of FileSystem
   * move blocks from srcs to trg
   * and delete srcs afterwards
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @param restricted - should the equal block sizes be enforced
   * @throws IOException
   */
  public void concat(Path trg, Path [] psrcs, boolean restricted) throws IOException {
    String [] srcs = new String [psrcs.length];
    for(int i=0; i<psrcs.length; i++) {
      srcs[i] = getPathName(psrcs[i]);
    }
    dfs.concat(getPathName(trg), srcs, restricted);
  }
  
  /**
   * THIS IS DFS only operations, it is not part of FileSystem
   * move blocks from srcs to trg
   * and delete srcs afterwards
   * All blocks should be of the same size
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @throws IOException
   */
  @Deprecated
  public void concat(Path trg, Path [] psrcs) throws IOException {
    concat(trg, psrcs, true);
  }
  
  /**
   * See {@link ClientProtocol#merge(String, String, int[]) 
   */
  public void merge(Path parity, Path source, String codecId, 
      int[] checksums) throws IOException {
    dfs.merge(getPathName(parity), getPathName(source), codecId, checksums);
  }
  
  /** 
   * See {@link ClientProtocol#hardLink(String, String)}. 
   */ 
  @Override
  public boolean hardLink(Path src, Path dst) throws IOException {  
    return dfs.hardLink(getPathName(src), getPathName(dst));  
  }

  /**
   * See {@link ClientProtocol#getHardLinkedFiles(String)}.
   */
  @Override
  public String[] getHardLinkedFiles(Path src) throws IOException {
    return dfs.getHardLinkedFiles(getPathName(src));
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    return dfs.rename(getPathName(src), getPathName(dst));
  }

  /**
   * Get rid of Path f, whether a true file or dir.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }
  
  /**
   * requires a boolean check to delete a non 
   * empty directory recursively.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    return delete(f, recursive, false);
  }
  
  /**
   * See {@link ClientProtocol#raidFile(String, String, short)
   */
  public boolean raidFile(Path source, String codecId, short expectedSourceRepl)
    throws IOException {
    return dfs.raidFile(getPathName(source), codecId, expectedSourceRepl);
  }

  @Override
  public boolean delete(Path f, boolean recursive, boolean skipTrash)
      throws IOException {
    if (skipTrash) {
      return deleteSkipTrash(getPathName(f), recursive);
    } else {
      return deleteInternal(getConf(), getPathName(f), recursive);
    }
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    return dfs.getContentSummary(getPathName(f));
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long) 
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    dfs.setQuota(getPathName(src), namespaceQuota, diskspaceQuota);
  }
  
  public FileStatus[] listStatus(Path p) throws IOException {
    FileStatus[] infos = dfs.listPaths(getPathName(p));
    if (infos == null) return null;
    for (int i = 0; i < infos.length; i++) {
      infos[i].makeQualified(this);
    }
    return infos;
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter)
  throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private RemoteIterator<LocatedFileStatus> itor =
        dfs.listPathWithLocation(getPathName(p));
      private LocatedFileStatus curStat = null;


      @Override
      public boolean hasNext() throws IOException {
        while (curStat == null && itor.hasNext()) {
          LocatedFileStatus next =itor.next();
          next.makeQualified(DistributedFileSystem.this);
          if (filter.accept(next.getPath())) {
            curStat = next;
          }
        }
        return curStat != null;
      }
     
      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException("No more entry in " + p);
        }
        LocatedFileStatus tmp = curStat;
        curStat = null;
        return tmp;
      }
    };
  }
 
  @Override
  public RemoteIterator<LocatedBlockFileStatus> listLocatedBlockStatus(
      final Path p, final PathFilter filter)
  throws IOException {
    return new RemoteIterator<LocatedBlockFileStatus>() {
      private RemoteIterator<LocatedBlockFileStatus> itor =
        dfs.listPathWithBlockLocation(getPathName(p));
      private LocatedBlockFileStatus curStat = null;


      @Override
      public boolean hasNext() throws IOException {
        while (curStat == null && itor.hasNext()) {
          LocatedBlockFileStatus next =itor.next();
          next.makeQualified(DistributedFileSystem.this);
          if (filter.accept(next.getPath())) {
            curStat = next;
          }
        }
        return curStat != null;
      }
     
      @Override
      public LocatedBlockFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException("No more entry in " + p);
        }
        LocatedBlockFileStatus tmp = curStat;
        curStat = null;
        return tmp;
      }
    };
  }
 
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return dfs.mkdirs(getPathName(f), permission);
  }

  /**
   * Fetch the list of files that have been open longer than a
   * specified amount of time.
   * @param prefix path prefix specifying subset of files to examine
   * @param millis select files that have been open longer that this
   * @param start where to start searching in the case of large number
   * of files returns, or null
   * @return array of OpenFileInfo objects
   * @throw IOException
   */
  @Override
  public OpenFileInfo[] iterativeGetOpenFiles(
    Path prefix, int millis, String start)
    throws IOException{
    return dfs.iterativeGetOpenFiles(prefix, millis, start);
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
    try {
      super.processDeleteOnExit();
      if (dfs != null) {
        dfs.close();
      }
    } finally {
      super.close();
    }
  }

  public String toString() {
    return "DFS[" + dfs + "]";
  }

  public DFSClient getClient() {
    return dfs;
  }        
  
  public static class DiskStatus {
    private long capacity;
    private long dfsUsed;
    private long remaining;
    public DiskStatus(long capacity, long dfsUsed, long remaining) {
      this.capacity = capacity;
      this.dfsUsed = dfsUsed;
      this.remaining = remaining;
    }
    
    public long getCapacity() {
      return capacity;
    }
    public long getDfsUsed() {
      return dfsUsed;
    }
    public long getRemaining() {
      return remaining;
    }
  }
  

  /** Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space */
  public DiskStatus getDiskStatus() throws IOException {
    return dfs.getDiskStatus();
  }
  
  /** Return the total raw capacity of the filesystem, disregarding
   * replication .*/
  public long getRawCapacity() throws IOException{
    return dfs.totalRawCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication .*/
  public long getRawUsed() throws IOException{
    return dfs.totalRawUsed();
  }
   
  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * 
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * 
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * 
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    return new CorruptFileBlockIterator(dfs, path);
  }

  /** Return statistics for each live datanode. */
  public DatanodeInfo[] getLiveDataNodeStats() throws IOException {
    return dfs.datanodeReport(DatanodeReportType.LIVE);
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return dfs.datanodeReport(DatanodeReportType.ALL);
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
  throws IOException {
    return dfs.setSafeMode(action);
  }

  /**
   * Save namespace image.
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public void saveNamespace(boolean force, boolean uncompressed)
  throws AccessControlException, IOException {
    dfs.saveNamespace(force, uncompressed);
  }

  /**
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }
  
  /**
   * Rolls edit log at the namenode manually.
   */
  public void rollEditLog() throws IOException {
    dfs.rollEditLog();
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    dfs.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
  ) throws IOException {
    return dfs.distributedUpgradeProgress(action);
  }

  /*
   * Requests the namenode to dump data strcutures into specified 
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    dfs.metaSave(pathname);
  }

  /**
   * Enable/Disable Block Replication
   */
  public void blockReplication(boolean isEnable) throws IOException {
    dfs.blockReplication(isEnable);
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {

    LocatedBlock lblocks[] = new LocatedBlock[2];

    // Find block in data stream.
    DFSClient.DFSDataInputStream dfsIn = (DFSClient.DFSDataInputStream) in;
    Block dataBlock = dfsIn.getCurrentBlock();
    if (dataBlock == null) {
      LOG.error("Error: Current block in data stream is null! ");
      return false;
    }
    DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()}; 
    lblocks[0] = new LocatedBlock(dataBlock, dataNode);
    LOG.info("Found checksum error in data stream at block="
        + dataBlock + " on datanode="
        + dataNode[0].getName());

    // Find block in checksum stream
    DFSClient.DFSDataInputStream dfsSums = (DFSClient.DFSDataInputStream) sums;
    Block sumsBlock = dfsSums.getCurrentBlock();
    if (sumsBlock == null) {
      LOG.error("Error: Current block in checksum stream is null! ");
      return false;
    }
    DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()}; 
    lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
    LOG.info("Found checksum error in checksum stream at block="
        + sumsBlock + " on datanode="
        + sumsNode[0].getName());

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus fi = dfs.getFileInfo(getPathName(f));
    if (fi != null) {
      fi.makeQualified(this);
      return fi;
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }

  /** {@inheritDoc} */
  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
    return dfs.getFileChecksum(getPathName(f));
  }

  /** {@inheritDoc} */
  public int getFileCrc(Path f) throws IOException {
    return dfs.getFileCrc(getPathName(f));
  }

  
  /** {@inheritDoc }*/
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    dfs.setPermission(getPathName(p), permission);
  }

  /** {@inheritDoc }*/
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    dfs.setOwner(getPathName(p), username, groupname);
  }

  /** {@inheritDoc }*/
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    dfs.setTimes(getPathName(p), mtime, atime);
  }

  public String getClusterName() throws IOException {
    return dfs.getClusterName();
  }

  /** Re-populate the namespace and diskspace count of every node with quota */
  public void recount() throws IOException {
    dfs.recount();
  }

  private boolean deleteUsingTrash(Configuration conf, String file,
      boolean recursive) throws IOException {
    String errmsg = "File " + file + " is being deleted only through"
        + " Trash.";
    LOG.info(errmsg);
    Path p = new Path(file);
    try {
      DeleteUtils.delete(conf, p, this, recursive, false, false, false);
      return true;  // successful deletion
    } catch (RemoteException rex) {
      throw rex.unwrapRemoteException(AccessControlException.class);
    } catch (AccessControlException ace) {
      throw ace;
    } catch (IOException e) {
      return false;                 // deletion unsuccessful
    }
  }
  
   private boolean deleteSkipTrash(String pathName, boolean recursive)
      throws IOException {
    return dfs.delete(pathName, recursive);          
  }
   
  private boolean deleteInternal(Configuration conf, String file, boolean recursive)
      throws IOException {
     boolean retValue = deleteUsingTrash(conf, file, recursive);
     if (retValue) {
       return true;
     }
     
     // To keep FileSystem contract (for tmp directory, throw exception in the
     // same way as skipTrash, instead of return false like non-temp directories),
     // we process the file name and redo a skipTrash delete if it is a temp file.
     // We don't do it before hand as inside deleteUsingTrash(), temp directory
     // is checked after checking directory empty. If we move the temp directory
     // checking before it, some other contracts will be broken. However, we
     // don't want to check temp directories twice for normal cases, so that
     // we recheck it only after failure cases.
       
     if (!DeleteUtils.isTempPath(conf, file)) {
       return false;
     } else {
       return deleteSkipTrash(file, recursive);
     }
   }  

  @Override
  public boolean undelete(Path f, String userName) throws IOException {
    Trash trash = new Trash(this, getConf(), userName);
    return trash.moveFromTrash(f);
  }

  ClientProtocol getNewNameNode(ClientProtocol rpcNamenode, Configuration conf)
      throws IOException {
    return DFSClient.createNamenode(rpcNamenode, conf);
  }
}

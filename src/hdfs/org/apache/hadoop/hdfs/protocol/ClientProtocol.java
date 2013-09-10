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
package org.apache.hadoop.hdfs.protocol;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.BlockSynchronizationProtocol;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;

/**********************************************************************
 * ClientProtocol is used by user code via 
 * {@link org.apache.hadoop.hdfs.DistributedFileSystem} class to communicate 
 * with the NameNode.  User code can manipulate the directory namespace, 
 * as well as open/close file streams, etc.
 *
 **********************************************************************/
public interface ClientProtocol extends VersionedProtocol,
    BlockSynchronizationProtocol {

  public static final long OPTIMIZE_FILE_STATUS_VERSION = 42L;
  public static final long ITERATIVE_LISTING_VERSION = 43L;
  public static final long BULK_BLOCK_LOCATIONS_VERSION = 44L;
  public static final long CONCAT_VERSION = 52L;
  public static final long LIST_CORRUPT_FILEBLOCKS_VERSION = 53L;
  public static final long SAVENAMESPACE_FORCE = 54L;
  public static final long RECOVER_LEASE_VERSION = 55L;
  public static final long CLOSE_RECOVER_LEASE_VERSION = 56L;

  /**
   * Compared to the previous version the following changes have been introduced:
   * (Only the latest change is reflected.
   * The log of historical changes can be retrieved from the svn).
   * 52: concat()
   * 53: Replace getCorruptFiles() with listCorruptFileBlocks()
   * 54: Add saveNamespace(boolean force)
   * 55: a lightweight recoverLease introduced.
   * 56: make recoverLease returns if the file is closed or not
   */

  public static final long versionID = CLOSE_RECOVER_LEASE_VERSION;
  
  ///////////////////////////////////////
  // File contents
  ///////////////////////////////////////
  /**
   * Get locations of the blocks of the specified file within the specified range.
   * DataNode locations for each block are sorted by
   * the proximity to the client.
   * <p>
   * Return {@link LocatedBlocks} which contains
   * file length, blocks and their locations.
   * DataNode locations for each block are sorted by
   * the distance to the client's address.
   * <p>
   * The client will then have to contact 
   * one of the indicated DataNodes to obtain the actual data.
   * 
   * @param src file name
   * @param offset range start offset
   * @param length range length
   * @return file length and array of blocks with their locations
   * @throws IOException
   */
  public LocatedBlocks  getBlockLocations(String src,
                                          long offset,
                                          long length) throws IOException;
  
  public VersionedLocatedBlocks open(String src,
                                     long offset,
                                     long length) throws IOException;
                
  /**
   * Get locations of the blocks of the specified file within the specified range.
   * DataNode locations for each block are sorted by
   * the proximity to the client.
   * <p>
   * Return {@link LocatedBlocks} which contains
   * file length, blocks and their locations,
   * and some meta info from NameNode
   * 
   * @param src file name
   * @param offset range start offset
   * @param length range length
   * @return file length and array of blocks with their locations, and
   *         namenode meta info
   * @throws IOException
   */
  public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(String src,
                                                        long offset,
                                                        long length) throws IOException;

  /**
   * Create a new file entry in the namespace.
   * <p>
   * This will create an empty file specified by the source path.
   * The path should reflect a full path originated at the root.
   * The name-node does not have a notion of "current" directory for a client.
   * <p>
   * Once created, the file is visible and available for read to other clients.
   * Although, other clients cannot {@link #delete(String)}, re-create or 
   * {@link #rename(String, String)} it until the file is completed
   * or explicitly as a result of lease expiration.
   * <p>
   * Blocks have a maximum size.  Clients that intend to
   * create multi-block files must also use {@link #addBlock(String, String)}.
   *
   * @param src path of the file being created.
   * @param masked masked permission.
   * @param clientName name of the current client.
   * @param overwrite indicates whether the file should be 
   * overwritten if it already exists.
   * @param createParent create missing parent directory if true
   * @param replication block replication factor.
   * @param blockSize maximum block size.
   * 
   * @throws AccessControlException if permission to create file is 
   * denied by the system. As usually on the client side the exception will 
   * be wrapped into {@link org.apache.hadoop.ipc.RemoteException}.
   * @throws QuotaExceededException if the file creation violates 
   *                                any quota restriction
   * @throws IOException if other errors occur.
   */
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite, 
                             boolean createParent,
                             short replication,
                             long blockSize
                             ) throws IOException;

  /**
   * Create a new file entry in the namespace.
   * 
   * @deprecated use the version with createParent above
   */
  @Deprecated
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite, 
                             short replication,
                             long blockSize
                             ) throws IOException;
  /**
   * Append to the end of the file. 
   * @param src path of the file being created.
   * @param clientName name of the current client.
   * @return information about the last partial block if any.
   * @throws AccessControlException if permission to append file is 
   * denied by the system. As usually on the client side the exception will 
   * be wrapped into {@link org.apache.hadoop.ipc.RemoteException}.
   * Allows appending to an existing file if the server is
   * configured with the parameter dfs.support.append set to true, otherwise
   * throws an IOException.
   * @throws IOException if other errors occur.
   */
  public LocatedBlock append(String src, String clientName) throws IOException;

  /**
   * Append to the end of the file. 
   * @param src path of the file being created.
   * @param clientName name of the current client.
   * @return information about the last partial block if any.
   * @throws AccessControlException if permission to append file is 
   * denied by the system. As usually on the client side the exception will 
   * be wrapped into {@link org.apache.hadoop.ipc.RemoteException}.
   * Allows appending to an existing file if the server is
   * configured with the parameter dfs.support.append set to true, otherwise
   * Return the LocatedBlock with namespace id 
   * 
   * throws an IOException.
   * @throws IOException if other errors occur.
   */
  public LocatedBlockWithMetaInfo appendAndFetchMetaInfo(String src, String clientName) throws IOException;
  
  /**
   * Append to the end of the file. 
   * @param src path of the file being created.
   * @param clientName name of the current client.
   * @return information about the last partial block if any.
   * @throws AccessControlException if permission to append file is 
   * denied by the system. As usually on the client side the exception will 
   * be wrapped into {@link org.apache.hadoop.ipc.RemoteException}.
   * Allows appending to an existing file if the server is
   * configured with the parameter dfs.support.append set to true, otherwise
   * Return the LocatedBlock with namespace id and old generation stamp.
   * 
   * throws an IOException.
   * @throws IOException if other errors occur.
   */
  public LocatedBlockWithOldGS appendAndFetchOldGS(String src, String clientName) throws IOException;
  
  /**
   * Start lease recovery
   *
   * @param src path of the file to start lease recovery
   * @param clientName name of the current client
   * @throws IOException
   */
  public void recoverLease(String src, String clientName) throws IOException;

  /**
   * Start lease recovery
   *
   * @param src path of the file to start lease recovery
   * @param clientName name of the current client
   * @param discardLastBlock discard the last block if not yet hsync-ed
   * @return if lease recovery completes or not
   * @throws IOException
   */
  public boolean closeRecoverLease(String src, String clientName,
                                   boolean discardLastBlock) throws IOException;

  /**
   * Start lease recovery
   *
   * @param src path of the file to start lease recovery
   * @param clientName name of the current client
   * @return if lease recovery completes or not
   * @throws IOException
   */
  public boolean closeRecoverLease(String src, String clientName)
    throws IOException;

  /**
   * Set replication for an existing file.
   * <p>
   * The NameNode sets replication to the new value and returns.
   * The actual block replication is not expected to be performed during  
   * this method call. The blocks will be populated or removed in the 
   * background as the result of the routine block maintenance procedures.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, 
                                short replication
                                ) throws IOException;

  /**
   * Set permissions for an existing file/directory.
   */
  public void setPermission(String src, FsPermission permission
      ) throws IOException;

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param src
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  public void setOwner(String src, String username, String groupname
      ) throws IOException;

  /**
   * The client can give up on a blcok by calling abandonBlock().
   * The client can then
   * either obtain a new block, or complete or abandon the file.
   * Any partial writes to the block will be discarded.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException;

  /**
   * The client can give up on a file by calling abandonFile
   * Any partial writes may be recovered and the lease will 
   * be relinquished
   */
  public void abandonFile(String src, String holder) throws IOException;

  /**
   * A client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing)
   * should call addBlock().  
   *
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlock addBlock(String src, String clientName) throws IOException;

  /**
   * A client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing)
   * should call addBlock().  
   *
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * This method enables client compatibility 
   * which uses this method for adding blocks. 
   *
   * @param excludedNodes a list of nodes that should not be allocated;
   * implementation may ignore this
   *
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlock addBlock(String src, String clientName,
                               DatanodeInfo[] excludedNodes) throws IOException;

   /**
   * A client that wants to write an additional block to the indicated filename
   * (which must currently be open for writing) should call addBlock().
   * 
   * addBlock() allocates a new block and datanodes the block data should be
   * replicated to.
   * 
   * @param excludedNodes
   *          a list of nodes that should not be allocated
   * @param favoredNodes
   *          a list of nodes that should be favored for allocation
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlock addBlock(String src, String clientName,
      DatanodeInfo[] excludedNodes, DatanodeInfo[] favoredNodes)
      throws IOException;
  
  
  /**
   * Return a block to the client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing). It also
   * piggyback some cluster meta info, like protoccol version# and namespaceid.
   *
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * This method enables client compatibility 
   * which uses this method for adding blocks. 
   *
   * @param excludedNodes a list of nodes that should not be allocated;
   * implementation may ignore this
   *
   * @return LocatedBlock allocated block information and data transfer protocol version
   */
  public VersionedLocatedBlock addBlockAndFetchVersion(String src, String clientName,
                               DatanodeInfo[] excludedNodes) throws IOException;
  
  /**
   * Return a block to the client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing). It also
   * piggyback some cluster meta info, like protoccol version# and namespaceid.
   * 
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * This method enables client compatibility 
   * which uses this method for adding blocks. 
   *
   * @param excludedNodes a list of nodes that should not be allocated;
   * implementation may ignore this
   *
   * @return LocatedBlock allocated block information and data transfer protocol version
   */
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
                               String src, String clientName,
                               DatanodeInfo[] excludedNodes) throws IOException;

  /**
   * Return a block to the client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing). It also
   * piggyback some cluster meta info, like protoccol version# and namespaceid.
   * 
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * This method enables client compatibility 
   * which uses this method for adding blocks. 
   *
   * @param excludedNodes a list of nodes that should not be allocated;
   * implementation may ignore this
   *
   * @return LocatedBlock allocated block information and data transfer protocol version
   */
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes, long startPos)
      throws IOException;
  
  /**
   * A client that wants to write an additional block to the indicated filename
   * (which must currently be open for writing) should call addBlock().
   * 
   * addBlock() allocates a new block and datanodes the block data should be
   * replicated to.
   * 
   * @param excludedNodes
   *          a list of nodes that should not be allocated
   * @param favoredNodes
   *          a list of nodes that should be favored for allocation
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes)
      throws IOException;
  
  /**
   * A client that wants to write an additional block to the indicated filename
   * (which must currently be open for writing) should call addBlock().
   * 
   * addBlock() allocates a new block and datanodes the block data should be
   * replicated to.
   * 
   * @param excludedNodes
   *          a list of nodes that should not be allocated
   * @param favoredNodes
   *          a list of nodes that should be favored for allocation
   * @param startPos
   *          expected starting position of the next block
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
      String src, String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes, long startPos)
      throws IOException;

  /**
   * A client that wants to write an additional block to the indicated filename
   * (which must currently be open for writing) should call addBlock().
   * 
   * addBlock() allocates a new block and datanodes the block data should be
   * replicated to. It will double check the supposed starting pos and fail
   * the request if it doesn't match. In the case that the last block
   * is the second last one, and the last one is empty, the name-node assumes
   * that it is the duplicated call, so it will return the last empty block.
   * 
   * @param excludedNodes
   *          a list of nodes that should not be allocated
   * @param favoredNodes
   *          a list of nodes that should be favored for allocation
   * @param startPos
   *          expected starting position of the next block
   * @param lastBlock
   *          metadata for last block of the file which the client knows.
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
      String src, String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes, long startPos, Block lastBlock)
      throws IOException;
  
  /**
   * The client is done writing data to the given filename, and would 
   * like to complete it.  
   *
   * The function returns whether the file has been closed successfully.
   * If the function returns false, the caller should try again.
   *
   * A call to complete() will not return true until all the file's
   * blocks have been replicated the minimum number of times.  Thus,
   * DataNode failures may cause a client to call complete() several
   * times before succeeding.
   */
  public boolean complete(String src, String clientName) throws IOException;

  /**
   * The client is done writing data to the given filename, and would 
   * like to complete it. The file length is also attached so that
   * name-node can confirm that name-node has records of all the
   * blocks
   *
   * The function returns whether the file has been closed successfully.
   * If the function returns false, the caller should try again.
   *
   * A call to complete() will not return true until all the file's
   * blocks have been replicated the minimum number of times.  Thus,
   * DataNode failures may cause a client to call complete() several
   * times before succeeding.
   */
  public boolean complete(String src, String clientName, long fileLen)
      throws IOException;
  
  /**
   * The client is done writing data to the given filename, and would 
   * like to complete it. The file length is also attached so that
   * name-node can confirm that name-node has records of all the
   * blocks
   *
   * The function returns whether the file has been closed successfully.
   * If the function returns false, the caller should try again.
   *
   * A call to complete() will not return true until all the file's
   * blocks have been replicated the minimum number of times.  Thus,
   * DataNode failures may cause a client to call complete() several
   * times before succeeding.
   * 
   * Client sends the last block of the file to the name-node. For a
   * closed file, name-node will double check file length, last block
   * ID. If both matches, name-node will simply make sure edit log
   * has been written and return success.
   */
  public boolean complete(String src, String clientName, long fileLen,
      Block lastBlock) throws IOException;
  
  /**
   * The client wants to report corrupted blocks (blocks with specified
   * locations on datanodes).
   * @param blocks Array of located blocks to report
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

  ///////////////////////////////////////
  // Namespace management
  ///////////////////////////////////////
  
  /** 
   * Hard link the dst file to the src file 
   *  
   * @param src the src file  
   * @param dst the dst file  
   * @return true if successful, or false if the src file does not exists, or the src is a directory
   * or the dst file has already exited, or dst file's parent directory does not exist  
   * @throws IOException if the new name is invalid.  
   * @throws QuotaExceededException if the hardlink would violate   
   *                                any quota restriction 
   */
  public boolean hardLink(String src, String dst) throws IOException;

  /**
   * Computes the list of files hardlinked to the given file
   *
   * @param src
   *          the file to look for
   * @return a list of files that are hardlinked to the given file, return an
   *         empty list if no files are found or the file has a reference count
   *         of 1
   * @throws IOException
   *           if the given name is invalid
   */
  public String[] getHardLinkedFiles(String src) throws IOException;

  /**
   * Rename an item in the file system namespace.
   * 
   * @param src existing file or directory name.
   * @param dst new name.
   * @return true if successful, or false if the old name does not exist
   * or if the new name already belongs to the namespace.
   * @throws IOException if the new name is invalid.
   * @throws QuotaExceededException if the rename would violate 
   *                                any quota restriction
   */
  public boolean rename(String src, String dst) throws IOException;

  /**
   * moves blocks from srcs to trg and delete srcs
   * 
   * @param trg existing file
   * @param srcs - list of existing files (same block size, same replication)
   * @throws IOException if some arguments are invalid
   * @throws QuotaExceededException if the rename would violate 
   *                                any quota restriction
   */
  @Deprecated
  public void concat(String trg, String [] srcs) throws IOException;
  
  /**
   * moves blocks from srcs to trg and delete srcs
   * 
   * @param trg existing file
   * @param srcs - list of existing files (same block size, same replication)
   * @param restricted - true if equal block sizes are to be enforced, false otherwise
   * @throws IOException if some arguments are invalid
   * @throws QuotaExceededException if the rename would violate 
   *                                any quota restriction
   */
  public void concat(String trg, String [] srcs, boolean restricted) throws IOException;
  
  /**
   * Merge parity file and source file into one Raided file 
   * @param parity - parity file
   * @param source - source file
   * @param codecId - the codec id of raid codec
   * @param checksums - list of checksums of source blocks 
   * @throws IOException
   */
  public void merge(String parity, String source, String codecId, 
      int[] checksums) throws IOException;

  /**
   * Delete the given file or directory from the file system.
   * <p>
   * Any blocks belonging to the deleted files will be garbage-collected.
   * 
   * @param src existing name.
   * @return true only if the existing file or directory was actually removed 
   * from the file system. 
   */
  public boolean delete(String src) throws IOException;

  /**
   * Delete the given file or directory from the file system.
   * <p>
   * same as delete but provides a way to avoid accidentally 
   * deleting non empty directories programmatically. 
   * @param src existing name
   * @param recursive if true deletes a non empty directory recursively,
   * else throws an exception.
   * @return true only if the existing file or directory was actually removed 
   * from the file system. 
   */
  public boolean delete(String src, boolean recursive) throws IOException;
  
  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param masked The masked permission of the directory being created
   * @return True if the operation success.
   * @throws {@link AccessControlException} if permission to create file is 
   * denied by the system. As usually on the client side the exception will 
   * be wraped into {@link org.apache.hadoop.ipc.RemoteException}.
   * @throws QuotaExceededException if the operation would violate 
   *                                any quota restriction.
   */
  public boolean mkdirs(String src, FsPermission masked) throws IOException;

  /**
   * Fetch the list of files that have been open longer than a
   * specified amount of time.
   * @param prefix path prefix specifying subset of files to examine
   * @param millis select files that have been open longer that this
   * @param start where to start searching, or null
   * @return array of OpenFileInfo objects
   * @throw IOException
   */
  public OpenFileInfo[] iterativeGetOpenFiles(
    String prefix, int millis, String start) throws IOException;

  /**
   * Get a listing of the indicated directory
   */
  public FileStatus[] getListing(String src) throws IOException;

  /**
   * Get a listing of the indicated directory
   */
  public HdfsFileStatus[] getHdfsListing(String src) throws IOException;


  /**
   * Get a partial listing of the indicated directory,
   * starting from the first child whose name is greater than startAfter
   *
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing getPartialListing(String src, byte[] startAfter)
  throws IOException;

  /**
   * Get a partial listing of the indicated directory,
   * piggybacking block locations to each FileStatus
   *
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @return a partial listing starting after startAfter
   */
  public LocatedDirectoryListing getLocatedPartialListing(String src,
      byte[] startAfter)
  throws IOException;

  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /**
   * Client programs can cause stateful changes in the NameNode
   * that affect other clients.  A client may obtain a file and 
   * neither abandon nor complete it.  A client might hold a series
   * of locks that prevent other clients from proceeding.
   * Clearly, it would be bad if a client held a bunch of locks
   * that it never gave up.  This can happen easily if the client
   * dies unexpectedly.
   * <p>
   * So, the NameNode will revoke the locks and live file-creates
   * for clients that it thinks have died.  A client tells the
   * NameNode that it is still alive by periodically calling
   * renewLease().  If a certain amount of time passes since
   * the last call to renewLease(), the NameNode assumes the
   * client has died.
   */
  public void renewLease(String clientName) throws IOException;

  public int GET_STATS_CAPACITY_IDX = 0;
  public int GET_STATS_USED_IDX = 1;
  public int GET_STATS_REMAINING_IDX = 2;
  public int GET_STATS_UNDER_REPLICATED_IDX = 3;
  public int GET_STATS_CORRUPT_BLOCKS_IDX = 4;
  public int GET_STATS_MISSING_BLOCKS_IDX = 5;
  
  /**
   * Get a set of statistics about the filesystem.
   * Right now, only three values are returned.
   * <ul>
   * <li> [0] contains the total storage capacity of the system, in bytes.</li>
   * <li> [1] contains the total used space of the system, in bytes.</li>
   * <li> [2] contains the available storage of the system, in bytes.</li>
   * <li> [3] contains number of under replicated blocks in the system.</li>
   * <li> [4] contains number of blocks with a corrupt replica. </li>
   * <li> [5] contains number of blocks without any good replicas left. </li>
   * <li> [6] contains the total used space of the system for the name space, in bytes. </li>
   * </ul>
   * Use public constants like {@link #GET_STATS_CAPACITY_IDX} in place of 
   * actual numbers to index into the array.
   */
  public long[] getStats() throws IOException;

  /**
   * Get a report on the system's current datanodes.
   * One DatanodeInfo object is returned for each DataNode.
   * Return live datanodes if type is LIVE; dead datanodes if type is DEAD;
   * otherwise all datanodes if type is ALL.
   */
  public DatanodeInfo[] getDatanodeReport(FSConstants.DatanodeReportType type)
  throws IOException;

  /**
   * Get the block size for the given file.
   * @param filename The name of the file
   * @return The number of bytes in each block
   * @throws IOException
   */
  public long getPreferredBlockSize(String filename) throws IOException;

  /**
   * Enter, leave or get safe mode.
   * <p>
   * Safe mode is a name node state when it
   * <ol><li>does not accept changes to name space (read-only), and</li>
   * <li>does not replicate or delete blocks.</li></ol>
   * 
   * <p>
   * Safe mode is entered automatically at name node startup.
   * Safe mode can also be entered manually using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_GET)}.
   * <p>
   * At startup the name node accepts data node reports collecting
   * information about block locations.
   * In order to leave safe mode it needs to collect a configurable
   * percentage called threshold of blocks, which satisfy the minimal 
   * replication condition.
   * The minimal replication condition is that each block must have at least
   * <tt>dfs.replication.min</tt> replicas.
   * When the threshold is reached the name node extends safe mode
   * for a configurable amount of time
   * to let the remaining data nodes to check in before it
   * will start replicating missing blocks.
   * Then the name node leaves safe mode.
   * <p>
   * If safe mode is turned on manually using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_ENTER)}
   * then the name node stays in safe mode until it is manually turned off
   * using {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_LEAVE)}.
   * Current state of the name node can be verified using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_GET)}
   * <h4>Configuration parameters:</h4>
   * <tt>dfs.safemode.threshold.pct</tt> is the threshold parameter.<br>
   * <tt>dfs.safemode.extension</tt> is the safe mode extension parameter.<br>
   * <tt>dfs.replication.min</tt> is the minimal replication parameter.
   * 
   * <h4>Special cases:</h4>
   * The name node does not enter safe mode at startup if the threshold is 
   * set to 0 or if the name space is empty.<br>
   * If the threshold is set to 1 then all blocks need to have at least 
   * minimal replication.<br>
   * If the threshold value is greater than 1 then the name node will not be 
   * able to turn off safe mode automatically.<br>
   * Safe mode can always be turned off manually.
   * 
   * @param action  <ul> <li>0 leave safe mode;</li>
   *                <li>1 enter safe mode;</li>
   *                <li>2 get safe mode state.</li></ul>
   * @return <ul><li>0 if the safe mode is OFF or</li> 
   *         <li>1 if the safe mode is ON.</li></ul>
   * @throws IOException
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException;

  /**
   * Save namespace image.
   * <p>
   * Saves current namespace into storage directories and reset edits log.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if the superuser privilege is violated.
   * @throws IOException if image creation failed.
   */
  public void saveNamespace() throws IOException;
  
  /**
   * Roll edit log manually.
   * @throws IOException if the roll failed.
   */
  public void rollEditLogAdmin() throws IOException;
  
  /**
   * Save namespace image.
   * 
   * @param force not require safe mode if true
   * @param uncompressed save namespace uncompressed if true
   * @throws AccessControlException if the superuser privilege is violated.
   * @throws IOException if image creation failed.
   */
  public void saveNamespace(boolean force, boolean uncompressed)
  throws IOException;

  /**
   * Tells the namenode to reread the hosts and exclude files. 
   * @throws IOException
   */
  public void refreshNodes() throws IOException;

  /**
   * Finalize previous upgrade.
   * Remove file system state saved during the upgrade.
   * The upgrade will become irreversible.
   * 
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException;

  /**
   * Report distributed upgrade progress or force current upgrade to proceed.
   * 
   * @param action {@link FSConstants.UpgradeAction} to perform
   * @return upgrade status information or null if no upgrades are in progress
   * @throws IOException
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) 
  throws IOException;

  /**
   * Dumps namenode data structures into specified file. If file
   * already exists, then append.
   * @throws IOException
   */
  public void metaSave(String filename) throws IOException;
  
  /**
   * Enable/Disable Block Replication.
   */
  public void blockReplication(boolean isEnable) throws IOException;

  /**
   * @return Array of FileStatus objects referring to corrupted files.
   * @throws AccessControlException
   * @throws IOException
   */
  @Deprecated
  FileStatus[] getCorruptFiles() 
    throws AccessControlException, IOException; 

  /** 
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  public CorruptFileBlocks
    listCorruptFileBlocks(String path, String cookie)
    throws IOException;

  /**
   * Get the file info for a specific file or directory.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system 
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public FileStatus getFileInfo(String src) throws IOException;

  /**
   * Get the file info for a specific file or directory.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system 
   * @return HdfsFileStatus containing information regarding the file
   *         or null if file not found
   */
  public HdfsFileStatus getHdfsFileInfo(String src) throws IOException;

  /**
   * Get {@link ContentSummary} rooted at the specified directory.
   * @param path The string representation of the path
   */
  public ContentSummary getContentSummary(String path) throws IOException;

  /**
   * Set the quota for a directory.
   * @param path  The string representation of the path to the directory
   * @param namespaceQuota Limit on the number of names in the tree rooted 
   *                       at the directory
   * @param diskspaceQuota Limit on disk space occupied all the files under
   *                       this directory. 
   * <br><br>
   *                       
   * The quota can have three types of values : (1) 0 or more will set 
   * the quota to that value, (2) {@link FSConstants#QUOTA_DONT_SET}  implies 
   * the quota will not be changed, and (3) {@link FSConstants#QUOTA_RESET} 
   * implies the quota will be reset. Any other value is a runtime error.
   *                        
   * @throws FileNotFoundException if the path is a file or 
   *                               does not exist 
   * @throws QuotaExceededException if the directory size 
   *                                is greater than the given quota
   */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
                      throws IOException;
  
  /**
   * Write all metadata for this file into persistent storage.
   * The file must be currently open for writing.
   * @param src The string representation of the path
   * @param client The string representation of the client
   */
  public void fsync(String src, String client) throws IOException;

  /**
   * Sets the modification and access time of the file to the specified time.
   * @param src The string representation of the path
   * @param mtime The number of milliseconds since Jan 1, 1970.
   *              Setting mtime to -1 means that modification time should not be set
   *              by this call.
   * @param atime The number of milliseconds since Jan 1, 1970.
   *              Setting atime to -1 means that access time should not be set
   *              by this call.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException;
  
  /**
   * Update a pipeline for a block under construction
   * 
   * @param clientName   the name of the client
   * @param oldBlock    the old block
   * @param newBlock    the new block containing new generation stamp and length
   * @param newNodes    datanodes in the pipeline
   * @throws IOException  if any error happens
   */
  public void updatePipeline(String clientName, Block oldBlock, 
        Block newBlock, DatanodeID[] newNodes) 
      throws IOException;

  /**
   * Get the datanode's data transfer protocol version
   * @return the datanode's data transfer protocol version
   * @throws IOException
   */
  public int getDataTransferProtocolVersion() throws IOException;
  
  /**
   * Get the name of the file the block belongs to and the locations
   * of the block.
   * 
   * @param blockId
   * @return
   * @throws IOException
   */
  public LocatedBlockWithFileName getBlockInfo(long blockId) throws IOException;

  /**
   * Get cluster name of the name node
   * 
   * @return
   * @throws IOException
   */
  public String getClusterName() throws IOException;
  
  /** Re-populate the namespace and diskspace count of every node with quota */
  public void recount() throws IOException;
  
  /**
   * Raid a file with given codec 
   * No guarantee file will be raided when the call returns.
   * Namenode will schedule raiding asynchronously. 
   * If raiding is done when raidFile is called again, namenode will set 
   * replication of source blocks to expectedSourceRepl 
   * 
   * @param source 
   * @param codecId
   * @param expectedSourceRepl 
   * @throws IOException
   */
  public boolean raidFile(String source, String codecId, short expectedSourceRepl) 
      throws IOException;
}
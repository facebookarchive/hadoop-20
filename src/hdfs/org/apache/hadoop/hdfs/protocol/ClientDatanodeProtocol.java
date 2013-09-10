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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.VersionedProtocol;

/** An client-datanode protocol for block recovery
 */
public interface ClientDatanodeProtocol extends VersionedProtocol {
  public static final Log LOG = LogFactory.getLog(ClientDatanodeProtocol.class);

  public static final long GET_BLOCKINFO_VERSION = 4L;
  public static final long COPY_BLOCK_VERSION = 5L;

  /**
   * 3: add keepLength parameter.
   * 4: added getBlockInfo
   * 5: add copyBlock parameter.
   */
  public static final long versionID = 5L;

  /** Start generation-stamp recovery for specified block
   * @param block the specified block
   * @param keepLength keep the block length
   * @param targets the list of possible locations of specified block
   * @return the new blockid if recovery successful and the generation stamp
   * got updated as part of the recovery, else returns null if the block id
   * not have any data and the block was deleted.
   * @throws IOException
   */
  LocatedBlock recoverBlock(Block block, boolean keepLength,
      DatanodeInfo[] targets) throws IOException;

  /** Start generation-stamp recovery for specified block
   * @param namespaceid the block belongs to
   * @param block the specified block
   * @param keepLength keep the block length
   * @param targets the list of possible locations of specified block
   * @return the new blockid if recovery successful and the generation stamp
   * got updated as part of the recovery, else returns null if the block id
   * not have any data and the block was deleted.
   * @throws IOException
   */
  LocatedBlock recoverBlock(int namespaceId, Block block, boolean keepLength,
      DatanodeInfo[] targets) throws IOException;

  /** Start generation-stamp recovery for specified block
   * @param namespaceid the block belongs to
   * @param block the specified block
   * @param keepLength keep the block length
   * @param targets the list of possible locations of specified block
   * @param deadline after it, RPC should terminate
   * @return the new blockid if recovery successful and the generation stamp
   * got updated as part of the recovery, else returns null if the block id
   * not have any data and the block was deleted.
   * @throws IOException
   */
  LocatedBlock recoverBlock(int namespaceId, Block block, boolean keepLength,
      DatanodeInfo[] targets, long deadline) throws IOException;

  /** Returns a block object that contains the specified block object
   * from the specified Datanode.
   * @param block the specified block
   * @return the Block object from the specified Datanode
   * @throws IOException if the block does not exist
   */
  public Block getBlockInfo(Block block) throws IOException;

  /** Returns a block object that contains the specified block object
   * from the specified Datanode.
   * @param namespaceid the block belongs to
   * @param block the specified block
   * @return the Block object from the specified Datanode
   * @throws IOException if the block does not exist
   */
  public Block getBlockInfo(int namespaceid, Block block) throws IOException;

  /** Instruct the datanode to copy a block to specified target.
   * @param srcBlock the specified block on this datanode
   * @param destinationBlock the block identifier on the destination datanode
   * @param target the locations where this block needs to be copied
   * @throws IOException
   */
  public void copyBlock(Block srcblock, Block destBlock,
      DatanodeInfo target) throws IOException;
  
  /** Instruct the datanode to copy a block to specified target.
   * @param srcNamespaceId the namespaceId of srcBlock
   * @param srcBlock the specified block on this datanode
   * @param dstNamespaceId the namespace id of dstBlock
   * @param destinationBlock the block identifier on the destination datanode
   * @param target the locations where this block needs to be copied
   * @throws IOException
   */
  public void copyBlock(int srcNamespaceId, Block srcblock,
      int dstNamespaceId, Block destBlock,
      DatanodeInfo target) throws IOException;
  
  /** Instruct the datanode to copy a block to specified target.
   * @param srcBlock the specified block on this datanode
   * @param destinationBlock the block identifier on the destination datanode
   * @param target the locations where this block needs to be copied
   * @param async whether or not the call should block till the block is completely copied.
   * @throws IOException
   */
  public void copyBlock(Block srcblock, Block destBlock,
      DatanodeInfo target, boolean async) throws IOException;
  
  /** Instruct the datanode to copy a block to specified target.
   * @param srcNamespaceId the namespace source block belongs to
   * @param srcBlock the specified block on this datanode
   * @param dstNamespaceId the namespace destination block belongs to
   * @param destinationBlock the block identifier on the destination datanode
   * @param target the locations where this block needs to be copied
   * @param async if wait for block copy done or not
   * @throws IOException
   */
  public void copyBlock(int srcNamespaceId, Block srcblock, 
      int dstNamespaceId, Block destBlock,
      DatanodeInfo target, boolean async) throws IOException;

  /** Retrives the filename of the blockfile and the metafile from the datanode
   * @param namespaceid the block belongs to
   * @param block the specified block on this datanode
   * @return the BlockPathInfo of a block
   */
  BlockPathInfo getBlockPathInfo(Block block) throws IOException;

  /** Retrives the filename of the blockfile and the metafile from the datanode
   * @param block the specified block on this datanode
   * @return the BlockPathInfo of a block
   */
  BlockPathInfo getBlockPathInfo(int namespaceId, Block block) throws IOException;
  
  /** Refresh name nodes served by the datanode 
   * datanode reloads the configuration file and stops serving removed namenodes and 
   * starts serving newly-added namenodes
   * @throws IOException
   */
  public void refreshNamenodes() throws IOException;
  
  /**
   * @param serviceName service's name to refresh.
   */
  public void refreshOfferService(String serviceName) throws IOException;
  
  /**
   * Informs the datanode it should stop serving the given namespace.
   * @param nameserviceId the id of the namespace the datanode should stop
   * serving
   * @throws IOEXception
   */
  public void removeNamespace(String nameserviceId) throws IOException;

  /** Reads in data dirs from a textfile containing these directories
   * datanode will refresh the configuration file with the new list 
   * of directories 
   * @throws IOException
   */
  public void refreshDataDirs(String confVolumes) throws IOException; 
}

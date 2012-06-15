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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.ipc.VersionedProtocol;

/** An inter-datanode protocol for updating generation stamp
 */
public interface InterDatanodeProtocol extends VersionedProtocol {
  public static final Log LOG = LogFactory.getLog(InterDatanodeProtocol.class);

  /**
   * 4: added a new RPC to copy blocks between datanodes on the same host.
   */
  public static final long versionID = 4L;

  /** @return the BlockMetaDataInfo of a block;
   *  null if the block is not found 
   */
  BlockMetaDataInfo getBlockMetaDataInfo(int namespaceId, Block block) throws IOException;


  /**
   * Begin recovery on a block - this interrupts writers and returns the
   * necessary metadata for recovery to begin.
   * @return the BlockRecoveryInfo for a block
   * @return null if the block is not found
   */
  BlockRecoveryInfo startBlockRecovery(int namespaceId, Block block) throws IOException;

  /**
   * Update the block to the new generation stamp and length.  
   */
  void updateBlock(int namespaceId, Block oldblock, Block newblock, boolean finalize) throws IOException;

  /**
   * Copies a local block from a datanode on the same host.
   * 
   * @param srcFileSystem
   *          the file system that the src block belongs to
   * @param srcNamespaceId
   *          the namespaceId of srcBlock
   * @param srcBlock
   *          the block that needs to be copied
   * @param dstNamespaceId
   *          the namespaceId of dstBlock
   * @param dstBlock
   *          the block to which the srcBlock needs to be copied
   * @param srcBlockFile
   *          the absolute path to the block data file on the datanode
   * @throws IOException
   */
  void copyBlockLocal(String srcFileSystem, int srcNamespaceId, Block srcBlock,
      int dstNamespaceId, Block dstBlock,
      String srcBlockFilePath) throws IOException;
}

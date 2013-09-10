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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.Counters;

/**
 * This class fixes source file blocks using the parity file,
 * and parity file blocks using the source file.
 * It periodically fetches the list of corrupt files from the namenode,
 * and figures out the location of the bad block by reading through
 * the corrupt file.
 */
public class LocalBlockIntegrityMonitor extends BlockIntegrityMonitor 
                                        implements Runnable {
  public static final Log LOG = LogFactory.getLog(LocalBlockIntegrityMonitor.class);

  private BlockReconstructor.CorruptBlockReconstructor helper;
  public RaidProtocol raidnode;
  private UnixUserGroupInformation ugi;
  RaidProtocol rpcRaidnode;

  
  void initializeRpc(Configuration conf, InetSocketAddress address) throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }

    this.rpcRaidnode = RaidShell.createRPCRaidnode(address, conf, ugi);
    this.raidnode = RaidShell.createRaidnode(rpcRaidnode);
  }
  
  public LocalBlockIntegrityMonitor(Configuration conf) throws Exception {
    this(conf, true);
  }
  
  public LocalBlockIntegrityMonitor(Configuration conf, boolean initializeRPC)
      throws Exception {
    super(conf);
    helper = new BlockReconstructor.CorruptBlockReconstructor(getConf());
    if (initializeRPC) {
      for (int i = 0; i < 3; i++) {
        try {
          initializeRpc(conf, RaidNode.getAddress(conf));
        } catch (Exception e) {
          LOG.warn("Fail to initialize RPC", e);
          if (i == 2) {
            throw e;
          }
          Thread.sleep(2000);
        }
      }
    }
  }

  public void run() {
    try {
      while (running) {
        try {
          LOG.info("LocalBlockFixer continuing to run...");
          doFix();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } catch (Error err) {
          LOG.error("Exiting after encountering " +
                      StringUtils.stringifyException(err));
          throw err;
        }
      }
    } finally {   
      RPC.stopProxy(rpcRaidnode);
    }
  }

  void doFix() throws InterruptedException, IOException {
    while (running) {
      // Sleep before proceeding to fix files.
      Thread.sleep(blockCheckInterval);

      List<String> corruptFiles = getCorruptFiles();
      FileSystem parityFs = new Path("/").getFileSystem(getConf());
      filterUnreconstructableSourceFiles(parityFs, corruptFiles.iterator());
      RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID).
          numFilesToFix.set(corruptFiles.size());
      approximateNumRecoverableFiles = corruptFiles.size();
      if (corruptFiles.isEmpty()) {
        // If there are no corrupt files, retry after some time.
        continue;
      }
      LOG.info("Found " + corruptFiles.size() + " corrupt files.");
      long detectionTime = System.currentTimeMillis();

      helper.sortLostFiles(corruptFiles);

      for (String srcPathStr: corruptFiles) {
        if (!running) break;
        long recoveryTime = -1;
        Path srcPath = new Path(srcPathStr);
        try {
          boolean fixed = helper.reconstructFile(srcPath, null);
          if (fixed) {
            incrFilesFixed();
            recoveryTime = System.currentTimeMillis() - detectionTime;
            lastSuccessfulFixTime = System.currentTimeMillis();
          }
        } catch (IOException ie) {
          incrFileFixFailures();
          LOG.error("Hit error while processing " + srcPath +
            ": " + StringUtils.stringifyException(ie));
          // Do nothing, move on to the next file.
          recoveryTime = Integer.MAX_VALUE;
        } finally {
          if (recoveryTime > 0) {
            try {
              raidnode.sendRecoveryTime(srcPathStr, recoveryTime, null);
            } catch (Exception e) {
              LOG.error("Failed to send recovery time ", e);
            }
          }
        }
      }
    }
  }

  /**
   * @return A list of corrupt files as obtained from the namenode
   */
  List<String> getCorruptFiles() throws IOException {
    DistributedFileSystem dfs = helper.getDFS(new Path("/"));

    String[] files = DFSUtil.getCorruptFiles(dfs);
    List<String> corruptFiles = new LinkedList<String>();
    for (String f: files) {
      corruptFiles.add(f);
    }
    RaidUtils.filterTrash(getConf(), corruptFiles);
    return corruptFiles;
  }

  @Override
  public BlockIntegrityMonitor.Status getAggregateStatus() {
    throw new UnsupportedOperationException(LocalBlockIntegrityMonitor.class +
        " doesn't do getAggregateStatus()");
  }

  @Override
  public Runnable getCorruptionMonitor() {
    return this;
  }

  @Override
  public Runnable getDecommissioningMonitor() {
    // This class does not monitor decommissioning files. 
    return null;
  }
  
  @Override
  public Runnable getCorruptFileCounter() {
    return null;
  }
}


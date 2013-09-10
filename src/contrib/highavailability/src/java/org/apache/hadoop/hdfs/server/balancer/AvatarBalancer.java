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

package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.hdfs.DistributedAvatarFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.FailoverClient;
import org.apache.hadoop.hdfs.FailoverClientHandler;
import org.apache.hadoop.hdfs.server.FailoverNameNodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.Stat;

/**
 * Extension of the HDFS Balancer utility such that it works correctly with
 * AvatarNodes and can run during and after an AvatarNode failover.
 */
public class AvatarBalancer extends Balancer implements FailoverClient {

  private FailoverNameNodeProtocol failoverNamenode;
  private FailoverClientHandler failoverHandler;
  private URI logicalName;

  public AvatarBalancer() {
    super();
  }

  public AvatarBalancer(Configuration conf) {
    super(conf);
  }

  public AvatarBalancer(Configuration conf, double threshold) {
    super(conf, threshold);
  }

  static int runBalancer(String[] args) throws Exception {
    return ToolRunner.run(null, new AvatarBalancer(), args);
  }

  @Override
  public void setConf(Configuration conf) {
    conf.setClass("dfs.balancer.impl", AvatarBalancer.class, Balancer.class);
    conf.setClass("fs.hdfs.impl", DistributedAvatarFileSystem.class,
        FileSystem.class);
    conf.setBoolean("fs.ha.retrywrites", true);
    super.setConf(conf);
  }

  /**
   * Run a balancer
   *
   * @param args
   */
  public static void main(String[] args) {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    try {
      System.exit(runBalancer(args));
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }

  }

  @Override
  protected void initFS() throws IOException {
    super.initFS();
    logicalName = this.fs.getUri();
    failoverHandler = new FailoverClientHandler(this.conf, logicalName, this);
  }

  @Override
  protected void initNameNodes() throws IOException {
    if (this.fs instanceof FilterFileSystem) {
      this.client = ((DistributedFileSystem) ((FilterFileSystem) this.fs)
          .getRawFileSystem()).getClient().getNameNodeRPC();
    } else {
      this.client = ((DistributedFileSystem) this.fs).getClient()
          .getNameNodeRPC();
    }
    initNamenodeProtocol(false);
  }

  private void initNamenodeProtocol(boolean failover) throws IOException {
    boolean firstAttempt = true;
    Stat stat = new Stat();
    while (true) {
      try {
        String primaryAddr = failoverHandler.getPrimaryAvatarAddress(
            logicalName, stat, true, firstAttempt);
        String parts[] = primaryAddr.split(":");
        if (parts.length != 2) {
          throw new IOException("Invalid address : " + primaryAddr);
        }
        InetSocketAddress nnAddr = new InetSocketAddress(parts[0],
            Integer.parseInt(parts[1]));
        NamenodeProtocol nn = createNamenode(nnAddr, conf);
        if (failover) {
          newNamenode(nn);
        } else {
          failoverNamenode = new FailoverNameNodeProtocol(this.namenode,
              failoverHandler);
          this.namenode = failoverNamenode;
        }
        break;
      } catch (Exception e) {
        if (firstAttempt && failoverHandler.isZKCacheEnabled()) {
          firstAttempt = false;
          continue;
        } else {
          Balancer.LOG.error(e);
          throw new IOException(e);
        }
      }
    }

  }

  @Override
  public boolean tryFailover() throws IOException {
    initNamenodeProtocol(true);
    return true;
  }

  @Override
  public boolean isShuttingdown() {
    return this.shuttingDown;
  }

  @Override
  public boolean isFailoverInProgress() {
    return failoverNamenode.getNameNode() == null;
  }

  @Override
  public void nameNodeDown() {
    failoverNamenode.setNameNode(null);
  }

  @Override
  public void newNamenode(VersionedProtocol namenode) {
    failoverNamenode.setNameNode((NamenodeProtocol) namenode);
  }


}

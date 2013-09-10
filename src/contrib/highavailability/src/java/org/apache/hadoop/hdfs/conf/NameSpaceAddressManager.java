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

package org.apache.hadoop.hdfs.conf;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.zookeeper.KeeperException;

/**
 * The purpose of this class is to abstract out Zookeeper status
 * and configuration details from Avatar classes.
 * 
 */
public abstract class NameSpaceAddressManager implements Closeable {
  /** The configuration object for which this serves as a wrapper for */
  protected Configurable          confSrc;
  private static final Log        LOG = LogFactory
                                          .getLog(NameSpaceAddressManager.class
                                              .getName());
  protected NameNodeAddress       nodeZero;
  protected NameNodeAddress       nodeOne;
  protected NameNodeAddress       primaryNode;
  protected NameNodeAddress       standbyNode;

  /** The ZookeeperClient used to determine which AvatarNode is primary */
  protected AvatarZooKeeperClient zkClient;

  protected NameSpaceAddressManager(Configurable parent) {
    setConfSource(parent);
  }

  /**
   * The purpose of this method is to update in memory references to primary 
   * and standby. Please override this method and do the zookeeper specific 
   * logic in the method and call this method on successful zookeeper write.
   */
  public void setPrimary(InstanceId ofPrimary) throws IOException {
    if (ofPrimary == null) {
      // failover in progress
      primaryNode = null;
      standbyNode = null;
      return;
    }
    switch (ofPrimary) {
      case NODEZERO:
        primaryNode = getNodeZero();
        standbyNode = getNodeOne();
      case NODEONE:
        primaryNode = getNodeOne();
        standbyNode = getNodeZero();
    }
  }

  public boolean isFailoverInProgress() {
    return getPrimary() == null || getStandby() == null;
  }

  /**
   * Reads all the configuration related information from the
   * Zookeeper and updates the in memory references (instance variables)
   *  accordingly.
   */
  public abstract void refreshZookeeperInfo() throws IOException,
      KeeperException, InterruptedException;

  /**
   * Sets the backing configuration source
   */
  public void setConfSource(Configurable src) {
    validateConfigFile(src.getConf());
    confSrc = src;
    zkClient = new AvatarZooKeeperClient(confSrc.getConf(), null, true);
  }

  public void finalize() throws IOException {
    close();
  }

  public void close() {
    if (zkClient != null) {
      try {
        zkClient.shutdown();
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
  }

  /**
   * Validates the configuration file. Called at startup to fail fast and avoid
   * errors while running.
   */
  public abstract void validateConfigFile(Configuration parent);

  public NameNodeAddress getPrimary() {
    return primaryNode;
  }

  public NameNodeAddress getStandby() {
    return standbyNode;
  }

  public NameNodeAddress getNodeZero() {
    return nodeZero;
  }

  public NameNodeAddress getNodeOne() {
    return nodeOne;
  }

  public abstract String getTxidPath() throws UnknownHostException;

  public abstract String getSsidPath() throws UnknownHostException;
}

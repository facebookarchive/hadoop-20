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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper around other {@link ZooKeeperIface} implementations
 * (e.g., {@link BasicZooKeeper}) that will correctly recover from common
 * transient issues e.g., connection loss, timeouts. Ported from
 * jcommon-zookeeper:
 * https://github.com/facebook/jcommon/blob/master/zookeeper/src/main/java/com/facebook/zookeeper/RecoveringZooKeeper.java
 */
public class RecoveringZooKeeper implements ZooKeeperIface {
  private static final Log LOG = LogFactory.getLog(RecoveringZooKeeper.class);

  private final ZooKeeperIface zk;
  private final RetryCounterFactory retryCounterFactory;

  public RecoveringZooKeeper(
      ZooKeeperIface zk, int maxRetries, int retryIntervalMillis
  ) {
    this.zk = zk;
    this.retryCounterFactory =
        new RetryCounterFactory(maxRetries, retryIntervalMillis);
  }

  @Override
  public long getSessionId() {
    return zk.getSessionId();
  }

  @Override
  public void close() throws InterruptedException {
    zk.close();
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException {
    switch (createMode) {
      case EPHEMERAL:
      case PERSISTENT:
        return createNonSequential(path, data, acl, createMode);

      case EPHEMERAL_SEQUENTIAL:
        // NOTE: this does not reliably support creating multiple ephemeral
        // sequential nodes with the same prefix under the same path
        return createEphemeralSequential(path, data, acl, createMode);

      case PERSISTENT_SEQUENTIAL:
        // No recovery for persistent sequential b/c no way to verify
        // insertion after disconnect w/o application help
        return zk.create(path, data, acl, createMode);

      default:
        throw new IllegalArgumentException("Unrecognized CreateMode: " + createMode);
    }
  }

  @Override
  public void delete(String path, int version)
      throws InterruptedException, KeeperException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.delete(path, version);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
          case NONODE:
            return; // Delete was successful

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper delete failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public Stat exists(String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public Stat exists(String path, boolean watch)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public byte[] getData(String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getData(path, watcher, stat);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public byte[] getData(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getData(path, watch, stat);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public Stat setData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.setData(path, data, version);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper setData failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public List<String> getChildren(String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public ZooKeeper.States getState() {
    return zk.getState();
  }

  // ------------------------- Internal Helpers ------------------------ //

  private String createNonSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.create(path, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case NODEEXISTS:
            // Non-sequential node was successfully created
            return path;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper create failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  private String createEphemeralSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean first = true;
    while (true) {
      try {
        if (!first) {
          // Check if we succeeded on a previous attempt
          String myNode = findMyEphemeralSequentialNode(path);
          if (myNode != null) {
            return myNode;
          }
        }
        first = false;
        return zk.create(path, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper create failed after "
                  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      LOG.info("Retrying ZooKeeper after sleeping...");
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  private String findMyEphemeralSequentialNode(String path)
      throws KeeperException, InterruptedException {
    int lastSlashIdx = path.lastIndexOf('/');
    assert(lastSlashIdx != -1);
    String parent = path.substring(0, lastSlashIdx);
    String nodePrefix = path.substring(lastSlashIdx+1);

    List<String> nodes = zk.getChildren(parent, false);
    List<String> matching = ZkUtil.filterByPrefix(nodes, nodePrefix);
    for (String node : matching) {
      String nodePath = parent + "/" + node;
      Stat stat = zk.exists(nodePath, false);
      if (stat != null && stat.getEphemeralOwner() == zk.getSessionId()) {
        return nodePath;
      }
    }
    return null;
  }

  private static class RetryCounterFactory {
    private final int maxRetries;
    private final int retryIntervalMillis;

    private RetryCounterFactory(int maxRetries, int retryIntervalMillis) {
      this.maxRetries = maxRetries;
      this.retryIntervalMillis = retryIntervalMillis;
    }

    public RetryCounter create() {
      return
          new RetryCounter(
              maxRetries, retryIntervalMillis, TimeUnit.MILLISECONDS
          );
    }
  }

  private static class RetryCounter {
    private final int maxRetries;
    private int retriesRemaining;
    private final int retryIntervalMillis;
    private final TimeUnit timeUnit;

    private RetryCounter(
        int maxRetries, int retryIntervalMillis, TimeUnit timeUnit
    ) {
      this.maxRetries = maxRetries;
      this.retriesRemaining = maxRetries;
      this.retryIntervalMillis = retryIntervalMillis;
      this.timeUnit = timeUnit;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void sleepUntilNextRetry() throws InterruptedException {
      timeUnit.sleep(retryIntervalMillis);
    }

    public boolean shouldRetry() {
      return retriesRemaining > 0;
    }

    public void useRetry() {
      retriesRemaining--;
    }

  }

}
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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.ipc.metrics.RpcMetrics;


/**
 * Contains all the Server connections. 
 */
class ConnectionSet {
  final private int numBuckets;
  final ConnectionBucket[] connectionBuckets;
  final RpcMetrics rpcMetrics;
  final String serverName;
  final AtomicInteger numConnections = new AtomicInteger(0);
  final AtomicInteger nextBucketToCleanIdleConn = new AtomicInteger(0);
  
  public ConnectionSet(String serverName, int numBuckets, RpcMetrics rpcMetrics) {
    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug("Use " + numBuckets
          + " buckets for bucketizing connections of " + serverName);
    }
    this.numBuckets = numBuckets;
    connectionBuckets = new ConnectionBucket[numBuckets];
    this.rpcMetrics = rpcMetrics;
    this.serverName = serverName;
    for (int i = 0; i < connectionBuckets.length; i++) {
      connectionBuckets[i] = new ConnectionBucket();
    }
  }

  void addConnection(Connection c) {
    ConnectionBucket bucket = getBucket(c);
    bucket.addConnection(c);
    rpcMetrics.numOpenConnections.inc(1);
  }
  
  void removeConnection(Connection c) {
    if (c == null) {
      return;
    }
    ConnectionBucket bucket = getBucket(c);
    bucket.removeConnection(c);
    rpcMetrics.numOpenConnections.inc(-1);
  }
  
  boolean ifConnectionsClean() {
    for (ConnectionBucket bucket : connectionBuckets) {
      if (!bucket.isConnectionsClean()) {
        return false;
      }
    }
    return true;
  }

  void cleanConnections() {
    for (ConnectionBucket bucket : connectionBuckets) {
      bucket.cleanConnections();
    }
  }
  
  void cleanIdleConnections(boolean oneBucket, String serverName) {
    if (oneBucket) {
      // Find first unempty bucket and clean it up.
      ConnectionBucket bucket = null;
      int i = 0;
      for (i = 0; i < numBuckets; i++) {
        int bucketIndex = nextBucketToCleanIdleConn.getAndIncrement()
            % numBuckets;
        ConnectionBucket b = getBucket(bucketIndex);
        if (b.hasConnection()) {
          bucket = b;
          break;
        } else if (Server.LOG.isDebugEnabled()) {
          Server.LOG
              .debug("Skip "
                  + i
                  + "th buckets of server "
                  + serverName
                  + " to clean up idle connections, since there is no connection there.");
        }
      }
      if (bucket != null) {
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug("Cleaning up idle connections for " + i
              + "th buckets of server " + serverName + ".");
        }
        bucket.cleanupIdleConnections(serverName);
      }
    } else {
      for (ConnectionBucket bucket : connectionBuckets) {
        bucket.cleanupIdleConnections(serverName);
      }
    }
  }
  
  ConnectionBucket getBucket(Connection c) {
    return getBucket(getBucketIndexFromConnection(c));
  }
  
  int getBucketIndexFromConnection(Connection c) {
    String connString = null;
    if (c == null || (connString = c.toString()) == null) {
      return 0;
    }
    int hashCode = Math.abs(connString.hashCode());
    return hashCode % numBuckets;
  }

  ConnectionBucket getBucket(int i) {
    if (i < 0 || i >= numBuckets) {
      return null;
    }
    return connectionBuckets[i];
  }

  private class ConnectionBucket {
    final private List<Connection> connectionArray = new ArrayList<Connection>();

    private synchronized boolean hasConnection() {
      return !connectionArray.isEmpty();
    }
    
    private synchronized void addConnection(Connection c) {
      connectionArray.add(c);
      numConnections.incrementAndGet();
    }
    private synchronized void removeConnection(Connection c) {
      int index = connectionArray.indexOf(c);
      if (index == -1) {
        return;
      }
      replaceConnectionWithTheLastOne(index);
    }

    synchronized private void cleanConnections() {
      for (Connection c : connectionArray) {
        closeConnectionWithoutException(c);
      }
      numConnections.addAndGet(-connectionArray.size());
      connectionArray.clear();
    }
    
    synchronized private void cleanupIdleConnections(String serverName) {
      long currentTime = System.currentTimeMillis();
      for (int i = 0; i < connectionArray.size();) {
        Connection c = connectionArray.get(i);
        if (c.timedOut(currentTime)) {
          if (Server.LOG.isDebugEnabled()) {
            Server.LOG.debug(serverName + ": disconnecting client "
                + c.getHostAddress());
          }
          closeConnectionWithoutException(c);
          replaceConnectionWithTheLastOne(i);
        } else {
          i++;
        }
      }
    }

    private synchronized boolean isConnectionsClean() {
      for (Connection c : connectionArray) {
        if (!c.responseQueue.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    private void replaceConnectionWithTheLastOne(int index) {
      int idxLastConn = connectionArray.size() - 1;
      if (index != idxLastConn) {
        connectionArray.set(index, connectionArray.get(idxLastConn));
      }
      connectionArray.remove(idxLastConn);
      numConnections.decrementAndGet();
    }

    private void closeConnectionWithoutException(Connection c) {
      try {
        c.close();
      } catch (IOException e) {
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug("IOException when closing connection", e);
        }
      }
    }
  }
}

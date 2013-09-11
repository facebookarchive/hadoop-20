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

import java.net.SocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;

/**
 * A cache of sockets
 */
public class SocketCache {
  public static final Log LOG = LogFactory.getLog(SocketCache.class);
  
  private final LinkedListMultimap<SocketAddress, Socket> multimap;
  private final int capacity;
  
  /**
   * Create a SocketCache with the given capacity.
   * @param capacity  Max cache size.
   */
  public SocketCache(int capacity) {
    multimap = LinkedListMultimap.create();
    this.capacity = capacity;
  }
  
  /**
   * Get a cached socket to the given address
   * @param remote  Remote address the socket is connected to.
   * @return  A socket with unknown state, possibly closed underneath. Or null.
   */
  public Socket get(SocketAddress remote) {
    synchronized(multimap) {
      List<Socket> sockList = multimap.get(remote);
      if (sockList == null) {
        return null;
      }
      
      Iterator<Socket> iter = sockList.iterator();
      while (iter.hasNext()) {
        Socket candidate = iter.next();
        iter.remove();
        if (!candidate.isClosed()) {
          return candidate;
        }
      }
    }
    return null;
  }
  
  /**
   * Give an unused socket to the cache.
   * @param sock  Socket not used by anyone.
   */
  public void put(Socket sock) {
    Preconditions.checkNotNull(sock);
    
    SocketAddress remoteAddr = sock.getRemoteSocketAddress();
    if (remoteAddr == null) {
      LOG.warn("Cannot cache (unconnected) socket with no remote address: " + 
               sock);
      IOUtils.closeSocket(sock);
      return;
    }
   
    Socket oldestSock = null;
    synchronized(multimap) {
      if (capacity == multimap.size()) {
        oldestSock = evictOldest();
      }
      multimap.put(remoteAddr, sock);
    }
    
    if (oldestSock != null) {
      IOUtils.closeSocket(oldestSock);
    }
  }
  
  public int size() {
    synchronized(multimap) {
      return multimap.size();
    }
  }
  
  /**
   * Evict the oldest entry in the cache.
   */
  private Socket evictOldest() {
    Iterator<Entry<SocketAddress, Socket>> iter = 
        multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalArgumentException("Cannot evict from empty cache!");
    }
    
    Entry<SocketAddress, Socket> entry = iter.next();
    iter.remove();
    return entry.getValue();
  }
  
  /**
   * Empty the cache, and close all sockets.
   */
  public void clear() {
    List<Socket> socketsToClear = new LinkedList<Socket>();
    synchronized(multimap) {
      for (Socket sock : multimap.values()) {
        socketsToClear.add(sock);
      }
      multimap.clear();
    }
    
    for (Socket sock : socketsToClear) {
      IOUtils.closeSocket(sock);
    }
  }
  
  protected void finalize() {
    clear();
  }
}

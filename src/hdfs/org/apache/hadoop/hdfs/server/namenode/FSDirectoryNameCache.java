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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * FSDirectoryNameCache wraps NameCache and offers fast update method for image
 * loading phase, where caching can be delegated to a separate thread.
 */
public class FSDirectoryNameCache {
  // buffer this many elements in temporary queue
  private static final int MAX_QUEUE_SIZE = 10000;
  
  // actual cache
  private final NameCache<ByteArray> nameCache;
  private volatile boolean imageLoaded;

  // initial caching utils
  private ExecutorService cachingExecutor;
  private List<Future<Void>> cachingTasks;
  private List<INode> cachingTempQueue;

  public FSDirectoryNameCache(int threshold) {
    nameCache = new NameCache<ByteArray>(threshold);
    imageLoaded = false;
    
    // executor for processing temporary queue (only 1 thread!!)
    cachingExecutor = Executors.newFixedThreadPool(1);
    cachingTempQueue = new ArrayList<INode>(MAX_QUEUE_SIZE);
    cachingTasks = new ArrayList<Future<Void>>();
  }

  /**
   * Adds cached entry to the map and updates INode
   */
  private void cacheNameInternal(INode inode) {
    // Name is cached only for files
    if (inode.isDirectory()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }

  void cacheName(INode inode) {
    if (inode.isDirectory()) {
      return;
    }
    if (this.imageLoaded) {
      // direct caching
      cacheNameInternal(inode);
      return;
    } 

     // otherwise add it to temporary queue
    cachingTempQueue.add(inode);
    
    // if queue is too large, submit a task
    if (cachingTempQueue.size() >= MAX_QUEUE_SIZE) {
      cachingTasks.add(cachingExecutor
          .submit(new CacheWorker(cachingTempQueue)));
      cachingTempQueue = new ArrayList<INode>(MAX_QUEUE_SIZE);
    }
  }
 
  /**
   * Worker for processing a list of inodes.
   */
  class CacheWorker implements Callable<Void> {
    private final List<INode> inodesToProcess;

    CacheWorker(List<INode> inodes) {
      this.inodesToProcess = inodes;
    }

    @Override
    public Void call() throws Exception {
      for (INode inode : inodesToProcess) {
        cacheNameInternal(inode);
      }
      return null;
    }
  }

  /**
   * Inform that from now on all caching is done synchronously.
   * Cache remaining inodes from the queue.
   * @throws IOException 
   */
  void imageLoaded() throws IOException {
    for (Future<Void> task : cachingTasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        throw new IOException("FSDirectory cache received interruption");
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    
    // will not be used after startup
    this.cachingTasks = null;
    this.cachingExecutor.shutdownNow();
    this.cachingExecutor = null;
    
    // process remaining inodes
    for(INode inode : cachingTempQueue) {
      cacheNameInternal(inode);
    }
    this.cachingTempQueue = null;
    
    this.imageLoaded = true;
  }
  
  void initialized() {
    this.nameCache.initialized();
  }

  int size() {
    return nameCache.size();
  }

  int getLookupCount() {
    return nameCache.getLookupCount();
  }
}

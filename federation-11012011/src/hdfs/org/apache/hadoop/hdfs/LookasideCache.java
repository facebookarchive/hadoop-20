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

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.*;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.metrics.LookasideMetrics.LocalMetrics;

/**
 * The cache is a typical implementation of a LRU cache.
 */

public class LookasideCache {

  public static final Log LOG = LogFactory.getLog(LookasideCache.class);

  // The size of the cache and its default value.
  public static String CACHESIZE = "fs.lookasidecache.size";
  public static long CACHESIZE_DEFAULT = 10 * 1024 * 1024;

  // The cache eviction percentage and its default value
  public static String CACHEEVICT_PERCENT = "fs.lookasidecache.evict.percent";
  public static long CACHEEVICT_PERCENT_DEFAULT = 10;

  // A static object to record metrics for all instances of this cache
  public static LocalMetrics localMetrics = new LocalMetrics();

  /*
   * Metrics are locally gathers in the static structure called 'stats'.
   * These are returned here to the metrics module, and the local values
   * in 'stats' are zeroed out.
   */
  public static LocalMetrics copyZeroLocalMetrics() {
    return new LocalMetrics(localMetrics);
  }

  /**
   * Returns a copy of the local Metrics
   */
  static LocalMetrics getLocalMetrics() {
    return localMetrics;
  }

  // The configuration
  private Configuration conf;

  // The global stamp is a monotonically increasing number, simulates
  // the passage of time.
  private static AtomicLong globalStamp = new AtomicLong(0);

  // a hashMap that keeps a  record of all entries in the cache. The
  // key to this hashMap is the full path name of the hdfs file. The
  // value is a record that describes the local file and its age.
  private ConcurrentHashMap<Path, CacheEntry> cacheMap;

  // the total size occupied by the cache. This is a virtual size,
  // the application can specify a size with each entry to be cached.
  private AtomicLong cacheSize = new AtomicLong(0);

  // is eviction in progress?
  private boolean evictionInProgress = false;

  // The maximum size of the cache. If it exceeds this number,
  // then eviction will start. However, current threads may
  // temporarily exceed this specified threshold.
  private long cacheSizeMax;

  // The percentage of the cache that will be evicted
  // if (and when) it becomes full.
  private long cacheEvictPercent;

  // call back into the application when an entry needs to be evicted
  private final Eviction evictionIface;

  // The details of each cache entry. It maps a hdfs path to
  // a local path.
  static class CacheEntry {
    Path hdfsPath;
    Path localPath;
    long genStamp;  // the timestamp of last access
    long entrySize; // the size of this entry

    CacheEntry(Path hdfsPath, Path localPath, long entrySize) {
      this.hdfsPath = hdfsPath;
      this.localPath = localPath;
      this.genStamp = globalStamp.incrementAndGet();
      this.entrySize = entrySize;
    }

    // stamp the latest timestamp in this entry
    synchronized void setGenstamp(long newValue) {
      assert this.genStamp < newValue;
      this.genStamp = newValue;
    }
  }

  // a comparator to sort CacheEntry in ascending order of genStamp
  static class CacheEntryComparator implements Comparator<CacheEntry> {
    public int compare(CacheEntry t1, CacheEntry t2) {
      if (t1.genStamp < t2.genStamp) {
        return -1;
      } else if (t1.genStamp > t2.genStamp) {
        return 1;
      } else {
        return 0;
      }
    }
  }
  static CacheEntryComparator LRU_COMPARATOR = new CacheEntryComparator();

  /**
   * The application that uses this cachemodule have to define this
   * interface so that the cache module can call back into the applcation
   * when an entry needs to be evicted from the cache.
   */
  interface Eviction {
    void evictCache(Path hdfsPath, Path localPath, long size)
      throws IOException;
  }

  /**
   * Create an instance of this Cache. Eviction notices are not
   * served to the application.
   */
  LookasideCache(Configuration conf) throws IOException {
    this(conf, null);
  }

  /**
   * Create an instance of this Cache. It has a maximum size. If
   * it fills up, we evict oldest entries from the cache until it has
   * a configured percentage of free space.
   */
  LookasideCache(Configuration conf, Eviction evictIface) throws IOException {
    this.conf = conf;
    this.cacheMap = new ConcurrentHashMap<Path, CacheEntry>();

    this.cacheSizeMax = conf.getLong(CACHESIZE, CACHESIZE_DEFAULT);
    this.cacheEvictPercent = conf.getLong(CACHEEVICT_PERCENT,
                                          CACHEEVICT_PERCENT_DEFAULT);
    this.evictionIface = evictIface;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache size " + this.cacheSizeMax +
                " Cache evict percentage " + this.cacheEvictPercent);
    }
  }

  /**
   * Returns the max size of the cache
   */
  long getCacheMaxSize() {
    return cacheSizeMax;
  }

  /**
   * Returns the eviction percentage of the cache
   */
  long getCacheEvictPercent() {
    return cacheEvictPercent;
  }

  /**
   * Returns the current size of the cache
   */
  long getCacheSize() {
    return cacheSize.get();
  }

  /**
   * Adds an entry into the cache.
   * The size is the virtual size of this entry.
   */
  void addCache(Path hdfsPath, Path localPath, long size) throws IOException {
    localMetrics.numAdd++;
    CacheEntry c = new CacheEntry(hdfsPath, localPath, size);
    CacheEntry found = cacheMap.putIfAbsent(hdfsPath, c);
    if (found != null) {
      // If entry was already in the cache, update its timestamp
      assert size == found.entrySize;
      assert localPath.equals(found.localPath);
      found.setGenstamp(globalStamp.incrementAndGet());
      localMetrics.numAddExisting++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("LookasideCache updating path " + hdfsPath);
      }
    } else {
      // We just inserted an entry in the cache. Increment the
      // recorded size of the cache.
      cacheSize.addAndGet(size);
      localMetrics.numAddNew++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("LookasideCache add new path:" + hdfsPath +
                  " cachedPath:" + localPath +
                  " size " + size);
      }
    }
    // check if we need to evict because cache is full
    if (cacheSize.get() > cacheSizeMax) {
      checkEvict();
    }
  }

  /**
   * Change the localPath in the cache. The size remains the
   * same. The accesstime is updated.
   */
  void renameCache(Path oldhdfsPath, Path newhdfsPath,
    Path localPath) throws IOException {
    CacheEntry found = cacheMap.remove(oldhdfsPath);
    if (found == null) {
      String msg = "LookasideCache error renaming path: " + oldhdfsPath +
                   " to: " + newhdfsPath +
                   " Path " + newhdfsPath +
                   " because it does not exists in the cache.";
      LOG.warn(msg);
      return;
    }
    // Update its timestamp and localPath
    found.hdfsPath = newhdfsPath;
    found.setGenstamp(globalStamp.incrementAndGet());
    found.localPath = localPath;

    // add it back to the cache
    CacheEntry empty = cacheMap.putIfAbsent(newhdfsPath, found);
    if (empty != null) {
      String msg = "LookasideCache error renaming path: " + oldhdfsPath +
                   " to: " + newhdfsPath +
                   " Path " + newhdfsPath +
                   " already exists in the cache.";
      LOG.warn(msg);
      throw new IOException(msg);
    }
    localMetrics.numRename++;
    if (LOG.isDebugEnabled()) {
      LOG.debug("LookasideCache renamed path:" + oldhdfsPath +
                " to:" + newhdfsPath +
                " cachedPath: " + localPath);
    }
  }

  /**
   * Delete an entry from the cache.
   */
  void removeCache(Path hdfsPath) {
    CacheEntry c = cacheMap.remove(hdfsPath);
    if (c != null) {
      cacheSize.addAndGet(-c.entrySize);
      localMetrics.numRemove++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("LookasideCache removed path:" + hdfsPath +
                  " freed up size: " + c.entrySize);
      }
    }
  }

  /**
   * Evicts an entry from the cache. This calls back into
   * the application to indicate that a cache entry has been
   * reclaimed.
   */
  void evictCache(Path hdfsPath) throws IOException {
    CacheEntry c = cacheMap.remove(hdfsPath);
    if (c != null) {
      cacheSize.addAndGet(-c.entrySize);
      if (evictionIface != null) {
        evictionIface.evictCache(c.hdfsPath, c.localPath, c.entrySize);
      }
      localMetrics.numEvict++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("LookasideCache removed path:" + hdfsPath +
                  " freed up size: " + c.entrySize);
      }
    }
  }

  /**
   * Maps the hdfs pathname to a local pathname. Returns null
   * if this is not found in the cache.
   */
  Path getCache(Path hdfsPath) {
    CacheEntry c = cacheMap.get(hdfsPath);
    localMetrics.numGetAttempts++;
    if (c != null) {
      // update the accessTime before returning to caller
      c.setGenstamp(globalStamp.incrementAndGet());
      localMetrics.numGetHits++;
      return c.localPath;
    }
    return null; // not in cache
  }

  /**
   * Eviction occurs if the cache is full, we free up a specified
   * percentage of the cache on every run. This method is synchronized
   * so that only one thread is doing the eviction.
   */
  synchronized void checkEvict() throws IOException {
    if (cacheSize.get() < cacheSizeMax) {
      return;        // nothing to do, plenty of free space
    }

    // Only one thread should be doing the eviction. Do not block
    // current thread, it is ok to oversubscribe the cache size
    // temporarily.
    if (evictionInProgress) {
      return;
    }

    // record the fact that eviction has started.
    evictionInProgress = true;

    try {
      // if the cache has reached a threshold size, then free old entries.
      long curSize = cacheSize.get();

      // how much to evict in one iteration
      long targetSize = cacheSizeMax -
                        (cacheSizeMax * cacheEvictPercent)/100;

      if (LOG.isDebugEnabled()) {
        LOG.debug("Cache size " + curSize + " has exceeded the " +
                  " maximum configured cacpacity " + cacheSizeMax +
                  ". Eviction has to reduce cache size to " +
                  targetSize);
      }

      // sort all entries based on their accessTimes
      Collection<CacheEntry> values = cacheMap.values();
      CacheEntry[] records = values.toArray(new CacheEntry[values.size()]);
      Arrays.sort(records, LRU_COMPARATOR);

      for (int i = 0; i < records.length; i++) {
        if (cacheSize.get() <= targetSize) {
          break;           // we reclaimed everything we wanted to
        }
        CacheEntry c = records[i];
        evictCache(c.hdfsPath);
      }
    } finally {
      evictionInProgress = false; // eviction done.
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache eviction complete. Current cache size is " +
                cacheSize.get());
    }
  }
}

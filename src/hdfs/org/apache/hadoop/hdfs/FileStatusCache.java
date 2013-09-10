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


import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
	/**
	 * FileStatusCache to avoid issuing duplicating requests for the same path.
	 * The cache uses LRU policy to replace an entry when the cache is full.
	 */
	public class FileStatusCache {
	  private class FileStatusCacheEntry {

		  public long lastUpdateTime;
		  public FileStatus fileStatus; 
		  
		  public FileStatusCacheEntry(long currentTime, FileStatus fileStatus) {
			  this.lastUpdateTime = currentTime;
			  this.fileStatus = fileStatus;
		  }  
	  }
	  
	  private final long expireTime;
	  private final int cacheSize;
	  //Set the default cache size to 1000
	  private static final int DEFAULT_FILE_STATUS_CACHE_SIZE = 10;
	  //Set the default expire time to 1s
	  private static final long DEFAULT_FILE_STATUS_CACHE_INTERVAL = 1000L; 	  
	  public static final FileStatus nullFileStatus = new FileStatus();
    private final LinkedHashMap<String, FileStatusCacheEntry> cache = new LinkedHashMap<String, FileStatusCacheEntry>() {
      protected boolean removeEldestEntry(Map.Entry<String, FileStatusCacheEntry> eldest) {
      return size() > cacheSize;
      }
    };
	  
	  public FileStatusCache(long timeInterval, int cacheSize){
		  expireTime = timeInterval;
		  this.cacheSize = cacheSize;
		  //cacheSize equals zero would cause priorityBlockingQueue create exception.
		  if (cacheSize == 0) {
			  cacheSize = DEFAULT_FILE_STATUS_CACHE_SIZE;
		  }
	  }
	  
	  public FileStatusCache() {
		  expireTime = DEFAULT_FILE_STATUS_CACHE_INTERVAL; 
		  cacheSize = DEFAULT_FILE_STATUS_CACHE_SIZE;
	  }
	  
	  public synchronized FileStatus get(String path) {
		  FileStatusCacheEntry cacheEntry;
		  long elapsedTime;
		  
		  if (expireTime == 0) {
		    return null;
		  }
		  
		  cacheEntry = cache.get(path); 		  
		  if (cacheEntry == null) {
		    return nullFileStatus;
		  }			  
		  		  
		  elapsedTime = System.currentTimeMillis() - cacheEntry.lastUpdateTime;  
		  if (elapsedTime > expireTime) {
			  return nullFileStatus;
		  }
		  
		  return cacheEntry.fileStatus; 		  
	  }
	  
	  //Invalidate all cache entry in the cache which will simplify the implementation.
	  public synchronized void clear() {
		  cache.clear();
	  }
	  
	  public synchronized void set(String path, FileStatus fileStatus) {
		  FileStatusCacheEntry cacheEntry;
		  
		  if (expireTime == 0) {
		    return;
		  }
		  //Force the cache update
      //Remove current cache entry from the linked list and insert it later.
		  cacheEntry = cache.remove(path);
		  if (cacheEntry == null) { 			  
			  //Create a new entry to insert.
        cacheEntry = new FileStatusCacheEntry(System.currentTimeMillis(), fileStatus); 
      }
      // Cache will remove the eldest entry.
      cache.put(path, cacheEntry);
      cacheEntry.fileStatus = fileStatus;
      cacheEntry.lastUpdateTime = System.currentTimeMillis();
	  }
}

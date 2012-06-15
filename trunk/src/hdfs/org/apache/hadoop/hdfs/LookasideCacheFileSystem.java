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
import java.io.OutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;

import org.apache.commons.logging.*;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.metrics.LookasideMetrics;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This is an implementation of a FileSystem that caches data on locally
 * mounted devices. It is a stackable filesystem on the client. It uses
 * a local path to cache entire files.
 *
 * Typically, this should be used when IO from a locally mounted
 * device has lower latencies than the underlying filesystem.
 * The cache is a typical implementation of a LRU cache. It uses
 * the name of the specified file as an index into the local cache.
 * This is a write-through cache, every write to a file is
 * cached in the cache. Also, writes are synchronous, every write
 * is written to the cache as well as the underlying filessytem.
 * Invalidation of the cache occurs when
 * the client removes a file from the filesystem.
 *
 * The cache is local to this instance of the LookasideCacheFileSystem
 * object. If the application is using another instance of the FileSystem
 * object to manipulate files, then this cache can stale.
 *
 * There are two configurable settings:
 * fs.lookasidecache.size :         The size (in bytes) of the cache
 *                                  when eviction starts.
 *                                  Default is 10 MB.
 * fs.lookasidecache.evict.percent: A single eviction iteration will
 *                                  continue until the percent of free
 *                                  space in the cache reaches this value.
 *                                  Default is 10%.
 *  fs.lookasidecache.dir         : The pathname where files are cached
 *                                  There is no default value.
 *  fs.hdfs.impl                  : Set to org.apache.hadoop.
 *                                  hdfs.LookasideCacheFileSystem to enable
 *                                  this feature.
 *  fs.lookasidecache.underlyingfs.impl: The name of the FileSystem above
 *                                  which the LookasideCacheFileSystem is
 *                                  layered.
 *                                  The default is DistributedFileSystem.
 */

public class LookasideCacheFileSystem extends FilterFileSystem
  implements LookasideCache.Eviction {

  public static final Log LOG = LogFactory.getLog(LookasideCacheFileSystem.class);

  Configuration conf;
  Path cachePath;       // the directory where to cache files
  String cacheDir;     // string representation of cachePath
  URI cacheURI;        // URI representation of cachePath
  FileSystem cacheFs;  // the fs where the local cache resides
  LookasideMetrics metrics; // metrics updater.

  // The LRU cache is here.
  LookasideCache lookasideCache;


  LookasideCacheFileSystem() throws IOException {
  }

  public LookasideCacheFileSystem(FileSystem fs) throws IOException {
    super(fs);
  }

  /* Initialize a LookasideCacheFileSystem
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    this.conf = conf;

    Class<?> clazz = conf.getClass("fs.lookasidecache.underlyingfs.impl",
        DistributedFileSystem.class);
    if (clazz == null) {
      throw new IOException("No FileSystem for " +
                            "fs.lookasidecache.underlyingfs.impl.");
    }

    this.fs = (FileSystem)ReflectionUtils.newInstance(clazz, null);
    super.initialize(name, conf);

    // find configured cache directory
    cacheDir = conf.get("fs.lookasidecache.dir");
    if (cacheDir == null) {
      LOG.info("fs.lookasidecache.dir is not defined");
      return;
    }
    cacheDir += "/" + System.currentTimeMillis();
    cachePath = new Path(cacheDir);
    assert cachePath.isAbsolute();
    cacheURI = URI.create("file:///" + cacheDir);

    if (cacheURI == null) {
      LOG.info("fs.lookasidecache.dir is not defined");
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("CacheDir is " + cacheURI);
    }

    // create an instance of a RawLocalFileSystem to read from the cache
    cacheFs = new RawLocalFileSystem();
    cacheFs.initialize(cacheURI, conf);

    // Create the cachedir if it does not exist
    // There could be pre-exisiting files in the cachedir, do not do
    // anything to them. They will not be served by this cache, but they
    // will not be deleted by this cache either.
    cacheFs.mkdirs(cachePath);

    // create a lookaside cache
    lookasideCache = new LookasideCache(conf, this);

    // create a metrics updater object
    metrics = new LookasideMetrics();
  }

  /*
   * Returns the underlying filesystem
   */
  public FileSystem getFileSystem() throws IOException {
    return fs;
  }

  /*
   * Returns the local filesystem where the cache is available
   */
  public FileSystem getCacheFileSystem() throws IOException {
    return cacheFs;
  }

  /**
   * Maps a hdfs path into a pathname in the local cache. In the current
   * implementation, the cachePath is the same as the hdfs pathname.
   */
  Path mapCachePath(Path hdfsPath) {
    assert hdfsPath.isAbsolute();
    Path value =  new Path(cacheDir + Path.SEPARATOR + hdfsPath);
    return value;
  }

  /**
   * Insert a file into the cache. If the cache is exceeding capacity,
   * then this call can, in turn, call backinto evictCache().
   */
  void addCache(Path hdfsPath, Path localPath, long size) throws IOException {
    assert size == new File(localPath.toString()).length();
    assert size == fs.getFileStatus(hdfsPath).getLen();
    lookasideCache.addCache(hdfsPath, localPath, size);
  }

  /**
   * Remove a file from the cache.
   */
  void removeCache(Path hdfsPath) throws IOException {
    lookasideCache.removeCache(hdfsPath);
  }

  /**
   * Evicts a file from the cache. If the cache is exceeding capacity,
   * then the cache calls this method to indicate that it is evicting
   * a file from the cache. This is part of the Eviction Interface.
   */
  public void evictCache(Path hdfsPath, Path localPath, long size)
    throws IOException {
    boolean done = cacheFs.delete(localPath, false);
    if (!done) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Evict for path: " + hdfsPath +
                  " local path " + localPath + " unsuccessful.");
      }
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // if the file exists, is non-zero size and exists in the cache, then
    // open it from the cache. If we open it successfully, then we do not have
    // to worry about it being evicted from the cache while we have it open
    // because the Linux OS will not actually delete the file till all open
    // handles are closed.
    Path localPath = null;
    try {
      FileStatus stat = getFileStatus(f);
      if (cacheFs != null && stat.getLen() > 0) {
        localPath = lookasideCache.getCache(f);
        if (localPath != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("LookasideCache open " + f + " from local cache at " +
                      localPath);
          }
          // paranoia, verify that sizes match
          File localFile = new File(localPath.toString());
          if (localFile.length() != stat.getLen()) {
            LOG.warn("LookasideCache hdfsfile " + f +
                      " has size " + stat.getLen() +
                      " but does not match cache file " + localFile +
                      " which has size " + localFile.length() +
                      ". Ignoring... not using the cache.");
          } else {
            return cacheFs.open(localPath, bufferSize);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("Unable to find hdfs file " + f + " in cache file " +
                localPath + ". Reading from HDFS..");
    }
    return fs.open(f, bufferSize); // send to underlying filesystem
  }

  /**
   * Close filesystem
   */
  @Override
  public void close() throws IOException {
    super.close();
    if (fs != null) {
      try {
        fs.close();
        fs = null;
      } catch(IOException ie) {
        //this might already be closed, ignore
      }
    }
    if (cacheFs != null) {
      try {
        cacheFs.close();
        cacheFs = null;
      } catch(IOException ie) {
        //this might already be closed, ignore
      }
    }
  }

  /**
   * Create new file. We start writing the data into underlying
   * filessystem as well as the cacheFileSystem.
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite,
    int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    FSDataOutputStream fd = new FSDataOutputStream(
                                new CacheOutputStream(conf, this, f,
                                    permission, overwrite, bufferSize,
                                    replication, blockSize, progress));

    return fd;
  }

  /**
   * Delete a file.
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    boolean val = fs.delete(f, recursive);
    if (cacheFs != null) {
      Path localPath = mapCachePath(f);
      lookasideCache.removeCache(f);
      try {
        cacheFs.delete(localPath, recursive);
      } catch (FileNotFoundException nfe) {
        // do nothing
      } catch (Exception e) {
        LOG.info("LookasideCacheFileSystem unable to find entry in " +
                 " local cache to delete " + localPath +
                 ". Ignoring...");
      }
    }
    return val;
  }

  @Override
  public boolean delete(Path f) throws IOException {
    boolean val = fs.delete(f);
    if (cacheFs != null) {
      Path localPath = mapCachePath(f);
      try {
        cacheFs.delete(localPath);
      } catch (FileNotFoundException nfe) {
        // do nothing
      } catch (Exception e) {
        LOG.info("LookasideCacheFileSystem unable to find entry in " +
                 " local cache to delete " + localPath +
                 ". Ignoring...");
      }
      lookasideCache.removeCache(f);
    }
    return val;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean val = fs.rename(src, dst);
    if (cacheFs != null) {
      Path localPath = mapCachePath(dst);
      try {
        cacheFs.rename(mapCachePath(src), localPath);
      } catch (FileNotFoundException nfe) {
        // do nothing
      } catch (Exception e) {
        LOG.info("LookasideCacheFileSystem unable to find entry in " +
                 " local cache to rename " + mapCachePath(src) +
                 " to " + localPath +
                 ". Ignoring...");
      }
      lookasideCache.renameCache(src, dst, localPath);
    }
    return val;
  }

  /**
   * Layered FileSystem OuputStream. We use it to write to both
   * the underlying filessytem as well as the cache file system.
   * We do not reserve space in the cache filesystem up-front, rather
   * we start writing to the cache file system, and when the entire
   * file is written and closed, we insert an entry into the cache.
   * This, in turn, triggers a cache eviction process if necessary.
   */
  private static class CacheOutputStream extends OutputStream
    implements Syncable {

    private final Configuration conf;
    private LookasideCacheFileSystem lfs;
    private FileSystem cacheFs;
    private Path hdfsPath;
    private Path localPath;
    private FSDataOutputStream cachefd;
    private FSDataOutputStream hd;
    private boolean hasError = false;
    private long filesize;

    CacheOutputStream(Configuration conf, LookasideCacheFileSystem lfs,
                      Path hdfsPath, FsPermission permission, boolean overwrite,
                      int buffersize, short replication, long blockSize,
                      Progressable progress) throws IOException {
      this.conf = conf;
      this.hdfsPath = hdfsPath;
      this.localPath = lfs.mapCachePath(hdfsPath);
      this.lfs = lfs;
      this.filesize = 0;
      this.cacheFs = lfs.getCacheFileSystem();
      this.hd = lfs.fs.create(hdfsPath, permission, overwrite,
                                  buffersize, replication, blockSize,
                                  progress);
      // if we are creating a file with the overwrite flag, then
      // delete any earlier versions from the cache. It will be
      // re-inserted into the cache when the file is closed.
      if (overwrite) {
        lfs.removeCache(hdfsPath);
      }
      try {
        this.cachefd = cacheFs.create(localPath, permission, overwrite,
                                      buffersize, replication, blockSize,
                                      progress);
      } catch (Exception e) {
        this.hasError = true; // do not write to cache anymore
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to create cache file " + localPath);
        }
      }
    }

    /**
     * Once we successfully close the cache file, add it to the cache.
     */
    public void close() throws IOException {
      hd.close();
      if (!hasError) {
        cachefd.close();
        lfs.addCache(hdfsPath, localPath, filesize);
      }
    }

    public void flush() throws IOException {
      hd.flush();
      try {
        if (!hasError) {
          cachefd.flush();
        }
      } catch (IOException e) {                // unexpected exception
        hasError = true;                       // no more caching
      }
    }

    public void write(byte[] b, int off, int len) throws IOException {
      hd.write(b, off, len);
      filesize += len;
      try {
        if (!hasError) {
          cachefd.write(b, off, len);
        }
      } catch (IOException e) {                // unexpected exception
        hasError = true;                       // no more caching
      }
    }

    public void write(int b) throws IOException {
      hd.write(b);
      filesize += 1;
      try {
        if (!hasError) {
          cachefd.write(b);
        }
      } catch (IOException e) {                // unexpected exception
        hasError = true;                       // no more caching
      }
    }

    /** {@inheritDoc} */
    public void sync() throws IOException {
      hd.sync();
      try {
        if (!hasError) {
          cachefd.sync();
        }
      } catch (IOException e) {                // unexpected exception
        hasError = true;                       // no more caching
      }
    }
  }
}

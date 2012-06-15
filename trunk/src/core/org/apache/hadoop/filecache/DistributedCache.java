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

package org.apache.hadoop.filecache;

import org.apache.commons.logging.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import java.net.URI;

/**
 * Distribute application-specific large, read-only files efficiently.
 *
 * <p><code>DistributedCache</code> is a facility provided by the Map-Reduce
 * framework to cache files (text, archives, jars etc.) needed by applications.
 * </p>
 *
 * <p>Applications specify the files, via urls (hdfs:// or http://) to be cached
 * via the {@link org.apache.hadoop.mapred.JobConf}.
 * The <code>DistributedCache</code> assumes that the
 * files specified via hdfs:// urls are already present on the
 * {@link FileSystem} at the path specified by the url.</p>
 *
 * <p>The framework will copy the necessary files on to the slave node before
 * any tasks for the job are executed on that node. Its efficiency stems from
 * the fact that the files are only copied once per job and the ability to
 * cache archives which are un-archived on the slaves.</p>
 *
 * <p><code>DistributedCache</code> can be used to distribute simple, read-only
 * data/text files and/or more complex types such as archives, jars etc.
 * Archives (zip, tar and tgz/tar.gz files) are un-archived at the slave nodes.
 * Jars may be optionally added to the classpath of the tasks, a rudimentary
 * software distribution mechanism.  Files have execution permissions.
 * Optionally users can also direct it to symlink the distributed cache file(s)
 * into the working directory of the task.</p>
 *
 * <p><code>DistributedCache</code> tracks modification timestamps of the cache
 * files. Clearly the cache files should not be modified by the application
 * or externally while the job is executing.</p>
 *
 * <p>Here is an illustrative example on how to use the
 * <code>DistributedCache</code>:</p>
 * <p><blockquote><pre>
 *     // Setting up the cache for the application
 *
 *     1. Copy the requisite files to the <code>FileSystem</code>:
 *
 *     $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat
 *     $ bin/hadoop fs -copyFromLocal map.zip /myapp/map.zip
 *     $ bin/hadoop fs -copyFromLocal mylib.jar /myapp/mylib.jar
 *     $ bin/hadoop fs -copyFromLocal mytar.tar /myapp/mytar.tar
 *     $ bin/hadoop fs -copyFromLocal mytgz.tgz /myapp/mytgz.tgz
 *     $ bin/hadoop fs -copyFromLocal mytargz.tar.gz /myapp/mytargz.tar.gz
 *
 *     2. Setup the application's <code>JobConf</code>:
 *
 *     JobConf job = new JobConf();
 *     DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"),
 *                                   job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/map.zip", job);
 *     DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytar.tar", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytgz.tgz", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
 *
 *     3. Use the cached files in the {@link org.apache.hadoop.mapred.Mapper}
 *     or {@link org.apache.hadoop.mapred.Reducer}:
 *
 *     public static class MapClass extends MapReduceBase
 *     implements Mapper&lt;K, V, K, V&gt; {
 *
 *       private Path[] localArchives;
 *       private Path[] localFiles;
 *
 *       public void configure(JobConf job) {
 *         // Get the cached archives/files
 *         localArchives = DistributedCache.getLocalCacheArchives(job);
 *         localFiles = DistributedCache.getLocalCacheFiles(job);
 *       }
 *
 *       public void map(K key, V value,
 *                       OutputCollector&lt;K, V&gt; output, Reporter reporter)
 *       throws IOException {
 *         // Use data from the cached archives/files here
 *         // ...
 *         // ...
 *         output.collect(k, v);
 *       }
 *     }
 *
 * </pre></blockquote></p>
 *
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 */
public class DistributedCache {
  // cacheID to cacheStatus mapping
  private static TreeMap<String, CacheStatus> cachedArchives = new TreeMap<String, CacheStatus>();

  private static TreeMap<Path, Long> baseDirSize = new TreeMap<Path, Long>();
  private static TreeMap<Path, Integer> baseDirNumberSubDir =
    new TreeMap<Path, Integer>();

  // default total cache size
  private static final long DEFAULT_CACHE_SIZE = 10737418240L;
  private static final long DEFAULT_CACHE_SUBDIR_LIMIT = 10000;

  private static final Log LOG =
    LogFactory.getLog(DistributedCache.class);
    private static Random random = new Random();

  /**
   * Get the locally cached file or archive; it could either be
   * previously cached (and valid) or copy it from the {@link FileSystem} now.
   *
   * @param cache the cache to be localized, this should be specified as
   * new URI(hdfs://hostname:port/absolute_path_to_file#LINKNAME). If no schema
   * or hostname:port is provided the file is assumed to be in the filesystem
   * being used in the Configuration
   * @param conf The Confguration file which contains the filesystem
   * @param baseDir The base cache Dir where you wnat to localize the files/archives
   * @param fileStatus The file status on the dfs.
   * @param isArchive if the cache is an archive or a file. In case it is an
   *  archive with a .zip or .jar or .tar or .tgz or .tar.gz extension it will
   *  be unzipped/unjarred/untarred automatically
   *  and the directory where the archive is unzipped/unjarred/untarred is
   *  returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param confFileStamp this is the hdfs file modification timestamp to verify that the
   * file to be cached hasn't changed since the job started
   * @param currentWorkDir this is the directory where you would want to create symlinks
   * for the locally cached files/archives
   * @return the path to directory where the archives are unjarred in case of archives,
   * the path to the file where the file is copied locally
   * @throws IOException
   */
  public static Path getLocalCache(URI cache, Configuration conf,
                                   Path baseDir, FileStatus fileStatus,
                                   boolean isArchive, long confFileStamp,
                                   Path currentWorkDir,
                                   MRAsyncDiskService asyncDiskService)
  throws IOException {
    return getLocalCache(cache, conf, baseDir, fileStatus, isArchive,
        confFileStamp, fileStatus.getLen(), currentWorkDir, true,
        asyncDiskService, new LocalDirAllocator("mapred.local.dir"));
  }

  public static Path getLocalCacheFromTimestamps(URI cache, Configuration conf,
      Path subDir, FileStatus fileStatus,
      boolean isArchive, long confFileStamp,
      long fileLength, Path currentWorkDir,
      boolean honorSymLinkConf,
      MRAsyncDiskService asyncDiskService,
      LocalDirAllocator lDirAllocator) 
  throws IOException {
    return getLocalCache(cache, conf, subDir, fileStatus, isArchive,
        confFileStamp, fileLength, currentWorkDir, honorSymLinkConf,
        asyncDiskService, lDirAllocator);
  }

  public static Path getLocalCacheFromURI(URI cache, Configuration conf,
      Path subDir, boolean isArchive, long fileLength, 
      Path currentWorkDir, boolean honorSymLinkConf,
      MRAsyncDiskService asyncDiskService, LocalDirAllocator lDirAllocator)
  throws IOException {
    return getLocalCache(cache, conf, subDir, null, isArchive,
        0, fileLength, currentWorkDir, honorSymLinkConf,
        asyncDiskService, lDirAllocator);
  }

  /**
   * Added for back compatibility.
   */
  public static Path getLocalCache(URI cache, Configuration conf, Path subdir,
      FileStatus fileStatus, boolean isArchive, long confFileStamp, Path
      currentWorkDir, boolean honorSymLinkConf, MRAsyncDiskService
      asyncDiskService, LocalDirAllocator lDirAllocator)
  throws IOException {
    return getLocalCache(cache, conf, subdir, fileStatus, isArchive,
        confFileStamp, fileStatus.getLen(), currentWorkDir, honorSymLinkConf,
        asyncDiskService, lDirAllocator);
  }


  /**
   * Get the locally cached file or archive; it could either be
   * previously cached (and valid) or copy it from the {@link FileSystem} now.
   *
   * @param cache the cache to be localized, this should be specified as
   * new URI(hdfs://hostname:port/absolute_path_to_file#LINKNAME). If no schema
   * or hostname:port is provided the file is assumed to be in the filesystem
   * being used in the Configuration
   * @param conf The Confguration file which contains the filesystem
   * @param subDir The sub cache Dir where you want to localize the
   * files/archives
   * @param fileStatus The file status on the dfs.
   * @param isArchive if the cache is an archive or a file. In case it is an
   *  archive with a .zip or .jar or .tar or .tgz or .tar.gz extension it will
   *  be unzipped/unjarred/untarred automatically
   *  and the directory where the archive is unzipped/unjarred/untarred is
   *  returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param confFileStamp this is the hdfs file modification timestamp to verify
   * that the file to be cached hasn't changed since the job started
   * @param fileLength this is the length of the cache file
   * @param currentWorkDir this is the directory where you would want to create
   * symlinks for the locally cached files/archives
   * @param honorSymLinkConf if this is false, then the symlinks are not
   * created even if conf says so (this is required for an optimization in task
   * launches
   * @param lDirAllocator LocalDirAllocator of the tracker
   * @return the path to directory where the archives are unjarred in case of
   * archives, the path to the file where the file is copied locally
   * @throws IOException
   */
  private static Path getLocalCache(URI cache, Configuration conf,
      Path subDir, FileStatus fileStatus,
      boolean isArchive, long confFileStamp,
      long fileLength, 
      Path currentWorkDir, boolean honorSymLinkConf,
      MRAsyncDiskService asyncDiskService,
      LocalDirAllocator lDirAllocator)
  throws IOException {
    String key = getKey(cache, conf, confFileStamp);

    CacheStatus lcacheStatus;
    Path localizedPath;
    synchronized (cachedArchives) {
      lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        // was never localized
        Path uniqueParentDir =
          new Path(subDir, String.valueOf(random.nextLong()));
        String cachePath = new Path(uniqueParentDir,
                                    makeRelative(cache, conf)).toString();
        Path localPath = lDirAllocator.getLocalPathForWrite(cachePath,
          fileLength, conf);
        lcacheStatus =
          new CacheStatus(new Path(localPath.toString().replace(cachePath, "")),
                          localPath, uniqueParentDir);
        cachedArchives.put(key, lcacheStatus);
      }
      lcacheStatus.refcount++;
    }
    boolean initSuccessful = false;
    try {
      synchronized (lcacheStatus) {
        if (!lcacheStatus.isInited()) {
          localizedPath = localizeCache(conf, cache, confFileStamp,
              lcacheStatus, isArchive);
          lcacheStatus.initComplete();
        } else {
          if (fileStatus != null) {
            localizedPath = checkCacheStatusValidity(conf, cache, confFileStamp,
                lcacheStatus, fileStatus, isArchive);
          }
          else {
            // if fileStatus is null, then the md5 must be correct
            // so there is no need to check for cache validity
            localizedPath = lcacheStatus.localizedLoadPath;
          }
        }
        createSymlink(conf, cache, lcacheStatus, isArchive, currentWorkDir,
            honorSymLinkConf);
      }

      // try deleting stuff if you can
      long size = 0;
      int numberSubDir = 0;
      synchronized (lcacheStatus) {
        synchronized (baseDirSize) {
          Long get = baseDirSize.get(lcacheStatus.getBaseDir());
          if (get != null) {
            size = get.longValue();
          } else {
            LOG.warn("Cannot find size of baseDir: "
                + lcacheStatus.getBaseDir());
          }
        }
        synchronized (baseDirNumberSubDir) {
          Integer get = baseDirNumberSubDir.get(lcacheStatus.getBaseDir());
          if ( get != null ) {
            numberSubDir = get.intValue();
          } else {
            LOG.warn("Cannot find subdirectories limit of baseDir: " +
                     lcacheStatus.getBaseDir());
          }
        }
      }
      // setting the cache size to a default of 10GB
      long allowedSize = conf.getLong("local.cache.size", DEFAULT_CACHE_SIZE);
      long allowedNumberSubDir = conf.getLong("local.cache.numbersubdir",
                                             DEFAULT_CACHE_SUBDIR_LIMIT);
      if (allowedSize < size || allowedNumberSubDir < numberSubDir) {
        // try some cache deletions
        LOG.debug("Start deleting released cache because" +
                  " [size, allowedSize, numberSubDir, allowedNumberSubDir] =" +
                  " [" + size + ", " + allowedSize + ", " + numberSubDir + ", "
                  + allowedNumberSubDir + "]");
        deleteCache(conf, asyncDiskService);
      }
      initSuccessful = true;
      return localizedPath;
    } finally {
      if (!initSuccessful) {
        synchronized (cachedArchives) {
          lcacheStatus.refcount--;
        }
      }
    }
  }


  /**
   * Get the locally cached file or archive; it could either be
   * previously cached (and valid) or copy it from the {@link FileSystem} now.
   *
   * @param cache the cache to be localized, this should be specified as
   * new URI(hdfs://hostname:port/absolute_path_to_file#LINKNAME). If no schema
   * or hostname:port is provided the file is assumed to be in the filesystem
   * being used in the Configuration
   * @param conf The Confguration file which contains the filesystem
   * @param baseDir The base cache Dir where you wnat to localize the files/archives
   * @param isArchive if the cache is an archive or a file. In case it is an
   *  archive with a .zip or .jar or .tar or .tgz or .tar.gz extension it will
   *  be unzipped/unjarred/untarred automatically
   *  and the directory where the archive is unzipped/unjarred/untarred
   *  is returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param confFileStamp this is the hdfs file modification timestamp to verify that the
   * file to be cached hasn't changed since the job started
   * @param currentWorkDir this is the directory where you would want to create symlinks
   * for the locally cached files/archives
   * @return the path to directory where the archives are unjarred in case of archives,
   * the path to the file where the file is copied locally
   * @throws IOException

   */
  public static Path getLocalCache(URI cache, Configuration conf,
                                   Path baseDir, boolean isArchive,
                                   long confFileStamp, Path currentWorkDir,
                                   MRAsyncDiskService asyncDiskService)
  throws IOException {
    return getLocalCache(cache, conf,
                         baseDir, null, isArchive,
                         confFileStamp, currentWorkDir,
                         asyncDiskService);
  }

  /**
   * This is the opposite of getlocalcache. When you are done with
   * using the cache, you need to release the cache
   * @param cache The cache URI to be released
   * @param conf configuration which contains the filesystem the cache
   * is contained in.
   * @throws IOException
   */
  public static void releaseCache(URI cache, Configuration conf, long timeStamp)
    throws IOException {
    String cacheId = getKey(cache, conf, timeStamp);

    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = cachedArchives.get(cacheId);
      if (lcacheStatus == null) {
        LOG.warn("Cannot find localized cache: " + cache +
                 " (key: " + cacheId + ") in releaseCache!");
        return;
      }
      lcacheStatus.refcount--;
    }
  }

  /**
   * Runnable which removes the cache directories from the disk
   */
  private static class CacheFileCleanTask implements Runnable {
    private MRAsyncDiskService asyncDiskService;
    private LocalFileSystem fs;
    private List<CacheStatus> toBeDeletedCache;
    public CacheFileCleanTask(MRAsyncDiskService asyncDiskService,
                              LocalFileSystem fs,
                              List<CacheStatus> toBeDeletedCache) {
      this.asyncDiskService = asyncDiskService;
      this.fs = fs;
      this.toBeDeletedCache = toBeDeletedCache;
    }

    @Override
    public void run() {
      for (CacheStatus lcacheStatus : toBeDeletedCache) {
        synchronized (lcacheStatus) {
          Path fullUniqueParentDir = new Path(lcacheStatus.localizedBaseDir,
              lcacheStatus.uniqueParentDir);
          try {
            LOG.info ("Deleting local cached path: " + fullUniqueParentDir.toString());
            deleteLocalPath(asyncDiskService, fs, fullUniqueParentDir);
            // decrement the size of the cache from baseDirSize
            deleteCacheInfoUpdate(lcacheStatus);
            LOG.info("Removed cache " + lcacheStatus.localizedLoadPath);
          } catch (IOException e) {
            LOG.warn("Error when deleting " + fullUniqueParentDir, e);
          }
        }
      }
    }
  }
  // To delete the caches which have a refcount of zero

  private static void deleteCache(Configuration conf,
      MRAsyncDiskService asyncDiskService) throws IOException {
    List<CacheStatus> deleteSet = new LinkedList<CacheStatus>();
    // try deleting cache Status with refcount of zero
    synchronized (cachedArchives) {
      for (Iterator<String> it = cachedArchives.keySet().iterator();
           it.hasNext();) {
        String cacheId = (String) it.next();
        CacheStatus lcacheStatus = cachedArchives.get(cacheId);
        if (lcacheStatus.refcount == 0) {
          // delete this cache entry from the global list
          // and mark the localized file for deletion
          deleteSet.add(lcacheStatus);
          it.remove();
        }
      }
    }
    // do the deletion asynchronously, after releasing the global lock
    Thread cacheFileCleaner = new Thread(
        new CacheFileCleanTask(asyncDiskService,
                               FileSystem.getLocal(conf),
                               deleteSet));
    cacheFileCleaner.start();
  }

  /**
   * Delete a local path with asyncDiskService if available,
   * or otherwise synchronously with local file system.
   */
  private static void deleteLocalPath(MRAsyncDiskService asyncDiskService,
      LocalFileSystem fs, Path path) throws IOException {
    boolean deleted = false;
    if (asyncDiskService != null) {
      // Try to delete using asyncDiskService
      String localPathToDelete =
        path.toUri().getPath();
      deleted = asyncDiskService.moveAndDeleteAbsolutePath(localPathToDelete);
      if (!deleted) {
        LOG.warn("Cannot find DistributedCache path " + localPathToDelete
            + " on any of the asyncDiskService volumes!");
      }
    }
    if (!deleted) {
      // If no asyncDiskService, we will delete the files synchronously
      fs.delete(path, true);
    }
    LOG.info("Deleted path " + path);
  }

  /*
   * Returns the relative path of the dir this cache will be localized in
   * relative path that this cache will be localized in. For
   * hdfs://hostname:port/absolute_path -- the relative path is
   * hostname/absolute path -- if it is just /absolute_path -- then the
   * relative path is hostname of DFS this mapred cluster is running
   * on/absolute_path
   */
  public static String makeRelative(URI cache, Configuration conf)
    throws IOException {
    String host = cache.getHost();
    if (host == null) {
      host = cache.getScheme();
    }
    if (host == null) {
      URI defaultUri = FileSystem.get(conf).getUri();
      host = defaultUri.getHost();
      if (host == null) {
        host = defaultUri.getScheme();
      }
    }
    String path = host + cache.getPath();
    path = path.replace(":/","/");                // remove windows device colon
    return path;
  }

  static String getKey(URI cache, Configuration conf, long timeStamp)
      throws IOException {
    return makeRelative(cache, conf) + String.valueOf(timeStamp);
  }

  private static Path checkCacheStatusValidity(Configuration conf,
      URI cache, long confFileStamp,
      CacheStatus cacheStatus,
      FileStatus fileStatus,
      boolean isArchive
      ) throws IOException {
    FileSystem fs = FileSystem.get(cache, conf);
    // Has to be
    if (!ifExistsAndFresh(conf, fs, cache, confFileStamp,
                          cacheStatus, fileStatus)) {
      throw new IOException("Stale cache file: " +
                            cacheStatus.localizedLoadPath +
                            " for cache-file: " + cache);
    }
    LOG.info(String.format("Using existing cache of %s->%s",
             cache.toString(), cacheStatus.localizedLoadPath));
    return cacheStatus.localizedLoadPath;
  }

  private static void createSymlink(Configuration conf, URI cache,
      CacheStatus cacheStatus, boolean isArchive,
      Path currentWorkDir, boolean honorSymLinkConf) throws IOException {
    boolean doSymlink = honorSymLinkConf && DistributedCache.getSymlink(conf);
    if(cache.getFragment() == null) {
      doSymlink = false;
    }
    String link =
      currentWorkDir.toString() + Path.SEPARATOR + cache.getFragment();
    File flink = new File(link);
    if (doSymlink){
      if (!flink.exists()) {
        FileUtil.symLink(cacheStatus.localizedLoadPath.toString(), link);
      }
    }
  }

  // the method which actually copies the caches locally and unjars/unzips them
  // and does chmod for the files
  private static Path localizeCache(Configuration conf,
                                    URI cache, long confFileStamp,
                                    CacheStatus cacheStatus,
                                    boolean isArchive)
  throws IOException {
    FileSystem fs = getFileSystem(cache, conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    Path parchive = null;

    if (isArchive) {
      parchive = new Path(cacheStatus.localizedLoadPath,
        new Path(cacheStatus.localizedLoadPath.getName()));
    } else {
      parchive = cacheStatus.localizedLoadPath;
    }
    if (!localFs.mkdirs(parchive.getParent())) {
      throw new IOException("Mkdirs failed to create directory " +
          cacheStatus.localizedLoadPath.toString());
    }
    String cacheId = cache.getPath();

    fs.copyToLocalFile(new Path(cacheId), parchive);
    if (isArchive) {
      String tmpArchive = parchive.toString().toLowerCase();
      File srcFile = new File(parchive.toString());
      File destDir = new File(parchive.getParent().toString());
      if (tmpArchive.endsWith(".jar")) {
        RunJar.unJar(srcFile, destDir);
      } else if (tmpArchive.endsWith(".zip")) {
        FileUtil.unZip(srcFile, destDir);
      } else if (isTarFile(tmpArchive)) {
        FileUtil.unTar(srcFile, destDir);
      }
      // else will not do anyhting
      // and copy the file into the dir as it is
    }
    long cacheSize = FileUtil.getDU(new File(parchive.getParent().toString()));
    cacheStatus.size = cacheSize;
    addCacheInfoUpdate(cacheStatus);

    // do chmod here
    try {
      //Setting recursive permission to grant everyone read and execute
      Path localDir = new Path(cacheStatus.localizedBaseDir,
                               cacheStatus.uniqueParentDir);
      LOG.info("Doing chmod on localdir :" + localDir);
      FileUtil.chmod(localDir.toString(), "ugo+rx", true);
    } catch(InterruptedException e) {
      LOG.warn("Exception in chmod" + e.toString());
    }

    // update cacheStatus to reflect the newly cached file
    cacheStatus.mtime = getTimestamp(conf, cache);
    return cacheStatus.localizedLoadPath;
  }

  private static boolean isTarFile(String filename) {
    return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
           filename.endsWith(".tar"));
  }

  // Checks if the cache has already been localized and is fresh
  private static boolean ifExistsAndFresh(Configuration conf, FileSystem fs,
                                          URI cache, long confFileStamp,
                                          CacheStatus lcacheStatus,
                                          FileStatus fileStatus)
  throws IOException {
    // check for existence of the cache
    long dfsFileStamp;
    if (fileStatus != null) {
      dfsFileStamp = fileStatus.getModificationTime();
    } else {
      dfsFileStamp = getTimestamp(conf, cache);
    }

    // ensure that the file on hdfs hasn't been modified since the job started
    if (dfsFileStamp != confFileStamp) {
      LOG.fatal("File: " + cache + " has changed on HDFS since job started");
      throw new IOException("File: " + cache +
          " has changed on HDFS since job started");
    }

    if (dfsFileStamp != lcacheStatus.mtime) {
      // needs refreshing
      return false;
    }

    return true;
  }

  /**
   * Returns mtime of a given cache file on hdfs.
   * @param conf configuration
   * @param cache cache file
   * @return mtime of a given cache file on hdfs
   * @throws IOException
   */
  public static long getTimestamp(Configuration conf, URI cache)
    throws IOException {
    FileSystem fileSystem = FileSystem.get(cache, conf);
    Path filePath = new Path(cache.getPath());

    return fileSystem.getFileStatus(filePath).getModificationTime();
  }

  /**
   * Returns the status of a given cache file on hdfs.
   * @param conf configuration
   * @param cache cache file
   * @return FileStatus object of the file
   * @throws IOException
   */
  public static FileStatus getFileStatus(Configuration conf, URI cache)
    throws IOException {
      FileSystem fileSystem = FileSystem.get(cache, conf);
      Path filePath = new Path(cache.getPath());

      return fileSystem.getFileStatus(filePath);
    }

  /**
   * This method create symlinks for all files in a given dir in another
   * directory
   * @param conf the configuration
   * @param jobCacheDir the target directory for creating symlinks
   * @param workDir the directory in which the symlinks are created
   * @throws IOException
   */
  public static void createAllSymlink(Configuration conf, File jobCacheDir, File
      workDir)
    throws IOException{
    if ((jobCacheDir == null || !jobCacheDir.isDirectory()) ||
           workDir == null || (!workDir.isDirectory())) {
      return;
    }
    boolean createSymlink = getSymlink(conf);
    if (createSymlink){
      File[] list = jobCacheDir.listFiles();
      for (int i=0; i < list.length; i++){
        FileUtil.symLink(list[i].getAbsolutePath(),
                         new File(workDir, list[i].getName()).toString());
      }
    }
  }

  private static String getFileSysName(URI url) {
    String fsname = url.getScheme();
    if ("hdfs".equals(fsname)) {
      String host = url.getHost();
      int port = url.getPort();
      return (port == (-1)) ? host : (host + ":" + port);
    } else {
      return null;
    }
  }

  private static FileSystem getFileSystem(URI cache, Configuration conf)
    throws IOException {
    String fileSysName = getFileSysName(cache);
    if (fileSysName != null)
      return FileSystem.get(cache, conf);
    else
      return FileSystem.get(conf);
  }

  /**
   * Set the configuration with the given set of archives
   * @param archives The list of archives that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheArchives(URI[] archives, Configuration conf) {
    String sarchives = StringUtils.uriToString(archives);
    conf.set("mapred.cache.archives", sarchives);
  }

  /**
   * Set the configuration with the given set of files
   * @param files The list of files that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheFiles(URI[] files, Configuration conf) {
    String sfiles = StringUtils.uriToString(files);
    conf.set("mapred.cache.files", sfiles);
  }

  /**
   * Get cache archives set in the Configuration
   * @param conf The configuration which contains the archives
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings("mapred.cache.archives"));
  }

  /**
   * Get cache archives set in the Configuration
   * @param conf The configuration which contains the archives
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */
  public static URI[] getSharedCacheArchives(Configuration conf)
    throws IOException {
    return
      StringUtils.stringToURI(conf.getStrings("mapred.cache.shared.archives"));
  }

  /**
   * Get cache files set in the Configuration
   * @param conf The configuration which contains the files
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */

  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings("mapred.cache.files"));
  }

  /**
   * Get cache files set in the Configuration
   * @param conf The configuration which contains the files
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */

  public static URI[] getSharedCacheFiles(Configuration conf)
    throws IOException {
    return
      StringUtils.stringToURI(conf.getStrings("mapred.cache.shared.files"));
  }

  /**
   * Return the path array of the localized caches
   * @param conf Configuration that contains the localized archives
   * @return A path array of localized caches
   * @throws IOException
   */
  public static Path[] getLocalCacheArchives(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf
                                    .getStrings("mapred.cache.localArchives"));
  }

 /**
   * Return the path array of the localized caches
   * @param conf Configuration that contains the localized archives
   * @return A path array of localized caches
   * @throws IOException
   */
  public static Path[] getLocalSharedCacheArchives(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf
        .getStrings("mapred.cache.shared.localArchives"));
  }

  /**
   * Return the path array of the localized files
   * @param conf Configuration that contains the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public static Path[] getLocalCacheFiles(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf.getStrings("mapred.cache.localFiles"));
  }

 /**
   * Return the path array of the localized files
   * @param conf Configuration that contains the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public static Path[] getLocalSharedCacheFiles(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf
        .getStrings("mapred.cache.shared.localFiles"));
  }

  /**
   * Get the timestamps of the archives
   * @param conf The configuration which stored the timestamps
   * @return a string array of timestamps
   * @throws IOException
   */
  public static String[] getArchiveTimestamps(Configuration conf) {
    return conf.getStrings("mapred.cache.archives.timestamps");
  }


  /**
   * Get the timestamps of the files
   * @param conf The configuration which stored the timestamps
   * @return a string array of timestamps
   * @throws IOException
   */
  public static String[] getFileTimestamps(Configuration conf) {
    return conf.getStrings("mapred.cache.files.timestamps");
  }

  public static String[] getSharedArchiveLength(Configuration conf) {
    return conf.getStrings("mapred.cache.shared.archives.length");
  }

  public static String[] getSharedFileLength(Configuration conf) {
    return conf.getStrings("mapred.cache.shared.files.length");
  }

  /**
   * This is to check the timestamp of the archives to be localized
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of archives.
   * The order should be the same as the order in which the archives are added.
   */
  public static void setArchiveTimestamps(Configuration conf, String timestamps)
  {
    conf.set("mapred.cache.archives.timestamps", timestamps);
  }

  public static void setSharedArchiveLength(Configuration conf, String length) {
    conf.set("mapred.cache.shared.archives.length", length);
  }

  /**
   * This is to check the timestamp of the files to be localized
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of files.
   * The order should be the same as the order in which the files are added.
   */
  public static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set("mapred.cache.files.timestamps", timestamps);
  }

  public static void setSharedFileLength(Configuration conf, String length) {
    conf.set("mapred.cache.shared.files.length", length);
  }

  /**
   * Set the conf to contain the location for localized archives
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  public static void setLocalArchives(Configuration conf, String str) {
    conf.set("mapred.cache.localArchives", str);
  }

  /**
   * Set the conf to contain the location for localized archives
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  public static void setLocalSharedArchives(Configuration conf, String str) {
    conf.set("mapred.cache.shared.localArchives", str);
  }

  /**
   * Set the conf to contain the location for localized files
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set("mapred.cache.localFiles", str);
  }

  /**
   * Set the conf to contain the location for localized files
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  public static void setLocalSharedFiles(Configuration conf, String str) {
    conf.set("mapred.cache.shared.localFiles", str);
  }

  /**
   * Add a archives to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheArchive(URI uri, Configuration conf) {
    String archives = conf.get("mapred.cache.archives");
    conf.set("mapred.cache.archives", archives == null ? uri.toString()
             : archives + "," + uri.toString());
  }

  /**
   * Add a archives to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addSharedCacheArchive(URI uri, Configuration conf) {
    String archives = conf.get("mapred.cache.shared.archives");
    conf.set("mapred.cache.shared.archives", archives == null ? uri.toString()
        : archives + "," + uri.toString());
  }

  /**
   * Add a file to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheFile(URI uri, Configuration conf) {
    String files = conf.get("mapred.cache.files");
    conf.set("mapred.cache.files", files == null ? uri.toString() : files + ","
             + uri.toString());
  }

  /**
   * Add a file to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addSharedCacheFile(URI uri, Configuration conf) {
    String files = conf.get("mapred.cache.shared.files");
    conf.set("mapred.cache.shared.files", files == null ?
        uri.toString() : files + ","+ uri.toString());
  }

  /**
   * Add an file path to the current set of classpath entries It adds the file
   * to cache as well.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   */
  public static void addFileToClassPath(Path file, Configuration conf)
    throws IOException {
    String classpath = conf.get("mapred.job.classpath.files");
    conf.set("mapred.job.classpath.files", classpath == null ? file.toString()
             : classpath + System.getProperty("path.separator") + file.toString());
    URI uri = file.makeQualified(file.getFileSystem(conf)).toUri();

    addCacheFile(uri, conf);
  }

  /**
   * Get the file entries in classpath as an array of Path
   *
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getFileClassPaths(Configuration conf) {
    String classpath = conf.get("mapred.job.classpath.files");
    if (classpath == null)
      return null;
    ArrayList list = Collections.list(new StringTokenizer(classpath, System
                                                          .getProperty("path.separator")));
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path((String) list.get(i));
    }
    return paths;
  }

  private static URI addArchiveToClassPathHelper(Path archive,
                                                  Configuration conf)
    throws IOException {

    String classpath = conf.get("mapred.job.classpath.archives");

    // the scheme/authority use ':' as separator. put the unqualified path in classpath
    String archivePath = archive.toUri().getPath();

    conf.set("mapred.job.classpath.archives", classpath == null ? archivePath
             : classpath + System.getProperty("path.separator")
             + archivePath);
    return archive.makeQualified(archive.getFileSystem(conf)).toUri();
  }


  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.
   *
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   */
  public static void addArchiveToClassPath(Path archive, Configuration conf)
    throws IOException {
    URI uri = addArchiveToClassPathHelper(archive, conf);
    addCacheArchive(uri, conf);
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.
   *
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   */
  public static void addSharedArchiveToClassPath(Path archive, Configuration
      conf)
    throws IOException {
    URI uri = addArchiveToClassPathHelper(archive, conf);
    addSharedCacheArchive(uri, conf);
  }

  /**
   * Get the archive entries in classpath as an array of Path
   *
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getArchiveClassPaths(Configuration conf) {
    String classpath = conf.get("mapred.job.classpath.archives");
    if (classpath == null)
      return null;
    ArrayList list = Collections.list(new StringTokenizer(classpath, System
                                                          .getProperty("path.separator")));
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path((String) list.get(i));
    }
    return paths;
  }

  /**
   * This method allows you to create symlinks in the current working directory
   * of the task to all the cache files/archives
   * @param conf the jobconf
   */
  public static void createSymlink(Configuration conf){
    conf.set("mapred.create.symlink", "yes");
  }

  /**
   * This method checks to see if symlinks are to be create for the
   * localized cache files in the current working directory
   * @param conf the jobconf
   * @return true if symlinks are to be created- else return false
   */
  public static boolean getSymlink(Configuration conf){
    String result = conf.get("mapred.create.symlink");
    if ("yes".equals(result)){
      return true;
    }
    return false;
  }

  /**
   * This method checks if there is a conflict in the fragment names
   * of the uris. Also makes sure that each uri has a fragment. It
   * is only to be called if you want to create symlinks for
   * the various archives and files.
   * @param uriFiles The uri array of urifiles
   * @param uriArchives the uri array of uri archives
   */
  public static boolean checkURIs(URI[]  uriFiles, URI[] uriArchives){
    if ((uriFiles == null) && (uriArchives == null)){
      return true;
    }
    if (uriFiles != null){
      for (int i = 0; i < uriFiles.length; i++){
        String frag1 = uriFiles[i].getFragment();
        if (frag1 == null)
          return false;
        for (int j=i+1; j < uriFiles.length; j++){
          String frag2 = uriFiles[j].getFragment();
          if (frag2 == null)
            return false;
          if (frag1.equalsIgnoreCase(frag2))
            return false;
        }
        if (uriArchives != null){
          for (int j = 0; j < uriArchives.length; j++){
            String frag2 = uriArchives[j].getFragment();
            if (frag2 == null){
              return false;
            }
            if (frag1.equalsIgnoreCase(frag2))
              return false;
            for (int k=j+1; k < uriArchives.length; k++){
              String frag3 = uriArchives[k].getFragment();
              if (frag3 == null)
                return false;
              if (frag2.equalsIgnoreCase(frag3))
                return false;
            }
          }
        }
      }
    }
    return true;
  }

  private static class CacheStatus {
    // the local load path of this cache
    Path localizedLoadPath;

    //the base dir where the cache lies
    Path localizedBaseDir;

    // the unique directory in localizedBaseDir, where the cache lies
    Path uniqueParentDir;

    //the size of this cache
    long size;

    // number of instances using this cache
    int refcount;

    // the cache-file modification time
    long mtime;

    // is it initialized
    boolean inited = false;

    public CacheStatus(Path baseDir, Path localLoadPath, Path uniqueParentDir) {
      super();
      this.localizedLoadPath = localLoadPath;
      this.refcount = 0;
      this.mtime = -1;
      this.localizedBaseDir = baseDir;
      this.size = 0;
       this.uniqueParentDir = uniqueParentDir;
    }

    // get the base dir for the cache
    Path getBaseDir() {
      return localizedBaseDir;
    }

    // Is it initialized?
    boolean isInited() {
      return inited;
    }

    // mark it as initalized
    void initComplete() {
      inited = true;
    }
  }

  /**
   * Clear the entire contents of the cache and delete the backing files. This
   * should only be used when the server is reinitializing, because the users
   * are going to lose their files.
   */
  public static void purgeCache(Configuration conf,
      MRAsyncDiskService service) throws IOException {
    synchronized (cachedArchives) {
      LocalFileSystem localFs = FileSystem.getLocal(conf);
      for (Map.Entry<String,CacheStatus> f: cachedArchives.entrySet()) {
        try {
          deleteLocalPath(service, localFs, f.getValue().localizedLoadPath);
        } catch (IOException ie) {
          LOG.debug("Error cleaning up cache", ie);
        }
      }
      cachedArchives.clear();
    }
  }

  /**
   * Update the maps baseDirSize and baseDirNumberSubDir when deleting cache.
   * @param cacheStatus cache status of the cache is deleted
   */
  private static void deleteCacheInfoUpdate(CacheStatus cacheStatus) {
    if (!cacheStatus.isInited()) {
      // if it is not created yet, do nothing.
      return;
    }
    synchronized (baseDirSize) {
      Long dirSize = baseDirSize.get(cacheStatus.getBaseDir());
      if ( dirSize != null ) {
        dirSize -= cacheStatus.size;
        baseDirSize.put(cacheStatus.getBaseDir(), dirSize);
      }
    }
    synchronized (baseDirNumberSubDir) {
      Integer dirSubDir = baseDirNumberSubDir.get(cacheStatus.getBaseDir());
      if ( dirSubDir != null ) {
        dirSubDir--;
        baseDirNumberSubDir.put(cacheStatus.getBaseDir(), dirSubDir);
      }
    }
  }

  /**
   * Update the maps baseDirSize and baseDirNumberSubDir when adding cache.
   * @param cacheStatus cache status of the cache is added
   */
  private static void addCacheInfoUpdate(CacheStatus cacheStatus) {
    long cacheSize = cacheStatus.size;
    synchronized (baseDirSize) {
      Long dirSize = baseDirSize.get(cacheStatus.getBaseDir());
      if( dirSize == null ) {
        dirSize = Long.valueOf(cacheSize);
      } else {
        dirSize += cacheSize;
      }
      baseDirSize.put(cacheStatus.getBaseDir(), dirSize);
    }
    synchronized (baseDirNumberSubDir) {
      Integer dirSubDir = baseDirNumberSubDir.get(cacheStatus.getBaseDir());
      if( dirSubDir == null ) {
        dirSubDir = 1;
      } else {
        dirSubDir += 1;
      }
      baseDirNumberSubDir.put(cacheStatus.getBaseDir(), dirSubDir);
    }
  }
}

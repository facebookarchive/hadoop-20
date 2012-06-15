package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class CachingAvatarZooKeeperClient extends AvatarZooKeeperClient {
  private String cacheDir = null;
  private boolean useCache = false;

  private Map<String, Long> lastCacheMod = Collections
      .synchronizedMap(new HashMap<String, Long>());

  public CachingAvatarZooKeeperClient(Configuration conf, Watcher watcher) {
    super(conf, watcher);
    this.cacheDir = conf.get("fs.ha.zookeeper.cache.dir", "/tmp");
    this.useCache = conf.getBoolean("fs.ha.zookeeper.cache", false);
  }

  private String getFromCache(URI address, Stat stat, boolean retry)
      throws IOException, InterruptedException {
    FileLock lock = tryLock(true);
    String result = null;
    try {
      File cache = new File(cacheDir, address.getAuthority());
      if (!cache.exists()) {
        return null;
      }
      lastCacheMod.put(address.getAuthority(), cache.lastModified());
      BufferedReader reader = new BufferedReader(new FileReader(cache));
      result = reader.readLine();
    } finally {
      lock.release();
    }
    return result;
  }

  private FileLock tryLock(boolean retry) throws IOException,
      InterruptedException {
    File lockFile = new File(cacheDir, ".lock");
    RandomAccessFile file = new RandomAccessFile(lockFile, "rws");
    FileLock lock = null;
    do {
      try {
        lock = file.getChannel().tryLock();
      } catch (OverlappingFileLockException ex) {
        // A thread inside of this JVM has the lock on the file
        lock = null;
        Thread.sleep(1000);
      }
    } while (lock == null && retry);
    return lock;
  }

  private String populateCache(URI address, Stat stat, boolean retry)
      throws IOException, InterruptedException, KeeperException {
    FileLock lock = tryLock(true);
    String val = null;
    try {
      File cache = new File(cacheDir, address.getAuthority());
      long fileModTime = cache.lastModified();
      Long lastFileReadTime = lastCacheMod.get(address.getAuthority());
      if (lastFileReadTime == null || lastFileReadTime >= fileModTime) {
        // Cache has not been updated and we need to populate it
        val = super.getPrimaryAvatarAddress(address, stat, retry);
        FileWriter writer = new FileWriter(cache);
        writer.write(val);
        writer.close();
        lastCacheMod.put(address.getAuthority(), cache.lastModified());
      }
    } finally {
      if (lock != null) {
        lock.release();
      }
    }
    if (val == null) {
      val = getFromCache(address, stat, retry);
    }
    return val;
  }

  public String getPrimaryAvatarAddress(URI address, Stat stat, boolean retry,
      boolean firstAttempt) throws IOException, KeeperException,
      InterruptedException {
    String result = null;
    if (!useCache) {
      return super.getPrimaryAvatarAddress(address, stat, retry);
    }
    if (firstAttempt) {
      result = getFromCache(address, stat, retry);
    }
    if (result == null) {
      result = populateCache(address, stat, retry);
    }
    return result;
  }

  public boolean isCacheEnabled() {
    return useCache;
  }
}

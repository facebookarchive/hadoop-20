package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class CachingAvatarZooKeeperClient extends AvatarZooKeeperClient {
  private String cacheDir = null;
  private boolean useCache = false;

  // This needs to be thread local so that each thread maintains its own copy of
  // last file read time for a cache file. In this way multiple threads using
  // the same client would still go to zookeeper only once. We use a linked list
  // since the number of elements in this data structure won't be large and
  // hence we don't pay a huge lookup penalty and at the same time save memory.
  private ThreadLocal<LinkedList<CacheEntry>> lastCacheMod =
    new ThreadLocal<LinkedList<CacheEntry>>() {
    @Override
    protected LinkedList<CacheEntry> initialValue() {
      return new LinkedList<CacheEntry>();
    }
  };

  private static class CacheEntry {
    public final String key;
    public long readTime;

    public CacheEntry(String key, long readTime) {
      this.key = key;
      this.readTime = readTime;
    }
  }

  private CacheEntry findEntry(String key) {
    for (CacheEntry entry : lastCacheMod.get()) {
      if (entry.key.equals(key)) {
        return entry;
      }
    }
    return null;
  }

  private Long getReadTime(String key) {
    CacheEntry entry = findEntry(key);
    return (entry != null) ? entry.readTime : null;
  }

  private void updateReadTime(String key, long readTime) {
    CacheEntry entry = findEntry(key);
    if (entry != null) {
      entry.readTime = readTime;
    } else {
      entry = new CacheEntry(key, readTime);
      lastCacheMod.get().add(entry);
    }
  }

  public abstract class ZooKeeperCall {

    private final String dataFileSuffix;
    private final String lockFileSuffix;
    protected final URI address;

    private ZooKeeperCall(String dataFileSuffix, String lockFileSuffix,
        URI address) {
      this.dataFileSuffix = dataFileSuffix;
      this.lockFileSuffix = lockFileSuffix;
      this.address = address;
    }

    public File getDataFile(String cacheDir) {
      return new File(cacheDir, address.getAuthority() + dataFileSuffix);
    }

    public File getLockFile(String cacheDir) {
      return new File(cacheDir, ".avatar_zk_cache_lock" + lockFileSuffix);
    }

    public abstract String invoke() throws IOException, InterruptedException,
        KeeperException;
  }

  public class GetAddr extends ZooKeeperCall {
    private final boolean retry;
    private final Stat stat;

    public GetAddr(URI address, Stat stat, boolean retry) {
      super("", "", address);
      this.stat = stat;
      this.retry = retry;
    }

    public String invoke() throws IOException, InterruptedException,
        KeeperException {
      return CachingAvatarZooKeeperClient.super.getPrimaryAvatarAddress(
          address, stat,
          retry);
    }
  }

  public class GetStat extends ZooKeeperCall {
    public GetStat(URI address) {
      super("_stat", "_stat", address);
    }

    public String invoke() throws IOException, InterruptedException,
        KeeperException {
      return ""
          + CachingAvatarZooKeeperClient.super
              .getPrimaryRegistrationTime(address);
    }
  }

  public CachingAvatarZooKeeperClient(Configuration conf, Watcher watcher) {
    super(conf, watcher);
    this.cacheDir = conf.get("fs.ha.zookeeper.cache.dir", "/tmp");
    this.useCache = conf.getBoolean("fs.ha.zookeeper.cache", false);
  }

  private String retrieveAndPopulateCache(File cache, ZooKeeperCall call)
      throws IOException, KeeperException, InterruptedException {
    String val = call.invoke();
    FileWriter writer = new FileWriter(cache);
    writer.write(val);
    writer.close();
    updateReadTime(cache.getName(), cache.lastModified());
    return val;
  }

  private String getFromCache(ZooKeeperCall call) throws IOException,
          InterruptedException, KeeperException {
    FileLock lock = tryLock(true, call);
    String result = null;
    BufferedReader reader = null;
    try {
      File cache = call.getDataFile(cacheDir);
      if (!cache.exists()) {
        return retrieveAndPopulateCache(cache, call);
      }
      updateReadTime(cache.getName(), cache.lastModified());
      reader = new BufferedReader(new FileReader(cache));
      result = reader.readLine();
    } finally {
      if (reader != null) {
        reader.close();
      }
      lock.release();
    }
    return result;
  }

  private FileLock tryLock(boolean retry, ZooKeeperCall call)
      throws IOException, InterruptedException {
    File lockFile = call.getLockFile(cacheDir);
    RandomAccessFile file = null;

    for (int i = 0; i < 10; i++) {
      try {
        file = new RandomAccessFile(lockFile, "rws");
        break;
      } catch (FileNotFoundException fnfe) {
        // We experience this exception for unknown reason, we retry
        // a few times.
        if (!retry) {
          return null;
        }
        if (!new File(cacheDir).exists()) {
          new File(cacheDir).mkdir();
        }
        if (i == 9) {
          throw fnfe;
        } else {
          Thread.sleep(250);
        }
      }
    }

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

  private String populateCache(ZooKeeperCall call)
      throws IOException, InterruptedException, KeeperException {
    FileLock lock = tryLock(true, call);
    String val = null;
    try {
      File cache = call.getDataFile(cacheDir);
      long fileModTime = cache.lastModified();
      Long lastFileReadTime = getReadTime(cache.getName());
      if (lastFileReadTime != null && lastFileReadTime >= fileModTime) {
        // Cache has not been updated and we need to populate it
        val = retrieveAndPopulateCache(cache, call);
      }
    } finally {
      if (lock != null) {
        lock.release();
      }
    }
    if (val == null) {
      val = getFromCache(call);
    }
    return val;
  }

  @Override
  public long getPrimaryRegistrationTime(URI address) throws IOException,
      KeeperException, InterruptedException {
    if (!useCache) {
      return super.getPrimaryRegistrationTime(address);
    }
    ZooKeeperCall call = new GetStat(address);
    return Long.parseLong(populateCache(call));
  }

  public String getPrimaryAvatarAddress(URI address, Stat stat, boolean retry,
      boolean firstAttempt) throws IOException, KeeperException,
         InterruptedException {
    if (!useCache) {
      return super.getPrimaryAvatarAddress(address, stat, retry);
    }
    ZooKeeperCall call = new GetAddr(address, stat, retry);
    return populateCache(call);
  }

  public boolean isCacheEnabled() {
    return useCache;
  }

  String getCacheDir() {
    return this.cacheDir;
  }
}

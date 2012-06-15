package org.apache.hadoop.filecache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestDistributedCache extends TestCase {
  
  static final URI LOCAL_FS = URI.create("file:///");
  private static String TEST_CACHE_BASE_DIR = "cachebasedir";
  private static String TEST_ROOT_DIR =
    System.getProperty("test.build.data", "/tmp/distributedcache");
  private static String MAPRED_LOCAL_DIR = TEST_ROOT_DIR + "/mapred/local";
  private static final int LOCAL_CACHE_LIMIT = 5 * 1024; //5K
  private static final int LOCAL_CACHE_FILES = 2;
  private Configuration conf;
  private Path firstCacheFile;
  private Path secondCacheFile;
  private Path thirdCacheFile;
  private Path fourthCacheFile;
  private FileSystem localfs;
  
  /**
   * @see TestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
    conf.set("mapred.local.dir", MAPRED_LOCAL_DIR);
    conf.setLong("local.cache.numbersubdir", LOCAL_CACHE_FILES);
    FileUtil.fullyDelete(new File(TEST_CACHE_BASE_DIR));
    FileUtil.fullyDelete(new File(TEST_ROOT_DIR));
    localfs = FileSystem.get(LOCAL_FS, conf);
    firstCacheFile = new Path(TEST_ROOT_DIR+"/firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR+"/secondcachefile");
    thirdCacheFile = new Path(TEST_ROOT_DIR+"/thirdcachefile");
    fourthCacheFile = new Path(TEST_ROOT_DIR+"/fourthcachefile");
    createTempFile(localfs, firstCacheFile, 4 * 1024);
    createTempFile(localfs, secondCacheFile, 2 * 1024);
    createTempFile(localfs, thirdCacheFile, 1);
    createTempFile(localfs, fourthCacheFile, 1);
  }
  
  /** test delete cache */
  public void testDeleteCache() throws Exception {
    // We first test the size of files exceeds the limit
    long now = System.currentTimeMillis();
    Path firstLocalCache = DistributedCache.getLocalCache(
        firstCacheFile.toUri(), conf, new Path(TEST_CACHE_BASE_DIR),
        localfs.getFileStatus(firstCacheFile),
        false, now, new Path(TEST_ROOT_DIR), null);
    // Release the first cache so that it can be deleted when sweeping
    DistributedCache.releaseCache(firstCacheFile.toUri(), conf, now);
    DistributedCache.getLocalCache(
        secondCacheFile.toUri(), conf, new Path(TEST_CACHE_BASE_DIR), 
        localfs.getFileStatus(firstCacheFile),
        false, now, new Path(TEST_ROOT_DIR), null);
    // The total size is about 6 * 1024 which is greater than 5 * 1024.
    // So released cache should be deleted.
    checkCacheDeletion(localfs, firstLocalCache);
    
    // Now we test the number of files limit
    Path thirdLocalCache = DistributedCache.getLocalCache(
        thirdCacheFile.toUri(), conf, new Path(TEST_CACHE_BASE_DIR), 
        localfs.getFileStatus(firstCacheFile),
        false, now, new Path(TEST_ROOT_DIR), null);
    // Release the third cache so that it can be deleted when sweeping
    DistributedCache.releaseCache(thirdCacheFile.toUri(), conf, now);
    DistributedCache.getLocalCache(
        fourthCacheFile.toUri(), conf, new Path(TEST_CACHE_BASE_DIR), 
        localfs.getFileStatus(firstCacheFile),
        false, now, new Path(TEST_ROOT_DIR), null);
    // The total number of caches is now 3 which is greater than 2.
    // So released cache should be deleted.
    checkCacheDeletion(localfs, thirdLocalCache);
  }
  
  /**
   * Periodically checks if a file is there, return if the file is no longer
   * there. Fails the test if a files is there for 5 minutes.
   */
  private void checkCacheDeletion(FileSystem fs, Path cache) throws Exception {
    // Check every 100ms to see if the cache is deleted
    boolean cacheExists = true;
    for (int i = 0; i < 3000; i++) {
      if (!fs.exists(cache)) {
        cacheExists = false;
        break;
      }
      TimeUnit.MILLISECONDS.sleep(100L);
    }
    // If the cache is still there after 5 minutes, test fails.
    assertFalse("DistributedCache failed deleting old cache",
                cacheExists);
  }

  private void createTempFile(FileSystem fs, Path p, int size) throws IOException {
    FSDataOutputStream out = fs.create(p);
    byte[] toWrite = new byte[size];
    new Random().nextBytes(toWrite);
    out.write(toWrite);
    out.close();
    FileSystem.LOG.info("created: " + p + ", size=" + size);
  }
  
  /**
   * @see TestCase#tearDown()
   */
  @Override
  protected void tearDown() throws IOException {
    localfs.delete(firstCacheFile, true);
    localfs.delete(secondCacheFile, true);
    localfs.delete(thirdCacheFile, true);
    localfs.delete(fourthCacheFile, true);
    localfs.close();
  }
}

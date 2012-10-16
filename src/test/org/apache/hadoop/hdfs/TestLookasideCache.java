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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hdfs.metrics.LookasideMetrics.LocalMetrics;
import org.apache.log4j.Level;

/**
 * This class tests the lookaside cache.
 */
public class TestLookasideCache extends junit.framework.TestCase {
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.TestLookasideCache");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/lookasidecache/test/data")).getAbsolutePath();
  final static int NUM_DATANODES = 3;

  {
    ((Log4JLogger)LookasideCache.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LookasideCacheFileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  public void testCache() throws IOException {
    Configuration conf = new Configuration();
    long maxSize = 5 * 1024;
    long evictPercent = 20;
    conf.setLong(LookasideCache.CACHESIZE, maxSize);
    conf.setLong(LookasideCache.CACHEEVICT_PERCENT,  evictPercent);
    LookasideCache cache = new LookasideCache(conf);
    LocalMetrics metrics = LookasideCache.getLocalMetrics();
    metrics.reset();

    assertTrue(cache.getCacheMaxSize() == maxSize);
    assertTrue(cache.getCacheSize() == 0);
    assertTrue(cache.getCacheEvictPercent() == evictPercent);

    // insert five elements into the cache, each of size 1024
    cache.addCache(new Path("one"), new Path("one"), 1024);
    cache.addCache(new Path("two"), new Path("two"), 1024);
    cache.addCache(new Path("three"), new Path("three"), 1024);
    cache.addCache(new Path("four"), new Path("four"), 1024);
    cache.addCache(new Path("five"), new Path("five"), 1024);
    assertTrue(cache.getCacheSize() == 5 * 1024);
    assertTrue(metrics.numAdd == 5);
    assertTrue(metrics.numAddNew == 5);
    assertTrue(metrics.numAddExisting == 0);

    // the cache is now full. If we add one more element, the oldest
    // two should be evicted
    cache.addCache(new Path("six"), new Path("six"), 512);
    assertTrue("cachesize is " + cache.getCacheSize(),
               cache.getCacheSize() == 3 * 1024 + 512);

    // verify that first two are not there. and the rest is there
    assertTrue(cache.getCache(new Path("one")) == null);
    assertTrue(cache.getCache(new Path("two")) == null);
    assertTrue(cache.getCache(new Path("three")) != null);
    assertTrue(cache.getCache(new Path("four")) != null);
    assertTrue(cache.getCache(new Path("five")) != null);
    assertTrue(cache.getCache(new Path("six")) != null);
    assertTrue(metrics.numEvict == 2);

    // make three the most recently used
    assertTrue(cache.getCache(new Path("three")) != null);
    assertTrue(metrics.numGetAttempts == 7);
    assertTrue(metrics.numGetHits == 5);

    // now we insert seven.
    cache.addCache(new Path("seven"), new Path("seven"), 512);
    assertTrue(cache.getCacheSize() == 4 * 1024);

    assertTrue(cache.getCache(new Path("one")) == null);
    assertTrue(cache.getCache(new Path("two")) == null);
    assertTrue(cache.getCache(new Path("three")) != null);
    assertTrue(cache.getCache(new Path("four")) != null);
    assertTrue(cache.getCache(new Path("five")) != null);
    assertTrue(cache.getCache(new Path("six")) != null);
    assertTrue(cache.getCache(new Path("seven")) != null);
  }

  public void testCacheFileSystem() throws IOException {

    // configure a cached filessytem, cache size is 10 KB.
    mySetup(10*1024L);

    try {
      // create a 5K file using the LookasideCache. This write
      // should be cached in the cache.
      Path file = new Path("/hdfs/testRead");
      long crc = createTestFile(lfs, file, 1, 5, 1024L);
      FileStatus stat = lfs.getFileStatus(file);
      LOG.info("Created " + file + ", crc=" + crc + ", len=" + stat.getLen());
      assertTrue(lfs.lookasideCache.getCacheSize() == 5 * 1024);

      // Test that readFully via the Lookasidecache fetches correct data
      // from the cache.
      FSDataInputStream stm = lfs.open(file);
      byte[] filebytes = new byte[(int)stat.getLen()];
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
      stm.close();

      // assert that there is one element of size 5K in the cache
      assertEquals(5*1024, lfs.lookasideCache.getCacheSize());

      // create a 6K file using the LookasideCache. This is an
      // overwrite of the earlier file, so the cache should reflect
      // the new size of the file.
      crc = createTestFile(lfs, file, 1, 6, 1024L);
      stat = lfs.getFileStatus(file);
      LOG.info("Created " + file + ", crc=" + crc + ", len=" + stat.getLen());

      // assert that there is one element of size 6K in the cache
      assertEquals(6*1024, lfs.lookasideCache.getCacheSize());

      // verify reading file2 from the cache
      stm = lfs.open(file);
      filebytes = new byte[(int)stat.getLen()];
      stm.readFully(0, filebytes);
      assertEquals(crc, bufferCRC(filebytes));
      stm.close();

      // add a 5 KB file to the cache. This should start eviction of
      // the earlier file.
      Path file2 = new Path("/hdfs/testRead2");
      crc = createTestFile(lfs, file2, 1, 5, 1024L);
      stat = lfs.getFileStatus(file2);
      LOG.info("Created " + file2 + ", crc=" + crc + ", len=" + stat.getLen());
      assertEquals(5*1024, lfs.lookasideCache.getCacheSize());

      // move file2 to file3
      Path file3 = new Path("/hdfs/testRead3");
      assertTrue(lfs.rename(file2, file3));

      // delete file3. This should clear out the cache.
      lfs.delete(file3, false);
      assertEquals(0, lfs.lookasideCache.getCacheSize());

    } finally {
      myTearDown();
    }
  }

  private MiniDFSCluster dfs;
  private FileSystem fileSys;
  private LookasideCacheFileSystem lfs;
  private String namenode;
  private String hftp;

  // setup a LookasideCachedFileSystem
  private void mySetup(long cacheSize) throws IOException {
    Configuration conf = new Configuration();

    // create a HDFS cluster
    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();
    FileSystem.setDefaultUri(conf, namenode);

    // create a client-side layered filesystem.
    // The cache size is 10 KB.
    lfs = getCachedHdfs(fileSys, conf, cacheSize);
  }

  private void myTearDown() throws IOException {
    if (dfs != null) { dfs.shutdown(); }
  }


  /**
   * Returns a cached filesystem layered on top of the HDFS cluster
   */
  private LookasideCacheFileSystem getCachedHdfs(FileSystem fileSys,
                        Configuration conf, long cacheSize) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
    Configuration clientConf = new Configuration(conf);

    clientConf.setLong(LookasideCache.CACHESIZE, cacheSize);
    clientConf.set("fs.lookasidecache.dir", TEST_DIR);
    clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.LookasideCacheFileSystem");
    clientConf.set("fs.lookasidecache.underlyingfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");
    URI dfsUri = dfs.getUri();
    FileSystem.closeAll();
    FileSystem lfs = FileSystem.get(dfsUri, clientConf);
    assertTrue("lfs not an instance of LookasideCacheFileSystem",
               lfs instanceof LookasideCacheFileSystem);
    return (LookasideCacheFileSystem)lfs;
  }

  //
  // creates a file and populate it with random data. Returns its crc.
  //
  private static long createTestFile(FileSystem fileSys, Path name, int repl,
                        int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    Random rand = new Random();
    FSDataOutputStream stm = fileSys.create(name, true,
                        fileSys.getConf().getInt("io.file.buffer.size", 4096),
                        (short)repl, blocksize);
    // fill random data into file
    final byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }

  /**
   * returns the CRC32 of the buffer
   */
  private long bufferCRC(byte[] buf) {
    CRC32 crc = new CRC32();
    crc.update(buf, 0, buf.length);
    return crc.getValue();
  }
}

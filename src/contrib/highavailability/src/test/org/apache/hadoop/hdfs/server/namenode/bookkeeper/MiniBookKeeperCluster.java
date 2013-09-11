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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Starts and manages an individual BookKeeper cluster, with each Bookie
 * running in a separate thread with data stored in a separate directory.
 * Allows individual bookies to be manually stopped for testing purposes.
 */
public class MiniBookKeeperCluster {

  private static final Log LOG = LogFactory.getLog(MiniBookKeeperCluster.class);

  private static final String DEFAULT_TEST_DIR =
      "build/test/data";
  public static final String TEST_DIR =
      new File(System.getProperty("test.build.data", DEFAULT_TEST_DIR)).
          getAbsolutePath();

  // Per-bookie data lives in bk.bookie.data${n} where n is the bookie id
  private static final String BK_BOOKIE_DATA_DIR_PREFIX =
      TEST_DIR + "/bk.bookie.data";

  private final int numBookies;
  private final int zkSessionTimeoutMs;
  private final int zkConnectionTimeoutMs;
  private final String zkQuorum;
  private final Map<Integer, EmbeddedBookie> bookieById;

  // The bookies run in a separate thread pool
  private final ExecutorService bookieThreadPool;

  private ZooKeeper zkClient;

  /**
   * Create the base configuration and the thread pools for an embedded
   * BookKeeper cluster
   * @param zkQuorum ZooKeeper connection string (e.g., "host:port")
   * @param numBookies Number of bookies to start
   * @param zkSessionTimeoutMs ZooKeeper session timeout in milliseconds
   * @param zkConnectionTimeoutMs Maximum time in milliseconds to wait
   *                              until we connect to ZooKeeper
   */
  public MiniBookKeeperCluster(String zkQuorum,
      int numBookies,
      int zkSessionTimeoutMs,
      int zkConnectionTimeoutMs) {
    this.numBookies = numBookies;
    this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    this.zkQuorum = zkQuorum;
    bookieById = Maps.newHashMap();
    bookieThreadPool = Executors.newFixedThreadPool(numBookies);
  }

  /**
   * Start BookKeeper cluster with the number of nodes specified at
   * construction time
   */
  public void start() throws IOException {
    initZK();
    // prepare zk
    // Start all the bookies in separate threads
    ServerConfiguration baseConf = new ServerConfiguration();
    for (int i = 0; i < numBookies; i++) {
      EmbeddedBookie bookie = new EmbeddedBookie(baseConf, zkQuorum, i);
      bookieById.put(i, bookie);
      bookieThreadPool.submit(bookie);
    }
  }

  /**
   * Shutdown the BookKeeper cluster and its threadpool
   */
  public void shutdown() throws IOException {
    try {
      stopAllBookies();
    } finally {
      bookieThreadPool.shutdown();
    }
  }

  /**
   * Shutdown the BookKeeper cluster
   */
  public void stopAllBookies() {
    for (EmbeddedBookie bookie : bookieById.values()) {
      try {
        bookie.shutdown();
      } catch (Exception e) {
        LOG.error("Error shutting down bookie " + bookie.getBookieId(), e);
      }
    }
  }

  /**
   * Stop a specified bookie
   * @param bookieId A number, 0 <= bookieId <= numBookies
   * @throws IllegalArgumentException If no bookie with id bookieId exists
   */
  public void stopBookie(int bookieId) throws IOException {
    EmbeddedBookie bookie = bookieById.get(bookieId);
    if (bookie == null) {
      throw new IllegalArgumentException("No such bookie: " + bookieId);
    }
    bookie.shutdown();
  }

  /**
   * Returns an initialized ZooKeeper client connected to the local
   * ZooKeeper server
   */
  public ZooKeeper initZK() throws IOException {
    if (zkClient == null || zkClient.getState() == States.CLOSED) {
      LOG.info("Attempting to connect to ZooKeeper quorum at " + zkQuorum);
      SyncConnectedWatcher syncWatcher = new SyncConnectedWatcher();
      zkClient = new ZooKeeper(zkQuorum, zkSessionTimeoutMs, syncWatcher);
      syncWatcher.awaitSyncConnected(zkConnectionTimeoutMs);
      if (zkClient.getState() != States.CONNECTED) {
        throw new IOException("Timed out connecting to ZooKeeper after " +
            zkConnectionTimeoutMs + " ms.");
      }
      BookKeeperJournalManager.prepareBookKeeperEnv("/ledgers/available",
          zkClient);
      LOG.info("Successfully connected to ZooKeeper quorum " + zkQuorum);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.info("Already connected to " + zkQuorum);
      }
    }
    return zkClient;
  }

  public int getNumBookiesAvailable(int numBookiesExpected, int maxRetries)
      throws IOException {
    initZK();
    int mostRecentSize = 0;
    for (int i = 0; i < maxRetries; i++) {
      try {
        List<String> children = zkClient.getChildren("/ledgers/available",
            false);
        mostRecentSize = children.size();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found " + mostRecentSize + " bookies up, "
              + "waiting for " + numBookiesExpected);
          LOG.debug("Servers currently up: " + Joiner.on(",").join(children));
        }
        if (mostRecentSize >= numBookiesExpected) {
          LOG.info("All " + numBookiesExpected + " bookies are up: " +
              Joiner.on(",").join(children));
          break;
        }
      } catch (KeeperException e) {
        if (i == numBookiesExpected - 1) {
          LOG.error("Unrecoverable ZooKeeper error after " +
              numBookiesExpected + " retries. Giving up!", e);
        } else {
          LOG.info("Ignoring ZooKeeper exception " + e);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted listing available bookies", e);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted sleeping for 1000 ms!", e);
      }
    }
    return mostRecentSize;
  }

  /**
   * A wrapper class that configures a Bookie server and allows the Bookie to
   * be started inside an {@link ExecutorService}
   */
  public static class EmbeddedBookie implements Runnable {

    private final int bookieId;
    private final ServerConfiguration bookieConf;
    private final File tmpDir;

    private final AtomicReference<BookieServer> bookieRef;

    /**
     * Create an empty data directory for the Bookie server and configure
     * the Bookie server.
     * @param baseConf  Base Bookie server configuration
     * @param zkConnect ZooKeeper quorum specification (e.g., "host:port" or a
     *                  list of "host:port" pairs)
     * @param bookieId Unique identifier for this bookie
     * @throws IOException If unable to create the data directory.
     */
    EmbeddedBookie(ServerConfiguration baseConf, String zkConnect, int bookieId)
        throws IOException {
      this.bookieId = bookieId;
      File bkTmpDir = new File(TEST_DIR) ;
      bkTmpDir.mkdirs();
      tmpDir = new File(BK_BOOKIE_DATA_DIR_PREFIX + bookieId);
      FileUtil.fullyDelete(tmpDir);
      if (!tmpDir.mkdirs()) {
        throw new IOException("Unable to create bookie dir " + tmpDir);
      }
      bookieConf = new ServerConfiguration(baseConf);
      bookieConf.setBookiePort(MiniDFSCluster.getFreePort());
      bookieConf.setZkServers(zkConnect);
      bookieConf.setJournalDirName(tmpDir.getPath());
      bookieConf.setLedgerDirNames(new String[] { tmpDir.getPath() });
      bookieConf.setLogger(LogFactory.getLog(this.getClass().toString()));
      bookieRef = new AtomicReference<BookieServer>(null);
    }

    public void shutdown() {
      BookieServer bookie = getBookie();
      if (bookie == null) {
        LOG.warn("Trying to shutdown a bookie " + bookieId +
            " that has not been initialized!");
      } else {
        bookie.shutdown();
      }
    }

    public int getBookieId() {
      return bookieId;
    }

    public BookieServer getBookie() {
      return bookieRef.get();
    }

    @Override
    public void run() {
      LOG.info("Starting bookie " + bookieId);
      try {
        if (getBookie() != null ||
            !bookieRef.compareAndSet(null, new BookieServer(bookieConf))) {
          throw new IllegalStateException("bookie " + bookieId +
              " already initialized");
        }
        getBookie().start();
        LOG.info("Bookie " + bookieId + " started!");
      } catch (Exception e) {
        LOG.error("Error running BookieServer, aborting!", e);
      }
    }
  }

  /**
   * A watcher that allows clients to synchronously connect to a
   * ZooKeeper quorum.
   */
  public static class SyncConnectedWatcher implements Watcher {
    private final CountDownLatch syncLatch;

    public SyncConnectedWatcher() {
      this.syncLatch = new CountDownLatch(1);
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.None &&
          event.getState() == KeeperState.SyncConnected) {
        syncLatch.countDown();
        LOG.info("Connected to ZooKeeper!");
      }
    }

    /**
     * Block the calling thread until we are have establish a connection to
     * a ZooKeeper quorum or specified timeout has elapsed or the thread is
     * interrupted.
     * @param connectTimeoutMs Maximum time (in milliseconds) to wait until
     *                         the connection happens.
     * @throws IOException If interrupted while waiting for a connection.
     */
    public void awaitSyncConnected(long connectTimeoutMs) throws IOException {
      try {
         syncLatch.await(connectTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted trying to connect to ZooKeeper", e);
      }
    }
  }
}

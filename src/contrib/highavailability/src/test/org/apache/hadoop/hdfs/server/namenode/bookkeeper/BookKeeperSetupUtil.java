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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.BasicZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZooKeeperIface;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * A utility class for intended to start a local BookKeeper and ZooKeeper
 * servers for unit-test purposes. {@see TestBookKeeperEditLogOutputStream}
 * for an example
 */
public class BookKeeperSetupUtil {

  private static final Log LOG = LogFactory.getLog(BookKeeperSetupUtil.class);

  /** ZooKeeper connection timeout, in milliseconds */
  public static final int ZK_TIMEOUT_MS = 10000;

  // Maximum number of ZooKeeper retries
  public static final int MAX_ZK_RETRIES = 3;

  // Retry interval in milliseconds
  public static final int ZK_RETRY_MS = 1000;

  /** Total number of bookies in the mini-cluster */
  public static final int BOOKIES = 3;

  /** The size of the quorum (i.e. this many bookies must ack the write) */
  public static final int QUORUM_SIZE = 2;

  /** Number of bookies eligible to participate in a read or write quorum */
  public static final int ENSEMBLE_SIZE = 3;

  /** Ledger password */
  public static final byte[] LEDGER_PW = "".getBytes();

  public static String zkQuorum;

  private static MiniBookKeeperCluster bookKeeperCluster;
  private static BookKeeper bookKeeperClient;
  private static ZooKeeper zooKeeperClient;
  private static RecoveringZooKeeper recoveringZooKeeperClient;

  // List of ledgers created so far, so that we can delete them after each
  // test
  private List<LedgerHandle> createdLedgers;

  @BeforeClass
  public static void setUpStatic() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    createAndStartBookKeeperCluster();
    zooKeeperClient = bookKeeperCluster.initZK();
    bookKeeperClient = new BookKeeper(new ClientConfiguration(),
        zooKeeperClient);
    recoveringZooKeeperClient = new RecoveringZooKeeper(
        new BasicZooKeeper(zooKeeperClient), MAX_ZK_RETRIES, ZK_RETRY_MS);
  }

  @Before
  public void setUp() throws Exception {
    createdLedgers = new ArrayList<LedgerHandle>();
  }

  public static MiniBookKeeperCluster createAndStartBookKeeperCluster()
      throws IOException {
    zkQuorum = "127.0.0.1:" + MiniAvatarCluster.zkClientPort;
    bookKeeperCluster = new MiniBookKeeperCluster(zkQuorum,
        BOOKIES,
        ZK_TIMEOUT_MS,
        ZK_TIMEOUT_MS);
    bookKeeperCluster.start();
    assertTrue(bookKeeperCluster.getNumBookiesAvailable(BOOKIES, 10) >=
        BOOKIES);
    return bookKeeperCluster;
  }

  public static URI createJournalURI(String nsPath) {
    return URI.create(createJournalURIAsString(nsPath));
  }

  public static String createJournalURIAsString(String nsPath) {
    return "bookkeeper://" + zkQuorum + Path.SEPARATOR_CHAR + nsPath;
  }

  @AfterClass
  public static void shutDownStatic() throws Exception {
    try {
      if (bookKeeperClient != null) {
        bookKeeperClient.close();
      }
    } finally {
      if (bookKeeperCluster != null) {
        bookKeeperCluster.shutdown();
      }
      MiniAvatarCluster.shutDownZooKeeper();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (createdLedgers != null && bookKeeperClient != null) {
      for (LedgerHandle ledger : createdLedgers) {
        LOG.info("Deleting ledger with id " + ledger.getId());
        bookKeeperClient.deleteLedger(ledger.getId());
      }
    }
  }

  /**
   * Creates a new BookKeeper ledger
   * @return The newly BookKeeper ledger
   * @throws IOException If unable to create the ledger
   */
  public final LedgerHandle createLedger() throws IOException {
    try {
      LedgerHandle ledger = bookKeeperClient.createLedger(ENSEMBLE_SIZE,
          QUORUM_SIZE, DigestType.MAC, LEDGER_PW);
      createdLedgers.add(ledger);
      LOG.info("Created a new ledger with id " + ledger.getId());
      return ledger;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted creating the ledger", e);
    } catch (BKException e) {
      throw new IOException("Unrecoverable BookKeeper error", e);
    }
  }

  /**
   * Open an existing ledger for either reading or recovery and writing.
   * @param ledgerId Id of the ledger to open
   * @param fence If true the ledger will be fenced and may be recovered,
   *              otherwise the ledger is opened in read-only mode.
   * @return Handle of the opened ledger
   * @throws IOException If unable to open the specified ledger
   */
  public static LedgerHandle openLedger(long ledgerId, boolean fence)
    throws IOException {
    String shouldFenceDesc = ", with fencing " +
        (fence ? "enabled" : "disabled") + ".";
    LOG.info("Trying to open ledger id " + ledgerId + shouldFenceDesc);
    try {
      LedgerHandle retVal;
      if (fence) {
        retVal = bookKeeperClient.openLedger(ledgerId, DigestType.MAC,
            LEDGER_PW);
      } else {
        retVal = bookKeeperClient.openLedgerNoRecovery(ledgerId,
            DigestType.MAC, LEDGER_PW);
      }
      LOG.info("Opened ledger id " + ledgerId + shouldFenceDesc);
      return retVal;
    } catch (BKException e) {
      throw new IOException(
          "Unrecoverable BookKeeper error opening the ledger", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted opening the ledger", e);
    }
  }

  /**
   * Return the initialized ZooKeeper client
   */
  public static ZooKeeper getZooKeeperClient() {
    return zooKeeperClient;
  }

  /**
   * Return a {@link RecoveringZooKeeper} instance
   */
  public RecoveringZooKeeper getRecoveringZookeeperClient() {
    return recoveringZooKeeperClient;
  }

  /**
   * Recursively delete a ZNode. Regular delete method in ZooKeeper
   * will not delete an non-empty directory. This method will, given a
   * path to a ZNode, first invoke itself on all of the ZNode's children
   * (if any) and then invoke {@link ZooKeeperIface#delete(String, int)}
   * on itself.
   * @param path The path to recursively delete
   * @throws Exception If a ZooKeeper error occurs
   */
  public static void zkDeleteRecursively(String path)
      throws Exception {
    ZkUtil.deleteRecursively(recoveringZooKeeperClient, path);
  }

}

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

import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;

public class BookKeeperJournalConfigKeys {

  // BookKeeper quorum related settings

  /**
   * Total number of bookies that can be striped for a given ledger.
   * Note: must be an odd number!
   */
  public static final String BKJM_BOOKKEEPER_ENSEMBLE_SIZE
      = "dfs.namenode.bookkeeperjournal.ensemble-size";
  public static final int BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;
  /**
   * Majority quorum size: this many nodes in the ensemble must
   * respond to a request for a request to be considered successful
   * Note: for basic consistency quorum size must be
   * <code>>= ( ensemble size / 2 )</code>
   */
  public static final String BKJM_BOOKKEEPER_QUORUM_SIZE
      = "dfs.namenode.bookkeeperjournal.quorum-size";
  public static final int BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT = 2;

  // Other BK client specific settings

  /**
   * If set, use this password to authenticate with BookKeeper
   */
  public static final String BKJM_BOOKKEEPER_DIGEST_PW
      = "dfs.namenode.bookkeeperjournal.digestPw";
  public static final String BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT = "";
  /**
   * If set, enable the Naggle algorithm when talking to BookKeeper (
   * this is accompanying BookKeeper server's setting to disable or
   * enable Naggle algorithm).
   */
  public static final String BKJM_BOOKKEEPER_CLIENT_TCP_NODELAY =
      "dfs.namenode.bookkeeperjournal.client.tcp-nodelay";

  public static final boolean BKJM_BOOKKEEPER_CLIENT_TCP_NO_DELAY_DEFAULT = true;
  /**
   * Maximum number of BookKeeper client requests that may be queued at the
   * time.
   */
  public static final String BKJM_BOOKKEEPER_CLIENT_THROTTLE
      = "dfs.namenode.bookkeeperjournal.bk.throttle";
  public static final int BKJM_BOOKKEEPER_CLIENT_THROTTLE_DEFAULT = 10000;


  // ZooKeeper client related settings

  /**
   * ZooKeeper session timeout for BookKeeper related ZooKeeper clients
   */
  public static final String BKJM_ZK_SESSION_TIMEOUT
      = "dfs.namenode.bookkeeperjournal.zk.session.timeout";
  public static final int BKJM_ZK_SESSION_TIMEOUT_DEFAULT = 3000;
  /**
   * Maximum number of retries {@link RecoveringZooKeeper} may attempt
   * before the error is considered unrecoverable.
   */
  public static final String BKJM_ZK_MAX_RETRIES =
      "dfs.namenode.bookkeeperjournal.zk.max.retries";
  public static final int BKJM_ZK_MAX_RETRIES_DEFAULT = 12;
  /**
   * Interval (in milliseconds) between each ZooKeeper retry as performed
   * by {@link RecoveringZooKeeper}.
   */
  public static final String BKJM_ZK_RETRY_INTERVAL =
      "dfs.namenode.bookkeeperjournal.retry.interval";
  public static final int BKJM_ZK_RETRY_INTERVAL_DEFAULT = 10000;
  /**
   * The path in ZooKeeper that BookKeeper uses to keep track of available
   * ledgers
   */
  public static final String BKJM_ZK_LEDGERS_AVAILABLE_PATH
      = "dfs.namenode.bookkeeperjournal.zk.availablebookies";
  public static final String BKJM_ZK_LEDGERS_AVAILABLE_PATH_DEFAULT
      = "/ledgers/available";
}

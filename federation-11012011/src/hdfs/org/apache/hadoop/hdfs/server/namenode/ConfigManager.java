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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.LinkedList;


/***************************************************
 * Manager FSNamesystem dynamic loadingof configurations.
 ***************************************************/
public class ConfigManager {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  /** Time to wait between checks of the allocation file */
  public static final long CONFIG_RELOAD_INTERVAL = 300 * 1000;

  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long DEFAULT_CONFIG_RELOAD_WAIT = 10 * 1000;

  private FSNamesystem namesys;
  private String whitelistFile; // Path to file that contains whitelist directories

  private long configReloadWait = DEFAULT_CONFIG_RELOAD_WAIT;
  private long lastReloadAttempt; // Last time we tried to reload the pools file
  private long lastSuccessfulReload; // Last time we successfully reloaded pools
  private boolean lastReloadAttemptFailed = false;

  public ConfigManager(FSNamesystem namesys, Configuration conf) {
    this.namesys = namesys;

    // the name of the whitelist file
    this.whitelistFile = conf.get("dfs.namenode.whitelist.file");
    if (whitelistFile == null) {
      LOG.warn("No whitelist file specified in dfs.namenode.whitelist.file." +
               " The namenode will allow deletion/renaming of any directory.");
    }

    // periodicity of reload of the config file
    long value = conf.getLong("dfs.namenode.config.reload.wait", 0);
    if (value != 0) {
      configReloadWait = value;
    }
  }
  
  /**
   * Checks to see if the namenode config file is updated on 
   * disk, If so, then read all it contents. At present, only
   * the whitelist config is updated, but we will enahnce this to
   * update all possible namenode configs in future.
   */
  public void reloadConfigIfNecessary() {
    if (whitelistFile == null) {
      return;
    }
    long time = System.currentTimeMillis();
    if (time > lastReloadAttempt + CONFIG_RELOAD_INTERVAL) {
      lastReloadAttempt = time;
      try {
        File file = new File(whitelistFile);
        long lastModified = file.lastModified();
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + configReloadWait) {
          reloadWhitelist();
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
        }
      } catch (Exception e) {
        // Throwing the error further out here won't help - the RPC thread
        // will catch it and report it in a loop. Instead, just log it and
        // hope somebody will notice from the log.
        // We log the error only on the first failure so we don't fill up the
        // server's log with these messages.
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload whitelist file - " +
              "will use existing allocations.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
  }

  /**
   * Removes all the entries currently in neverDeletePaths 
   * and add the new ones specified
   */
  void reloadWhitelist() throws IOException {

    // read the entire whitelist into memory outside the
    // FSNamessytem lock.
    //
    LinkedList<String> paths = new LinkedList<String>();
    FileInputStream fstream = new FileInputStream(whitelistFile);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    int count = 0;
    while (true) {
      String str = br.readLine();
      if (str == null) {
        break;                  // end of file
      }
      str = str.trim();         // remove all whitespace from start and end
      if (str.startsWith("#")) {
        continue;               // ignore lines with starting with #
      }
      paths.add(str);
      LOG.info("Whitelisted directory [" + count + "] " + str);
      count++;
    }
    in.close();

    // acquire the writelock and insert newly read entries into 
    // the Namenode's configuration.
    namesys.writeLock();
    try {
      namesys.neverDeletePaths.clear();
      for (String s: paths) {
        namesys.neverDeletePaths.add(s);
      }
    } finally {
      namesys.writeUnlock();
    }
  }
}

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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.Util;

/**
 * Set of functions for retrieving
 * fs.checkpoint.dir
 * fs.checkpoint.edits.dir
 * dfs.name.dir
 * dfs.name.edits.dir
 */
public class NNStorageConfiguration {
  
  private static final Log LOG = LogFactory.getLog(NNStorageConfiguration.class.getName());

  static Collection<URI> getCheckpointDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = conf.getStringCollection("fs.checkpoint.dir");
    if (dirNames.isEmpty() && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  static Collection<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> editsDirNames = conf
        .getStringCollection("fs.checkpoint.edits.dir");
    if (editsDirNames.isEmpty() && defaultName != null) {
      editsDirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(editsDirNames);
  }
  
  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getNamespaceDirs(conf, "/tmp/hadoop/dfs/name");
  }
  
  public static Collection<URI> getNamespaceDirs(Configuration conf, String def) {
    Collection<String> dirNames = conf.getStringCollection("dfs.name.dir");
    if (dirNames.isEmpty() && def != null) {
      dirNames.add(def);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }
  
  public static Collection<URI> getNamespaceEditsDirs(Configuration conf) {
    return getNamespaceEditsDirs(conf, "/tmp/hadoop/dfs/name");
  }

  public static Collection<URI> getNamespaceEditsDirs(Configuration conf, String def) {
    Collection<String> editsDirNames =
      conf.getStringCollection("dfs.name.edits.dir");
    if (editsDirNames.isEmpty() && def != null) {
      editsDirNames.add(def);
    }
    return Util.stringCollectionAsURIs(editsDirNames);
  }
  
  public static Collection<URI> getRequiredNamespaceEditsDirs(Configuration conf) {
    Collection<String> requiredDirNames =
        conf.getStringCollection("dfs.name.edits.dir.required");
    return Util.stringCollectionAsURIs(requiredDirNames);
  }
  
  public static URI getURIKey(Configuration conf, String key)
      throws IOException {
    Collection<String> keys = conf.getStringCollection(key);
    if (keys.size() > 1) {
      LOG.info("Requested a single entry but the configuration contains multiple uris: "
          + keys);
      return null;
    }
    if (keys.size() == 0) {
      LOG.info("No value specified for: " + key + " in the configuration");
      return null;
    }
    return Util.stringAsURI(keys.iterator().next());
  }
  
  /**
   * Gets the checkpoint transaction count. If transaction count
   * is not set, try to infer it from checkpoint size (for older
   * configurations)
   *
   * @return the number of transactions after which a checkpoint
   * is triggered.
   */
  static long getCheckpointTxnCount(Configuration conf) {
    long defaultCount = 700000;
    long bytesPerTxn = 100;

    return conf.getLong("fs.checkpoint.txns",
        conf.getLong("fs.checkpoint.size", defaultCount * bytesPerTxn)
            / bytesPerTxn);
  }
}

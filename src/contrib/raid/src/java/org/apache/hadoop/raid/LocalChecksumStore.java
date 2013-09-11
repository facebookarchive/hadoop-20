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

package org.apache.hadoop.raid;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Instead of storing mappings of block and checksums into a file,
 * We store each mapping as an empty file like "blockid:checksum";
 */
public class LocalChecksumStore extends ChecksumStore {
  public static final Log LOG = LogFactory.getLog(LocalChecksumStore.class);
  public ConcurrentHashMap<String, Long> store =
      new ConcurrentHashMap<String, Long>();
  public static final String LOCAL_CHECK_STORE_DIR_KEY =
      "hdfs.raid.local.checksum.dir";
  public static final String DELIMITER = ":"; 
  private File storeDir;
  private String storeDirName;
  
  public void initialize(Configuration conf, boolean createStore)
      throws IOException {
    storeDirName = conf.get(LOCAL_CHECK_STORE_DIR_KEY, "");
    if (storeDirName.isEmpty()) {
      throw new IOException("Key " + LOCAL_CHECK_STORE_DIR_KEY +
          " is not defined");
    }
    storeDir = new File(storeDirName);
    if (createStore) { 
      storeDir.mkdirs();  
    }  
    if (!storeDir.exists()) {  
      throw new IOException(storeDir + " doesn't exist");  
    }
    
    for (String file: storeDir.list()) {
      String[] pair = file.split(DELIMITER);
      if (pair.length != 2) {
        continue;
      }
      Long oldVal = store.putIfAbsent(pair[0], Long.parseLong(pair[1]));
      if (oldVal != null) {
        throw new IOException("Key: " + pair[0] + " has old values " + oldVal
            + " and new value " + pair[1]);
      }
    }
  }

  @Override
  public int size() throws IOException {
    return store.size();
  }

  @Override
  public boolean isEmpty() throws IOException {
    return store.isEmpty();
  }

  @Override
  public boolean hasChecksum(Block blk) throws IOException {
    return store.containsKey(blk.toString());
  }

  @Override
  public Long getChecksum(Block blk) throws IOException {
    Long checksum = store.get(blk.toString());
    LOG.info("Fetch " + blk + " -> " + checksum);
    return checksum;
  }
  
  private File getFile(Block blk, Long checksum) {
    return new File(storeDir, blk + DELIMITER + checksum);
  }

  @Override
  public Long putIfAbsent(Block blk, Long newChecksum) throws IOException {
    Long oldChecksum = store.putIfAbsent(blk.toString(), newChecksum);
    if (oldChecksum == null) {
      LOG.info("Put " + blk + " -> " + newChecksum);
      File newFile = getFile(blk, newChecksum);
      newFile.createNewFile();
    }
    return oldChecksum;
  }
  
  @Override
  public void putChecksum(Block blk, Long newChecksum) throws IOException {
    Long oldChecksum = store.put(blk.toString(), newChecksum);
    File newFile = getFile(blk, newChecksum);
    if (oldChecksum == null) {
      newFile.createNewFile();
    } else {
      File oldFile = getFile(blk, oldChecksum);
      oldFile.renameTo(newFile);
    }
  }

  @Override
  public void clear() throws IOException {
    store.clear();
    FileUtil.fullyDelete(storeDir);
    storeDir.mkdirs();
  }
  
}

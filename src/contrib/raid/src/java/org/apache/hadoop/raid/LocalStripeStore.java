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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * for each stripe blk1,blk2,...,blkn under one codec
 * we generates n files:
 * codecId:blk1
 * codecId:blk2
 * ...
 * codecId:blkn
 * they are all hardlinks of the same file whose content is 
 * "blk1:blk2:...:blkn"
 */
public class LocalStripeStore extends StripeStore { 
  public static final Log LOG = LogFactory.getLog(LocalStripeStore.class);
  public ConcurrentHashMap<String, LocalStripeInfo> blockToStripeStore =
      new ConcurrentHashMap<String, LocalStripeInfo>();
  public ConcurrentHashMap<List<Block>, Long> stripeSet = 
      new ConcurrentHashMap<List<Block>, Long>();
  public static final String LOCAL_STRIPE_STORE_DIR_KEY =
      "hdfs.raid.local.stripe.dir";
  public static final String[] STRIPESTORE_SPECIFIC_KEYS = 
      new String[] {
        LOCAL_STRIPE_STORE_DIR_KEY
      };
  public static final String DELIMITER = ":"; 
  private File storeDir;
  private String storeDirName;
  long maxStripeId = 0L;
  Random rand = new Random();
  
  static public class LocalStripeInfo extends StripeStore.StripeInfo {
    public LocalStripeInfo(Codec newCodec, Block newBlock,
        List<Block> newParityBlocks, List<Block> newSrcBlocks) {
      super(newCodec, newBlock, newParityBlocks, newSrcBlocks);
    }
    
    public String getKey() {
      return LocalStripeInfo.getKey(codec, block);
    }
    
    static public String getKey(Codec codec, Block blk) {
      return codec.id + DELIMITER + blk.toString();
    }
    
    static private Block getBlock(String str) {
      String[] blkParts = str.split("_");
      return new Block(Long.parseLong(blkParts[1]), 0L,
                           Long.parseLong(blkParts[2]));
    }
    
    static public LocalStripeInfo getStripeInfo(File storeDir, String fileName)
        throws IOException {
      String[] parts = fileName.split(DELIMITER);
      if (parts.length <= 1) {
        return null;
      }
      String codecId = parts[0];
      Block blk = getBlock(parts[1]);
      Codec codec = Codec.getCodec(codecId);
      File diskFile = new File(storeDir, fileName);
      // read the file
      FileInputStream fis = null;
      DataInputStream in = null;
      BufferedReader br = null;
      String line = null;
      try {
        fis = new FileInputStream(diskFile);
        in = new DataInputStream(fis);
        br = new BufferedReader(new InputStreamReader(in));
        line = br.readLine();
        if (line == null) {
          throw new IOException("Cannot read file " + diskFile);
        }
      } finally {
        br.close();
        in.close(); 
        fis.close();
      }
      parts = line.split(DELIMITER);
      if (parts.length <= codec.parityLength) {
        throw new IOException("stored entry :" + diskFile +
            " doesn't have enough elements for codec:" + codec.id +
            " parity:" + codec.parityLength);
      }
      ArrayList<Block> parityBlocks = new ArrayList<Block>();
      ArrayList<Block> srcBlocks = new ArrayList<Block>();
      for (int i = 0; i < parts.length; i++) {
        Block newBlock = getBlock(parts[i]);
        if (i < codec.parityLength) {
          parityBlocks.add(newBlock);
        } else {
          srcBlocks.add(newBlock);
        }
      }
      return new LocalStripeInfo(codec, blk, parityBlocks, srcBlocks);
    }
  }
  
  public void initialize(Configuration conf, boolean createStore, 
      FileSystem fs) throws IOException {
    Configuration newConf = initializeConf(STRIPESTORE_SPECIFIC_KEYS, conf, fs);
    storeDirName = newConf.get(LOCAL_STRIPE_STORE_DIR_KEY, "");
    if (storeDirName.isEmpty()) {
      throw new IOException("Key " + LOCAL_STRIPE_STORE_DIR_KEY +
          " is not defined");
    }
    storeDir = new File(storeDirName);
    if (createStore) {
      storeDir.mkdirs();
    }
    if (!storeDir.exists()) {
      throw new IOException(storeDir + " doesn't exist");
    }
    
    for (String fileName: storeDir.list()) {
      LocalStripeInfo si = LocalStripeInfo.getStripeInfo(storeDir, fileName);
      if (si == null) {
        continue;
      }
      // Add to stripeStore
      LocalStripeInfo oldSi = blockToStripeStore.put(si.getKey(), si);
      if (oldSi != null) {
        throw new IOException("There are two stripes for the same key: " +
          si.getKey() + "\n old: " +oldSi.toString() + "\n new: " +
          si.toString());
      }
      stripeSet.put(si.parityBlocks, 1L);
    }
    LOG.info("Load from " + storeDir);
  }
  
  private void persistent(LocalStripeInfo si, File tmpFile) throws IOException {
    blockToStripeStore.put(si.getKey(), si);
    File f = new File(storeDir, si.getKey());
    if (f.exists()) {
      f.delete();
    }
    HardLink.createHardLink(tmpFile, f);
  }
  
  private File createStripeFile(List<Block> parityBlks, List<Block> srcBlks)
      throws IOException {
    File tmpFile = new File(storeDir, "tmp." + rand.nextLong());
    PrintWriter out = new PrintWriter(new BufferedWriter(
        new FileWriter(tmpFile)));
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parityBlks.size(); i++) {
      if (i > 0) {
        sb.append(DELIMITER);
      }
      sb.append(parityBlks.get(i).toString());
    }
    for (Block blk: srcBlks) {
      sb.append(DELIMITER);
      sb.append(blk.toString());
    }
    out.println(sb);
    out.close();
    return tmpFile;
  }

  @Override
  public void putStripe(Codec codec, List<Block> parityBlks,
      List<Block> srcBlks) throws IOException {
    File tmpFile = createStripeFile(parityBlks, srcBlks);
    synchronized(this) {
      for (Block blk : parityBlks) {
        LocalStripeInfo si = new LocalStripeInfo(codec, blk, parityBlks, 
            srcBlks);
        persistent(si, tmpFile);
      }
      for (Block blk : srcBlks) {
        LocalStripeInfo si = new LocalStripeInfo(codec, blk, parityBlks,
            srcBlks);
        persistent(si, tmpFile);
      }
      // parityBlks could be considered the id of stripe
      stripeSet.put(parityBlks, 1L);
    }
    tmpFile.delete();
    StripeInfo si = new StripeInfo(codec, null, parityBlks, srcBlks);
    LOG.info(" Put " + si);
  }
  
  @Override
  public StripeInfo getStripe(final Codec codec, final Block blk)
      throws IOException {
    String key = LocalStripeInfo.getKey(codec, blk);
    StripeInfo si = blockToStripeStore.get(key);
    LOG.info("Fetch " + key + " -> " + si);
    return si;
  }
  
  @Override
  public int numStripes() {
    return stripeSet.size();
  }
  
  @Override
  public void clear() throws IOException {
    this.blockToStripeStore.clear();
    this.stripeSet.clear();
    FileUtil.fullyDelete(storeDir); 
    storeDir.mkdirs();
    LOG.info("Clear " + storeDir);
  }
}

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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import com.google.common.base.Joiner;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A class with the information of a raid codec.
 * A raid codec has the information of
 * 1. Which erasure code class to use 
 * 2. numDataBlocks and numParityBlocks
 * 3. stripe chunk size
 * 4. parity replication
 */
public class RaidCodec implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final String HDFS_RAID_CODEC_JSON = "hdfs.raid.codec.json";
  
  public static final Log LOG = LogFactory.getLog(RaidCodec.class);
  // Used by offline raiding, it means one stripe chunk is equal to the block size
  public static final long FULL_BLOCK = -1L;
  
  public static final Joiner joiner = Joiner.on(",");

  /**
   * Used by ErasureCode.init() to get Code specific extra parameters.
   */
  public final String jsonStr;

  /**
   * id of the codec
   */
  public final String id;

  /**
   * Number of data blocks in one stripe
   */
  public final int numDataBlocks;

  /**
   * Number of parity blocks in one stripe
   */
  public final int numParityBlocks;
  
  /**
   * Number of blocks in one stripe = numDataBlocks + numParityBlocks;
   */
  public final int numStripeBlocks;
  
  /**
   * size of data store in one datanode for one stripe
   * stripeChunkSize = one stripe size / numDataBlocks;
   */
  public final long stripeChunkSize;
  
  /**
   * Define the number of replicas for parity blocks
   */
  public final short parityReplication;
  
  /**
   * Define the minimum number of replicas for source blocks
   */
  public final short minSourceReplication;

  /**
   * The full class name of the ErasureCode used
   */
  public final String erasureCodeClass;

  /**
   * Human readable description of the codec
   */
  public final String description;
  

  private static List<RaidCodec> codecs;
  private static Map<String, RaidCodec> idToCodec;

  public String getCodecJson() {
    return joiner.join(
      " { " +
        "\"id\":\"" + id  + "\"",
        "\"num_data_blocks\":" + numDataBlocks,
        "\"num_parity_blocks\":" + numParityBlocks,
        "\"stripe_chunk_size\":" + stripeChunkSize,
        "\"parity_repl\":" + parityReplication,
        "\"min_source_repl\":" + minSourceReplication,
        "\"erasure_code\":\"" + erasureCodeClass + "\"",
        "\"description\":\"" + description + "\"",
      " }, ");
  }

  /**
   * Get single instantce of the list of codecs ordered by priority.
   */
  public static List<RaidCodec> getCodecs() {
    return RaidCodec.codecs;
  }

  /**
   * Get the instance of the codec by id
   */
  public static RaidCodec getCodec(String id) {
    return idToCodec.get(id);
  }

  static {
    try {
      Configuration.addDefaultResource("hdfs-default.xml");
      Configuration.addDefaultResource("hdfs-site.xml");
      initializeCodecs(new Configuration());
    } catch (Exception e) {
      LOG.fatal("Fail initialize Raid codecs", e);
      System.exit(-1);
    }
  }

  public static void initializeCodecs(Configuration conf) throws IOException {
    try {
      String source = conf.get(HDFS_RAID_CODEC_JSON);
      if (source == null) {
        codecs = Collections.emptyList();
        idToCodec = Collections.emptyMap();
        if (LOG.isDebugEnabled()) {
          LOG.debug("None Codec is specified");
        }
        return;
      }
      JSONArray jsonArray = new JSONArray(source);
      List<RaidCodec> localCodecs = new ArrayList<RaidCodec>();
      Map<String, RaidCodec> localIdToCodec = new HashMap<String, RaidCodec>();
      for (int i = 0; i < jsonArray.length(); ++i) {
        RaidCodec codec = new RaidCodec(jsonArray.getJSONObject(i));
        localIdToCodec.put(codec.id, codec);
        localCodecs.add(codec);
      }
      codecs = Collections.unmodifiableList(localCodecs);
      idToCodec = Collections.unmodifiableMap(localIdToCodec);
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  private RaidCodec(JSONObject json) throws JSONException {
    this.jsonStr = json.toString();
    this.id = json.getString("id");
    this.numParityBlocks = json.getInt("num_parity_blocks");
    this.numDataBlocks = json.getInt("num_data_blocks");
    this.numStripeBlocks = this.numParityBlocks + this.numDataBlocks;
    this.stripeChunkSize = json.getLong("stripe_chunk_size");
    this.parityReplication = (short)json.getInt("parity_repl");
    this.minSourceReplication = (short)json.getInt("min_source_repl");
    this.erasureCodeClass = json.getString("erasure_code");
    this.description = getJSONString(json, "description", "");
  }

  static private String getJSONString(
      JSONObject json, String key, String defaultResult) {
    String result = defaultResult;
    try {
      result = json.getString(key);
    } catch (JSONException e) {
    }
    return result;
  }

  public ErasureCode createErasureCode(Configuration conf) {
    // Create the scheduler
    Class<?> erasureCode = null;
    try {
      erasureCode = conf.getClassByName(this.erasureCodeClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ErasureCode code = (ErasureCode) ReflectionUtils.newInstance(erasureCode,
        conf);
    code.init(this.numDataBlocks, this.numParityBlocks);
    return code;
  }

  @Override
  public String toString() {
    if (jsonStr == null) {
      return "Test codec " + id;
    } else {
      return jsonStr;
    }
  }
  
  public int getNumStripes(int numBlocks) {
    if (numBlocks <= 0) {
      return 0;
    }
    return (numBlocks - 1) / this.numDataBlocks + 1;
  }
  
  public int getNumParityBlocks(int numBlocks) {
    return getNumStripes(numBlocks) * this.numParityBlocks;
  }

  /**
   * Used by unit test only
   */
  static void addCodec(RaidCodec codec) {
    List<RaidCodec> newCodecs = new ArrayList<RaidCodec>();
    newCodecs.addAll(codecs);
    newCodecs.add(codec);
    codecs = Collections.unmodifiableList(newCodecs);

    Map<String, RaidCodec> newIdToCodec = new HashMap<String, RaidCodec>();
    newIdToCodec.putAll(idToCodec);
    newIdToCodec.put(codec.id, codec);
    idToCodec = Collections.unmodifiableMap(newIdToCodec);
  }

  /**
   * Used by unit test only
   */
  static void clearCodecs() {
    codecs = Collections.emptyList();
    idToCodec = Collections.emptyMap();
  }

  /**
   * Used by unit test only
   */
  RaidCodec(String id,
            int numParityBlocks,
            int numDataBlocks,
            long stripeChunkSize,
            short parityReplication,
            short minSourceReplication,
            String erasureCodeClass,
            String description) {
    this.jsonStr = null;
    this.id = id;
    this.numParityBlocks = numParityBlocks;
    this.numDataBlocks = numDataBlocks;
    this.numStripeBlocks = this.numDataBlocks + this.numParityBlocks;
    this.stripeChunkSize = stripeChunkSize;
    this.parityReplication = parityReplication;
    this.minSourceReplication = minSourceReplication;
    this.erasureCodeClass = erasureCodeClass;
    this.description = description;
  }

  // Return only the source blocks of the raided file
  public BlockInfo[] getSourceBlocks(BlockInfo[] blocks) {
    int numSourceBlocks = blocks.length - 
        (blocks.length / numStripeBlocks) * numParityBlocks - 
        ((blocks.length % numStripeBlocks == 0) ? 0 : numParityBlocks);
    BlockInfo[] sourceBlocks = new BlockInfo[numSourceBlocks];
    int pos = numParityBlocks;
    int stripeEnd = numStripeBlocks;
    for (int i = 0; i < numSourceBlocks; i++) {
      sourceBlocks[i] = blocks[pos];
      pos++;
      if (pos == stripeEnd) {
        pos += numParityBlocks;
        stripeEnd += numStripeBlocks;
      }
    }
    return sourceBlocks;
  }
  
  // Used only by testing
  // Return only the parity blocks of the raided file
  public BlockInfo[] getParityBlocks(BlockInfo[] blocks) {
    int numBlocks = (blocks.length / numStripeBlocks) * numParityBlocks
        + ((blocks.length % numStripeBlocks == 0) ? 0 : numParityBlocks);
    BlockInfo[] parityBlocks = new BlockInfo[numBlocks];
    int pos = 0;
    int parityEnd = numParityBlocks;
    for (int i = 0; i < numBlocks; i++) {
      parityBlocks[i] = blocks[pos];
      pos++;
      if (pos == parityEnd) {
        pos += numDataBlocks;
        parityEnd += numStripeBlocks;
      }
    }
    return parityBlocks;
  }
  
  public Block getLastBlock(BlockInfo[] blocks) {
    if (blocks == null ||
        blocks.length == 0)
      return null;
    int mod = (blocks.length - 1) % numStripeBlocks;
    Block lastBlock = blocks[blocks.length - 1];
    if (mod < numParityBlocks) {
      LOG.error("Last block is not source block " + lastBlock + 
          " numBlocks: " + blocks.length + " codec: " + this);
      return null;
    }
    return lastBlock;
  }
  
  public long getFileSize(BlockInfo[] blocks) {
    if (blocks == null) {
      return 0L;
    }
    long fileSize = 0L;
    for (int i = 0; i < blocks.length; i+=numStripeBlocks) {
      for (int dataBlockId = numParityBlocks;
          i + dataBlockId < blocks.length && dataBlockId < numStripeBlocks;
          dataBlockId++) {
        fileSize += blocks[i + dataBlockId].getNumBytes();
      }
    }
    return fileSize;
  }
  
  public long diskspaceConsumed(Block[] blocks, boolean isUnderConstruction,
      long preferredBlockSize, short replication) {
    long dataSize = 0;
    long paritySize = 0;
    if(blocks == null || blocks.length == 0) {
      return 0;
    }
    for (int i = 0, stripeIdx = 0; i < blocks.length; i++) {
      if (blocks[i] != null) {
        if (stripeIdx < numParityBlocks) {
          paritySize += blocks[i].getNumBytes();
        } else { 
          dataSize += blocks[i].getNumBytes();
        }
      }
      stripeIdx = (stripeIdx + 1) % numStripeBlocks;
    }
    return dataSize * replication + paritySize * parityReplication;
  }
  
  public BlockInfo[] convertToRaidStorage(BlockInfo[] parityBlocks, 
      BlockInfo[] blocks, int[] checksums, BlocksMap blocksMap,
      short replication, INodeFile inode) throws IOException {
    BlockInfo[] newList = new BlockInfo[parityBlocks.length + blocks.length];
    int pPos = 0;
    int sPos = 0;
    int pos = 0;
    int numStripes = getNumStripes(blocks.length);
    for (int i = 0; i < numStripes; i++) {
      System.arraycopy(parityBlocks, pPos, newList, pos, numParityBlocks);
      for (int j = pos; j < pos + numParityBlocks; j++) {
        blocksMap.updateINode(newList[j],
            new RaidBlockInfo(newList[j], parityReplication, j), inode,
            parityReplication, true);
      }
      pPos += numParityBlocks;
      pos += numParityBlocks;
      for (int j = 0; j < numDataBlocks && sPos < blocks.length; 
          j++, pos++, sPos++) {
        newList[pos] = blocks[sPos];
        if (checksums != null) {
          if (blocks[sPos].getChecksum() != BlockInfo.NO_BLOCK_CHECKSUM
              && blocks[sPos].getChecksum() != checksums[sPos]) {
            throw new IOException("Checksum mismatch for the " + sPos +
                "th source blocks. New=" + checksums[sPos] +
                ", Existing=" + blocks[sPos].getChecksum());
          }
          blocks[sPos].setChecksum(checksums[sPos]);
        }
        blocksMap.updateINode(newList[pos], new RaidBlockInfo(newList[pos], 
            replication, pos), inode, replication, true);
      }
    }
    return newList;
  }
  
  /**
   * Count the number of live replicas of each parity block in the raided file
   * If any stripe has not enough parity block replicas, add the stripe to 
   *  raidEncodingTasks to schedule encoding.
   * If forceAdd is true, we always add the stripe to raidEncodingTasks 
   * without checking
   * @param sourceINode
   * @param raidTasks
   * @param fs
   * @param forceAdd
   * @return true if all parity blocks of the file have enough replicas
   * @throws IOException
   */
  public boolean checkRaidProgress(INodeFile sourceINode, 
      LightWeightLinkedSet<RaidBlockInfo> raidEncodingTasks, FSNamesystem fs,
      boolean forceAdd) throws IOException {
    boolean result = true;
    BlockInfo[] blocks = sourceINode.getBlocks();
    for (int i = 0; i < blocks.length;
        i += numStripeBlocks) {
      boolean hasParity = true;
      if (!forceAdd) {
        for (int j = 0; j < numParityBlocks; j++) {
          if (fs.countLiveNodes(blocks[i + j]) < this.parityReplication) {
            hasParity = false;
            break;
          }
        }
      }
      if (!hasParity || forceAdd) {
        raidEncodingTasks.add(new RaidBlockInfo(blocks[i], parityReplication, i));
        result = false; 
      }
    }
    return result;
  }
  
  public BlockInfo[] getBlocksInOneStripe(BlockInfo[] blocks, 
      RaidBlockInfo rbi) {
    int size = Math.min(this.numStripeBlocks, blocks.length - rbi.getIndex());
    BlockInfo[] stripeBlocks = new BlockInfo[size];
    System.arraycopy(blocks, rbi.getIndex(), stripeBlocks, 0, size);
    return stripeBlocks;
  }
}

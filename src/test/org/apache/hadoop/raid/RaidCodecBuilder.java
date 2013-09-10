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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class RaidCodecBuilder {
  public static final Log LOG = LogFactory.getLog(RaidCodecBuilder.class);
  public static RaidCodec getRSCodec(String id, int numParityBlocks,
      int numDataBlocks, long chunkSize, short parityReplication,
      short minSourceReplication) {
    return new RaidCodec(id, numParityBlocks, numDataBlocks, chunkSize,
        parityReplication, minSourceReplication,
        "org.apache.hadoop.raid.ReedSolomonCode",
        "RS");
  }
  
  public static RaidCodec getXORCodec(String id, int numParityBlocks,
      int numDataBlocks, long chunkSize, short parityReplication,
      short minSourceReplication) {
    return new RaidCodec(id, numParityBlocks, numDataBlocks, chunkSize, 
        parityReplication, minSourceReplication,
        "org.apache.hadoop.raid.XORCode",
        "XOR");
  }
  
  public static void loadDefaultFullBlocksCodecs(Configuration conf,
      int numRSParityBlocks, int numDataBlocks) throws IOException {
    loadAllTestCodecs(conf, new RaidCodec[] {
       getRSCodec("rs", numRSParityBlocks, numDataBlocks, 
           RaidCodec.FULL_BLOCK, (short)1, (short)1),
       getXORCodec("xor", 1, numDataBlocks, RaidCodec.FULL_BLOCK, (short)2,
           (short)2)
    });
  }
  
  public static void loadAllTestCodecs(Configuration conf, RaidCodec[] codecs) 
    throws IOException {
    RaidCodec.clearCodecs();
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (RaidCodec codec: codecs) {
      sb.append(codec.getCodecJson());
    }
    sb.append(" ] ");
    LOG.info(RaidCodec.HDFS_RAID_CODEC_JSON + "=" + sb.toString());
    conf.set(RaidCodec.HDFS_RAID_CODEC_JSON, sb.toString());
    RaidCodec.initializeCodecs(conf);
    LOG.info("Test codec loaded");
    for (RaidCodec c : RaidCodec.getCodecs()) {
      LOG.info("Loaded raid code:" + c.id);
    }
  }
}
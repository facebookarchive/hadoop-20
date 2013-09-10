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

package org.apache.hadoop.hdfs.server.protocol;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.RaidTask;
import org.apache.hadoop.raid.RaidCodec;
import org.junit.Test;

public class TestRaidTask {
  private static final Log LOG = LogFactory.getLog(TestRaidTask.class);
  Random random = new Random();
  
  private long nextPositive() {
    long ret = random.nextLong();
    if (ret < 0) {
      ret = -ret;
    }
    
    return ret;
  }
  
  private RaidTask generateRaidTask(RaidCodec codec, int replication, 
      int numBlocks, int numMissingBlocks) {
    Block[] blocks = new Block[numBlocks];
    ArrayList<ArrayList<DatanodeInfo>> locations = 
        new ArrayList<ArrayList<DatanodeInfo>>(numBlocks);
    int[] toRaidIdxs = new int[numMissingBlocks];
    
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block(random.nextLong(), nextPositive(), nextPositive());
      ArrayList<DatanodeInfo> dns = new ArrayList<DatanodeInfo>(replication);
      for (int j = 0; j < replication; j++) {
        DatanodeInfo info = new DatanodeInfo(new DatanodeID("localhost"));
        dns.add(info);
      }
      locations.add(dns);
    }
    
    for (int i = 0; i < numMissingBlocks; i++) {
      toRaidIdxs[i] = random.nextInt(numBlocks);
    }
    
    return new RaidTask(codec, blocks, locations, toRaidIdxs);
  }
  
  private boolean verifyRaidTask(RaidTask expect, RaidTask actual) {
    if (!expect.equals(actual)) {
      return false;
    }
    
    int numBlocks = expect.stripeBlocks.length;
    for (int i = 0; i < numBlocks; i++) {
      if (!expect.stripeBlocks[i].equals(actual.stripeBlocks[i])) {
        return false;
      }
    }
    
    int numMissingBlocks = expect.toRaidIdxs.length;
    for (int i = 0; i < numMissingBlocks; i++) {
      if (expect.toRaidIdxs[i] != actual.toRaidIdxs[i]) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Test the serilization of the RaidTask object.
   * Will write the object, read it out and compare.
   * 
   */
  @Test
  public void testWriteRaidTask() throws IOException {
    // xor
    RaidTask task = generateRaidTask(RaidCodec.getCodec("xor"), 2, 11, 1);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    task.write(new DataOutputStream(out));
    byte[] buffer = out.toByteArray();
    
    // read and verify
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    RaidTask newTask = new RaidTask();
    newTask.readFields(new DataInputStream(in));
    assertTrue(verifyRaidTask(task, newTask));
    
    // rs
    task = generateRaidTask(RaidCodec.getCodec("rs"), 1, 14, 4);
    out = new ByteArrayOutputStream();
    task.write(new DataOutputStream(out));
    buffer = out.toByteArray();
    
    // read and verify
    in = new ByteArrayInputStream(buffer);
    newTask = new RaidTask();
    newTask.readFields(new DataInputStream(in));
    assertTrue(verifyRaidTask(task, newTask));
  }
  
  
  /**
   * Test RaidTask with illegal argument
   */
  @Test
  public void testIllegalArgument() throws IOException {
    RaidTask task = null;
    try {
      task = generateRaidTask(RaidCodec.getCodec("not_exist"), 2, 11, 1);
      fail("codec does not exist");
    } catch (IllegalArgumentException ex) {
      // ignore
      LOG.warn(ex);
    }
    
    try {
      task = new RaidTask(RaidCodec.getCodec("xor"), null, null, null);
      fail("null arguments");
    } catch (IllegalArgumentException ex) {
      // ignore
      LOG.warn(ex);
    }
    
    try {
      task = new RaidTask(RaidCodec.getCodec("xor"), new Block[] {new Block()},
          new ArrayList<ArrayList<DatanodeInfo>>(), new int[] {0});
      fail("all blocks should have locations");
    } catch (IllegalArgumentException ex) {
      // ignore
      LOG.warn(ex);
    }
  }
}

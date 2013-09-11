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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestNamenodeVerify {

  @Test
  public void testVerifyVersion() throws Exception {
    NameNode.verifyVersion(-41, -37, "test");
    NameNode.verifyVersion(41, 37, "test");
    try {
      NameNode.verifyVersion(-41, 37, "test");
      fail("Did not throw exception");
    } catch (IOException e) {}
    try {
      NameNode.verifyVersion(37, 40, "test");
      fail("Did not throw exception");
    } catch (IncorrectVersionException e) {}
    try {
      NameNode.verifyVersion(-37, -40, "test");
      fail("Did not throw exception");
    } catch (IncorrectVersionException e) {}
  }

  private DatanodeRegistration getReg(long ctime, int namespaceId) {
    StorageInfo info = new StorageInfo();
    info.layoutVersion = FSConstants.LAYOUT_VERSION;
    info.cTime = ctime;
    info.namespaceID = namespaceId;
    DatanodeRegistration reg = new DatanodeRegistration();
    reg.setStorageInfo(info, null);
    return reg;
  }

  @Test
  public void testVerifyRequest() throws Exception {
    NameNode nn = new MyNs();
    // Check for valid cases, where namespaceId matches and DN cTime <= NN cTime
    nn.verifyRequest(getReg(10, 5));
    nn.verifyRequest(getReg(30, 5));
    try {
      // NamespaceID doesn't match.
      nn.verifyRequest(getReg(30, 6));
      fail("Did not throw exception");
    } catch (Exception e) {}
    try {
      // Higher cTime for datanode.
      nn.verifyRequest(getReg(31, 5));
      fail("Did not throw exception");
    } catch (Exception e) {}
    try {
      //Higher cTime and namespaceId mismatch.
      nn.verifyRequest(getReg(35, 7));
      fail("Did not throw exception");
    } catch (Exception e) {}
  }

  class MyNs extends NameNode {
    public MyNs() throws IOException {
      super(new Configuration());
    }

    @Override
    protected void initialize() {
      myMetrics = new NameNodeMetrics(new Configuration(), this);
    }

    @Override
    long getCTime() {
      return 30;
    }

    @Override
    public int getNamespaceID() {
      return 5;
    }
  }
}

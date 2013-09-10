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
package org.apache.hadoop.hdfs.qjournal.client;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.HttpImageUploadChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Before;
import org.junit.Test;

public class TestHttpImageUploadChannel {

  static final Log LOG = LogFactory.getLog(TestHttpImageUploadChannel.class);

  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(12345, 0L,
      0);

  private JournalNode jn;
  private Journal journal;
  private Configuration conf = new Configuration();
  private String journalId;
  private String httpAddress;

  byte[] randomBytes = new byte[1024 * 1024];

  @Before
  public void setup() throws Exception {
    File editsDir = new File(MiniDFSCluster.getBaseDirectory(null)
        + File.separator + "TestJournalNode");
    FileUtil.fullyDelete(editsDir);

    conf.set(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
        editsDir.getAbsolutePath());
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    int port = MiniJournalCluster.getFreeHttpPortAndUpdateConf(conf, true);
    httpAddress = "http://localhost:" + port;

    jn = new JournalNode();
    jn.setConf(conf);
    jn.start();
    journalId = "test-journalid-" + QJMTestUtil.uniqueSequenceId();
    journal = jn.getOrCreateJournal(QuorumJournalManager
        .journalIdStringToBytes(journalId));
    journal.transitionJournal(FAKE_NSINFO, Transition.FORMAT, null);
    journal.transitionImage(FAKE_NSINFO, Transition.FORMAT, null);
  }

  public void tearDown() throws InterruptedException {
    if (jn != null) {
      jn.stopAndJoin(0);
    }
  }

  /**
   * Basic upload through the channel.
   */
  @Test
  public void testBasic() throws Exception {
    long txid = 101;
    int iterations = 10;

    HttpImageUploadChannel channel = new HttpImageUploadChannel(httpAddress,
        journalId, FAKE_NSINFO, txid, 0, 100);
    channel.start();

    ByteArrayOutputStream bos = genBos();
    for (int i = 0; i < iterations; i++) {
      channel.send(bos);
    }
    channel.close();

    // validate

    // the file should exist
    File uploaded = journal.getImageStorage().getCheckpointImageFile(txid);
    assertTrue(uploaded.exists());
    assertEquals(iterations * randomBytes.length, uploaded.length());

    // assert contents of the uploaded file
    InputStream is = new FileInputStream(uploaded);
    byte[] contents = new byte[randomBytes.length];

    for (int i = 0; i < iterations; i++) {
      is.read(contents);
      assertTrue(Arrays.equals(randomBytes, contents));
    }
    is.close();
  }
  
  @Test
  public void testMaxBuffer() throws Exception {
    long txid = 101;
    // set timeout to 5 seconds
    HttpImageUploadChannel.setTimeoutForTesting(5);
    // setting -1 will cause the first send operation to fail
    HttpImageUploadChannel channel = new HttpImageUploadChannel(httpAddress,
        journalId, FAKE_NSINFO, txid, 0, -1);
    channel.start();

    ByteArrayOutputStream bos = genBos();
    channel.send(bos);
    assertTrue(channel.isDisabled());
  }

  /**
   * Generate ByteOutputStream with random content.
   */
  private ByteArrayOutputStream genBos() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    new Random().nextBytes(randomBytes);
    bos.write(randomBytes);
    return bos;
  }
}

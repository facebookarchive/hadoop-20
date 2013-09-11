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
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.RedundantEditLogInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestQuorumJournalManagerInputStream {
  
  private static final Log LOG = LogFactory.getLog(
      TestQuorumJournalManagerInputStream.class);
  
  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    // Don't retry connections - it just slows down the tests.
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setLong(JournalConfigKeys.DFS_QJOURNAL_CONNECT_TIMEOUT_KEY, 100);
    
    cluster = new MiniJournalCluster.Builder(conf)
      .build();
    
    qjm = TestQuorumJournalManager.createSpyingQJM(conf, cluster, JID, FAKE_NSINFO);

    qjm.transitionJournal(QJMTestUtil.FAKE_NSINFO, Transition.FORMAT, null);
    qjm.recoverUnfinalizedSegments();
    assertEquals(1, qjm.getLoggerSetForTests().getEpoch());
  }
  
  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Get underlying streams.
   */
  URLLogInputStream[] getStreams(EditLogInputStream str) throws Exception {
    // there is a single redundant stream
    RedundantEditLogInputStream relis = (RedundantEditLogInputStream) str;
    Field streamsF = RedundantEditLogInputStream.class.getDeclaredField("streams");
    streamsF.setAccessible(true);
    EditLogInputStream[] underlyingStreams = (EditLogInputStream[])streamsF.get(relis);
    URLLogInputStream[] urlStreams = new URLLogInputStream[underlyingStreams.length];
    int i = 0;
    for(EditLogInputStream elis : underlyingStreams) {
      urlStreams[i++] = (URLLogInputStream) elis;
    }
    return urlStreams;
  }
  
  /**
   * Assert that refresh works properly, and if one node dies, the corresponding
   * stream is in the right state. The position thereof should be preserved, and
   * it should be disabled.
   */
  @Test
  public void testRefresh() throws Exception {
    // start new segment
    EditLogOutputStream stm = qjm.startLogSegment(0);
    // write a bunch of transactions
    QJMTestUtil.writeTxns(stm, 0, 10);
    
    // get input stream
    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 0, true, false);
    
    URLLogInputStream[] underlyingStreams = getStreams(streams.get(0));
    
    // 1) initial setup 
    
    // all streams start at 0
    for(EditLogInputStream elis : underlyingStreams) {
      assertEquals(0, elis.getFirstTxId());
      // read version info
      assertEquals(4, elis.getPosition());
    }
    
    // 2) refresh to position 10 - all should be refreshed 
    
    // refresh redundant stream - this should referesh all underlying streams
    streams.get(0).refresh(10, 0);
    // all streams are refreshed to 10 (position)
    for(EditLogInputStream elis : underlyingStreams) {
      assertEquals(0, elis.getFirstTxId());
      assertEquals(10, elis.getPosition());
    }
    
    // 3) refresh to position 15 after killing one node 
    
    // shutdown one journal node
    cluster.getJournalNode(0).stopAndJoin(0);
    
    // refresh redundant stream - this should referesh all underlying streams
    streams.get(0).refresh(15, 0);
    boolean foundCrashedOne = false;
    int numSuccessful = 0;
    for(EditLogInputStream stream : underlyingStreams) {
      URLLogInputStream elis = (URLLogInputStream) stream;
      if (!elis.isDisabled()) {
        numSuccessful++;
      } else if (!foundCrashedOne) {
        foundCrashedOne = true;
      } else {
        fail("This should not happen");
      }
    }
    assertEquals(2, numSuccessful);
    assertTrue(foundCrashedOne);
  }
  
  /**
   * Ensure that refresh functionality does not work for finalized streams (at
   * startup)
   */
  @Test
  public void testRefreshOnlyForInprogress() throws Exception {
    // start new segment
    EditLogOutputStream stm = qjm.startLogSegment(0);
    // write a bunch of transactions
    QJMTestUtil.writeTxns(stm, 0, 10);
    qjm.finalizeLogSegment(0, 9);
    
    // get input stream
    List<EditLogInputStream> streams = Lists.newArrayList();
    // get only finalized streams
    qjm.selectInputStreams(streams, 0, false, false);
    
    try {
      // try refreshing the stream (this is startup mode
      // inprogress segments not allowed -> refresh should fail
      streams.get(0).refresh(10, 0);
      fail("The shream should not allow refreshing");
    } catch (IOException e) { 
      LOG.info("Expected exception: ", e);
    } 
  }
  
  /**
   * Test fall back behaviour, after the tailing node fails, we should switch to
   * the next one and keep reading transactions.
   */
  @Test
  public void testReadFailsAfterFailedRefresh() throws Exception {
    // start new segment
    EditLogOutputStream stm = qjm.startLogSegment(0);
    // write a bunch of transactions
    QJMTestUtil.writeTxns(stm, 0, 10);
    QJMTestUtil.writeTxns(stm, 10, 10);
    
    // get input stream
    List<EditLogInputStream> streams = Lists.newArrayList();
    // get inprogress streams
    qjm.selectInputStreams(streams, 0, true, false);
    
    long lastReadTxId = -1;
    EditLogInputStream is = streams.get(0);
    for (int i = 0; i < 3; i++) {
      FSEditLogOp op = is.readOp();
      assertNotNull(op);
      lastReadTxId = op.getTransactionId();
      LOG.info("Read transaction: " + op + " with txid: "
          + op.getTransactionId());
    }
    
    // get the stream we are tailing from
    URLLogInputStream[] tailing = new URLLogInputStream[1];
    JournalNode jn = getTailingJN(is, tailing);
    
    long position = is.getPosition();
    
    // stop the node
    jn.stopAndJoin(0);
    
    // refresh the input stream
    is.refresh(position, 0);
    
    LOG.info("Checking failed stream");
    // this guy should be disabled
    // its position should be fixed
    URLLogInputStream urlis = tailing[0];
    assertTrue(urlis.isDisabled());
    assertEquals(position, urlis.getPosition());
    assertEquals(HdfsConstants.INVALID_TXID, urlis.getLastTxId());
    try {
      urlis.readOp();
      fail("This read should fail");
    } catch (IOException e) {
      LOG.info("Expected exception: ", e);
    } // expected
    
    // reads should fall back to another stream
    LOG.info("We should be able to read from the stream");
    for (int i = 0; i < 3; i++) {
      FSEditLogOp op = is.readOp();
      assertNotNull(op);
      assertEquals(++lastReadTxId, op.getTransactionId());
      LOG.info("Read transaction: " + op + " with txid: "
          + op.getTransactionId());
      position = is.getPosition();
    }
    LOG.info("Current state of the input stream: " + is.getName());
    
    // refresh again
    is.refresh(position, 0);
    assertEquals(position, urlis.getPosition());
    assertTrue(urlis.isDisabled());
    assertEquals(HdfsConstants.INVALID_TXID, urlis.getLastTxId());
  }
  
  /**
   * Get the journal node we are tailing from, and indicate which stream this is.
   */
  private JournalNode getTailingJN(EditLogInputStream str,
      URLLogInputStream[] tailingStream) throws Exception {
    RedundantEditLogInputStream is = (RedundantEditLogInputStream) str;

    Field curIdxF = RedundantEditLogInputStream.class
        .getDeclaredField("curIdx");
    curIdxF.setAccessible(true);
    int curIdx = curIdxF.getInt(is);

    URLLogInputStream[] streams = getStreams(is);

    JournalNode jn = null;
    for (JournalNode j : cluster.getJournalNodes()) {
      if (streams[curIdx].getName().contains(
          Integer.toString(j.getBoundHttpAddress().getPort()))) {
        jn = j;
        break;
      }
    }
    tailingStream[0] = streams[curIdx];
    return jn;
  }
}

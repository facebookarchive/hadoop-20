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

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.qjournal.server.JournalNodeHttpServer;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class TestImageUploadStream {

  static final Log LOG = LogFactory.getLog(TestImageUploadStream.class);
  static final int iterations = 10;
  static final int startTxId = 100;

  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;
  private List<String> httpAddrs;
  private int bufferSize = 100;
  private int maxNumChunks = 1000;

  private InjectionEventI[] failureInjections = new InjectionEventI[] {
      InjectionEvent.UPLOADIMAGESERVLET_START,
      InjectionEvent.UPLOADIMAGESERVLET_RESUME,
      InjectionEvent.UPLOADIMAGESERVLET_COMPLETE };

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    // Don't retry connections - it just slows down the tests.
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setLong(JournalConfigKeys.DFS_QJOURNAL_CONNECT_TIMEOUT_KEY, 100);

    cluster = new MiniJournalCluster.Builder(conf).build();

    createJournalStorage();

    assertEquals(1, qjm.getLoggerSetForTests().getEpoch());
    httpAddrs = cluster.getHttpJournalAddresses();
  }

  private void createJournalStorage() throws IOException, URISyntaxException {
    qjm = createSpyingQJM();

    qjm.transitionJournal(QJMTestUtil.FAKE_NSINFO, Transition.FORMAT, null);
    qjm.transitionImage(QJMTestUtil.FAKE_NSINFO, Transition.FORMAT, null);

    qjm.recoverUnfinalizedSegments();
  }

  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    InjectionHandler.clear();
  }

  /**
   * Test basic upload of multiple files, with non-failure scenario.
   */
  @Test
  public void testNormalImageUpload() throws Exception {
    LOG.info("----- testBasic -----");
    TestImageUploadStreamInjectionHandler h = new TestImageUploadStreamInjectionHandler(
        cluster.getNumNodes());
    InjectionHandler.set(h);

    for (int i = 0; i < iterations; i++) {
      LOG.info("-- iteration: " + i);
      // write should succeed
      MD5Hash digest = writeDataAndAssertContents(h, i);
      // clear hashes for next iteration
      h.clearHandler();

      // finalize the image
      assertManifest(i, digest, false);
    }
  }

  /**
   * Rolls the image and asserts contents of the manifests.
   */
  private void assertManifest(int iteration, MD5Hash digest,
      boolean skipPartial) throws IOException {
    if (!skipPartial) {
      for (int i = 0; i < cluster.getNumNodes(); i++) {
        JournalNode jn = cluster.getJournalNodes()[i];

        RemoteImageManifest rim = jn.getJournal(JID.getBytes())
            .getImageManifest(-1);
        assertEquals(iteration + 1, rim.getImages().size());
        for (int j = 0; j <= iteration; j++) {
          assertEquals(startTxId + j, rim.getImages().get(j).getTxId());
        }
      }
    }

    // get manifest through qjm
    RemoteImageManifest rm = qjm.getImageManifest(-1);
    for (int j = 0; j <= iteration; j++) {
      assertEquals(startTxId + j, rm.getImages().get(j).getTxId());
    }

    assertEquals(startTxId + iteration, qjm.getLatestImage()
        .getCheckpointTxId());
  }

  /**
   * Test upload with one failed channel.
   */
  @Test
  public void testSingleFailure() throws Exception {
    for (InjectionEventI e : failureInjections) {
      try {
        testSingleFailure(e);
      } finally {
        createJournalStorage();
      }
    }
  }

  /**
   * Test upload with one failed channel.
   */
  private void testSingleFailure(InjectionEventI failOn) throws Exception {
    LOG.info("----- testSingleFailure for event : " + failOn);
    Random r = new Random();
    int numNodes = cluster.getNumNodes();
    TestImageUploadStreamInjectionHandler h = new TestImageUploadStreamInjectionHandler(
        numNodes);
    InjectionHandler.set(h);

    for (int i = 0; i < iterations; i++) {
      LOG.info("-- iteration: " + i);
      int failJournal = r.nextInt(numNodes);

      h.setFailure(failJournal, failOn);

      // the write should succeed
      MD5Hash digest = writeDataAndAssertContents(h, i);

      // clear hashes for next iteration
      h.clearHandler();

      // finalize the image
      assertManifest(i, digest, true);
    }
  }

  /**
   * Test upload with multiple failures injected. The writes should not succeed.
   */
  @Test
  public void testMultipleFailure() throws Exception {
    LOG.info("----- testMultipleFailure -----");
    Random r = new Random();
    int numNodes = cluster.getNumNodes();
    TestImageUploadStreamInjectionHandler h = new TestImageUploadStreamInjectionHandler(
        numNodes);
    InjectionHandler.set(h);

    for (int i = 0; i < iterations; i++) {
      // choose events
      InjectionEventI eventOne = failureInjections[r
          .nextInt(failureInjections.length)];
      InjectionEventI eventTwo = failureInjections[r
          .nextInt(failureInjections.length)];

      // choose nodes
      int nodeOne = r.nextInt(numNodes);
      int nodeTwo = (nodeOne + 1) % numNodes;
      h.setFailure(nodeOne, eventOne);
      h.setFailure(nodeTwo, eventTwo);

      LOG.info("xxIteration: " + i + " for: " + eventOne + " and " + eventTwo);
      try {
        // the write should fail
        writeDataAndAssertContents(h, i);
        fail("Write should not succeed");
      } catch (Exception e) {
        assertTrue(e instanceof QuorumException);
      }
      // clear hashes for next iteration
      h.clearHandler();
    }
  }

  private MD5Hash writeDataAndAssertContents(
      TestImageUploadStreamInjectionHandler h, int iteration)
      throws IOException {

    // check write digest
    MessageDigest digester = MD5Hash.getDigester();

    // create stream
    HttpImageUploadStream ius = new HttpImageUploadStream(httpAddrs, JID, FAKE_NSINFO,
        startTxId + iteration, 1, bufferSize, maxNumChunks);

    DigestOutputStream ds = new DigestOutputStream(ius, digester);
    DataOutputStream dos = new DataOutputStream(ds);

    // write actual data
    byte[] written = writeData(dos, 10240);

    // flush
    dos.flush();

    // get written hash
    MD5Hash hash = new MD5Hash(digester.digest());

    // close the stream
    dos.close();
    assertContents(cluster, written, startTxId + iteration, hash, h);

    // roll image
    qjm.saveDigestAndRenameCheckpointImage(startTxId + iteration, hash);

    // final assert of the contents
    // get contents using input stream obtained from qjm
    InputStream is = qjm.getImageInputStream(startTxId + iteration)
        .getInputStream();
    byte[] contents = new byte[written.length];
    is.read(contents);
    assertTrue(Arrays.equals(written, contents));

    return hash;
  }

  /**
   * Write random data by using write(byte) and write(byt[]).
   */
  private byte[] writeData(OutputStream os, int size) throws IOException {
    Random r = new Random();
    int approxMaxLen = size;
    int bytesWritten = 0;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    while (bytesWritten < approxMaxLen) {
      if (r.nextBoolean()) {
        int b = r.nextInt();
        os.write(b);
        bos.write(b);
        bytesWritten++;
      } else {
        byte[] rand = new byte[r.nextInt(10) + 1];
        r.nextBytes(rand);
        os.write(rand);
        bos.write(rand);
        bytesWritten += rand.length;
      }
    }
    return bos.toByteArray();
  }

  /**
   * Assert contenst and hash for every journal.
   */
  private void assertContents(MiniJournalCluster cluster, byte[] written,
      long txid, MD5Hash writtenHash, TestImageUploadStreamInjectionHandler h)
      throws IOException {
    int numJournals = cluster.getNumNodes();

    // assert that each file contains what it should
    for (int i = 0; i < numJournals; i++) {
      if (h.failOn[i] != null) {
        continue;
      }
      Journal j = cluster.getJournalNode(i).getOrCreateJournal(JID.getBytes());
      assertContentsForJournal(j, written, txid);
    }

    // for failures assert the number of exceptions
    int expectedExceptionCount = 0;
    for (InjectionEventI e : h.failOn) {
      expectedExceptionCount += (e == null ? 0 : 1);
    }
    assertEquals(expectedExceptionCount, h.getExceptions());

    // assert hashes
    assertEquals(numJournals - expectedExceptionCount, h.uploadHashes.size());
    for (MD5Hash hash : h.uploadHashes) {
      assertEquals(writtenHash, hash);
    }

  }

  /**
   * Assert contents for a single journal.
   */
  private void assertContentsForJournal(Journal journal, byte[] written,
      long txid) throws IOException {
    LOG.info("---- validating contents ---- for txid: " + txid);
    InputStream is = null;
    try {
      File uploaded = journal.getImageStorage().getCheckpointImageFile(txid);
      assertTrue(uploaded.exists());
      assertEquals(written.length, uploaded.length());

      // assert contents of the uploaded file
      is = new FileInputStream(uploaded);
      byte[] contents = new byte[written.length];
      is.read(contents);

      assertTrue(Arrays.equals(written, contents));
    } finally {
      if (is != null)
        is.close();
    }
  }

  private QuorumJournalManager createSpyingQJM() throws IOException,
      URISyntaxException {
    return TestQuorumJournalManager.createSpyingQJM(conf, cluster, JID,
        FAKE_NSINFO);
  }

  class TestImageUploadStreamInjectionHandler extends InjectionHandler {
    List<MD5Hash> uploadHashes = Collections
        .synchronizedList(new ArrayList<MD5Hash>());
    int numJournals;
    InjectionEventI[] failOn;
    AtomicInteger exceptionsThrown = new AtomicInteger(0);

    TestImageUploadStreamInjectionHandler(int numJournals) {
      this.numJournals = numJournals;
      this.failOn = new InjectionEventI[numJournals];
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.UPLOADIMAGESERVLET_START
          || event == InjectionEvent.UPLOADIMAGESERVLET_RESUME
          || event == InjectionEvent.UPLOADIMAGESERVLET_COMPLETE) {
        simulateFailute(event, args);
      }
      processEvent(event, args);
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.UPLOADIMAGESERVLET_COMPLETE) {
        uploadHashes.add((MD5Hash) args[1]);
      }
    }

    void setFailure(int index, InjectionEventI event) {
      Preconditions.checkArgument(index >= 0);
      Preconditions.checkArgument(index < failOn.length);
      failOn[index] = event;
    }

    private void simulateFailute(InjectionEventI event, Object... args)
        throws IOException {
      // get the journal node
      ServletContext context = (ServletContext) args[0];
      JournalNode jn = (JournalNode) context
          .getAttribute(JournalNodeHttpServer.JN_ATTRIBUTE_KEY);
      // configuration stores the index of the node
      Configuration conf = jn.getConf();
      // check which node this is
      int jid = conf.getInt(MiniJournalCluster.DFS_JOURNALNODE_TEST_ID, 0);
      // fail if we are supposed to fail on this event
      if (event == failOn[jid]) {
        exceptionsThrown.incrementAndGet();
        throw new IOException("Testing failures");
      }
    }

    int getExceptions() {
      return exceptionsThrown.get();
    }

    void clearHandler() {
      for (int i = 0; i < failOn.length; i++) {
        failOn[i] = null;
      }
      uploadHashes.clear();
      exceptionsThrown.set(0);
    }
  }
}

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
package org.apache.hadoop.hdfs.qjournal.server;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.client.URLImageInputStream;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.UploadImageServlet.UploadImageParam;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImage;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestJournalNodeImageManifest {

  static final Log LOG = LogFactory.getLog(TestJournalNodeImageManifest.class);

  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(12345, 0L,
      0);

  private Map<Long, MD5Hash> digests = Maps.newHashMap();
  private Map<Long, byte[]> content = Maps.newHashMap();

  private JournalNode jn;
  private Journal journal;
  private Configuration conf = new Configuration();
  private String journalId;
  private String httpAddress;

  byte[] randomBytes = new byte[1024 * 10];
  Random rand = new Random();

  IPCLoggerChannel ch;

  @After
  public void tearDown() {
    digests.clear();
    content.clear();
  }

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

    ch = new IPCLoggerChannel(conf, FAKE_NSINFO, journalId,
        jn.getBoundIpcAddress());

    // this will setup the http port
    ch.getJournalState();
  }

  @Test
  public void testImagesManifestAndRead() throws Exception {
    int iterations = 10;
    long txid = 100;
    List<RemoteImage> expectedImages = Lists.newArrayList();

    // create valid images (possibly with missing md5s
    for (int i = 0; i < iterations; i++) {
      boolean removeMd5 = rand.nextBoolean();
      createImage(txid + i, true, removeMd5);
      // this should be in the manifest later
      expectedImages.add(new RemoteImage(txid + i, removeMd5 ? null : digests
          .get(txid + i)));
    }

    for (int i = 0; i < iterations; i++) {
      createImage(txid + i + iterations, false, false);
      // this should not be in the manifest later
    }

    RemoteImageManifest m = journal.getImageManifest(-1);
    assertEquals(expectedImages, m.getImages());

    // //// try reading images
    for (RemoteImage ri : expectedImages) {
      if (ri.getDigest() == null) {
        // download should fail
        try {
          URLImageInputStream is = new URLImageInputStream(ch, ri.getTxId(),
              1000);
          is.getSize();
          fail("Should fail here");
        } catch (IOException e) {
          LOG.info("Expected exception: ", e);
        }
      } else {
        // the node should contain a valid image
        URLImageInputStream stream = new URLImageInputStream(ch, ri.getTxId(), 1000);

        // verify the digest
        assertEquals(stream.getImageDigest(), digests.get(ri.getTxId()));
        // verify size
        assertEquals(stream.getSize(), content.get(ri.getTxId()).length);

        // read the bytes
        byte readBytes[] = new byte[(int) stream.getSize()];
        stream.read(readBytes);

        // assert contents
        assertTrue(Arrays.equals(content.get(ri.getTxId()), readBytes));
      }
    }
  }

  /**
   * Create image with given txid. Finalize and/or delete md5 file if specified.
   */
  private void createImage(long txid, boolean finalize, boolean removeMd5)
      throws IOException {
    // create image
    ContentBody cb = genContent(txid);
    HttpClient httpClient = new DefaultHttpClient();
    HttpPost postRequest = TestJournalNodeImageUpload.createRequest(
        httpAddress, cb);
    UploadImageParam.setHeaders(postRequest, journalId,
        FAKE_NSINFO.toColonSeparatedString(), 0, txid, 0, 0, true);
    httpClient.execute(postRequest);

    if (finalize) {
      // roll the image
      journal.saveDigestAndRenameCheckpointImage(txid, digests.get(txid));

      // remove md5 file if requested which leaves only the image file
      if (removeMd5) {
        MD5FileUtils.getDigestFileForFile(
            journal.getImageStorage().getImageFile(txid)).delete();
      }
    }
  }

  /**
   * Generate random contents for the image and store it together with the md5
   * for later comparison.
   */
  private ContentBody genContent(long txid) throws IOException {
    MessageDigest digester = MD5Hash.getDigester();

    // write through digester so we can roll the written image
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    DigestOutputStream ds = new DigestOutputStream(bos, digester);

    // generate random bytes
    new Random().nextBytes(randomBytes);
    ds.write(randomBytes);
    ds.flush();

    // get written hash
    MD5Hash hash = new MD5Hash(digester.digest());

    // store contents and digest
    digests.put(txid, hash);
    content.put(txid, Arrays.copyOf(randomBytes, randomBytes.length));

    return new ByteArrayBody(bos.toByteArray(), "filename");
  }
}

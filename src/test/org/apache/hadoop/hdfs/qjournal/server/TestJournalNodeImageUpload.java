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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.UploadImageServlet.UploadImageParam;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.Header;
import org.junit.Before;
import org.junit.Test;

public class TestJournalNodeImageUpload {

  static final Log LOG = LogFactory.getLog(TestJournalNodeImageUpload.class);

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

  @Test
  public void testMismatchEpoch() throws Exception {
    ContentBody cb = genContent();
    startUpload(cb, 100);
    // session - 0
    // next expected segment - 1
    // epoch - 0

    // epoch mismatch
    tryUploading(cb, journalId, 1, 0, 5, true, false);
  }

  @Test
  public void testMismatch() throws Exception {
    ContentBody cb = genContent();
    startUpload(cb, 100);
    // session - 0
    // next expected segment - 1
    // epoch - 0

    // no such session
    tryUploading(cb, journalId, 5, 2, 0, true, false);

    // journalId mismatch
    tryUploading(cb, journalId + "foo", 0, 1, 0, true, false);

    // out of order
    tryUploading(cb, journalId, 0, 2, 0, true, false);

    // valid segment
    tryUploading(cb, journalId, 0, 1, 0, false, false);
    // valid + close
    tryUploading(cb, journalId, 0, 2, 0, false, true);

    // try uploading after close
    tryUploading(cb, journalId, 0, 2, 0, true, true);
  }

  private void tryUploading(ContentBody cb, String journalId, long sessionId,
      long segmentId, long epoch, boolean expectedError, boolean close)
      throws IOException {
    HttpClient httpClient = new DefaultHttpClient();
    HttpPost postRequest = createRequest(httpAddress, cb);
    UploadImageParam.setHeaders(postRequest, journalId,
        FAKE_NSINFO.toColonSeparatedString(), epoch, 100, sessionId, segmentId,
        close);
    HttpResponse resp = httpClient.execute(postRequest);
    if (expectedError) {
      assertEquals(HttpServletResponse.SC_NOT_ACCEPTABLE, resp.getStatusLine()
          .getStatusCode());
    }
  }

  private void startUpload(ContentBody cb, long txid) throws IOException {
    HttpClient httpClient = new DefaultHttpClient();
    HttpPost postRequest = createRequest(httpAddress, cb);
    UploadImageParam.setHeaders(postRequest, journalId,
        FAKE_NSINFO.toColonSeparatedString(), 0, txid, 0, 0, false);
    httpClient.execute(postRequest);
  }

  @Test
  public void testBasic() throws Exception {
    long txid = 101;
    ContentBody cb = genContent();
    int iterations = 30;
    long sessionId = 0;
    for (int i = 0; i < iterations; i++) {
      HttpClient httpClient = new DefaultHttpClient();
      HttpPost postRequest = createRequest(httpAddress, cb);
      UploadImageParam.setHeaders(postRequest, journalId,
          FAKE_NSINFO.toColonSeparatedString(), 0, txid, sessionId, i,
          (i == iterations - 1));
      HttpResponse response = httpClient.execute(postRequest);

      if (i == 0) {
        // get the session id
        for (Header h : response.getAllHeaders()) {
          if (h.getName().equals("sessionId")) {
            sessionId = Long.parseLong(h.getValue());
            break;
          }
        }
      }
    }

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

  private ContentBody genContent() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    new Random().nextBytes(randomBytes);
    bos.write(randomBytes);
    return new ByteArrayBody(bos.toByteArray(), "filename");
  }

  static HttpPost createRequest(String httpAddress, ContentBody cb) {
    HttpPost postRequest = new HttpPost(httpAddress + "/uploadImage");
    MultipartEntity reqEntity = new MultipartEntity(
        HttpMultipartMode.BROWSER_COMPATIBLE);
    reqEntity.addPart("file", cb);
    postRequest.setEntity(reqEntity);
    return postRequest;
  }
}

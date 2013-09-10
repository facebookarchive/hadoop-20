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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.util.ServletUtil;
import org.mortbay.util.ajax.JSON;

/**
 * This servlet is used for obtaining edit log manifest for a given journal.
 */
public class GetJournalManifestServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private static final Log LOG = LogFactory
      .getLog(GetJournalManifestServlet.class);

  static final String START_TXID_PARAM = "startTxId";

  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final ServletContext context = getServletContext();
      final String journalId = request
          .getParameter(GetJournalEditServlet.JOURNAL_ID_PARAM);
      QuorumJournalManager.checkJournalId(journalId);
      final Journal journal = JournalNodeHttpServer.getJournalFromContext(
          context, journalId);
      final JNStorage storage = journal.getJournalStorage();

      // Check that the namespace info is correct
      if (!GetJournalEditServlet.checkStorageInfoOrSendError(storage, request,
          response)) {
        return;
      }

      long startTxId = ServletUtil.parseLongParam(request, START_TXID_PARAM);
      FileJournalManager fjm = journal.getJournalManager();

      LOG.info("getJournalManifest request: journalId " + journalId
          + ", start txid: " + startTxId + ", storage: "
          + storage.toColonSeparatedString());

      String output = JSON.toString(new ArrayList<String>());

      synchronized (journal) {
        // Synchronize on the journal so that the files do not change
        // get all log segments
        List<EditLogFile> logFiles = fjm.getLogFiles(startTxId, false);
        List<String> manifest = new ArrayList<String>();
        for (EditLogFile elf : logFiles) {
          manifest.add(elf.toColonSeparatedString());
        }
        output = JSON.toString(manifest);
      }
      JournalNodeHttpServer.sendResponse(output, response);
    } catch (Throwable t) {
      GetJournalEditServlet.handleFailure(t, response, "getJournalManifest");
    }
  }

  public static String buildPath(String journalId, long startTxId,
      StorageInfo nsInfo) {
    StringBuilder path = new StringBuilder("/getJournalManifest?");
    try {
      path.append(GetJournalEditServlet.JOURNAL_ID_PARAM).append("=")
          .append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&" + START_TXID_PARAM).append("=").append(startTxId);
      path.append("&" + GetJournalEditServlet.STORAGEINFO_PARAM).append("=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Never get here -- everyone supports UTF-8
      throw new RuntimeException(e);
    }
    return path.toString();
  }
}

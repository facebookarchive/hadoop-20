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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet.GetImageParams;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;

/**
 * This servlet is used for serving image stored in the underlying journal node.
 * Currently, it does not support image upload.
 */
@InterfaceAudience.Private
public class GetJournalImageServlet extends HttpServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Log LOG = LogFactory.getLog(GetJournalImageServlet.class);

  static final String STORAGEINFO_PARAM = "storageInfo";
  static final String JOURNAL_ID_PARAM = "jid";
  static final String TXID_PARAM = "txid";
  static final String THROTTLE_PARAM = "disableThrottler";
  
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final GetImageParams parsedParams = new GetImageParams(request, response);
      
      // here we only support getImage
      if (!parsedParams.isGetImage()) {
        throw new IOException("Only getImage requests are supported");
      }
      
      // get access to journal node storage
      final ServletContext context = getServletContext();
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      final String journalId = request.getParameter(JOURNAL_ID_PARAM);
      QuorumJournalManager.checkJournalId(journalId);

      final Journal journal = JournalNodeHttpServer.getJournalFromContext(
          context, journalId);
      final JNStorage imageStorage = journal.getImageStorage();
      
      final JournalMetrics metrics = journal.getMetrics();
      if (metrics != null) {
        metrics.numGetImageDoGet.inc();
      }

      // Check that the namespace info is correct
      if (!GetJournalEditServlet.checkStorageInfoOrSendError(imageStorage,
          request, response)) {
        return;
      }
      
      // we will serve image at txid
      long txid = parsedParams.getTxId();
      File imageFile = imageStorage.getImageFile(txid);
      
      // no such image in the storage
      if (imageFile == null) {
        throw new IOException("Could not find image with txid " + txid);
      }
      
      // set verification headers 
      setVerificationHeaders(response, imageFile);
      
      // send fsImage
      TransferFsImage.getFileServer(response.getOutputStream(), imageFile,
          GetImageServlet.getThrottler(conf, parsedParams.isThrottlerDisabled()));

    } catch (Throwable t) {
      GetJournalEditServlet.handleFailure(t, response, "getImage");
    } 
  }
  
  /**
   * 1. Set length of the file being served. 
   * 2. Find md5 for the given file, and
   * set it in the response. If md5 cannot be read, fail the connection.
   */
  private static void setVerificationHeaders(HttpServletResponse response,
      File file) throws IOException {
    response.setHeader(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
    MD5Hash hash = MD5FileUtils.readStoredMd5ForFile(file);
    // we require to send md5, if md5 is not present, we do not consider this
    // image as valid
    if (hash == null) {
      throw new IOException("No md5 digest could be obtained for image file: "
          + file);
    }
    response.setHeader(TransferFsImage.MD5_HEADER, hash.toString());
  }

  /**
   * Build path to fetch image at given txid for the given journal.
   * This path does not contain address.
   */
  public static String buildPath(String journalId, long txid,
      NamespaceInfo nsInfo, boolean throttle) {
    StringBuilder path = new StringBuilder("/getImage?getimage=1&");
    try {
      path.append(JOURNAL_ID_PARAM).append("=")
          .append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&" + TXID_PARAM).append("=")
          .append(txid);
      path.append("&" + THROTTLE_PARAM).append("=")
          .append(throttle);
      path.append("&" + STORAGEINFO_PARAM).append("=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Never get here -- everyone supports UTF-8
      throw new RuntimeException(e);
    }
    return path.toString();
  }
}

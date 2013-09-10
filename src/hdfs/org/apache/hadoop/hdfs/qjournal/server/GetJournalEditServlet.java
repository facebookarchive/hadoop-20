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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * This servlet is used in two cases:
 * <ul>
 * <li>The QuorumJournalManager, when reading edits, fetches the edit streams
 * from the journal nodes.</li>
 * <li>During edits synchronization, one journal node will fetch edits from
 * another journal node.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class GetJournalEditServlet extends HttpServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Log LOG = LogFactory.getLog(GetJournalEditServlet.class);

  static final String STORAGEINFO_PARAM = "storageInfo";
  static final String JOURNAL_ID_PARAM = "jid";
  static final String SEGMENT_TXID_PARAM = "segmentTxId";
  static final String POSITION_PARAM = "position";
  
  public static final String LAST_VALID_TXID = "Last-Valid-TxId";
  public static final String IS_IN_PROGRESS = "Is-In-progress";
  
  static boolean checkStorageInfoOrSendError(JNStorage storage,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String myStorageInfoString = storage.toColonSeparatedString();
    String theirStorageInfoString = StringEscapeUtils.escapeHtml(
        request.getParameter(STORAGEINFO_PARAM));

    if (theirStorageInfoString != null
        && !myStorageInfoString.equals(theirStorageInfoString)) {
      String msg = "This node has storage info '" + myStorageInfoString
          + "' but the requesting node expected '"
          + theirStorageInfoString + "'";
      
      response.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
      LOG.warn("Received an invalid request file transfer request from " +
          request.getRemoteAddr() + ": " + msg);
      return false;
    }
    return true;
  }
  
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final ServletContext context = getServletContext();
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      final String journalId = request.getParameter(JOURNAL_ID_PARAM);
      QuorumJournalManager.checkJournalId(journalId);
      final Journal journal = JournalNodeHttpServer
          .getJournalFromContext(context, journalId);
      final JNStorage storage = journal.getJournalStorage();
      final JournalMetrics metrics = journal.getMetrics();
      
      if (metrics != null) {
        metrics.numGetJournalDoGet.inc();
      }

      // Check that the namespace info is correct
      if (!checkStorageInfoOrSendError(storage, request, response)) {
        return;
      }
      
      long segmentTxId = ServletUtil.parseLongParam(request,
          SEGMENT_TXID_PARAM);
      long position = ServletUtil.parseLongParam(request, POSITION_PARAM);

      FileJournalManager fjm = journal.getJournalManager();
      File editFile;
      InputStream fStream;
      
      long lengthToSend;
      synchronized (journal) {
        // Synchronize on the journal so that the file doesn't get finalized
        // out from underneath us while we're in the process of opening
        // it up.

        // get all log segments
        List<EditLogFile> logFiles = fjm.getLogFiles(segmentTxId);
        
        // no files found
        if (logFiles.size() == 0) {
          response.sendError(HttpServletResponse.SC_NOT_FOUND,
              "No edit log found starting at txid " + segmentTxId);
          return;
        }
        
        // the first one must be the one that we request
        if (logFiles.get(0).getFirstTxId() != segmentTxId) {
          throw new IOException("Segment txid mismatch for segment txid: " 
              + segmentTxId + ", first file found starts at txid: " 
              + logFiles.get(0).getFirstTxId());
        }
        
        // the requested file is in the beginning of the list
        EditLogFile elf = logFiles.get(0);
        editFile = elf.getFile();
        
        long lastValidTxId;
        long maxValidLength;
        
        if (segmentTxId == journal.getCurrentSegmentTxId()) {
          if (!elf.isInProgress()) {
            throw new IOException(
                "Inconsistent state. Current segment must always be in progress.");
          }
          // this is the currently written segment, this will be the
          // most common scenario, where we want to tail this segment
          // we do not want to validate it at each request
          maxValidLength = journal.getValidSizeOfCurrentSegment();
          // we can safely read only up to this transaction
          // max - secures a degenerate case when we have multiple empty segments
          lastValidTxId = Math.max(segmentTxId - 1, journal.getCommittedTxnId());
        } else if (elf.isInProgress()) {
          // this is an in-progress segment, but there are newer segments already
          // (possibly in progress). This happens when the journal failed and was 
          // restored at later time, we do not know what the last valid txid is.
          // we can still serve the file, just in case there is logic on the
          // reader side to handle this
          lastValidTxId = HdfsConstants.INVALID_TXID;
          maxValidLength = editFile.length();
        } else {
          // finalized segment has the last transaction id,
          // ad this transaction has been committed in the quorum
          // entire segment can be safely served
          // (segment cannot be finalized if the last transaction
          // was not synced to the quorum)
          lastValidTxId = elf.getLastTxId();
          maxValidLength = editFile.length();
        }
        
        if (position > maxValidLength) {
          throw new IOException("Position: " + position
              + " is beyond valid length of the file. File size: " + maxValidLength);
        }
        lengthToSend = maxValidLength - position;
        
        // we are sending lengthToSend bytes
        response.setHeader(TransferFsImage.CONTENT_LENGTH,
            String.valueOf(lengthToSend));
        
        // indicate that this stream can only serve up to this transaction
        response.setHeader(LAST_VALID_TXID, String.valueOf(lastValidTxId));
        response.setHeader(IS_IN_PROGRESS, String.valueOf(elf.isInProgress()));
               
        GetImageServlet.setFileNameHeaders(response, editFile);
        
        // prepare input stream
        RandomAccessFile rp = new RandomAccessFile(editFile, "r");    
        fStream = new FileInputStream(rp.getFD());
        FileChannel fc = rp.getChannel();
        fc.position(position);
        fStream = new BufferedInputStream(fStream);
      }
      
      DataTransferThrottler throttler = GetImageServlet.getThrottler(conf, false);

      // send edits
      TransferFsImage.getFileServerForPartialFiles(response.getOutputStream(),
          editFile.getAbsolutePath(), fStream, throttler, position,
          lengthToSend);
      
      if (metrics != null) {
        metrics.sizeGetJournalDoGet.inc(lengthToSend);
      }

    } catch (Throwable t) {
      handleFailure(t, response, "getJournal");
    } 
  }

  public static String buildPath(String journalId, long segmentTxId,
      StorageInfo nsInfo, long position) {
    StringBuilder path = new StringBuilder("/getJournal?");
    try {
      path.append(JOURNAL_ID_PARAM).append("=")
          .append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&" + SEGMENT_TXID_PARAM).append("=")
          .append(segmentTxId);
      path.append("&" + POSITION_PARAM).append("=")
          .append(position);
      path.append("&" + STORAGEINFO_PARAM).append("=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Never get here -- everyone supports UTF-8
      throw new RuntimeException(e);
    }
    return path.toString();
  }
  
  static void handleFailure(Throwable t, HttpServletResponse response,
      String request) throws IOException {
    String errMsg = request + " failed. " + StringUtils.stringifyException(t);
    try {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errMsg);
    } catch (IOException e) {
      errMsg += " Cannot send error to client.";
    }
    LOG.warn(errMsg);
    throw new IOException(errMsg);
  }
}

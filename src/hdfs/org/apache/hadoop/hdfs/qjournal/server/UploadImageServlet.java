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

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.http.client.methods.HttpPost;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;

/**
 * Servlet for uploading image to the journal node. The servlet maintains a
 * session for the uploaded image, and maintains md5 digest for the image. Once
 * the upload is completed, the md5 for the written data is stored in the
 * journal node memroy, and later when the client finalizes the upload, the
 * stored md5 is compared with the client writer side digest. The image is
 * always stored in a temporary location. The finalization is done over rpc.
 */
public class UploadImageServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  static final Log LOG = LogFactory.getLog(UploadImageServlet.class);

  private final static Map<Long, SessionDescriptor> sessions = new HashMap<Long, SessionDescriptor>();
  private final static AtomicLong sessionIds = new AtomicLong(-1L);

  private final int BUFFER_SIZE = 1024 * 1024;

  /**
   * Clean the map from obsolete images.
   */
  public synchronized static void clearObsoleteImageUploads(long minTxIdToKeep,
      String journalId) {
    for (Iterator<Map.Entry<Long, SessionDescriptor>> it = sessions.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Long, SessionDescriptor> entry = it.next();
      if (entry.getValue().journalId.equals(journalId)
          && entry.getValue().txid < minTxIdToKeep) {
        it.remove();
      }
    }
  }

  /**
   * Get storage object from underlying journal node, given request parameters.
   * If yournal does not exist, an exception will be thrown. If the journal
   * metadata does not match the request metadata, we also fail.
   */
  private static Journal getStorage(ServletContext context,
      UploadImageParam params) throws IOException {
    final Journal journal = JournalNodeHttpServer
        .getJournalFromContextIfExists(context, params.journalId);
    if (journal == null) {
      throwIOException("Journal: " + params.journalId + " does not exist");
    }
    final JNStorage storage = journal.getImageStorage();

    // check namespace metadata
    storage.checkConsistentNamespace(params.getStorageInfo());

    // check if this is the active writer
    if (!journal.checkWriterEpoch(params.epoch)) {
      throwIOException("This is not the active writer");
    }
    return journal;
  }

  private static synchronized SessionDescriptor startImageUpload(
      UploadImageParam params, ServletContext context) throws IOException {

    // get and validate storage
    Journal journal = getStorage(context, params);
    JNStorage storage = journal.getImageStorage();

    // get tmp image file
    File outputFile = storage.getCheckpointImageFile(params.txId);

    // starting a new upload
    long sessionId = sessionIds.incrementAndGet();

    MessageDigest digester = MD5Hash.getDigester();
    // open the stream that will be used throughout the upload
    FileOutputStream fos = new FileOutputStream(outputFile);
    OutputStream os = new BufferedOutputStream(new DigestOutputStream(fos,
        digester));

    SessionDescriptor sd = new SessionDescriptor(journal, params.journalId,
        sessionId, os, params.txId, digester);
    sessions.put(sessionId, sd);
    InjectionHandler.processEventIO(InjectionEvent.UPLOADIMAGESERVLET_START,
        context);
    return sd;
  }

  private static synchronized SessionDescriptor resumeImageUpload(
      UploadImageParam params, ServletContext context) throws IOException {
    // validate that we still have the journal, and the metadata is consistent
    JNStorage storage = getStorage(context, params).getImageStorage();
    SessionDescriptor sd = sessions.get(params.sessionId);

    // no stored session
    if (sd == null) {
      throwIOException("No session: " + params.sessionId);
    }

    // check that the segments are consecutive
    if (sd.lastSegmentId != params.segmentId - 1) {
      throwIOException("Expected segment : " + (sd.lastSegmentId + 1)
          + " but got: " + params.segmentId);
    }

    // check that journal is consistent
    if (!sd.journalId.equals(params.journalId)) {
      throwIOException("Journal id mismatch, stored id: " + sd.journalId
          + ", request id: " + params.journalId);
    }

    // check namespace id
    storage.checkConsistentNamespace(params.getStorageInfo());

    // inc segment id for next segment
    sd.lastSegmentId++;
    InjectionHandler.processEventIO(InjectionEvent.UPLOADIMAGESERVLET_RESUME,
        context);
    return sd;
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    boolean isMultipart = ServletFileUpload.isMultipartContent(request);

    if (isMultipart) {
      OutputStream os = null;
      SessionDescriptor sd = null;
      try {
        byte buf[] = new byte[BUFFER_SIZE];

        // parse upload parameters
        UploadImageParam params = new UploadImageParam(request);

        ServletContext context = getServletContext();
        // obtain session descriptor
        if (params.segmentId == 0) {
          // new upload
          sd = startImageUpload(params, context);
        } else {
          // resumed upload
          sd = resumeImageUpload(params, context);
        }

        os = sd.os;
        FileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);

        List<?> items = upload.parseRequest(request);
        if (items.size() != 1) {
          throwIOException("Should have one item in the multipart contents.");
        }
        FileItem item = (FileItem) items.get(0);

        // write to the local output stream
        if (!item.isFormField()) {
          InputStream is = item.getInputStream();
          int num = 1;
          while (num > 0) {
            num = is.read(buf);
            if (num <= 0) {
              break;
            }
            os.write(buf, 0, num);
          }
        }

        // close if needed
        if (params.completed) {
          os.flush();
          sessions.remove(sd.sessionId);
          MD5Hash hash = new MD5Hash(sd.digester.digest());
          os.close();
          InjectionHandler.processEventIO(
              InjectionEvent.UPLOADIMAGESERVLET_COMPLETE, context, hash);
          // store hash to compare it when rolling the image
          sd.journal.setCheckpointImageDigest(sd.txid, hash);
        }

        // pass the sessionId in the response
        response.setHeader("sessionId", Long.toString(sd.sessionId));
      } catch (Exception e) {
        // cleanup this session
        IOUtils.cleanup(LOG, os);
        sessions.remove(sd != null ? sd.sessionId : -1);
        LOG.error("Error when serving request", e);
        response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, e.toString());
      }
    } else {
      LOG.error("Error when serving request, not multipart content.");
    }
  }

  /**
   * Used for keeping track of files being uploaded.
   */
  static class SessionDescriptor {
    private final Journal journal;
    private final String journalId;
    private final long sessionId;
    private final OutputStream os;
    private final long txid;
    private long lastSegmentId = 0;
    private final MessageDigest digester;

    SessionDescriptor(Journal journal, String journalId, long sessionId,
        OutputStream os, long txid, MessageDigest digester) {
      this.journal = journal;
      this.journalId = journalId;
      this.sessionId = sessionId;
      this.os = os;
      this.txid = txid;
      this.digester = digester;
    }

    MD5Hash getHash() {
      return new MD5Hash(digester.digest());
    }
  }

  /**
   * Parameters for image upload request.
   */
  public static class UploadImageParam {
    private String journalId;
    private String storageInfoString;
    private long epoch;
    private long txId;
    private long sessionId;
    private long segmentId;
    private boolean completed;

    public String toColonSeparatedString() {
      return Joiner.on(":").join(storageInfoString, txId, segmentId, completed,
          journalId, sessionId, epoch);
    }

    public static void setHeaders(HttpPost request, String journalId,
        String storageInfoString, long epoch, long txId, long sessionId,
        long segmentId, boolean completed) {
      request.setHeader("journalId", journalId);
      request.setHeader("storageInfo", storageInfoString);
      request.setHeader("epoch", Long.toString(epoch));
      request.setHeader("txid", Long.toString(txId));
      request.setHeader("sessionId", Long.toString(sessionId));
      request.setHeader("sid", Long.toString(segmentId));
      request.setHeader("end", Boolean.toString(completed));
    }

    public UploadImageParam(HttpServletRequest request) {
      journalId = request.getHeader("journalId");
      storageInfoString = request.getHeader("storageInfo");
      epoch = Long.parseLong(request.getHeader("epoch"));
      txId = Long.parseLong(request.getHeader("txid"));
      sessionId = Long.parseLong(request.getHeader("sessionId"));
      segmentId = Long.parseLong(request.getHeader("sid"));
      completed = Boolean.parseBoolean(request.getHeader("end"));
    }

    public StorageInfo getStorageInfo() {
      return new StorageInfo(storageInfoString);
    }
  }

  static void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }
}

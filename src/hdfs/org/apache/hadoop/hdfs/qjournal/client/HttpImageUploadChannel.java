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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.qjournal.server.UploadImageServlet.UploadImageParam;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.common.collect.ComparisonChain;

/**
 * Class representing a connection to a single journal node for uploading an
 * image. This is not a persistent connection, it is re-created for every image
 * upload. The image is uploaded in chunks, and each chunk is sent by a single
 * threaded executor.
 */
public class HttpImageUploadChannel {

  static final Log LOG = LogFactory.getLog(HttpImageUploadChannel.class);

  // metadata related to the uploaded image
  private final String uri;
  private final String journalId;
  private final String namespaceInfoString;
  private final long txid;
  private final long epoch;

  // executor for executing send tasks
  private final ExecutorService sendExecutor;
  private volatile IOException e = null;

  // each chunk has its segment id (must be consecutive)
  private volatile long segmentId = 0;
  // a single upload will have a session id
  private volatile long sessionId = -1;

  private boolean init = false;
  private boolean closed = false;

  // if any operation fails, this flag is set to true
  private volatile boolean isDisabled = false;
  
  // how much we can buffer before failing
  private final int maxBufferedChunks;
  
  // make sure we will wait the bufferred data to be sent first.
  private final Semaphore available;
  
  // block 10 mins for the next buffer.
  private static int WAIT_NEXT_BUFFER_TIME_OUT_SECONDS = 10 * 60;

  // all send tasks
  private final List<Future<Void>> tasks = new ArrayList<Future<Void>>();

  public HttpImageUploadChannel(String uri, String journalId,
      NamespaceInfo nsinfo, long txid, long epoch, int maxBufferedChunks) {
    this.uri = uri;
    this.journalId = journalId;
    this.namespaceInfoString = nsinfo.toColonSeparatedString();
    this.txid = txid;
    this.epoch = epoch;
    this.maxBufferedChunks = maxBufferedChunks;
    this.available = new Semaphore(maxBufferedChunks, true);

    sendExecutor = Executors.newSingleThreadExecutor();
  }

  /**
   * Create an image upload channel, based on image txid and other metadata.
   */
  public void start() {
    try {
      if (init) {
        setErrorStatus("Cannot initialize multiple times", null);
        return;
      }
      init = true;
      HttpPost postRequest = setupRequest(new ByteArrayOutputStream(0));
      UploadImageParam.setHeaders(postRequest, journalId, namespaceInfoString,
          epoch, txid, 0, segmentId++, false);

      HttpClient httpClient = new DefaultHttpClient();
      HttpResponse response = httpClient.execute(postRequest);
      if (response.getStatusLine().getStatusCode() == HttpServletResponse.SC_NOT_ACCEPTABLE) {
        throwIOException("Error when starting upload to : " + uri + " status: "
            + response.getStatusLine().toString());
      }

      // get the session id
      for (Header h : response.getAllHeaders()) {
        if (h.getName().equals("sessionId")) {
          sessionId = Long.parseLong(h.getValue());
          break;
        }
      }

      // we must have the session id
      if (sessionId < 0) {
        throw new IOException("Session id is missing");
      }
    } catch (Exception e) {
      setErrorStatus("Exception when starting upload channel for: " + uri, e);
    }
  }

  /**
   * Send a chunk of data.
   */
  public void send(ByteArrayOutputStream bos) {
    try {
      if (this.isDisabled) {
        return;
      }
      if (available.tryAcquire(WAIT_NEXT_BUFFER_TIME_OUT_SECONDS, TimeUnit.SECONDS)) {
        tasks.add(sendExecutor.submit(new SendWorker(bos, segmentId++, false)));
      } else {
        setErrorStatus("Number of chunks in the queue to be send exceeded the configured number " 
             + maxBufferedChunks, null);
      }
    } catch (Exception e) {
      setErrorStatus("Exception when submitting a task", e);
    }
  }

  /**
   * Close the upload
   */
  public void close() {
    if (this.isDisabled || this.closed) {
      // parent stream needs to check for success explicitly
      return;
    }
    closed = true;
    try {
      // send close request
      tasks.add(sendExecutor.submit(new SendWorker(
          new ByteArrayOutputStream(0), segmentId++, true)));

      // wait for all tasks to complete
      for (Future<Void> task : tasks) {
        task.get();
      }
    } catch (InterruptedException e) {
      setErrorStatus("Interrupted exception", e);
    } catch (ExecutionException e) {
      setErrorStatus("Execution exception", e);
    } finally {
      // close the executor
      sendExecutor.shutdownNow();
    }
  }

  /**
   * Worker for sending a single chunk of the image.
   */
  class SendWorker implements Callable<Void> {
    ByteArrayOutputStream bos;
    final long segmentId;
    final boolean complete;

    SendWorker(ByteArrayOutputStream bos, long segmentId, boolean complete) {
      this.bos = bos;
      this.segmentId = segmentId;
      this.complete = complete;
    }

    @Override
    public Void call() throws Exception {
      try {
        if (isDisabled) {
          // discard all tasks after the channel has been disabled
          bos = null;
          return null;
        }
        HttpPost postRequest = setupRequest(bos);

        // setup all headers
        UploadImageParam.setHeaders(postRequest, journalId,
            namespaceInfoString, epoch, txid, sessionId, segmentId, complete);

        HttpClient httpClient = new DefaultHttpClient();

        // send data
        HttpResponse response = httpClient.execute(postRequest);

        // if there is an error, we need to populate it to the channel level
        if (response.getStatusLine().getStatusCode() == HttpServletResponse.SC_NOT_ACCEPTABLE) {
          setErrorStatus("Error when senging segment: " + segmentId
              + " status: " + response.getStatusLine().toString() + " for "
              + uri, null);
        }
        return null;
      } catch (Exception ex) {
        setErrorStatus("Error when senging segment: " + segmentId + " for "
            + uri, ex);
        return null;
      } finally {
        available.release();
        // to make sure we can free resources asap
        bos = null;
      }
    }
  }

  /**
   * Create a post request encapsulating bytes from the given
   * ByteArrayOutputStream.
   */
  private HttpPost setupRequest(ByteArrayOutputStream bos) {
    ContentBody cb = new ByteArrayBody(bos.toByteArray(), "image");
    HttpPost postRequest = new HttpPost(uri + "/uploadImage");
    MultipartEntity reqEntity = new MultipartEntity(
        HttpMultipartMode.BROWSER_COMPATIBLE);

    // add a single part to the request
    reqEntity.addPart("file", cb);
    postRequest.setEntity(reqEntity);

    return postRequest;
  }

  static void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }

  /**
   * If any operation for this channel fail, the error status is set. Parent
   * output stream, based on this information, decides whether the upload can be
   * still continued with remaining channels.
   */
  void setErrorStatus(String msg, Exception e) {
    this.e = new IOException(msg + " " + (e == null ? "" : e.toString()));
    
    // no more writes will be accepted
    this.isDisabled = true;
    
    // close the executor
    sendExecutor.shutdown();
    LOG.error(msg, e);
  }

  IOException getErrorStatus() {
    return e;
  }

  /**
   * Checks if the stream is disabled.
   */
  boolean isDisabled() {
    return isDisabled;
  }

  public String toString() {
    return "(" + uri + ")";
  }

  @Override
  public int hashCode() {
    return (uri + txid + journalId + namespaceInfoString + epoch).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof HttpImageUploadChannel)) {
      return false;
    }
    HttpImageUploadChannel oc = (HttpImageUploadChannel) o;

    return ComparisonChain.start().compare(uri, oc.uri).compare(txid, oc.txid)
        .compare(journalId, oc.journalId)
        .compare(namespaceInfoString, oc.namespaceInfoString)
        .compare(epoch, oc.epoch).result() == 0;
  }
  
  static void setTimeoutForTesting(int time) {
    WAIT_NEXT_BUFFER_TIME_OUT_SECONDS = time;
  }
}

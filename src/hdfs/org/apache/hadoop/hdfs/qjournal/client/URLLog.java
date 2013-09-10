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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;

/**
 * URLLog wraps a http input stream for a single log segment from a journal
 * node. The class has the functionality of creating the http connection to the
 * journal node.
 */
public class URLLog {
  private final AsyncLogger logger;
  private final long firstTxId;
  private URL url;
  private long advertisedSize = -1;
  private long startPosition = -1;
  private long lastValidTxId = -1;
  private boolean isInProgress;
  
  private final int httpTimeout;

  private final static String CONTENT_LENGTH = "Content-Length";

  public URLLog(AsyncLogger logger, long firstTxId, int httpTimeout) {
    this.logger = logger;
    this.firstTxId = firstTxId;
    this.url = logger.buildURLToFetchLogs(firstTxId, 0);
    this.httpTimeout = httpTimeout;
  }

  public InputStream getInputStream(long position) throws IOException {
    url = logger.buildURLToFetchLogs(firstTxId, position);
    
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    
    // set timeout for connecting and reading
    connection.setConnectTimeout(httpTimeout);
    connection.setReadTimeout(httpTimeout);
    
    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(
          "Fetch of " + url +
          " failed with status code " + connection.getResponseCode() +
          "\nResponse message:\n" + connection.getResponseMessage());
    }

    String contentLength = connection.getHeaderField(CONTENT_LENGTH);
    if (contentLength != null) {
      advertisedSize = Long.parseLong(contentLength);
      if (advertisedSize < 0) {
        throw new IOException("Invalid " + CONTENT_LENGTH + " header: " +
            contentLength);
      }
    } else {
      throw new IOException(CONTENT_LENGTH + " header is not provided " +
                            "by the server when trying to fetch " + url);
    }
    
    String lastValidTxIdStr = connection.getHeaderField(GetJournalEditServlet.LAST_VALID_TXID);
    if (lastValidTxIdStr == null) {
      throw new IOException(GetJournalEditServlet.LAST_VALID_TXID + " is not provided");
    }
    
    String isInProgressStr = connection.getHeaderField(GetJournalEditServlet.IS_IN_PROGRESS);
    if (isInProgressStr == null) {
      throw new IOException(GetJournalEditServlet.IS_IN_PROGRESS + " is not provided");
    }
    
    isInProgress = Boolean.valueOf(isInProgressStr);
    lastValidTxId = Long.valueOf(lastValidTxIdStr);
    startPosition = position;
    return connection.getInputStream();
  }

  public long length() {
    if (advertisedSize == -1) {
      throw new IllegalStateException(
          "Input stream not initialized, call getInputStream()");
    }
    // advertisedSize is the size of the segment being transmitted
    // starting from position startPosition
    return advertisedSize + startPosition;
  }
  
  public long getLastValidTxId() {
    return lastValidTxId;
  }
  
  public boolean getIsInProgress() {
    return isInProgress;
  }

  URL getURL() {
    return url;
  }

  public String getName() {
    return url.toString();
  }
}

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
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalNodeHttpServer {
  public static final Log LOG = LogFactory.getLog(
      JournalNodeHttpServer.class);

  public static final String JN_ATTRIBUTE_KEY = "localjournal";

  private HttpServer httpServer;
  private int infoPort;
  private InetSocketAddress httpAddress;
  private JournalNode localJournalNode;

  private final Configuration conf;

  JournalNodeHttpServer(Configuration conf, JournalNode jn) {
    this.conf = conf;
    this.localJournalNode = jn;
  }

  void start() throws IOException {
    final InetSocketAddress bindAddr = JournalConfigHelper.getAddress(conf);

    // initialize the webserver for uploading/downloading files.
    LOG.info("Starting web server at: " + bindAddr);

    int tmpInfoPort = bindAddr.getPort();
    
    this.httpServer = new HttpServer("qjm", bindAddr.getAddress().getHostAddress(), tmpInfoPort,
        tmpInfoPort == 0, conf);
    
    httpServer.setAttribute(JN_ATTRIBUTE_KEY, localJournalNode);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.addInternalServlet("getJournal", "/getJournal",
        GetJournalEditServlet.class);
    httpServer.addInternalServlet("getJournalManifest", "/getJournalManifest",
        GetJournalManifestServlet.class);
    httpServer.addInternalServlet("journalStats", "/journalStats",
        JournalStatsServlet.class);
    httpServer.addInternalServlet("uploadImage", "/uploadImage",
        UploadImageServlet.class);
    httpServer.addInternalServlet("getImage", "/getImage",
        GetJournalImageServlet.class);
    httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    this.infoPort = httpServer.getPort();
    this.httpAddress = new InetSocketAddress(bindAddr.getAddress(), infoPort);

    LOG.info("Journal Web-server up at: " + bindAddr + ":" + infoPort);
  }

  void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
  
  /**
   * Return the actual address bound to by the running server.
   */
  public InetSocketAddress getAddress() {
    return httpAddress;
  }

  public static Journal getJournalFromContext(ServletContext context, String jid)
      throws IOException {
    JournalNode jn = (JournalNode)context.getAttribute(JN_ATTRIBUTE_KEY);
    return jn.getOrCreateJournal(QuorumJournalManager.journalIdStringToBytes(jid));
  }
  
  public static Journal getJournalFromContextIfExists(ServletContext context, String jid)
      throws IOException {
    JournalNode jn = (JournalNode)context.getAttribute(JN_ATTRIBUTE_KEY);
    return jn.getJournal(QuorumJournalManager.journalIdStringToBytes(jid));
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }
  
  /**
   * Get journal stats for webui.
   */
  public static Map<String, Map<String, String>> getJournalStats(
      Collection<Journal> journals) {
    Map<String, Map<String, String>> stats = new HashMap<String, Map<String, String>>();
    for (Journal j : journals) {
      try {
        Map<String, String> stat = new HashMap<String, String>();
        stats.put(j.getJournalId(), stat);
        stat.put("Txid committed", Long.toString(j.getCommittedTxnId()));
        stat.put("Txid segment", Long.toString(j.getCurrentSegmentTxId()));
        stat.put("Txid written", Long.toString(j.getHighestWrittenTxId()));
        stat.put("Current lag", Long.toString(j.getCurrentLagTxns()));
        stat.put("Writer epoch", Long.toString(j.getLastWriterEpoch()));
      } catch (IOException e) {
        LOG.error("Error when collectng stats", e);
      }
    }
    return stats;
  }
  
  /**
   * Send string output when serving http request.
   * @param output data to be sent
   * @param response http response
   * @throws IOException
   */
  static void sendResponse(String output, HttpServletResponse response)
      throws IOException {
    PrintWriter out = null;
    try {
      out = response.getWriter();
      out.write(output);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}

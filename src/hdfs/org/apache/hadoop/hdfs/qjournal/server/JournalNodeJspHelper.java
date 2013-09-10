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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Class used for connecting to journal nodes and obtaining
 * statistics about the underlying journals.
 */
public class JournalNodeJspHelper {

  private static final Log LOG = LogFactory.getLog(JournalNodeJspHelper.class);
  public static final ObjectMapper mapper = new ObjectMapper();
  
  private static final int HTTP_CONNECT_TIMEOUT = 2000; // 2 secs
  private static final int HTTP_READ_TIMEOUT = 5000; // 5 secs

  private final Configuration conf;
  private final String name;

  public JournalNodeJspHelper(JournalNode jn) {
    conf = jn.getConf();
    StringBuilder tempName = new StringBuilder("");
    for (InetSocketAddress addr : getJournals()) {
      tempName = tempName.append(getHostAddress(addr)).append(",");
    }
    if (tempName.length() > 0) {
      // Removing last ,
      tempName.deleteCharAt(tempName.length() - 1);
    }
    name = tempName.toString();
  }

  public String getName() {
    return name;
  }

  public QJMStatus generateQJMStatusReport() throws IOException {
    QJMStatus status = new QJMStatus(getJournals());

    Map<InetSocketAddress, Map<String, Map<String, String>>> temp 
      = new HashMap<InetSocketAddress, Map<String, Map<String, String>>>();
    Map<InetSocketAddress, Boolean> aliveMap 
      = new HashMap<InetSocketAddress, Boolean>();

    Set<String> allJournals = new HashSet<String>();

    for (InetSocketAddress jn : status.journalNodes) {
      LOG.info("Connecting to journal node: " + jn);
      // mapping from a single journal to key,value stats
      String json = fetchStats(jn);
      aliveMap.put(jn, json != null);
      Map<String, Map<String, String>> map = getStatsMap(json);
      // update the list of all available journals
      allJournals.addAll(map.keySet());
      // store the retrieved results
      temp.put(jn, map);
    }

    for (InetSocketAddress jn : temp.keySet()) {
      // if we haven't fetch anything, we need to handle this
      Map<String, Map<String, String>> stats = temp.get(jn);
      for (String journal : allJournals) {
        // for each journal
        // we create (node, stats) mapping
        status.addJournalStats(journal, jn, stats.get(journal),
            aliveMap.get(jn));
      }
    }
    return status;
  }

  public static class QJMStatus {
    // all journal nodes
    private final Collection<InetSocketAddress> journalNodes;

    // all stats exposed by journals
    private final Set<String> statNames;
    // all journal nodes
    private final Map<String, Boolean> aliveStatus;

    // (journalid) -> (list(node -> (k,v))) mapping
    final Map<String, List<StatsDescriptor>> stats;

    public QJMStatus(Collection<InetSocketAddress> jns) {
      statNames = new HashSet<String>();
      aliveStatus = new HashMap<String, Boolean>();
      journalNodes = jns;
      stats = new HashMap<String, List<StatsDescriptor>>();
    }

    void addJournalStats(String journal, InetSocketAddress jn,
        Map<String, String> stat, boolean alive) {
      StatsDescriptor sd = new StatsDescriptor(jn, stat);

      // update the set of available stats
      statNames.addAll(sd.statsPerJournal.keySet());
      // and the names of journalnodes
      aliveStatus.put(sd.journalNode, alive);

      // we add the mapping to the list of mappings for each journal
      List<StatsDescriptor> currentList = stats.get(journal);
      if (currentList == null) {
        currentList = new ArrayList<StatsDescriptor>();
        stats.put(journal, currentList);
      }
      currentList.add(sd);
    }

    public Map<String, List<StatsDescriptor>> getStatus() {
      return stats;
    }

    public Collection<String> getJournalIds() {
      return stats.keySet();
    }
    
    public Map<String, Boolean> getAliveMap() {
      return aliveStatus;
    }
  }

  public static class StatsDescriptor {
    String journalNode;
    Map<String, String> statsPerJournal;

    public StatsDescriptor(InetSocketAddress jn,
        Map<String, String> statsPerJournal) {
      this.journalNode = getHostAddress(jn) + " : " + jn.getPort();
      this.statsPerJournal = statsPerJournal == null ? new HashMap<String, String>()
          : statsPerJournal;
    }

    public String toString() {
      return journalNode + ":" + statsPerJournal.toString();
    }
  }

  /**
   * Retrieve value from the map corresponding to the given key.
   */
  private static String getValue(Map<String, String> map, String keyName) {
    String value = map.get(keyName);
    return value == null ? "-" : value;
  }

  /**
   * Fetch stats from a single given journal node over http.
   */
  private String fetchStats(InetSocketAddress jn) throws IOException {
    try {
      return DFSUtil.getHTMLContentWithTimeout(new URI("http", null, jn.getAddress().getHostAddress(), jn
          .getPort(), "/journalStats", null, null).toURL(), HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT);
    } catch (Exception e) {
      LOG.error("Problem connecting to " + getHostAddress(jn), e);
      return null;
    }
  }

  /**
   * Get the map corresponding to the JSON string
   */
  private static Map<String, Map<String, String>> getStatsMap(String json)
      throws IOException {
    if (json == null || json.isEmpty()) {
      return new HashMap<String, Map<String, String>>();
    }
    TypeReference<Map<String, Map<String, String>>> type = new TypeReference<Map<String, Map<String, String>>>() {
    };
    return mapper.readValue(json, type);
  }

  /**
   * Get the list of journal addresses to connect.
   */
  private Collection<InetSocketAddress> getJournals() {
    return JournalNode.getJournalHttpAddresses(conf);
  }

  /**
   * Generate health report for journal nodes
   */
  public static String getNodeReport(QJMStatus status) {
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=1 cellpadding=1 cellspacing=0 title=\"Journals\">");
    sb.append("<thead><tr><td><b>Journal node</b></td><td><b>Alive</b></td></tr></thead>");
    for (Entry<String, Boolean> e : status.getAliveMap().entrySet()) {
      if (e.getValue()) {
        sb.append("<tr><td>" + e.getKey()
            + "</td><td><font color=green>Active</font></td></tr>");
      } else {
        sb.append("<tr><td>" + e.getKey()
            + "</td><td><font color=red>Failed</font></td></tr>");
      }
    }
    sb.append("</table>");
    return sb.toString();
  }
  
  /**
   * Generate report for all journals and all journal nodes
   */
  public static String getJournalReport(QJMStatus status) {
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=1 cellpadding=1 cellspacing=0 title=\"Journals\">");
    sb.append("<thead><tr><td><b>JournalId</b></td><td><b>Statistics</b></td></tr></thead>");
    for (String journalId : status.getJournalIds()) {
      sb.append("<tr><td>" + journalId + "</td><td>");
      getHTMLTableForASingleJournal(status, journalId, sb);
      sb.append("</td></tr>");
    }
    sb.append("</table>");
    return sb.toString();
  }

  /**
   * Render html table for a single journal.
   */
  public static void getHTMLTableForASingleJournal(QJMStatus status,
      String journalName, StringBuilder sb) {
    List<StatsDescriptor> stats = status.stats.get(journalName);
    if (stats == null) {
      return;
    }
    Set<String> statsNames = status.statNames;

    // header
    sb.append("<table border=1 align=\"right\" cellpadding=1 "
        + "cellspacing=0 title=\"Journal statistics\">");
    sb.append("<thead><tr><td></td>");
    for (StatsDescriptor sd : stats) {
      sb.append("<td><b>" + sd.journalNode + "</b></td>");
    }
    sb.append("</tr></thead>");

    // contents
    for (String st : statsNames) {
      // for each available stat
      sb.append("<tr><td>" + st + "</td>");
      // for each available node
      for (StatsDescriptor sd : stats) {
        sb.append("<td align=\"right\">" + getValue(sd.statsPerJournal, st)
            + "</td>");
      }
      sb.append("</tr>");
    }
    sb.append("</table>");
  }
  
  /**
   * Returns the address of the host minimizing DNS lookups. 
   * @param addr
   * @return
   */
  private static String getHostAddress(InetSocketAddress addr) {
    String hostToAppend = "";
    if (addr.isUnresolved()) {
      hostToAppend = addr.getHostName();
    } else {
      hostToAppend = addr.getAddress().getHostAddress();
    }
    return hostToAppend;
  }
}

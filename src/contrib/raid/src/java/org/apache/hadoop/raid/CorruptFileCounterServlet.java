/*
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
package org.apache.hadoop.raid;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.net.URLEncoder;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class is used in RaidNode's jetty to report the corrupt file counters to
 * the namenode in the json form
 */
public class CorruptFileCounterServlet extends HttpServlet {
  final String CORRUPT_DIR_KEY = "path";

  public static String getHTMLLinksText(String url, String text) {
    return "<a class=\"warning\" href=\"" + url + "\">" + text + "</a>";
  }

  public static String generateTable(Map<String, Long> unRecoverableCounterMap,
      Map<String, Long> recoverableCounterMap,
      String infoAddr) throws UnsupportedEncodingException {
    StringBuilder htmlSb = new StringBuilder();
    htmlSb.append(JspUtils.tr(JspUtils.td("Root Directory")
        + JspUtils.td("Raid Unrecoverable Files") 
        + JspUtils.td("Raid Recoverable Files")));
    // the keysets of the two maps should be the same in most cases
    Set<String> monitDirs = new HashSet<String>();
    monitDirs.addAll(unRecoverableCounterMap.keySet());
    monitDirs.addAll(recoverableCounterMap.keySet());
    
    for (String path : unRecoverableCounterMap.keySet()) {
      Long unRecoverableCount = unRecoverableCounterMap.get(path);
      Long recoverableCount = recoverableCounterMap.get(path);
      
      // skip the line if no data.
      if ((unRecoverableCount == null || unRecoverableCount <= 0) && 
          (recoverableCount == null || recoverableCount <= 0)) {
        continue;
      }
      
      String unRecoverableDisplay = "";
      if (unRecoverableCount == null || unRecoverableCount <= 0) { 
        unRecoverableDisplay = "0";
      } else {
        StringBuffer url = new StringBuffer("http://" + infoAddr
            + "/raidfsck.jsp");
        url.append("?path=");
        url.append(URLEncoder.encode(path, "UTF-8"));
        url.append("&recoverable=0");
        unRecoverableDisplay = 
            getHTMLLinksText(url.toString(), String.valueOf(unRecoverableCount));
      }
      
      String recoverableDisplay = "";
      if (recoverableCount == null || recoverableCount <= 0) { 
        recoverableDisplay = "0";
      } else {
        StringBuffer url = new StringBuffer("http://" + infoAddr
            + "/raidfsck.jsp");
        url.append("?path=");
        url.append(URLEncoder.encode(path, "UTF-8"));
        url.append("&recoverable=1");
        recoverableDisplay = 
            getHTMLLinksText(url.toString(), String.valueOf(recoverableCount));
      }
      
      htmlSb.append(JspUtils.tr(JspUtils.td(path)
          + JspUtils.td(unRecoverableDisplay) 
          + JspUtils.td(recoverableDisplay)));
    }
    return JspUtils.table(htmlSb.toString());
  }

  public static void generateWarningText(PrintWriter out,
      Map<String, Long> unRecoverableCounterMap,
      Map<String, Long> recoverableCounterMap, RaidNode raidNode) {
    StringBuilder sb = new StringBuilder();
    if (raidNode.getInfoServer() == null)
      return;
    String infoAddr = raidNode.getHostName() + ":"
        + raidNode.getInfoServer().getPort();
    
    // if not data, return
    Long total = 0L;
    for (Long value : unRecoverableCounterMap.values()) {
      total += value;
    }
    for (Long value : recoverableCounterMap.values()) {
      total += value;
    }
    
    if (total <= 0) {
      return;
    }
    
    try {
      sb.append(getHTMLLinksText("http://" + infoAddr + "/missingblocks.jsp",
          "WARNING Corrupt files:"));
      sb.append(generateTable(unRecoverableCounterMap, recoverableCounterMap,
          infoAddr));
      out.print(sb.toString());
    } catch (Exception e) {
      RaidNode.LOG.error(e);
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    ServletContext context = getServletContext();
    RaidNode raidNode = (RaidNode) context.getAttribute("raidnode");
    PrintWriter out = response.getWriter();
    Map<String, Long> unRecoverableCounterMap = raidNode
        .getUnRecoverableFileCounterMap();
    Map<String, Long> recoverableCounterMap = raidNode
        .getRecoverableFileCounterMap();
    String path = request.getParameter(CORRUPT_DIR_KEY);
    if (path == null || path.length() == 0) {
      generateWarningText(out, unRecoverableCounterMap, recoverableCounterMap,
          raidNode);
    } else {
      if (unRecoverableCounterMap.containsKey(path)) {
        out.println(unRecoverableCounterMap.get(path));
      }
    }
  }
}

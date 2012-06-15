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
import java.util.Map;
import java.net.URLEncoder;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class is used in RaidNode's jetty to report the corrupt file counters 
 * to the namenode in the json form
 */
public class CorruptFileCounterServlet extends HttpServlet {
  final String CORRUPT_DIR_KEY = "path";
  
  public static String getHTMLLinksText(String url, String text) {
    return "<a class=\"warning\" href=\"" + url + "\">" 
        + text + "</a>";
  }
  
  public static String generateTable(Map<String, Long> countersMap, String infoAddr) 
      throws UnsupportedEncodingException {
    StringBuilder htmlSb = new StringBuilder();
    htmlSb.append(JspUtils.tr(
        JspUtils.td("Root Directory") +
        JspUtils.td("Files")));
    for (String path : countersMap.keySet()) {
      Long count = countersMap.get(path);
      if (count <= 0) continue;
      StringBuffer url = new StringBuffer(
          "http://"+infoAddr+"/raidfsck.jsp");
      url.append("?path=");
      url.append(URLEncoder.encode(path, "UTF-8"));
      htmlSb.append(JspUtils.tr(
          JspUtils.td(path) +
          JspUtils.td(getHTMLLinksText(url.toString(),
              countersMap.get(path).toString()))));
    } 
    return JspUtils.table(htmlSb.toString());
  }

  public static void generateWarningText(PrintWriter out,
                                         Map<String, Long> countersMap,
                                         RaidNode raidNode) {
    StringBuilder sb = new StringBuilder();
    if (raidNode.getInfoServer() == null)
      return;
    String infoAddr = raidNode.getHostName() + ":" + raidNode.getInfoServer().getPort();
    Long total = 0L;
    for (Long value : countersMap.values()) {
      total += value;
    }
    if (total <= 0) return;
    try {
      sb.append("<a class=\"warning\">WARNING: RAID Unrecoverable corrupt files:</a>");
      sb.append(generateTable(countersMap, infoAddr));
      out.print(sb.toString());
    } catch (Exception e) {
      RaidNode.LOG.error(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    ServletContext context = getServletContext();
    RaidNode raidNode = (RaidNode) context.getAttribute("raidnode");
    PrintWriter out = response.getWriter();
    Map<String, Long> countersMap = raidNode.getCorruptFileCounterMap();
    String path = request.getParameter(CORRUPT_DIR_KEY);
    if (path == null || path.length() == 0) {
      generateWarningText(out, countersMap, raidNode);
    } else {
      if (countersMap.containsKey(path)) {
        out.println(countersMap.get(path));
      }
    }
  }
}

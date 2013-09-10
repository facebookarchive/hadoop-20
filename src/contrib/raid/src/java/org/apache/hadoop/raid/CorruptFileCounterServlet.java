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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.net.URLEncoder;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFile;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFileStatus;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker;
import org.apache.hadoop.raid.RaidHistogram.BlockFixStatus;
import org.apache.hadoop.raid.RaidHistogram.Point;
import org.apache.hadoop.util.InjectionHandler;

/**
 * This class is used in RaidNode's jetty to report the corrupt file counters to
 * the namenode in the json form
 */
public class CorruptFileCounterServlet extends HttpServlet {
  public static final Log LOG = LogFactory.getLog(CorruptFileCounterServlet.class);
  private static final long serialVersionUID = 1L;
  final String CORRUPT_DIR_KEY = "path";
  public static CorruptFileStatus[] columns = new CorruptFileStatus[] {
      CorruptFileStatus.RAID_UNRECOVERABLE,
      CorruptFileStatus.NOT_RAIDED_UNRECOVERABLE,
      CorruptFileStatus.POTENTIALLY_CORRUPT,
      CorruptFileStatus.RECOVERABLE
  };

  public static String getHTMLLinksText(String url, String text) {
    return "<a class=\"warning\" href=\"" + url + "\">" + text + "</a>";
  }
  
  public static String getRecoveryLag(long window,
      TreeMap<Long, BlockFixStatus> countersMap, String path,
      String infoAddr) throws UnsupportedEncodingException {
    BlockFixStatus bfs = countersMap.get(window);
    StringBuilder sb1 = new StringBuilder();
    for (int i = 0; i < bfs.percents.size(); i++) {
      if (i > 0) {
        sb1.append("/");
      }
      if (bfs.percentValues != null && bfs.percentValues[i] >= 0) {
        StringBuffer url = new StringBuffer("http://" + infoAddr
            + "/corruptfilecounter");
        url.append("?root=");
        url.append(URLEncoder.encode(path, "UTF-8"));
        url.append("&recoverytime=" + bfs.percentValues[i]);
        sb1.append(JspUtils.linkWithColor(String.valueOf(bfs.percentValues[i]), i,
                url.toString()));
      } else {
        sb1.append("-");
      }
    }
    return format(window) + " " + sb1.toString();
  }
  
  public static String getFailedFiles(long window,
      TreeMap<Long, BlockFixStatus> countersMap, String path, 
      String infoAddr) throws UnsupportedEncodingException {
    BlockFixStatus bfs = countersMap.get(window);
    String counterDisplay = "";
    if (bfs.failedPaths <= 0) { 
      counterDisplay = "0";
    } else {
      StringBuffer url = new StringBuffer("http://" + infoAddr
          + "/corruptfilecounter");
      url.append("?root=");
      url.append(URLEncoder.encode(path, "UTF-8"));
      url.append("&recoverytime=" + RaidHistogram.RECOVERY_FAIL);
      counterDisplay = 
          getHTMLLinksText(url.toString(), String.valueOf(bfs.failedPaths));
    }
    return counterDisplay;
  }
  
  public static String getPercentHeader(RaidNode raidNode) {
    String[] percentStrs = raidNode.getBlockIntegrityMonitor().getPercentStrs();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < percentStrs.length; i++) {
      if (i > 0) {
        sb.append("/");
      }
      if (percentStrs[i].equals("0")) {
        sb.append(JspUtils.color(i, "min"));
      } else if (percentStrs[i].equals("100")) {
        sb.append(JspUtils.color(i, "max"));
      } else { 
        sb.append(JspUtils.color(i, percentStrs[i]));
      }
    }
    return sb.toString();
  }

  public static String generateTable(
      Map<String, Map<CorruptFileStatus, Long>> corruptFilesCounterMap,
      String infoAddr, double numDetectionsPerSec, RaidNode raidNode)
          throws UnsupportedEncodingException, IOException {
    StringBuilder htmlSb = new StringBuilder();
    int imageSize = 30;
    htmlSb.append(JspUtils.tr(JspUtils.td("Root Directory")
        + JspUtils.td(JspUtils.image(raidNode, "RURF.jpg",
            imageSize, imageSize), "Raid Unrecoverable Files")
        + JspUtils.td(JspUtils.image(raidNode, "NRURF.jpg",
            imageSize, imageSize), "Not-Raid Unrecoverable Files")
        + JspUtils.td(JspUtils.image(raidNode, "PURF.jpg",
            imageSize, imageSize), "Potential Unrecoverable Files")
        + JspUtils.td(JspUtils.image(raidNode, "RF.jpg",
            imageSize, imageSize), "Recoverable Files")
        + JspUtils.td(JspUtils.image(raidNode, "DL.jpg",
            imageSize, imageSize), "Detection Lag(s)")
        + JspUtils.td(JspUtils.image(raidNode, "RL.jpg",
            imageSize, imageSize) + " " + getPercentHeader(raidNode),
            "Recovery Lag(s)")
        + JspUtils.td(JspUtils.image(raidNode, "RFF.jpg",
            imageSize, imageSize), "Recovery Failed Files")));
    for (String path : corruptFilesCounterMap.keySet()) {
      Map<CorruptFileStatus, Long> counters = corruptFilesCounterMap.get(path);
      StringBuilder oneRow = new StringBuilder();
      TreeMap<Long, BlockFixStatus> countersMap = 
          raidNode.getBlockIntegrityMonitor().getBlockFixStatus(path, System.currentTimeMillis());
      int windowSize = countersMap.keySet().size();
      oneRow.append(JspUtils.th(windowSize, path));
      // Append corrupt file counters
      for (CorruptFileStatus cfs: columns) {
        Long count = counters.get(cfs);
        String counterDisplay = "";
        if (count == null || count <= 0) { 
          counterDisplay = "0";
        } else {
          StringBuffer url = new StringBuffer("http://" + infoAddr
              + "/corruptfilecounter");
          url.append("?root=");
          url.append(URLEncoder.encode(path, "UTF-8"));
          url.append("&status=");
          url.append(URLEncoder.encode(cfs.name(), "UTF-8"));
          counterDisplay = 
              getHTMLLinksText(url.toString(), String.valueOf(count));
        }
        oneRow.append(JspUtils.th(windowSize, counterDisplay)); 
      }
      // Append detection lag
      Long potentialCorruptFiles = counters.get(CorruptFileStatus.POTENTIALLY_CORRUPT);
      String detectionLag = "";
      if (potentialCorruptFiles == null || potentialCorruptFiles <= 0) {
        detectionLag = "0";
      } else if (numDetectionsPerSec < 1e-6) {
        detectionLag = "-";
      } else {
        long costTime = (long)Math.ceil((double)potentialCorruptFiles/
            numDetectionsPerSec);
        detectionLag = Long.toString(costTime);
      }
      oneRow.append(JspUtils.th(windowSize, detectionLag));
      oneRow.append(JspUtils.tdWithClass(getRecoveryLag(countersMap.firstKey(),
                                         countersMap, path, infoAddr),
                                         JspUtils.SMALL_CELL));
      oneRow.append(JspUtils.tdWithClass(getFailedFiles(countersMap.firstKey(),
                                         countersMap, path, infoAddr),
                                         JspUtils.SMALL_CELL));
      htmlSb.append(JspUtils.tr(oneRow.toString()));
      // Append recovery lags 
      boolean head = true;
      for (Long window: countersMap.keySet()) {
        if (head) {
          head = false;
          continue;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(JspUtils.tdWithClass(getRecoveryLag(window, countersMap,
                                       path, infoAddr),
                                       JspUtils.SMALL_CELL));
        sb.append(JspUtils.tdWithClass(getFailedFiles(window, countersMap,
            path, infoAddr), JspUtils.SMALL_CELL));
        htmlSb.append(JspUtils.tr(sb.toString()));
      }
    }
    return JspUtils.smallTable(htmlSb.toString());
  }
  
  public static String format(long msec) { 
    long mins = msec / 60 / 1000;
    long hours = mins / 60;
    mins %= 60;
    long days = hours / 24;
    hours %= 24;
    long weeks = days / 7;
    days %= 7;
    StringBuilder result = new StringBuilder();
    if (weeks > 0) result.append(weeks + "weeks ");
    if (days > 0) result.append(days + "days ");
    if (hours > 0) result.append(hours + "hours ");
    if (mins > 0) result.append(mins + "mins ");
    return result.toString().trim();
  }

  public static void generateWarningText(PrintWriter out,
      Map<String, Map<CorruptFileStatus, Long>> corruptFilesCounterMap,
      RaidNode raidNode) {
    StringBuilder sb = new StringBuilder();
    if (raidNode.getInfoServer() == null)
      return;
    String infoAddr = raidNode.getHostName() + ":"
        + raidNode.getInfoServer().getPort();
    try {
      sb.append(getHTMLLinksText("http://" + infoAddr + "/missingblocks.jsp",
          "WARNING Corrupt files:"));
      sb.append(generateTable(corruptFilesCounterMap, infoAddr,
          raidNode.getNumDetectionsPerSec(), raidNode));
      out.print(sb.toString());
    } catch (Exception e) {
      LOG.error("Get exception in generateWarningText", e);
    }
  }
  
  public static void generateFilesContent(PrintWriter out,
      String monitorDir, long recoveryTime, RaidNode raidNode) {
    RaidHistogram histogram = 
        raidNode.getBlockIntegrityMonitor().getRecoveryTimes().get(monitorDir);
    if (histogram == null) {
      return;
    }
    ArrayList<Point> points = histogram.getPointsWithGivenRecoveryTime(
        recoveryTime);
    Collections.sort(points);
    generateBlockFixJob(out, points, raidNode); 
  }
  
  public static String getTrackingUrl(String taskId, RaidNode raidNode) {
    JobID jobId = TaskAttemptID.forName(taskId).getJobID();
    return ((Worker)raidNode.blockIntegrityMonitor.getCorruptionMonitor()).getTrackingUrl(
        jobId);
  }
  
  public static void generateBlockFixJob(PrintWriter out, ArrayList<Point> points,
      RaidNode raidNode) {
    out.println(points.size() + " records in total<br>");
    StringBuilder htmlSb = new StringBuilder();
    htmlSb.append(JspUtils.tr(
        JspUtils.td("Time Since <br> Update") + 
        JspUtils.td("Path") + 
        JspUtils.td("Job")));
    for (Point p : points) {
      String jobCell = "N/A";
      if (p.taskId != null) {
        String trackingUrl = getTrackingUrl(p.taskId, raidNode);
        jobCell = JspUtils.link(p.taskId, trackingUrl);
      }
      htmlSb.append(JspUtils.tr(
          JspUtils.td(getTimeToNow(p.time)) +
          JspUtils.td(p.path) + 
          JspUtils.td(jobCell)));
    }
    out.println(JspUtils.table(htmlSb.toString()));
  }
  
  public static String getTimeToNow(long detecTime) {
    long currentTime = System.currentTimeMillis();
    long hoursSinceCorrupt = (currentTime - detecTime)/3600000;
    long remainderMinutes = ((currentTime - detecTime)/60000) % 60;
    return (detecTime > 0L) ?
        hoursSinceCorrupt + " hrs " + remainderMinutes + " mins":
        "now";
  }
  
  public static class CorruptFileComapare implements Comparator<CorruptFile> {
    public static final int
      FIELD_TIME_SINCE_BLOCK_MISSING = 1,
      FIELD_PATH                     = 2,
      FIELD_NUM_CORRUPT_BLOCKS       = 3, 
      SORT_ORDER_ASC          = 1,
      SORT_ORDER_DSC          = 2;
    int sortField = FIELD_TIME_SINCE_BLOCK_MISSING;
    int sortOrder = SORT_ORDER_DSC;

    public CorruptFileComapare(String field, String order) {
      if (field.equals("timesinceblockmissing")) {
        sortField = FIELD_TIME_SINCE_BLOCK_MISSING;
      } else if (field.equals("path")) {
        sortField = FIELD_PATH;
      } else if (field.equals("numcorruptblocks")) {
        sortField = FIELD_NUM_CORRUPT_BLOCKS;
      }
      if (order.equals("DSC")) {
        sortOrder = SORT_ORDER_DSC;
      } else {
        sortOrder = SORT_ORDER_ASC;
      }
    }

    public int compare(CorruptFile c1, CorruptFile c2) {
      int ret = 0;
      if (c2 == null) {
        ret = -1;
      } else {
        switch (sortField) {
          case FIELD_TIME_SINCE_BLOCK_MISSING:
            ret = (int) (c2.detectTime - c1.detectTime);
            if (ret == 0) {
              ret = c1.path.compareTo(c2.path);
            }
            break;
          case FIELD_PATH:
            ret = c1.path.compareTo(c2.path);
            break;
          case FIELD_NUM_CORRUPT_BLOCKS:
            ret = c1.numCorrupt - c2.numCorrupt;
            if (ret == 0) {
              ret = c1.path.compareTo(c2.path);
            }
            break;
        }
      }
      return (sortOrder == SORT_ORDER_DSC) ? -ret : ret;
    }
  }
  
  public static String NodeHeaderStr(String name, String monitorDir,
      String status, String sortField, String sortOrder) {
    String ret = "class=header";
    String order = "ASC";
    if (name.equals(sortField) ) {
      ret += sortOrder;
      if ( sortOrder.equals("ASC") )
        order = "DSC";
    }
    ret += " onClick=\"window.document.location=" +
           "'corruptfilecounter?root="+monitorDir+"&status="+status+
           "&sorter/field=" + name + "&sorter/order=" +
    order + "'\" title=\"sort on this column\"";
    return ret;
  }
  
  public static void generateFileStatus(PrintWriter out, String monitorDir, 
      String status, RaidNode raidNode, String field, String order) {
    CorruptFileStatus matched = null;
    for (CorruptFileStatus cfs: CorruptFileStatus.values()) {
      if (cfs.name().equals(status)) {
        matched = cfs;
        break;
      }
    }
    if (matched == null) {
      return;
    }
    ArrayList<CorruptFile> corruptFiles =
        raidNode.getCorruptFileList(monitorDir, matched);
    if (field == null) {
      field = "timesinceblockmissing";
    }
    if (order == null) {
      order = "DSC";
    }
    Collections.sort(corruptFiles, new CorruptFileComapare(field, order));
    out.println("<style> th:hover{text-decoration:underline;cursor:hand;cursor:pointer;}</style>");
    out.println( "<div id=\"dfsnodetable\"> ");
    out.println("<br> <a name=\"CorruptFiles\" id=\"title\"> " +
        corruptFiles.size() + " files in total" + "</a><br><br>");
    StringBuilder htmlSb = new StringBuilder();
    htmlSb.append( "<tr class=\"headerRow\"> <th " +
        NodeHeaderStr("timesinceblockmissing", monitorDir, status, field, order) +
        "> Time Since<br>Block Missing <th " + 
        NodeHeaderStr("numcorruptblocks", monitorDir, status, field, order) + 
        "> Number Of <br> Corrupt Blocks <th " + 
        NodeHeaderStr("path", monitorDir, status, field, order) +
        "> Path \n");
    for (CorruptFile cf: corruptFiles) {
      htmlSb.append(
          JspUtils.tr(
              JspUtils.tdWithClass(getTimeToNow(cf.detectTime),
                  "timesinceblockmissing") +  
              JspUtils.tdWithClass(Integer.toString(cf.numCorrupt), "numcorruptblocks") + 
              JspUtils.tdWithClass(cf.path, "path")));
    }
    out.println(JspUtils.table(htmlSb.toString()));
    out.println("</div>");
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    ServletContext context = getServletContext();
    RaidNode raidNode = (RaidNode) context.getAttribute("raidnode");
    PrintWriter out = response.getWriter();
    Map<String, Map<CorruptFileStatus, Long>> corruptFilesCounterMap = raidNode
        .getCorruptFilesCounterMap();
    String path = request.getParameter(CORRUPT_DIR_KEY);
    String sorterField = request.getParameter("sorter/field");
    String sorterOrder = request.getParameter("sorter/order");
    if (path == null || path.length() == 0) {
      String monitorDir = request.getParameter("root");
      if (monitorDir == null || monitorDir.length() == 0) {
        generateWarningText(out, corruptFilesCounterMap, raidNode);
      } else {
        String status = request.getParameter("status");
        if (status == null || status.length() == 0) {
          String recoveryTime = request.getParameter("recoverytime");
          if (recoveryTime == null || recoveryTime.length() == 0) {
            generateWarningText(out, corruptFilesCounterMap, raidNode);
          } else {
            generateFilesContent(out, monitorDir, Long.parseLong(recoveryTime), raidNode);
          }
        } else {
          generateFileStatus(out, monitorDir, status, raidNode,
              sorterField, sorterOrder);
        }
      }
    } else {
      if (corruptFilesCounterMap.containsKey(path)) {
        out.println(corruptFilesCounterMap.get(path).get(
                        CorruptFileStatus.RAID_UNRECOVERABLE) + 
                    corruptFilesCounterMap.get(path).get(
                        CorruptFileStatus.NOT_RAIDED_UNRECOVERABLE));
      }
    }
    InjectionHandler.processEventIO(InjectionEvent.RAID_HTTPSERVER_TIMEOUT);
  }
}

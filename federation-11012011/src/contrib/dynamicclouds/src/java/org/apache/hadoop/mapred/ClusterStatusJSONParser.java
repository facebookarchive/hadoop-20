package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.mortbay.util.ajax.JSON;

public class ClusterStatusJSONParser {

  private static Object getJSONObject(URL jsonURL)
          throws IOException {
    URLConnection conn = jsonURL.openConnection();
    BufferedReader reader = new BufferedReader(new InputStreamReader(
            conn.getInputStream()));
    StringBuffer buffer = new StringBuffer();
    char[] buff = new char[4096];
    int read = 0;
    while((read = reader.read(buff)) > 0) {
      buffer.append(buff, 0, read);
    }
    DynamicCloudsDaemon.LOG.info("Read " + buffer.length() + " bytes");
    Object res = JSON.parse(buffer.toString());
    return res;
  }

  public static Map<String, String> getClusterConf(String clusterAddress)
          throws IOException {
    URL jobTrackerStatusJsp = new URL(clusterAddress +
            "/jobtrackersdetailsjson.jsp?jobTrackerConf=1");

    Map<String, String> confFilesLocation =
            (Map<String, String>) getJSONObject(jobTrackerStatusJsp);

    return confFilesLocation;
  }

  public static List<TaskTrackerLoadInfo> getJobTrackerStatus(
          String jobTrackerUrl) throws IOException {

    List<TaskTrackerLoadInfo> result = new ArrayList<TaskTrackerLoadInfo>();

    URL jobTrackerStatusJsp = new URL(jobTrackerUrl +
            "/jobtrackersdetailsjson.jsp?status=1");
    Map<String, Object> trackersStatus =
            (Map<String, Object>) getJSONObject(jobTrackerStatusJsp);
    for (String taskTrackerName : trackersStatus.keySet()) {
      Map<String, Object> trackerInfo =
              (Map<String, Object>) trackersStatus.get(taskTrackerName);

      TaskTrackerLoadInfo status = new TaskTrackerLoadInfo(taskTrackerName);
      status.parseMap(trackerInfo);

      result.add(status);
    }
    return result;
  }
}

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
package org.apache.hadoop.raid;

/**
 * Methods that creates jsp pages
 */
public class JspUtils {
  public static String[] COLORS = new String[] {
    "indigo", "green", "olive", "red", "blue"
  };
  public static String SMALL_CELL = "smallcell";

  private JspUtils() {};

  public static String tr(String s) {
    return "<tr>" + s + "</tr>\n";
  }
  public static String td(String s) {
    return "<td>" + s + "</td>";
  }
  
  public static String tdWithClass(String s, String classId) {
    return "<td class=\"" + classId + "\">" + s + "</td>";
  }
  
  public static String td(String s, String title) {
    return "<td title=\'" + title + "\'>" + s + "</td>"; 
  }
  
  public static String th(int rowSpan, String s) {
    return "<th rowspan=\"" + rowSpan + "\">" + s + "</th>";
  }
  public static String table(String s) {
    return "<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n" +
        s + "</table>";
  }
  public static String smallTable(String s) {
    return "<table border=\"1\" cellpadding=\"0\" cellspacing=\"0\">\n" +
           "<style> ." + SMALL_CELL +  " {" +
           "font-size:12px; text-align:center; font-weight: bolder;" +
           "}</style>\n" + 
           s + "</table>";
  }
  public static String tableSimple(String s) {
    return "<table>\n" + s + "</table>";
  }
  
  public static String linkWithColor(String s, int index, String url) {
    if (url == null || url == "") {
      return s;
    }
    return "<a href=" + url + " style=\"color:" + COLORS[index % COLORS.length]
        + "\">" + s + "</a>";
  }
  
  public static String link(String s, String url) {
    if (url == null || url == "") {
      return s;
    }
    return "<a href=" + url + ">" + s + "</a>";
  }

  public static String color(int index, String s) {
    return "<span style=\"color:" + COLORS[index % COLORS.length]
        + "\">" + s + "</span>";
  }
  
  public static String image(RaidNode raidNode, String imageName,
      int width, int height) {
    return "<img src=\"http://" + raidNode.getHostName() + ":" + 
        raidNode.getInfoServer().getPort() + "/static/" + imageName +
        "\" width=\"" + width + "\" height=\"" + height + "\" />";
  }
}

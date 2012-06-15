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

  private JspUtils() {};

  public static String tr(String s) {
    return "<tr>" + s + "</tr>\n";
  }
  public static String td(String s) {
    return "<td>" + s + "</td>";
  }
  public static String table(String s) {
    return "<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n" +
        s + "</table>";
  }
  public static String tableSimple(String s) {
    return "<table>\n" + s + "</table>";
  }
  public static String link(String s, String url) {
    if (url == null || url == "") {
      return s;
    }
    return "<a href=" + url + ">" + s + "</a>";
  }


}

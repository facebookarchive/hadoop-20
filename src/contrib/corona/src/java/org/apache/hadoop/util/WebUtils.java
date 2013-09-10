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

package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.corona.PoolInfo;
import org.apache.hadoop.corona.ResourceType;

/**
 * Helper static utils for the jsp pages
 */
public class WebUtils {
  /**
   * True if should be shown for this filtering.
   * @param user User to check
   * @param userFilterSet Set of valid users
   * @param poolInfo Pool info to check
   * @param poolGroupFilterSet Set of valid pool groups
   * @param poolInfoFilterSet Set of valid pool infos
   * @return True if should be shown.
   */
  public static boolean showUserPoolInfo(
      String user,
      Set<String> userFilterSet, PoolInfo poolInfo,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet) {
    boolean showUser = false;
    if (userFilterSet.isEmpty() || userFilterSet.contains(user)) {
      showUser = true;
    }

    return showUser &&
        showPoolInfo(poolInfo, poolGroupFilterSet, poolInfoFilterSet);
  }

  /**
   * True if should be shown for this filtering.
   * @param poolInfo Pool info to check
   * @param poolGroupFilterSet Set of valid pool groups
   * @param poolInfoFilterSet Set of valid pool infos
   * @return True if should be shown.
   */
  public static boolean showPoolInfo(
      PoolInfo poolInfo,
      Set<String> poolGroupFilterSet,
      Set<PoolInfo> poolInfoFilterSet) {
    // If there are no filters, show everything
    if (poolGroupFilterSet.isEmpty() && poolInfoFilterSet.isEmpty()) {
      return true;
    }

    if (poolGroupFilterSet.contains(poolInfo.getPoolGroupName())) {
      return true;
    } else if (poolInfoFilterSet.contains(poolInfo)) {
      return true;
    }

    return false;
  }

  /**
   * Convert resource types of a collection of String objects
   * @param resourceTypes Collection of types to convert
   * @return Collection of resource types
   */
  public static Collection<String> convertResourceTypesToStrings(
      Collection<ResourceType> resourceTypes) {
    List<String> retList = new ArrayList<String>(resourceTypes.size());
    for (ResourceType resourceType : resourceTypes) {
      retList.add(resourceType.toString());
    }

    return retList;
  }

  /**
   * Helper class to store/retrieve the parameter filters
   */
  public static class JspParameterFilters {
    /** Users allowed, null for all */
    private final Set<String> userFilterSet = new HashSet<String>();
    /** Pool groups allowed, null for all */
    private final Set<String> poolGroupFilterSet = new HashSet<String>();
    /** Pool infos  allowed, null for all */
    private final Set<PoolInfo> poolInfoFilterSet = new HashSet<PoolInfo>();
    /** HTML output */
    private final StringBuilder htmlOutput = new StringBuilder();

    public Set<String> getUserFilterSet() {
      return userFilterSet;
    }

    public Set<String> getPoolGroupFilterSet() {
      return poolGroupFilterSet;
    }

    public Set<PoolInfo> getPoolInfoFilterSet() {
      return poolInfoFilterSet;
    }

    public StringBuilder getHtmlOutput() {
      return htmlOutput;
    }
  }

  /**
   * Check the attribute names
   * @param attributeNames Atttribute names to check
   * @return Null if all attribute names are invalid, non-empty if the check
   *         failed, with an appropriate error message
   */
  public static String validateAttributeNames(
      Enumeration<String> attributeNames) {
    while (attributeNames.hasMoreElements()) {
      String attribute = attributeNames.nextElement();
      if (!attribute.equals("users") && !attribute.equals("poolGroups") &&
          !attribute.equals("poolInfos") && !attribute.equals("toKillSessionId")
          && !attribute.equals("killSessionsToken")) {
        return 
          "Illegal parameter " + attribute + ", only 'users, " +
          "poolGroups, 'poolInfos', 'toKillSessionId' and 'killSessionsToken'" +
          "parameters allowed.";
      }
    }
    return null;
  }
  
 
  private static final String[] VALID_TOKENS = {"prism"};
  /**
   * Check if the kill sessions token is a valid one
   * @param token The value of user supplied token
   * @return true for valid, false for invalid
   */
  public static boolean isValidKillSessionsToken(String token) {
    if (token == null || token.isEmpty()) {
      return false;
    }
    
    for (String validToken:VALID_TOKENS) {
      if (token.equals (validToken)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Convert the parameters to filters and html output
   * @param userFilter User filter parameter
   * @param poolGroupFilter Pool group filter parameter
   * @param poolInfoFilter Pool info filter parameter
   * @return Filters
   */
  public static JspParameterFilters getJspParameterFilters(
      String userFilter,
      String poolGroupFilter,
      String poolInfoFilter) {
    JspParameterFilters filters = new JspParameterFilters();
    if (userFilter != null && !userFilter.equals("null")) {
      filters.getUserFilterSet().addAll(Arrays.asList(userFilter.split(",")));
      filters.getHtmlOutput().append(("<b>users:</b> " + userFilter + "<br>"));
    }
    if (poolGroupFilter != null && !poolGroupFilter.equals("null")) {
      filters.getPoolGroupFilterSet().addAll(
          Arrays.asList(poolGroupFilter.split(",")));
      filters.getHtmlOutput().append("<b>poolGroups:</b> " + poolGroupFilter +
          "<br>");
    }
    if (poolInfoFilter != null && !poolInfoFilter.equals("null")) {
      filters.getHtmlOutput().append(
          "<b>poolInfos:</b> " + poolInfoFilter + "<br>");
      for (String poolInfoString : poolInfoFilter.split(",")) {
        String[] poolInfoStrings = poolInfoString.split("[.]");
        if (poolInfoStrings.length == 2) {
          filters.getPoolInfoFilterSet().add(new PoolInfo(poolInfoStrings[0],
              poolInfoStrings[1]));
        }
      }
    }
    return filters;
  }

  /**
   * Pool info cell html
   */
  public static class PoolInfoHtml {
    private final String groupHtml;
    private final String poolHtml;

    public PoolInfoHtml(String groupHtml, String poolHtml) {
      this.groupHtml = groupHtml;
      this.poolHtml = poolHtml;
    }

    public String getGroupHtml() {
      return groupHtml;
    }

    public String getPoolHtml() {
      return poolHtml;
    }
  }

  /**
   * Generate the appropriate HTML for pool name (redirected info if necessary)
   * @param redirects
   * @param poolInfo
   * @return Pair of group and pool html
   */
  public static PoolInfoHtml getPoolInfoHtml(
      Map<PoolInfo, PoolInfo> redirects,
      PoolInfo poolInfo) {
    String redirectAttributes = null;
    if (redirects != null) {
      PoolInfo destination = redirects.get(poolInfo);
      if (destination != null) {
        redirectAttributes = "Redirected to " +
            PoolInfo.createStringFromPoolInfo(destination);
      }
    }

    String spanTag = (redirectAttributes != null) ?
        "<span class=\"ui-state-disabled\" title=\"" + redirectAttributes +
        "\">" : "<span>";
    String groupHtml = spanTag + poolInfo.getPoolGroupName() + "</span>";
    String poolHtml = spanTag +
        (poolInfo.getPoolName() == null ? "-" : poolInfo.getPoolName())
        + "</span>";

    return new PoolInfoHtml(groupHtml, poolHtml);
  }
}

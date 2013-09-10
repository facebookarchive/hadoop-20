<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.corona.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.util.StringUtils"
%>

<%!
  public boolean showUserPoolInfo(String user,
      Set<String> userFilterSet, PoolInfo poolInfo,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet) {
    boolean showUser = false;
    if (userFilterSet.isEmpty() || userFilterSet.contains(user)) {
      showUser = true;
    }

    return showUser &&
        showPoolInfo(poolInfo, poolGroupFilterSet, poolInfoFilterSet);
  }

  public boolean showPoolInfo(PoolInfo poolInfo,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet) {
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

  public Collection<String> convertResourceTypesToStrings(
      Collection<ResourceType> resourceTypes) {
    List<String> retList = new ArrayList<String>(resourceTypes.size());
    for (ResourceType resourceType : resourceTypes) {
      retList.add(resourceType.toString());
    }

    return retList;
  }

  public void generateSummaryTable(JspWriter out, NodeManager nm,
      SessionManager sm)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<table id=\"summaryTable\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    // populate header row with list of resource types
    sb.append("<thead><tr>");

    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String[] perTypeColumns =
        { "Running", "Waiting", "TotalSlots", "FreeSlots" };

    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes),
        perTypeColumns.length, 0, false));
    sb.append("<th colspan=3>Nodes</th>");
    sb.append("<th colspan=1>Session</th>");
    sb.append("</tr>\n");
    out.print(sb.toString());

    String typeColHeader =
        "<th>"
            + org.apache.commons.lang.StringUtils.join(perTypeColumns,
                "</th><th>") + "</th>";
    StringBuilder row = new StringBuilder("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      row.append(typeColHeader);
    }

    // Nodes
    row.append("<th>Alive</th>");
    row.append("<th>Blacklisted</th>");
    row.append("<th>Excluded</th>");
    // Session
    row.append("<th>Running</th></tr></thead>\n");
    out.print(row.toString());

    row = new StringBuilder("<tbody><tr>");

    for (ResourceType resourceType : resourceTypes) {
      int waiting = sm.getPendingRequestCountForType(resourceType);
      int running = sm.getGrantCountForType(resourceType);
      int totalslots = nm.getMaxCpuForType(resourceType);
      int freeslots = totalslots - nm.getAllocatedCpuForType(resourceType);
      row.append("<td>" + running + "</td>");
      row.append("<td>" + waiting + "</td>");
      row.append("<td>" + totalslots + "</td>");
      row.append("<td>" + freeslots + "</td>");
    }

    row.append("<td><a href=\"machines.jsp?type=alive\">" +
        nm.getAliveNodeCount() + "</a></td>");
    FaultManager fm = nm.getFaultManager();
    row.append("<td><a href=\"machines.jsp?type=blacklisted\">" +
        fm.getBlacklistedNodeCount() + "</a></td>");
    row.append("<td><a href=\"machines.jsp?type=excluded\">" +
        nm.getExcludedNodeCount() + "</a></td>");
    row.append("<td>" + sm.getRunningSessionCount() + "</td>");
    row.append("</tr></tbody>\n");
    row.append("</table>\n");

    out.print(row.toString());
    out.print("<br>");
  }

  public void generateActiveSessionTable(
      JspWriter out, NodeManager nm, SessionManager sm, Scheduler scheduler,
      Set<String> userFilterSet,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet,
      DateFormat dateFormat)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<table id=\"activeTable\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    // populate header row
    sb.append("<thead><tr>");

    // fixed headers
    String[] fixedHeaders = { "Id", "Start Time", "Name", "User", "Pool Group",
            "Pool", "Priority" };
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 2, false));

    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String[] perTypeColumns = { "Running", "Waiting", "Total" };
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes),
        perTypeColumns.length, 0, false));

    sb.append("</tr>\n");

    // populate sub-header row
    sb.append("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      for (int i = 0; i < perTypeColumns.length; i++)
        sb.append("<th>" + perTypeColumns[i] + "</th>");
    }
    sb.append("</tr></thead><tbody>\n");

    for (String id : sm.getSessions()) {
      Session s;
      try {
        s = sm.getSession(id);
      } catch (InvalidSessionHandle e) {
        continue;
      }

      synchronized (s) {
        if (s.isDeleted())
          continue;

        String url = (s.getUrl() == null || s.getUrl().length() == 0) ? id :
            "<a href=\"" + s.getUrl() + "\">" + id + "</a>";
        PoolInfo poolInfo = s.getPoolInfo();

        if (!showUserPoolInfo(s.getUserId(), userFilterSet,
            poolInfo, poolGroupFilterSet, poolInfoFilterSet)) {
          continue;
        }

        sb.append("<tr><td>" + url + "</td><td>" +
            dateFormat.format(new Date(s.getStartTime())) +
            "</td><td><a href=\"jobresources.jsp?id=" + id + "\">" +
            s.getName() + "</a></td><td>"  + s.getUserId() + "</td><td>" +
            poolInfo.getPoolGroupName() + "</td><td>" + poolInfo.getPoolName() +
            "</td><td>" + SessionPriority.findByValue(s.getPriority()) +
            "</td>");
        for (ResourceType resourceType : resourceTypes) {
          int total = s.getRequestCountForType(resourceType);
          int waiting = s.getPendingRequestForType(resourceType).size();
          int running = s.getGrantedRequestForType(resourceType).size();
          sb.append("<td>" + running + "</td><td>" + waiting + "</td><td>"
              + total + "</td>");
        }
        sb.append("</tr>\n");
      }
    }
    out.print(sb.toString());
    out.print("</tbody></table><br>");
  }

  private String getPoolInfoTableData(Map<PoolInfo, PoolInfo> redirects,
                                      PoolInfo poolInfo) {
    StringBuffer sb = new StringBuffer();
    String redirectAttributes = "";
    if (redirects != null) {
      PoolInfo destination = redirects.get(poolInfo);
      if (destination != null) {
        redirectAttributes = " title=\"Redirected to " +
            PoolInfo.createStringFromPoolInfo(destination) +
            "\" class=\"ui-state-disabled\"";
      }
    }

    sb.append("<td" + redirectAttributes + ">" + poolInfo.getPoolGroupName() +
        "</td>");
    sb.append("<td" + redirectAttributes + ">" +
        (poolInfo.getPoolName() == null ? "-" : poolInfo.getPoolName()) +
        "</td>");
    return sb.toString();
  }

  public void generatePoolTable(
      JspWriter out, Scheduler scheduler, Collection<ResourceType> types,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet)
      throws IOException {
    List<String> metricsNames = new ArrayList<String>();
    for (PoolInfoMetrics.MetricName name :
        PoolInfoMetrics.MetricName.values()) {
      metricsNames.add(name.toString());
    }
    StringBuilder sb = new StringBuilder();
    sb.append("<table id=\"poolTable\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    ConfigManager configManager = scheduler.getConfigManager();
    Map<PoolInfo, PoolInfo> redirects = configManager.getRedirects();

    // Generate headers
    sb.append("<thead><tr>");
    sb.append("<th rowspan=2>Pool Group</th>");
    sb.append("<th rowspan=2>Pool</th>");
    sb.append("<th rowspan=2>Scheduling</th>");
    sb.append("<th rowspan=2>Preemptable</th>");
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(types),
        metricsNames.size(), 0, false));
    sb.append("</tr>\n");

    sb.append("<tr>");
    for (int i = 0; i < types.size(); ++i) {
      for (String name : metricsNames) {
        sb.append("<th>" + name + "</th>");
      }
    }
    sb.append("</tr></thead><tbody>\n");

    // Initialize the total metrics
    int totalEntries =
      PoolInfoMetrics.MetricName.values().length * types.size();
    List<Long> totalMetrics = new ArrayList<Long>(totalEntries);
    for (int i = 0; i < totalEntries; ++i) {
      totalMetrics.add(new Long(0));
    }
    for (PoolInfo poolInfo : scheduler.getPoolInfos()) {
      if (!showPoolInfo(poolInfo, poolGroupFilterSet, poolInfoFilterSet)) {
        continue;
      }
      sb.append("<tr>");
      sb.append(getPoolInfoTableData(redirects, poolInfo));
      sb.append("<td>" + configManager.getPoolComparator(poolInfo) + "</td>");
      sb.append("<td>" + configManager.isPoolPreemptable(poolInfo) + "</td>");

      int metricsIndex = 0;
      for (ResourceType type : types) {
        Map<PoolInfo, PoolInfoMetrics> poolInfoMetrics =
            scheduler.getPoolInfoMetrics(type);
        PoolInfoMetrics metric = poolInfoMetrics.get(poolInfo);
        for (PoolInfoMetrics.MetricName metricsName :
            PoolInfoMetrics.MetricName.values()) {
          Long val = null;
          if (metric != null) {
            val = metric.getCounter(metricsName);
          }
          if (val == null) {
            val = 0L;
          }
          if (metricsName == PoolInfoMetrics.MetricName.MAX &&
              val == Integer.MAX_VALUE) {
            sb.append("<td>-</td>");

          } else {
            sb.append("<td>" + val + "</td>");
            // Only add the pool infos, not pool groups metrics.
            if (poolInfo.getPoolName() != null) {
              totalMetrics.set(metricsIndex,
                (totalMetrics.get(metricsIndex) + val));
            }
          }
          ++metricsIndex;
        }
      }
      sb.append("</tr>\n");
    }
    // Add totals row
    sb.append("<tr><td>all pool groups</td><td>all pools</td>" +
        "<td>-</td><td>-</td>");
    for (Long metricsValue : totalMetrics) {
      sb.append("<td>" + metricsValue + "</td>");
    }
    sb.append("</tbody></table><br>");
    out.print(sb.toString());
  }

  private String generateTableHeaderCells(Collection<String> headers,
      int colspan, int rowspan,
      boolean useTableData) {
    StringBuilder sb = new StringBuilder();
    String tag = useTableData ? "td" : "th";
    StringBuilder joinFrag = new StringBuilder("<" + tag);
    if (colspan > 0) {
      joinFrag.append(" colspan=" + colspan);
    }
    if (rowspan > 0) {
      joinFrag.append(" rowspan=" + rowspan);
    }
    joinFrag.append(">");
    sb.append(joinFrag);
    sb.append(org.apache.commons.lang.StringUtils.join(headers, "</" + tag
        + ">" + joinFrag));
    sb.append("</" + tag + ">");
    return sb.toString();
  }

  public void generateRetiredSessionTable(JspWriter out, NodeManager nm,
      SessionManager sm, Set<String> userFilterSet,
      Set<String> poolGroupFilterSet, Set<PoolInfo> poolInfoFilterSet,
      DateFormat dateFormat)
      throws IOException {

    StringBuilder sb = new StringBuilder();
    sb.append("<table id=\"retiredTable\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    // populate header row
    sb.append("<thead><tr>");

    // fixed headers
    String[] fixedHeaders = { "Id", "Start Time", "Finished Time",
        "Name", "User", "Pool Group", "Pool", "Priority", "Status"};
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 0, false));

    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes), 1, 0, false));
    sb.append("</tr></thead><tbody>\n");

    Collection<RetiredSession> retiredSessions = sm.getRetiredSessions();
    synchronized (retiredSessions) {
      for (RetiredSession s : retiredSessions) {
        PoolInfo poolInfo = s.getPoolInfo();
        if (!showUserPoolInfo(s.getUserId(), userFilterSet,
          poolInfo, poolGroupFilterSet, poolInfoFilterSet)) {
          continue;
        }

        // populate row per retired session
        sb.append("<tr>");

        // fixed columns: Id/Url + Userid + Name + Status
        String url =
            (s.getUrl() == null || s.getUrl().length() == 0) ? s.getSessionId()
                :
                "<a href=\"" + s.getUrl() + "\">" + s.getSessionId() + "</a>";
        sb.append("<td>" + url + "</td>");
        sb.append("<td>" + dateFormat.format(new Date(s.getStartTime())) +
                "</td>");
        sb.append("<td>" + dateFormat.format(new Date(s.getDeletedTime())) +
                "</td>");
        sb.append("<td>" + s.getName() + "</td>");
        sb.append("<td>" + s.getUserId() + "</td>");
        sb.append("<td>" + poolInfo.getPoolGroupName() + "</td>");
        sb.append("<td>" + (poolInfo.getPoolName() == null ? "-" :
            poolInfo.getPoolName()) + "</td>");
        sb.append("<td>" + s.getPriority() + "</td>");
        sb.append("<td>" + s.getStatus() + "</td>");

        // variable columns
        for (ResourceType resourceType : resourceTypes) {
          int total = s.getFulfilledRequestCountForType(resourceType);
          sb.append("<td>" + total + "</td>");
        }
        sb.append("</tr>\n");
      }
    }
    out.print(sb.toString());
    out.print("</tbody></table><br>");
  }%>

<%
  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();
  String cmHostName = StringUtils.simpleHostname(cm.getHostName());
%>

<html>
<head>
<title><%=cmHostName%> Corona Cluster Manager </title>
<link rel="stylesheet" type="text/css" href="/static/jquery/css/ui-lightness/jquery-ui-1.8.20.custom.css"/>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/dataTables/css/jquery.dataTables.css">
<link rel="stylesheet" type="text/css" href="/static/multiselect/jquery.multiselect.css">
<script type="text/javascript" src="/static/jquery/js/jquery-1.7.2.min.js"></script>
<script type="text/javascript" src="/static/jquery/js/jquery-ui-1.8.20.custom.min.js"></script>
<script type="text/javascript" src="/static/cm_old.js"></script>
<script type="text/javascript" src="/static/dataTables/js/jquery.dataTables.min.js"></script>
<script type="text/javascript" src="/static/multiselect/jquery.multiselect.min.js"></script>
<style type="text/css">
  body {
    font-family: Verdana,Arial,sans-serif;
    font-size: 1.1em;
  }
  .ui-button {
    font-family: Verdana,Arial,sans-serif;
    font-size: .91em; /* ≅ 1/1.1 */
  }
</style>
</head>
<body>

<script type="text/javascript"
  src="http://jqueryui.com/themeroller/themeswitchertool/">
</script>
<div id="switcher"></div>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#pools">Pools</a></li>
    <li><a href="#active_sessions">Active Sessions</a></li>
    <li><a href="#retired_sessions">Retired Sessions</a></li>
  </ul>
</div>

<%
  Enumeration<String> attributeNames = request.getParameterNames();
  while (attributeNames.hasMoreElements()) {
    String attribute = attributeNames.nextElement();
    if (!attribute.equals("users") && !attribute.equals("poolGroups") &&
        !attribute.equals("poolInfos")) {
%>

<script type="text/javascript">
 alert('Illegal parameter <%=attribute%>, only "users", "poolGroups", and "poolInfos" parameters are allowed');
</script>

<%
      break;
    }
  }
%>

<h1><%=cmHostName%> Corona Cluster Manager </h1>
<b>Started:</b> <%=new Date(cm.getStartTime())%><br>
<b>Version:</b> <%=VersionInfo.getVersion()%>,
                r<%=VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%=VersionInfo.getDate()%> by
                 <%=VersionInfo.getUser()%><br>
<%
  // Check for user set, pool info set, or pool group set filters
  Set<String> userFilterSet = new HashSet<String>();
  String userFilter = request.getParameter("users");
  if (userFilter != null) {
    userFilterSet.addAll(Arrays.asList(userFilter.split(",")));
    out.write("<b>users:</b> " + userFilter + "<br>");
  }
  Set<String> poolGroupFilterSet = new HashSet<String>();
  String poolGroupFilter = request.getParameter("poolGroups");
  if (poolGroupFilter != null) {
    poolGroupFilterSet.addAll(Arrays.asList(poolGroupFilter.split(",")));
    out.write("<b>poolGroups:</b> " + poolGroupFilter + "<br>");
  }
  Set<PoolInfo> poolInfoFilterSet  = new HashSet<PoolInfo>();
  String poolInfoFilter = request.getParameter("poolInfos");
  if (poolInfoFilter != null) {
    out.write("<b>poolInfos:</b> " + poolInfoFilter + "<br>");
    for (String poolInfoString : poolInfoFilter.split(",")) {
      String[] poolInfoStrings = poolInfoString.split("[.]");
      if (poolInfoStrings.length == 2) {
        poolInfoFilterSet.add(new PoolInfo(poolInfoStrings[0],
                                           poolInfoStrings[1]));
      }
    }
  }
%>

<%
  Scheduler scheduler = cm.getScheduler();
  List<String> poolGroups = new ArrayList<String>();
  List<String> poolInfos = new ArrayList<String>();
  for (PoolInfo poolInfo : scheduler.getPoolInfos()) {
    if (poolInfo.getPoolName() == null) {
      poolGroups.add(poolInfo.getPoolGroupName());
    } else {
      poolInfos.add(PoolInfo.createStringFromPoolInfo(poolInfo));
    }
  }
%>

<hr>

<select id="poolGroupSelect" name="poolGroupSelect" multiple="multiple">
<%
  for (String poolGroupString : poolGroups) {
    out.write("<option value=\""+ poolGroupString + "\">" + poolGroupString +
        "</option>\n");
  }
%>
</select>

<select id="poolInfoSelect" name="poolInfoSelect" multiple="multiple">
<%
  for (String poolInfoString : poolInfos) {
    out.write("<option value=\""+ poolInfoString + "\">" + poolInfoString +
        "</option>\n");
  }
%>
</select>
<button id="addFilter" type="button">Use selected filters</button>

<h2>Cluster Summary</h2>

<%
  generateSummaryTable(out, cm.getNodeManager(), cm.getSessionManager());
%>

<h2 id="pools">Pools</h2>
<button id="poolToggle" type="button">Show/Hide</button>
<%
  generatePoolTable(out, cm.getScheduler(), cm.getTypes(),
      poolGroupFilterSet, poolInfoFilterSet);
%>

<h2 id="active_sessions">Active Sessions</h2>
<button id="activeToggle" type="button">Show/Hide</button>
<%
  generateActiveSessionTable(out, cm.getNodeManager(),
      cm.getSessionManager(), cm.getScheduler(), userFilterSet,
      poolGroupFilterSet, poolInfoFilterSet, dateFormat);
%>

<h2 id="retired_sessions">Retired Sessions</h2>
<button id="retiredToggle" type="button">Show/Hide</button>
<%
  generateRetiredSessionTable(out, cm.getNodeManager(),
      cm.getSessionManager(), userFilterSet,
      poolGroupFilterSet, poolInfoFilterSet, dateFormat);
%>

<%
  out.println(ServletUtil.htmlFooter());
%>

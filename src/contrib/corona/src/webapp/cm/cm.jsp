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
        WebUtils.convertResourceTypesToStrings(resourceTypes),
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
      row.append("<td><a href=\"machines.jsp?type=free&resourceType=" + resourceType + "\">" + freeslots + "</a></td>");
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
      DateFormat dateFormat,
      boolean canKillSessions)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<table id=\"activeTable\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    // populate header row
    sb.append("<thead><tr>");
   
    // fixed headers  
    String[] fixedHeaders = { "Id", "Start Time", "Name", "User", "Pool Group",
            "Pool", "Job Priority" };;
    String[] fixedHeadersCanKill = { "", "Id", "Start Time", "Name", "User", "Pool Group",
            "Pool", "Job Priority" };
    if (canKillSessions) {
      fixedHeaders = fixedHeadersCanKill;
    } 
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 2, false));
    
    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String[] perTypeColumns = { "Running", "Waiting", "Total", "MaxMemory", "MaxInstMemory", "MaxRSSMemory" };
    sb.append(generateTableHeaderCells(
        WebUtils.convertResourceTypesToStrings(resourceTypes),
        perTypeColumns.length, 0, false));

    sb.append("</tr>\n");

    // populate sub-header row
    sb.append("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      for (int i = 0; i < perTypeColumns.length; i++) {
        sb.append("<th>" + perTypeColumns[i] + "</th>");
        if (i == 2 && resourceType == ResourceType.JOBTRACKER) {
          break;
        }
      }
    }
    sb.append("</tr></thead><tbody>\n");
    out.print(sb.toString());
    out.print("</tbody></table><br>");
    
    if (canKillSessions) {
      out.print("<button id=\"killSession\" type=\"button\">Kill Session</button>");
    }
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
    sb.append("<th rowspan=2>RequestMax</th>");
    sb.append("<th rowspan=2>PoolPriority</th>");
    sb.append(generateTableHeaderCells(
        WebUtils.convertResourceTypesToStrings(types),
        metricsNames.size(), 0, false));
    sb.append("</tr>\n");

    sb.append("<tr>");
    for (int i = 0; i < types.size(); ++i) {
      for (String name : metricsNames) {
        sb.append("<th>" + name + "</th>");
      }
    }
    sb.append("</tr></thead><tbody>\n");
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
        "Name", "User", "Pool Group", "Pool", "Job Priority", "Status"};
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 2, false));

    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String[] perTypeColumns = { "Ran", "MaxMemory", "MaxInstMemory", "MaxRSSMemory" };
    sb.append(generateTableHeaderCells(
        WebUtils.convertResourceTypesToStrings(resourceTypes),
        perTypeColumns.length, 0, false));

    sb.append("</tr>\n");

    // populate sub-header row
    sb.append("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      for (int i = 0; i < perTypeColumns.length; i++) {
        sb.append("<th>" + perTypeColumns[i] + "</th>");
        if (resourceType == ResourceType.JOBTRACKER) {
          break;
        }
      }
    }
    sb.append("</tr></thead><tbody>\n");
    out.print(sb.toString());
    out.print("</tbody></table><br>");
  }

  public String getDowntimeString(Date lastDowntime) {
    Long daysElapsed = (new Date().getTime() - lastDowntime.getTime()) /
                        (24*60*60*1000);
    return new String(lastDowntime.toString() +
                     " (" + daysElapsed + " days ago)");
  }
%>

<%
  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();
  String cmHostName = StringUtils.simpleHostname(cm.getHostName());

  String toKillSessionId = request.getParameter("toKillSessionId");
  boolean canKillSessions = 
    WebUtils.isValidKillSessionsToken(request.getParameter("killSessionsToken"));
  
  if (toKillSessionId != null && canKillSessions) {
    String[] ids = toKillSessionId.split(" ");
    try {
      KillSessionsArgs killSessionsArgs = new KillSessionsArgs();
      killSessionsArgs.sessionIds = Arrays.asList(ids);
      killSessionsArgs.who = request.getRemoteHost();
      
      cm.killSessions(killSessionsArgs);
    } catch (Exception e) {
    }
  }
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
<script type="text/javascript" src="/static/cm.js"></script>
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
  src="/static/jqueryThemeRoller.js">
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
  String validationMessage =
    WebUtils.validateAttributeNames(request.getParameterNames());
  if (validationMessage != null) {
%>

<script type="text/javascript">
 alert('<%=validationMessage%>');
</script>

<%
  }
%>

<h1><%=cmHostName%> Corona Cluster Manager </h1>
<b>Last Restarted:</b> <%=new Date(cm.getLastRestartTime())%><br>
<b>Last Downtime:</b> <%=getDowntimeString(new Date(cm.getStartTime()))%><br>
<b>Version:</b> <%=VersionInfo.getVersion()%>,
                r<%=VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%=VersionInfo.getDate()%> by
                 <%=VersionInfo.getUser()%><br>
<b>Safe Mode :</b> <%=cm.getSafeMode() ? "ON" : "OFF"%>

<%
  WebUtils.JspParameterFilters filters = WebUtils.getJspParameterFilters(
    request.getParameter("users"),
    request.getParameter("poolGroups"),
    request.getParameter("poolInfos"));
  out.print(filters.getHtmlOutput().toString());
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
<div id="toolbar">
<select id="poolGroupSelect" name="poolGroupSelect" multiple="multiple">
<%
  for (String poolGroupString : poolGroups) {
    out.write("<option value=\""+ poolGroupString + "\">" + poolGroupString +
        "</option>\n");
  }
%>
</select>
<input id="poolInfoInput" placeholder="Enter a comma-separated list of pool infos" style="width:300px;height=30px;"/>
<select id="poolInfoSelect" name="poolInfoSelect" multiple="multiple">
<%
  for (String poolInfoString : poolInfos) {
    out.write("<option value=\""+ poolInfoString + "\">" + poolInfoString +
        "</option>\n");
  }
%>
</select>
<button id="addFilter" type="button">Use selected filters</button>
</div>
<h2>Cluster Summary</h2>

<%
  generateSummaryTable(out, cm.getNodeManager(), cm.getSessionManager());
%>

<h2 id="pools">Pools</h2>
<button id="poolToggle" type="button">Show/Hide</button>
<%
  generatePoolTable(out, cm.getScheduler(), cm.getTypes(),
      filters.getPoolGroupFilterSet(), filters.getPoolInfoFilterSet());
%>

<h2 id="active_sessions">Active Sessions</h2>
<button id="activeToggle" type="button">Show/Hide</button>
<%
  generateActiveSessionTable(out, cm.getNodeManager(),
      cm.getSessionManager(), cm.getScheduler(),
      filters.getUserFilterSet(),
      filters.getPoolGroupFilterSet(),
      filters.getPoolInfoFilterSet(), dateFormat,
      canKillSessions);
%>
<h2 id="retired_sessions">Retired Sessions</h2>
<button id="retiredToggle" type="button">Show/Hide</button>
<%
  generateRetiredSessionTable(out, cm.getNodeManager(),
      cm.getSessionManager(),
      filters.getUserFilterSet(),
      filters.getPoolGroupFilterSet(),
      filters.getPoolInfoFilterSet(), dateFormat);
%>

<%
  out.println(ServletUtil.htmlFooter());
%>

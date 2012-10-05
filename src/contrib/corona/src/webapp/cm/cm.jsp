<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.corona.*"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.util.StringUtils"
%>

<%!
  public Collection<String> convertResourceTypesToStrings(
      Collection<ResourceType> resourceTypes) {
  	List<String> retList = new ArrayList<String>(resourceTypes.size());
  	for (ResourceType resourceType : resourceTypes) {
      retList.add(resourceType.toString());
  	}

  	return retList;
  }

  public void generateSummaryTable(JspWriter out, NodeManager nm, SessionManager sm)
  		throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    // populate header row with list of resource types
    sb.append("<tr>");

    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String [] perTypeColumns = {"Running", "Waiting", "TotalSlots", "FreeSlots"};

    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes),
    	perTypeColumns.length, 0, false));
	sb.append("<th colspan=3>Nodes</th>");
    sb.append("<th colspan=1>Session</th>");
    sb.append("</tr>\n");
    out.print(sb.toString());

    String typeColHeader = "<td>" + org.apache.commons.lang.StringUtils.join(perTypeColumns, "</td><td>") + "</td>";
    StringBuilder row = new StringBuilder("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      row.append(typeColHeader);
    }

    // Nodes
    row.append("<td>Alive</td>");
    row.append("<td>Blacklisted</td>");
    row.append("<td>Excluded</td>");
    // Session
    row.append("<td>Running</td></tr>\n");
    out.print(row.toString());


    row = new StringBuilder("<tr>");

    for (ResourceType resourceType : resourceTypes) {
      int waiting = sm.getPendingRequestCountForType(resourceType);
      int running = sm.getRequestCountForType(resourceType) - waiting;
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
    row.append("</tr>\n");
    row.append("</table>\n");

    out.print(row.toString());
    out.print("<br>");
  }

  public void generateActiveSessionTable(
      JspWriter out, NodeManager nm, SessionManager sm, Scheduler scheduler)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    // populate header row
    sb.append("<thead><tr>");

    // fixed headers
    String [] fixedHeaders = {"Id", "User", "Pool", "Name"};
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 2, false));

    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    String [] perTypeColumns = {"Running", "Waiting", "Total"};
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes),
        perTypeColumns.length, 0, true));

    sb.append("</tr>\n");

    // populate sub-header row
    sb.append("<tr>");
    for (ResourceType resourceType : resourceTypes) {
      for (int i=0; i<perTypeColumns.length; i++)
        sb.append("<th>" + perTypeColumns[i] + "</th>");
    }
    sb.append("</tr></thead><tbody>\n");

    for(String id: sm.getSessions()) {
      Session s;
      try { s = sm.getSession(id); } catch (InvalidSessionHandle e) { continue; }

      synchronized (s) {
        if (s.isDeleted())
          continue;

        String url = (s.getUrl() == null || s.getUrl().length() == 0) ? id :
          "<a href=\"" + s.getUrl() + "\">" + id + "</a>";
        sb.append("<tr><td>" + url + "</td><td>" + s.getUserId() + "</td><td>"
          + scheduler.getPoolName(s) + "</td>");
        sb.append("<td><a href=\"jobresources.jsp?id="+ id + "\">" +
          s.getName() + "</a></td>");
        for (ResourceType resourceType : resourceTypes) {
          int total = s.getRequestCountForType(resourceType);
          int waiting = s.getPendingRequestForType(resourceType).size();
          int running = s.getGrantedRequestForType(resourceType).size();
          sb.append("<td>" + running + "</td><td>" + waiting + "</td><td>" + total + "</td>");
        }
        sb.append("</tr>\n");
      }
    }
    out.print(sb.toString());
    out.print("</tbody></table><br>");
  }

  public void generatePoolTable(
      JspWriter out, Scheduler scheduler, Collection<ResourceType> types)
      throws IOException {
    List<String> metricsNames = new ArrayList<String>();
    for (PoolMetrics.MetricName name : PoolMetrics.MetricName.values()) {
      metricsNames.add(name.toString());
    }
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    // Generate headers
    sb.append("<thead><tr>");
    sb.append("<th rowspan=2>id</th>");
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(types),
        metricsNames.size(), 0, true));
    sb.append("</tr>\n");

    sb.append("<tr>");
    for (int i = 0; i < types.size(); ++i) {
      for (String name : metricsNames) {
        sb.append("<th>" + name + "</th>");
      }
    }
    sb.append("</tr></thead><tbody>\n");

    for (String poolName : scheduler.getPoolNames()) {
      sb.append("<tr>");
      sb.append("<td>" + poolName + "</td>");
      for (ResourceType type : types) {
        Map<String, PoolMetrics> poolMetrics = scheduler.getPoolMetrics(type);
        PoolMetrics metric = poolMetrics.get(poolName);
        for (PoolMetrics.MetricName metricsName : PoolMetrics.MetricName.values()) {
          Long val = null;
          if (metric != null) {
            val = metric.getCounter(metricsName);
          }
          if (val == null) {val = 0L;}
          if (metricsName == PoolMetrics.MetricName.MAX && val == Integer.MAX_VALUE) {
            sb.append("<td>-</td>");
            continue;
          }
          sb.append("<td>" + val + "</td>");
        }
      }
      sb.append("</tr>\n");
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
    sb.append("</"+ tag + ">");
    return sb.toString();
  }

  public void generateRetiredSessionTable(JspWriter out, NodeManager nm, SessionManager sm)
  throws IOException {

    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    // populate header row
    sb.append("<thead><tr>");

    // fixed headers
    String [] fixedHeaders = {"Id", "User", "Name", "Status"};
    sb.append(generateTableHeaderCells(Arrays.asList(fixedHeaders), 1, 0, false));

    // header per type
    Collection<ResourceType> resourceTypes = nm.getResourceTypes();
    sb.append(generateTableHeaderCells(
        convertResourceTypesToStrings(resourceTypes), 1, 0, false));
    sb.append("</tr></thead><tbody>\n");

    Collection<RetiredSession> retiredSessions = sm.getRetiredSessions();
    synchronized(retiredSessions) {
      for(RetiredSession s: retiredSessions) {

        // populate row per retired session
        sb.append("<tr>");

        // fixed columns: Id/Url + Userid + Name + Status
        String url = (s.getUrl() == null || s.getUrl().length() == 0) ? s.getSessionId() :
          "<a href=\"" + s.getUrl() + "\">" + s.getSessionId() + "</a>";
        sb.append("<td>" + url + "</td>");
        sb.append("<td>" + s.getUserId() + "</td>");
        sb.append("<td>" + s.getName() + "</td>");
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
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();
  String cmHostName = StringUtils.simpleHostname(cm.getHostName());
%>

<html>
<head>
<title><%= cmHostName %> Corona Cluster Manager </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/tablesorter/style.css">
<script type="text/javascript" src="/static/jquery-1.7.1.min.js"></script>
<script type="text/javascript" src="/static/tablesorter/jquery.tablesorter.js"></script>
<script type="text/javascript" src="/static/tablesorter/jobtablesorter.js"></script>
</head>
<body>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#running_sessions">Running Jobs</a></li>
  </ul>
</div>

<h1><%= cmHostName %> Corona Cluster Manager </h1>
<b>Started:</b> <%= new Date(cm.getStartTime())%><br>
<hr>
<h2>Cluster Summary</h2>
<%
    generateSummaryTable(out, cm.getNodeManager(), cm.getSessionManager());
%>

<h2>Active Sessions</h2>
<%
    generateActiveSessionTable(out, cm.getNodeManager(),
        cm.getSessionManager(), cm.getScheduler());
%>

<h2>Pools</h2>
<%
    generatePoolTable(out, cm.getScheduler(), cm.getTypes());
%>

<h2>Retired Sessions</h2>
<%
    generateRetiredSessionTable(out, cm.getNodeManager(), cm.getSessionManager());
%>

<%
  out.println("</body></html>");
%>

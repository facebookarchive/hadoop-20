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
  import="org.json.*"
%>

<%
  WebUtils.JspParameterFilters filters = WebUtils.getJspParameterFilters(
      request.getParameter("users"),
      request.getParameter("poolGroups"),
      request.getParameter("poolInfos"));
  boolean canKillSessions = WebUtils.isValidKillSessionsToken(
    request.getParameter("killSessionsToken"));

  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();

  JSONObject result = new JSONObject();
  JSONArray array = new JSONArray();

  Collection<ResourceType> resourceTypes = nm.getResourceTypes();
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

      if (!WebUtils.showUserPoolInfo(s.getUserId(),
          filters.getUserFilterSet(),
          poolInfo,
          filters.getPoolGroupFilterSet(),
          filters.getPoolInfoFilterSet())) {
        continue;
      }

      JSONArray row = new JSONArray();
      if (canKillSessions) {
        String checkCase = 
          "<input type=\"checkbox\" class=\"case\" name=\"case\" value=\"" + 
          s.getSessionId() + "\">";
        row.put(checkCase);
      }
      row.put(url);
      row.put(dateFormat.format(new Date(s.getStartTime())));
      row.put("<a href=\"jobresources.jsp?id=" + id + "\">" +
          s.getName() + "</a>");
      row.put(s.getUserId());
      row.put(poolInfo.getPoolGroupName());
      row.put(poolInfo.getPoolName());
      row.put(SessionPriority.findByValue(s.getPriority()));
      
      for (ResourceType resourceType : resourceTypes) {
        row.put(s.getGrantedRequestForType(resourceType).size());
        row.put(s.getPendingRequestForType(resourceType).size());
        row.put(s.getRequestCountForType(resourceType));
        
        if (resourceType == ResourceType.JOBTRACKER) {
          continue;
        }
        
        List<Long> resourceUsage = s.getResourceUsageForType(resourceType);
        if (resourceUsage != null) {
          for (long val:resourceUsage) {
            row.put(StringUtils.humanReadableInt(val));
          }
        } else {
          row.put("0 B");
          row.put("0 B");
          row.put("0 B");
        }
        
      }
      array.put(row);
    }
  }

  result.put("aaData", array);
  response.setContentType("application/json");
  response.setHeader("Cache-Control", "no-store");
  out.print(result);
%>

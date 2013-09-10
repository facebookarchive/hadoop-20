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

  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();

  JSONObject result = new JSONObject();
  JSONArray array = new JSONArray();
  
  Collection<ResourceType> resourceTypes = nm.getResourceTypes();
  Collection<RetiredSession> retiredSessions = sm.getRetiredSessions();
  synchronized (retiredSessions) {
    for (RetiredSession s : retiredSessions) {
      PoolInfo poolInfo = s.getPoolInfo();
      if (!WebUtils.showUserPoolInfo(
          s.getUserId(),
          filters.getUserFilterSet(),
          s.getPoolInfo(),
          filters.getPoolGroupFilterSet(), filters.getPoolInfoFilterSet())) {
        continue;
      }

      JSONArray row = new JSONArray();
      // fixed columns: Id/Url + Userid + Name + Status
      String url =
          (s.getUrl() == null || s.getUrl().length() == 0) ? s.getSessionId()
              :
              "<a href=\"" + s.getUrl() + "\">" + s.getSessionId() + "</a>";
      row.put(url);
      row.put(dateFormat.format(new Date(s.getStartTime())));
      row.put(dateFormat.format(new Date(s.getDeletedTime())));
      row.put(s.getName());
      row.put(s.getUserId());
      row.put(poolInfo.getPoolGroupName());
      row.put(poolInfo.getPoolName() == null ? "-" :
          poolInfo.getPoolName());
      row.put(s.getPriority());
      row.put(s.getStatus());

      // variable columns
      for (ResourceType resourceType : resourceTypes) {
        int totalTypes =
            s.getFulfilledRequestCountForType(resourceType);
        row.put(totalTypes);
        
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

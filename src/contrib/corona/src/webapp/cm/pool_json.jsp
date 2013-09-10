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

  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();

  JSONObject result = new JSONObject();
  JSONArray array = new JSONArray();

  Collection<ResourceType> resourceTypes = nm.getResourceTypes();
  Scheduler scheduler = cm.getScheduler();
  ConfigManager configManager = scheduler.getConfigManager();
  Map<PoolInfo, PoolInfo> redirects = configManager.getRedirects();

  // Initialize the total metrics
  int totalEntries =
      PoolInfoMetrics.MetricName.values().length * resourceTypes.size();
  List<Long> totalMetrics = new ArrayList<Long>(totalEntries);
  for (int i = 0; i < totalEntries; ++i) {
    totalMetrics.add(new Long(0));
  }
  for (PoolInfo poolInfo : scheduler.getPoolInfos()) {
    if (!WebUtils.showPoolInfo(poolInfo, filters.getPoolGroupFilterSet(),
        filters.getPoolInfoFilterSet())) {
      continue;
    }

    JSONArray row = new JSONArray();
    WebUtils.PoolInfoHtml poolInfoHtml = WebUtils.getPoolInfoHtml(redirects,
        poolInfo);
    row.put(poolInfoHtml.getGroupHtml());
    row.put(poolInfoHtml.getPoolHtml());
    row.put(configManager.getPoolComparator(poolInfo));
    row.put(configManager.isPoolPreemptable(poolInfo));
    row.put(configManager.useRequestMax(poolInfo));
    row.put(configManager.getPriority(poolInfo));

    int metricsIndex = 0;
    for (ResourceType type : resourceTypes) {
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
          row.put("-");

        } else {
          row.put(val);
          // Only add the pool infos, not pool groups metrics.
          if (poolInfo.getPoolName() != null) {
            totalMetrics.set(metricsIndex,
                (totalMetrics.get(metricsIndex) + val));
          }
        }
        ++metricsIndex;
      }
    }
    array.put(row);
  }
  // Add totals row
  JSONArray row = new JSONArray();
  row.put("all pool groups");
  row.put("all pools");
  row.put("-");
  row.put("-");
  row.put("-");
  row.put("-");
  for (Long metricsValue : totalMetrics) {
    row.put(metricsValue);
  }
  array.put(row);

  result.put("aaData", array);
  response.setContentType("application/json");
  response.setHeader("Cache-Control", "no-store");
  out.print(result);
%>

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

<%
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
%>

<html>
<head>
<title><%= cm.getHostName() %> Corona Cluster Manager - exec </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>

<%
  ConfigManager configManager = cm.getScheduler().getConfigManager();
  String req = (String) request.getParameter("req");
  out.println("req = " + ((req == null) ? "<none>" : req) + "<br>");
  if (req == null) {
    response.sendError(HttpServletResponse.SC_BAD_REQUEST,
        "Bad request, set req parameter");
  } else if (req.equals("reloadServerPoolsConfig")) {
    String md5String = configManager.generatePoolsConfigIfClassSet();
    if (md5String == null) {
      response.sendError(HttpServletResponse.SC_EXPECTATION_FAILED,
          "Failed to generate a pools config");
    } else {
      if (configManager.reloadAllConfig(false)) {
        response.addHeader("md5sum", md5String);
        out.println("response = " + md5String + "<br>");
      } else {
        response.sendError(HttpServletResponse.SC_EXPECTATION_FAILED,
            "Success generating the pools config, but failed to reload" +
            " the config");
      }
    }
  } else {
    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
        "Req " + req + " not supported");
  }
%>

</body>
</html>

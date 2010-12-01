<%@ page
    import="javax.servlet.*"
    import="javax.servlet.http.*"
    import="java.io.*"
    import="java.text.*"
    import="java.util.*"
    import="java.net.URL"
    import="java.text.DecimalFormat"
    import="org.apache.hadoop.mapred.*"
    import="org.apache.hadoop.util.*"
    import="org.apache.hadoop.conf.*" %>
    <%
      DynamicCloudsDaemon cb = 
            (DynamicCloudsDaemon)application.getAttribute("cluster.balancer");
    %>
    
<html>
  <title> Cluster Balancer Daemon Status </title>
  <head>
  </head>
  <body>
    <script type="text/javascript">
      function $(id) {
        return document.getElementById(id);
      }

      function switchDisplay (data_id) {
        if ($(data_id).style.display == 'none') {
          $(data_id).style.display = 'block';
        } else {
          $(data_id).style.display = 'none';
        }
      }
    </script>
    <h1>Clusters Statuses: </h1>
    
    <center>
<%
  int clusterNum = 1;
  for (Map.Entry<String, Cluster> clusterEntry : cb.getRegisteredClusters()) {
    Cluster cluster = clusterEntry.getValue();
    String name = clusterEntry.getKey();
    String host = cluster.getHostName();
    String version = cluster.getVersion();
    String http = cluster.getHttpAddress();
%>
    <h2>Cluster #<%=clusterNum++%></h2>
    <ul style="list-style-type: none;">
      <li> <b> Name: <%=name%> </b>
      <li> <b> Job Tracker: <a href="<%=http%>"><%=host%></a> </b>
      <li> <b> Version: <%=version%>
      <li> <b> Average Load: <%=cluster.getAverageLoad()%>
    </ul>

    <h3> <a href="#" onclick="return switchDisplay('tt_list_<%=clusterNum%>')"> Task Trackers </a></h3>
    <div id='tt_list_<%=clusterNum%>' style="display:none">
      <table border="1px" width="80%" >
        <tr>
          <th>TT Host Name</th>
          <th>Active Status</th>
          <th>Last Heartbeat</th>
          <th>Map Slots</th>
          <th>Reduce Slots</th>
          <th>Total Maps</th>
        </tr>
<%
      for (TaskTrackerLoadInfo ttli : cluster.getTrackers()) {
        
%>
<tr>
  <td><%=ttli.getHostName()%></td>
  <td><%=ttli.isActive()?"Active":"Blacklisted"%></td>
  <td><%=(System.currentTimeMillis() - ttli.getLastSeen())/1000%></td>
  <td><%=ttli.getRunningMapTasks()%>/<%=ttli.getMaxMapTasks()%></td>
  <td><%=ttli.getRunningReduceTasks()%>/<%=ttli.getMaxReduceTasks()%></td>
  <td><%=ttli.getTotalMapTasks()%></td>
</tr>
<%
      }
%>
      </table>
    </div>
<%
  }
%>
    </center>
  </body>    
</html>

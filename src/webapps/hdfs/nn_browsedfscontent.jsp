<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.net.NetUtils"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.net.InetAddress"
  import="java.net.URLEncoder"
%>
<%!
  public void redirectToRandomDataNode(
                            NameNode nn, 
                            HttpServletResponse resp) throws IOException {
    FSNamesystem fsn = nn.getNamesystem();
    String datanode = fsn.randomDataNode();
    String redirectLocation;
    String nodeToRedirect;
    int redirectPort;
    if (datanode != null) {
      redirectPort = Integer.parseInt(datanode.substring(datanode.indexOf(':') + 1));
      nodeToRedirect = datanode.substring(0, datanode.indexOf(':'));
    }
    else {
      nodeToRedirect = nn.getHttpAddress().getAddress().getHostAddress();
      redirectPort = nn.getHttpAddress().getPort();
    }
    String addr = NetUtils.toIpPort(nn.getNameNodeAddress());
    String fqdn = InetAddress.getByName(nodeToRedirect).getHostAddress();
    redirectLocation = "http://" + NetUtils.toIpPort(fqdn, redirectPort) + 
                       "/browseDirectory.jsp?namenodeInfoPort=" + 
                       nn.getHttpAddress().getPort() +
                       "&dir=" + URLEncoder.encode("/", "UTF-8") +
                       JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr);
    resp.sendRedirect(redirectLocation);
  }
%>

<html>

<title></title>

<body>
<% 
  NameNode nn = (NameNode)application.getAttribute("name.node");
  redirectToRandomDataNode(nn, response); 
%>
<hr>

<h2>Local logs</h2>
<a href="logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>

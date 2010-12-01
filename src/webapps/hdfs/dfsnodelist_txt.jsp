<%@ page
  contentType="text/plain; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.common.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
%>
<%!
JspHelper jspHelper = new JspHelper();
public void generateDFSNodesList(JspWriter out,  NameNode nn,
                                 HttpServletRequest request)
                                 throws IOException {
  ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();    
  ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
  ArrayList<DatanodeDescriptor> excluded = new ArrayList<DatanodeDescriptor>();
  jspHelper.DFSNodesStatus(live, dead, excluded);
  String whatNodes = request.getParameter("whatNodes");
  ArrayList<DatanodeDescriptor> toBePrinted;
  if ("DEAD".equalsIgnoreCase(whatNodes)) {
    toBePrinted = dead;
  } else if ("EXCLUDED".equalsIgnoreCase(whatNodes)) {
    toBePrinted = excluded;
  } else if ("LIVE".equalsIgnoreCase(whatNodes)) {
    toBePrinted = live;
  } else { // Default is all nodes
    toBePrinted = new ArrayList<DatanodeDescriptor>();
    toBePrinted.addAll(dead);
    toBePrinted.addAll(excluded);
    toBePrinted.addAll(live);
  }
  for (DatanodeDescriptor d : toBePrinted) {
    out.print(d.getHostName() + "\n");
  }
}
%>
<%
  NameNode nn = (NameNode)application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  generateDFSNodesList(out, nn, request); 
%>

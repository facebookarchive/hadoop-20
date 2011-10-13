<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.server.common.Storage"
  import="org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
%>
<%!
  JspHelper jspHelper = new JspHelper();

  int rowNum = 0;
  int colNum = 0;

  String rowTxt() { colNum = 0;
      return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
          + "\"> "; }
  String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
  String spaces(int num) {
      return org.apache.commons.lang.StringUtils.repeat("&nbsp;", num);
  }
  void counterReset () { colNum = 0; rowNum = 0 ; }

  long diskBytes = 1024 * 1024 * 1024;
  String diskByteStr = "GB";

  String sorterField = null;
  String sorterOrder = null;

  String NodeHeaderStr(String name) {
      String ret = "class=header";
      String order = "ASC";
      if ( name.equals( sorterField ) ) {
          ret += sorterOrder;
          if ( sorterOrder.equals("ASC") )
              order = "DSC";
      }
      ret += " onClick=\"window.document.location=" +
          "'/dfshealth.jsp?sorter/field=" + name + "&sorter/order=" +
          order + "'\" title=\"sort on this column\"";
      
      return ret;
  }
      
  public void generateNodeData( JspWriter out, DatanodeDescriptor d,
                                    String suffix, boolean alive,
                                    int nnHttpPort )
    throws IOException {
      
    /* Say the datanode is dn1.hadoop.apache.org with ip 192.168.0.5
       we use:
       1) d.getHostName():d.getPort() to display.
           Domain and port are stripped if they are common across the nodes.
           i.e. "dn1"
       2) d.getHost():d.Port() for "title".
          i.e. "192.168.0.5:50010"
       3) d.getHostName():d.getInfoPort() for url.
          i.e. "http://dn1.hadoop.apache.org:50075/..."
          Note that "d.getHost():d.getPort()" is what DFS clients use
          to interact with datanodes.
    */
    // from nn_browsedfscontent.jsp:
    String url = "http://" + d.getHostName() + ":" + d.getInfoPort() +
                 "/browseDirectory.jsp?namenodeInfoPort=" +
                 nnHttpPort + "&dir=" +
                 URLEncoder.encode("/", "UTF-8");
     
    String name = d.getHostName() + ":" + d.getPort();
    if ( !name.matches( "\\d+\\.\\d+.\\d+\\.\\d+.*" ) ) 
        name = name.replaceAll( "\\.[^.:]*", "" );    
    int idx = (suffix != null && name.endsWith( suffix )) ?
        name.indexOf( suffix ) : -1;
    
    out.print( rowTxt() + "<td class=\"name\"><a title=\""
               + d.getHost() + ":" + d.getPort() +
               "\" href=\"" + url + "\">" +
               (( idx > 0 ) ? name.substring(0, idx) : name) + "</a>" +
               (( alive ) ? "" : "\n") );
    if ( !alive ) {
        out.print("<td class=\"decommissioned\"> " +
           d.isDecommissioned() + "\n");
        return;
    }
    long c = d.getCapacity();
    long u = d.getDfsUsed();
    long nu = d.getNonDfsUsed();
    long r = d.getRemaining();
    String percentUsed = StringUtils.limitDecimalTo2(d.getDfsUsedPercent());    
    String percentRemaining = StringUtils.limitDecimalTo2(d.getRemainingPercent());    
    String adminState = d.getAdminState().toString();
    
    long timestamp = d.getLastUpdate();
    long currentTime = System.currentTimeMillis();
    out.print("<td class=\"lastcontact\"> " +
              ((currentTime - timestamp)/1000) +
              "<td class=\"adminstate\">" +
              adminState +
              "<td align=\"right\" class=\"capacity\">" +
              StringUtils.limitDecimalTo2(c*1.0/diskBytes) +
              "<td align=\"right\" class=\"used\">" +
              StringUtils.limitDecimalTo2(u*1.0/diskBytes) +      
              "<td align=\"right\" class=\"nondfsused\">" +
              StringUtils.limitDecimalTo2(nu*1.0/diskBytes) +      
              "<td align=\"right\" class=\"remaining\">" +
              StringUtils.limitDecimalTo2(r*1.0/diskBytes) +      
              "<td align=\"right\" class=\"pcused\">" + percentUsed +
              "<td class=\"pcused\">" +
              ServletUtil.percentageGraph( (int)Double.parseDouble(percentUsed) , 100) +
              "<td align=\"right\" class=\"pcremaining`\">" + percentRemaining +
              "<td title=" + "\"blocks scheduled : " + d.getBlocksScheduled() + 
              "\" class=\"blocks\">" + d.numBlocks() + "\n");
  }
  
  
  public void generateConfReport( JspWriter out,
		  FSNamesystem fsn,
		  HttpServletRequest request)
  throws IOException {
	  long underReplicatedBlocks = fsn.getUnderReplicatedBlocks();
	  FSImage fsImage = fsn.getFSImage();
	  List<Storage.StorageDirectory> removedStorageDirs = fsImage.getRemovedStorageDirs();
	  String storageDirsSizeStr="", removedStorageDirsSizeStr="", storageDirsStr="", removedStorageDirsStr="", storageDirsDiv="", removedStorageDirsDiv="";

	  //FS Image storage configuration
	  out.print("<h3> NameNode Storage: </h3>");
	  out.print("<div id=\"dfstable\"> <table border=1 cellpadding=10 cellspacing=0 title=\"NameNode Storage\">\n"+
	  "<thead><tr><td><b>Storage Directory</b></td><td><b>Type</b></td><td><b>State</b></td></tr></thead>");
	  
	  StorageDirectory st =null;
	  for (Iterator<StorageDirectory> it = fsImage.dirIterator(); it.hasNext();) {
	      st = it.next();
	      String dir = "" +  st.getRoot();
		  String type = "" + st.getStorageDirType();
		  out.print("<tr><td>"+dir+"</td><td>"+type+"</td><td>Active</td></tr>");
	  }
	  
	  long storageDirsSize = removedStorageDirs.size();
	  for(int i=0; i< storageDirsSize; i++){
		  st = removedStorageDirs.get(i);
		  String dir = "" +  st.getRoot();
		  String type = "" + st.getStorageDirType();
		  out.print("<tr><td>"+dir+"</td><td>"+type+"</td><td><font color=red>Failed</font></td></tr>");
	  }
	  
	  out.print("</table></div><br>\n");
  }


  public void generateDFSHealthReport(JspWriter out,
                                      NameNode nn,
                                      HttpServletRequest request)
                                      throws IOException {
    FSNamesystem fsn = nn.getNamesystem();
    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    jspHelper.DFSNodesStatus(live, dead);

    int liveDecommissioned = 0;
    for (DatanodeDescriptor d : live) {
      liveDecommissioned += d.isDecommissioned() ? 1 : 0;
    }

    int deadDecommissioned = 0;
    int deadExcluded = 0;
    for (DatanodeDescriptor d : dead) {
      deadDecommissioned += d.isDecommissioned() ? 1 : 0;
      deadExcluded += fsn.inExcludedHostsList(d, null) ? 1 : 0;
    }

    ArrayList<DatanodeDescriptor> decommissioning = fsn
        .getDecommissioningNodes();
	
    sorterField = request.getParameter("sorter/field");
    sorterOrder = request.getParameter("sorter/order");
    if ( sorterField == null )
        sorterField = "name";
    if ( sorterOrder == null )
        sorterOrder = "ASC";

    // Find out common suffix. Should this be before or after the sort?
    String port_suffix = null;
    if ( live.size() > 0 ) {
        String name = live.get(0).getName();
        int idx = name.indexOf(':');
        if ( idx > 0 ) {
            port_suffix = name.substring( idx );
        }
        
        for ( int i=1; port_suffix != null && i < live.size(); i++ ) {
            if ( live.get(i).getName().endsWith( port_suffix ) == false ) {
                port_suffix = null;
                break;
            }
        }
    }
        
    counterReset();
    
    long total = fsn.getCapacityTotal();
    long remaining = fsn.getCapacityRemaining();
    long used = fsn.getCapacityUsed();
    long nonDFS = fsn.getCapacityUsedNonDFS();
    float percentUsed = fsn.getCapacityUsedPercent();
    float percentRemaining = fsn.getCapacityRemainingPercent();

    
    float mean = 0;
    float max = 0;
    float min = 0;
    float dev = 0;
    if (live.size() > 0) {
      float totalDFSUsed = 0;
      float[] usages = new float[live.size()];
      int i = 0;
      for (DatanodeDescriptor dn : live) {
        usages[i++] = dn.getDfsUsedPercent();
        totalDFSUsed += dn.getDfsUsedPercent();
      }
      totalDFSUsed /= live.size();
      Arrays.sort(usages);
      mean = usages[usages.length/2];
      max = usages[usages.length - 1];
      min = usages[0];
      
      for (i = 0; i < usages.length; i++) {
        dev += (usages[i] - totalDFSUsed) * (usages[i] - totalDFSUsed);
      }
      dev = (float) Math.sqrt(dev/live.size());
    }
    out.print( "<div id=\"dfstable\"> <table>\n" +
	       rowTxt() + colTxt() + "Configured Capacity" + colTxt() + ":" + colTxt() +
	       StringUtils.byteDesc( total ) +
	       rowTxt() + colTxt() + "DFS Used" + colTxt() + ":" + colTxt() +
	       StringUtils.byteDesc( used ) +
	       rowTxt() + colTxt() + "Non DFS Used" + colTxt() + ":" + colTxt() +
	       StringUtils.byteDesc( nonDFS ) +
	       rowTxt() + colTxt() + "DFS Remaining" + colTxt() + ":" + colTxt() +
	       StringUtils.byteDesc( remaining ) +
	       rowTxt() + colTxt() + "DFS Used%" + colTxt() + ":" + colTxt() +
	       StringUtils.limitDecimalTo2(percentUsed) + " %" +
	       rowTxt() + colTxt() + "DFS Remaining%" + colTxt() + ":" + colTxt() +
	       StringUtils.limitDecimalTo2(percentRemaining) + " %" +
	       rowTxt() + colTxt() + "DataNodes usages" + colTxt() + ":" + colTxt() +
	       "Min %" + colTxt() + "Median %" + colTxt() + "Max %" + colTxt() + 
	       "stdev %" + rowTxt() + colTxt() + colTxt() + colTxt() +
	       StringUtils.limitDecimalTo2(min) + " %" +
         colTxt() + StringUtils.limitDecimalTo2(mean) + " %" + colTxt() +
	       StringUtils.limitDecimalTo2(max) + " %" + colTxt() +
	       StringUtils.limitDecimalTo2(dev) + " %" +
         rowTxt() + colTxt() +
				 "Number of Under-Replicated Blocks" + colTxt() + ":" + colTxt() +
				 fsn.getNonCorruptUnderReplicatedBlocks() +
         "</table></div><br>\n");
    out.print("<hr>");
    // Display node status
    out.print("<h3> DataNode Health: </h3>");
    out.print("<div id=\"dfstable\"> <table border=1>\n" +
	       rowTxt() + colTxt() +
	       		"<a href=\"dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> " +
	       		colTxt() + "<b>" + live.size() + "</b>" +
	       rowTxt() + colTxt() + spaces(4) +
				 "<a href=\"dfsnodelist.jsp?whatNodes=LIVE&status=NORMAL\">" +
				 "In Service</a> " +
				 colTxt() + colTxt() +
         (live.size() - liveDecommissioned - decommissioning.size()) +
				 rowTxt() + colTxt() + spaces(4) +
         "<a href=\"dfsnodelist.jsp?whatNodes=LIVE&status=DECOMMISSIONED\">" +
         "Decommission: Completed</a> " +
	       colTxt() + colTxt() + liveDecommissioned +
	       rowTxt() + colTxt() + spaces(4) +
				 "<a href=\"dfsnodelist.jsp?whatNodes=DECOMMISSIONING\">" +
				 "Decommission: In Progress</a> " +
				 colTxt() + colTxt() + decommissioning.size() +
				 rowTxt() + colTxt() +
       	 "<a href=\"dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> " +
       	 colTxt() + "<b>" + dead.size() + "</b>" +
         rowTxt() + colTxt() + spaces(4) +
       	 "<a href=\"dfsnodelist.jsp?whatNodes=DEAD&status=EXCLUDED\">" +
         "Excluded</a> " +
       	 colTxt() + colTxt() + deadExcluded +
         rowTxt() + colTxt() + spaces(8) +
         "<a href=\"dfsnodelist.jsp?whatNodes=DEAD&status=DECOMMISSIONED\">" +
         "Decommission: Completed</a> " +
       	 colTxt() + colTxt() + colTxt() + deadDecommissioned +
         rowTxt() + colTxt() + spaces(8) +
       	 "<a href=\"dfsnodelist.jsp?whatNodes=DEAD&status=INDECOMMISSIONED\">" +
         "Decommission: Not Completed</a> " +
       	 colTxt() + colTxt() + colTxt() +
         (deadExcluded - deadDecommissioned) +
         rowTxt() + colTxt() + spaces(4) +
       	 "<a class=\"warning\" " +
         "href=\"dfsnodelist.jsp?whatNodes=DEAD&status=ABNORMAL\">" +
         "Not Excluded</a> " +
       	 colTxt() + colTxt() +
         (dead.size() - deadExcluded) +
         "</table></div><br>\n" );
    
    if (live.isEmpty() && dead.isEmpty()) {
        out.print("There are no datanodes in the cluster");
    }
  }%>

<%
  NameNode nn = (NameNode)application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  
  String namenodeLabel = JspHelper.nameNodeAddr.getHostName() + ":" + JspHelper.nameNodeAddr.getPort();
%>


<%@page import="java.net.InetSocketAddress"%><html>

<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop NameNode <%=namenodeLabel%></title>
    
<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>


<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getVersion()%>, r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by <%= VersionInfo.getUser()%>
<tr> <td id="col1"> Upgrades: <td> <%= jspHelper.getUpgradeStatusText()%>
</table></div><br>				      

<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<b><a href="/logs/">Namenode Logs</a></b>

<hr>
<h3>Cluster Summary</h3>
<b> <%= jspHelper.getSafeModeText()%> </b>
<b> <%= jspHelper.getInodeLimitText()%> </b>
<a class="warning" href="/corrupt_files.jsp" title="List corrupt files">
  <%= jspHelper.getWarningText(fsn)%>
</a>

<%
    generateDFSHealthReport(out, nn, request); 
%>
<hr>
<%
	generateConfReport(out, fsn, request);
%>
<%
out.println(ServletUtil.htmlFooter());
%>

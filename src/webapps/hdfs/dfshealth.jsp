<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream"
  import="org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType"
  import="org.apache.hadoop.hdfs.server.namenode.FSNamesystemDatanodeHelper.DatanodeStatus"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.server.common.Storage"
  import="org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.net.NetUtils"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
  import="java.net.InetAddress"
%>
<%!JspHelper jspHelper = new JspHelper();

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
          "'dfshealth.jsp?sorter/field=" + name + "&sorter/order=" +
          order + "'\" title=\"sort on this column\"";
      
      return ret;
  }
  
  public static String getDataNodeHostName(String datanodeAddress) {
    String datanodeHostName = "";
    try {
      datanodeHostName = InetAddress.getByName(datanodeAddress).getHostName();
    } catch (Exception e) {
      // Ignoring all exceptions because this is a non-essential DNS resolution
      // and we do not want this to bubble up the stack in any way.
    } catch (Throwable t) {
    
    } finally {
      if (!datanodeHostName.equals("")) {
        datanodeHostName = "(" + datanodeHostName + ")";
      }
      return datanodeHostName;
    }
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
    String url = "http://" + NetUtils.toIpPort(d.getHost(), d.getInfoPort()) +
                 "/browseDirectory.jsp?namenodeInfoPort=" +
                 nnHttpPort + "&dir=" +
                 URLEncoder.encode("/", "UTF-8");
     
    String name = NetUtils.toIpPort(d.getHost(), d.getPort());
    int idx = (suffix != null && name.endsWith( suffix )) ?
        name.indexOf( suffix ) : -1;
	String datanodeAddress = (idx > 0 ) ? name.substring(0, idx) : name; 
    out.print( rowTxt() + "<td class=\"name\"><a title=\""
               + NetUtils.toIpPort(d.getHost(), d.getPort()) +
               "\" href=\"" + url + "\">" + datanodeAddress + getDataNodeHostName(datanodeAddress)
               + "</a>" +
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
	  List<Storage.StorageDirectory> removedStorageDirs = fsImage.storage.getRemovedStorageDirs();
	  String storageDirsSizeStr="", removedStorageDirsSizeStr="", storageDirsStr="", removedStorageDirsStr="", storageDirsDiv="", removedStorageDirsDiv="";

	  //FS Image storage configuration
	  out.print("<h3> NameNode Storage: </h3>");
	  out.print("<div id=\"dfstable\"> <table border=1 cellpadding=10 cellspacing=0 title=\"NameNode Storage\">\n"+
	  "<thead><tr><td><b>Images:</b></td><td> </td><td><b>State</b></td></tr></thead>");
	  
	  List<ImageManager> images = fsImage.getImageManagers();
	  for(int i =0; i<images.size(); i++) {
	    ImageManager im = images.get(i);
	    if (im.isImageDisabled())
	      out.print("<tr><td>"+im.toHTMLString() + "</td><td> </td><td><font color=red>Failed</font></td></tr>");
	    else
	      out.print("<tr><td>"+im.toHTMLString() + "</td><td> </td><td>Active</td></tr>");
	  }
	  
	  out.print("<thead><tr><td><b>Edits:</b></td><td><b>Required</b></td><td><b>State</b></td></tr></thead>");
	  List<JournalAndStream> journals = fsImage.getEditLog().getJournals();
	  for(int i =0; i<journals.size(); i++) {
	    JournalAndStream jas = journals.get(i);
	    if (!jas.isResourceAvailable())
	      out.print("<tr><td>"+jas.toHTMLString() + "<br>" + jas.generateHTMLReport()+"</td><td>" + jas.isRequired() + "</td><td><font color=red>Failed</font></td></tr>");
	    else
	      out.print("<tr><td>"+jas.toHTMLString() + "<br>" + jas.generateHTMLReport()+"</td><td>" + jas.isRequired() + "</td><td>Active</td></tr>");
	  }
	  out.print("</table></div><br>\n");
  }

  public void generateClusterReport(JspWriter out,
                                    FSNamesystem fsn,
                                    ArrayList<DatanodeDescriptor> live)
                                    throws IOException
  {
    long total = fsn.getCapacityTotal();
    long remaining = fsn.getCapacityRemaining();
    long used = fsn.getCapacityUsed();
    long nsUsed = fsn.getCapacityNamespaceUsed();
    long nonDFS = fsn.getCapacityUsedNonDFS();
    float percentUsed = fsn.getCapacityUsedPercent();
    float percentRemaining = fsn.getCapacityRemainingPercent();
    float percentNSUsed = fsn.getCapacityNamespaceUsedPercent();


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
         rowTxt() + colTxt() + "Namespace Used" + colTxt() + ":" + colTxt() +
         StringUtils.byteDesc( nsUsed ) +
         rowTxt() + colTxt() + "DFS Used%" + colTxt() + ":" + colTxt() +
         StringUtils.limitDecimalTo2(percentUsed) + " %" +
         rowTxt() + colTxt() + "DFS Remaining%" + colTxt() + ":" + colTxt() +
         StringUtils.limitDecimalTo2(percentRemaining) + " %" +
         rowTxt() + colTxt() + "Namespace Used%" + colTxt() + ":" + colTxt() +
         StringUtils.limitDecimalTo2(percentNSUsed) + " %" +
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
         ((fsn.getMissingBlocksCount() > 0)?
             (rowTxt() + jspHelper.getMissingBlockWarningText(
                 fsn.getMissingBlocksCount())) : "") +
         "</table></div><br>\n");
    out.print("<hr>");
  }

  public void generateDFSHealthReport(JspWriter out,
                                      NameNode nn,
                                      HttpServletRequest request)
                                      throws IOException {
    FSNamesystem fsn = nn.getNamesystem();
    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    
    DatanodeStatus st = FSNamesystemDatanodeHelper.getDatanodeStats(fsn, live, dead);
	
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
    generateClusterReport(out, fsn, live);
    // Display node status
    StringBuilder sb = new StringBuilder();
    sb.append("<h3> DataNode Health: </h3>");
    InetSocketAddress nameNodeHttpAddress = nn.getHttpAddress();
    String toPointHtmlLinksTo = "http://" + NetUtils.toIpPort(nameNodeHttpAddress) + "/";
    
    
    sb.append("<div id=\"dfstable\"> <table border=1>\n" +
	       rowTxt() + colTxt() +
	       		"<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> " +
	       		colTxt() + "<b>" + live.size() + "</b>" +
	       rowTxt() + colTxt() + spaces(4) +
				 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=LIVE&status=NORMAL\">" +
				 "In Service</a> " +
				 colTxt() + colTxt() +
         (st.numLive - st.numLiveDecommissioned - st.numLiveDecommissioningInProgress) +
				 rowTxt() + colTxt() + spaces(4) +
       	 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=LIVE&status=EXCLUDED\">" +
         "Excluded</a> " +
       	 colTxt() + colTxt() + st.numLiveExcluded +
         rowTxt() + colTxt() + spaces(8) +
         "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=LIVE&status=DECOMMISSIONED\">" +
         "Decommission: Completed</a> " +
	       colTxt() + colTxt() + colTxt() + st.numLiveDecommissioned +
	       rowTxt() + colTxt() + spaces(8) +
				 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DECOMMISSIONING\">" +
				 "Decommission: In Progress</a> " +
				 colTxt() + colTxt() + colTxt() + st.numLiveDecommissioningInProgress +
				 rowTxt() + colTxt() +
       	 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> " +
       	 colTxt() + "<b>" + dead.size() + "</b>" +
         rowTxt() + colTxt() + spaces(4) +
       	 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DEAD&status=EXCLUDED\">" +
         "Excluded</a> " +
       	 colTxt() + colTxt() + st.numDeadExcluded +
         rowTxt() + colTxt() + spaces(8) +
         "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DEAD&status=DECOMMISSIONED\">" +
         "Decommission: Completed</a> " +
       	 colTxt() + colTxt() + colTxt() + st.numDeadDecommissioned +
         rowTxt() + colTxt() + spaces(8) +
       	 "<a href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DEAD&status=INDECOMMISSIONED\">" +
         "Decommission: Not Completed</a> " +
       	 colTxt() + colTxt() + colTxt() +
         (st.numDeadDecommissioningNotCompleted) +
         rowTxt() + colTxt() + spaces(4) +
       	 "<a class=\"warning\" " +
         "href=\"" + toPointHtmlLinksTo + "dfsnodelist.jsp?whatNodes=DEAD&status=ABNORMAL\">" +
         "Not Excluded</a> " +
       	 colTxt() + colTxt() +
         (dead.size() - st.numDeadExcluded) +
         "</table></div><br>\n" );
    
    if (live.isEmpty() && dead.isEmpty()) {
        sb.append("There are no datanodes in the cluster");
    }
    out.print("<div id=\"dfstable\"> <table>\n" +
              rowTxt() + colTxt() + sb.toString() +
                         colTxt() + jspHelper.generateWarningText(fsn) +
              "</table></div><br>\n" );
  }%>

<%
  NameNode nn = (NameNode)application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  
  String namenodeLabel = NameNode.getDefaultAddress(nn.getConf());
  String namenodeHttpAddress = NetUtils.toIpPort(nn.getHttpAddress());
%>


<%@page import="java.net.InetSocketAddress"%><html>

<link rel="stylesheet" type="text/css" href="static/hadoop.css">
<title>Hadoop NameNode <%=namenodeLabel%></title>
    
<body>

<table border="0">
<tr> 
	<td><img src="static/version.jpg" width="75" height="75" /></td>
	<td><h1>NameNode '<%=namenodeLabel%>'</h1></td>
</tr>
</table> 

<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getUrl()%>, r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by <%= VersionInfo.getUser()%>
<tr> <td id="col1"> Upgrades: <td> <%= jspHelper.getUpgradeStatusText()%>
<tr> <td id='col1'> Namespace ID: <td> <%= fsn.getNamespaceInfo().getNamespaceID()%>
</table></div><br>				      

<b><a href="http://<%=namenodeHttpAddress%>/nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<b><a href="http://<%=namenodeHttpAddress%>/logs/">Namenode Logs</a></b><br>
<b><a href="http://<%=namenodeHttpAddress%>/conf">HDFS Configuration</a></b>
<hr>
<h3>Cluster Summary</h3>
<b> <%= jspHelper.getSafeModeText()%> </b>
<b> <%= jspHelper.getInodeLimitText()%> </b>
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

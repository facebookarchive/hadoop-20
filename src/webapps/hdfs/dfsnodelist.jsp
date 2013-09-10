<%@ page
contentType="text/html; charset=UTF-8"
	import="java.io.*"
	import="java.net.InetAddress"
	import="java.net.InetSocketAddress"
	import="java.net.UnknownHostException"
	import="java.util.ArrayList"
	import="org.apache.hadoop.conf.Configuration"
	import="org.apache.hadoop.net.NetUtils"
	import="org.apache.hadoop.hdfs.*"
	import="org.apache.hadoop.hdfs.server.common.*"
	import="org.apache.hadoop.hdfs.server.namenode.*"
	import="org.apache.hadoop.hdfs.server.datanode.*"
	import="org.apache.hadoop.hdfs.protocol.*"
	import="org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates"
	import="org.apache.hadoop.util.*"
	import="java.text.DateFormat"
	import="java.lang.Math"
	import="java.net.URLEncoder"
	import="java.net.InetAddress"
%>
<%!
	JspHelper jspHelper = new JspHelper();

	int rowNum = 0;
	int colNum = 0;

	String rowTxt() { colNum = 0;
	return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
	+ "\"> "; }
	String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
	void counterReset () { colNum = 0; rowNum = 0 ; }

	long diskBytes = 1024 * 1024 * 1024;
	String diskByteStr = "GB";

	String sorterField = null;
	String sorterOrder = null;
	String whatNodes = "LIVE";
  String status = "ALL";

String NodeHeaderStr(String name) {
	String ret = "class=header";
	String order = "ASC";
	if ( name.equals( sorterField ) ) {
		ret += sorterOrder;
		if ( sorterOrder.equals("ASC") )
			order = "DSC";
	}
	ret += " onClick=\"window.document.location=" +
	"'dfsnodelist.jsp?whatNodes="+whatNodes+"&status="+status+
  "&sorter/field=" + name + "&sorter/order=" +
	order + "'\" title=\"sort on this column\"";

	return ret;
}

void generateDecommissioningNodeData(JspWriter out, DatanodeDescriptor d,
      String suffix, boolean alive, int nnHttpPort, String nnAddr) throws IOException {
    String url = "http://" + NetUtils.toIpPort(d.getHost(), d.getInfoPort())
      + "/browseDirectory.jsp?namenodeInfoPort=" + nnHttpPort + "&dir="
      + URLEncoder.encode("/", "UTF-8")
      + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);

    String name = NetUtils.toIpPort(d.getHost(), d.getInfoPort());
    int idx = (suffix != null && name.endsWith(suffix)) ? name
        .indexOf(suffix) : -1;

    out.print(rowTxt() + "<td class=\"name\"><a title=\"" + d.getHost() + ":"
        + d.getPort() + "\" href=\"" + url + "\">"
        + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
        + ((alive) ? "" : "\n"));
      if (!alive) {
        return;
      }

    long decommRequestTime = d.getStartTime();
    long timestamp = d.getLastUpdate();
    long currentTime = System.currentTimeMillis();
    long hoursSinceDecommStarted = (currentTime - decommRequestTime)/3600000;
    long remainderMinutes = ((currentTime - decommRequestTime)/60000) % 60;
    out.print("<td class=\"lastcontact\"> "
        + ((currentTime - timestamp) / 1000)
        + "<td class=\"underreplicatedblocks\">"
        + d.decommissioningStatus.getUnderReplicatedBlocks()
        + "<td class=\"blockswithonlydecommissioningreplicas\">"
        + d.decommissioningStatus.getDecommissionOnlyReplicas()
        + "<td class=\"underrepblocksinfilesunderconstruction\">"
        + d.decommissioningStatus.getUnderReplicatedInOpenFiles()
        + "<td class=\"timesincedecommissionrequest\">"
        + hoursSinceDecommStarted + " hrs " + remainderMinutes + " mins"
        + "\n");
}

enum NodeType {
	LIVE,
	DEAD
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
    	return (datanodeHostName.equals("")) ? datanodeHostName : "(" + datanodeHostName + ")";
    }
}

public void generateNodeData( JspWriter out, DatanodeDescriptor d,
		String suffix, NodeType nodeType,
		int nnHttpPort, String nnAddr, String url)
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

	if(url == null) {
		url = "http://" + NetUtils.toIpPort(d.getHost(), d.getInfoPort()) + "/browseDirectory.jsp?namenodeInfoPort=" +
			nnHttpPort + "&dir=" +
			URLEncoder.encode("/", "UTF-8") +
  			JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);
	}
	String name = NetUtils.toIpPort(d.getHost(), d.getInfoPort());
	int idx = (suffix != null && name.endsWith( suffix )) ?
			name.indexOf( suffix ) : -1;
	String datanodeAddress = (idx > 0) ? name.substring(0, idx) : name;
			out.print( rowTxt() + "<td class=\"name\"><a title=\""
					+ NetUtils.toIpPort(d.getHost(), d.getPort())+
					"\" href=\"" + url + "\">" + datanodeAddress + getDataNodeHostName(datanodeAddress)
					 + "</a>" +
					(( nodeType != NodeType.DEAD ) ? "" : "\n") );
			if (nodeType == NodeType.DEAD) {
                long deadTime = d.getStartTime();
                long currentTime = System.currentTimeMillis();
                long hoursSinceDead = (currentTime - deadTime)/3600000;
                long remainderMinutes = ((currentTime - deadTime)/60000) % 60;
                out.print("<td class=\"timesincedead\">"
                    + (deadTime > 0L ?
                       hoursSinceDead + " hrs " + remainderMinutes + " mins"
                       : "Never Alive")
                    + "\n");
                out.print("<td class=\"decommissioned\"> "
                    + d.isDecommissioned() + "\n");
                return;
            } else if (nodeType == NodeType.LIVE) {
			long c = d.getCapacity();
			long u = d.getDfsUsed();
			long nu = d.getNonDfsUsed();
			long r = d.getRemaining();
			String percentUsed = StringUtils.limitDecimalTo2(d.getDfsUsedPercent());    
			String percentRemaining = StringUtils.limitDecimalTo2(d.getRemainingPercent());    

      String adminState = d.getAdminState().toString();

			long timestamp = d.getLastUpdate();
			long currentTime = System.currentTimeMillis();
      long nsUsed = d.getNamespaceUsed();
      String percentNSUsed = StringUtils.limitDecimalTo2(d.getNamespaceUsedPercent());
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
					"\" class=\"blocks\">" + d.numBlocks() + "\n" +
          "<td align=\"right\" class=\"nsused\">" +
          StringUtils.limitDecimalTo2(nsUsed * 1.0 / diskBytes) +
          "<td align=\"right\" class=\"pcnsused\">" +
          percentNSUsed + "\n");
  }
}

public void generateDFSNodesList(JspWriter out, 
		NameNode nn,
		HttpServletRequest request)
throws IOException {
	ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();    
	ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
	jspHelper.DFSNodesStatus(live, dead);
  FSNamesystem fsn = nn.getNamesystem();

	whatNodes = request.getParameter("whatNodes"); // show only live or only dead nodes
  status = request.getParameter("status");
  if ( status == null )
    status = "ALL";
	sorterField = request.getParameter("sorter/field");
	sorterOrder = request.getParameter("sorter/order");
  String nnAddr = NetUtils.toIpPort(NameNode.getClientProtocolAddress(nn.getConf()));

	if ( sorterField == null )
		sorterField = "name";
	if ( sorterOrder == null )
		sorterOrder = "ASC";

	jspHelper.sortNodeList(live, sorterField, sorterOrder);

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

	try {
		Thread.sleep(1000);
	} catch (InterruptedException e) {}

	if (live.isEmpty() && dead.isEmpty()) {
		out.print("There are no datanodes in the cluster");
	}
	else {

		int nnHttpPort = nn.getHttpAddress().getPort();
		out.print( "<div id=\"dfsnodetable\"> ");
		if(whatNodes.equals("LIVE")) {
      ArrayList<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>();
      for (int i = 0; i < live.size(); i++) {
        boolean add = false;
        AdminStates state = live.get(i).getAdminState();
        if (status.equals("ALL")) {
          add = true;
        } else {
          boolean excluded = fsn.inExcludedHostsList(live.get(i), null);
          if (excluded) {
            if (status.equals("EXCLUDED")) {
              add = true;
            } else if (state == AdminStates.DECOMMISSIONED
              && status.equals("DECOMMISSIONED")) {
              add = true;
            }
          } else if (state == AdminStates.NORMAL
            && status.equals("NORMAL")) {
            add = true;
          }
        }
        if (add) {
          nodes.add(live.get(i));
        }
      }
			out.print( 
					"<a name=\"LiveNodes\" id=\"title\">" +
					"Live Datanodes : " + nodes.size() + "</a>" +
			"<br><br>\n<table border=1 cellspacing=0>\n" );

			counterReset();

			if ( nodes.size() > 0 ) {

				if ( live.get(0).getCapacity() > 1024 * diskBytes ) {
					diskBytes *= 1024;
					diskByteStr = "TB";
				}

				out.print( "<tr class=\"headerRow\"> <th " +
						NodeHeaderStr("name") + "> Node <th " +
						NodeHeaderStr("lastcontact") + "> Last <br>Contact <th " +
						NodeHeaderStr("adminstate") + "> Admin State <th " +
						NodeHeaderStr("capacity") + "> Configured <br>Capacity (" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("used") + "> Used <br>(" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("nondfsused") + "> Non DFS <br>Used (" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("remaining") + "> Remaining <br>(" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("pcused") + "> Used <br>(%) <th " + 
						NodeHeaderStr("pcused") + "> Used <br>(%) <th " +
						NodeHeaderStr("pcremaining") + "> Remaining <br>(%) <th " +
						NodeHeaderStr("blocks") + "> Namespace <br>Blocks <th " +
            NodeHeaderStr("nsused") + "> Namespace <br>Used (" +
            diskByteStr + ") <th " +
            NodeHeaderStr("pcnsused") +
            "> Namespace <br>Used (%)\n");

				jspHelper.sortNodeList(nodes, sorterField, sorterOrder);
				for ( int i=0; i < nodes.size(); i++ ) {
					generateNodeData(out, nodes.get(i), port_suffix, NodeType.LIVE, nnHttpPort, nnAddr, null);
				}
			}
			out.print("</table>\n");
		} else if (whatNodes.equals("DEAD")) {

      ArrayList<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>();
      for (int i=0; i<dead.size(); i++) {
        boolean add = false;
        AdminStates state = dead.get(i).getAdminState();
        if (status.equals("ALL")) {
          add = true;
        } else {
          boolean excluded = fsn.inExcludedHostsList(dead.get(i), null);
          if (excluded) {
            if (status.equals("EXCLUDED")) {
              add = true;
            } else if (state == AdminStates.DECOMMISSIONED
              && status.equals("DECOMMISSIONED")) {
              add = true;
            } else if (state != AdminStates.DECOMMISSIONED
              && status.equals("INDECOMMISSIONED")) {
              add = true;
            }
          } else if (status.equals("ABNORMAL")) {
            add = true;
          }
        }
        if (add) {
          nodes.add(dead.get(i));
        }
      }

			out.print("<br> <a name=\"DeadNodes\" id=\"title\"> " +
					" Dead Datanodes : " +nodes.size() + "</a><br><br>\n");

			if ( nodes.size() > 0 ) {
			    out.print("<table border=1 cellspacing=0> <tr class=\"headRow\"> "
                          + "<th " + NodeHeaderStr("name")
					  + "> Node <th " + NodeHeaderStr("timesincedeclaredasdead")
            + "> Time Since Declared As Dead <th " + NodeHeaderStr("decommissioned")
            + "> Decommissioned\n" );

				jspHelper.sortNodeList(nodes, "name", "ASC");
				for ( int i=0; i < nodes.size() ; i++ ) {
						String url = null;
						String toFormat = getDeadDataNodeUrl(nn.getConf());
						if (toFormat != null) {
							try {
								String hostNameToSubstitute = InetAddress.getByName(nodes.get(i).getHost()).getHostName();
								if(hostNameToSubstitute.endsWith(".")) {
									hostNameToSubstitute = hostNameToSubstitute.substring(0, hostNameToSubstitute.length() - 1);
								}
								url = String.format(toFormat, hostNameToSubstitute);
							} catch (UnknownHostException uhe) {
								url = null;
							}
						}
					generateNodeData(out, nodes.get(i), port_suffix, NodeType.DEAD, nnHttpPort, nnAddr, url);
				}

				out.print("</table>\n");
			}
		} else if (whatNodes.equals("DECOMMISSIONING")) {
			// Decommissioning Nodes
			ArrayList<DatanodeDescriptor> decommissioning = nn.getNamesystem()
			    .getDecommissioningNodesList();
			out.print("<br> <a name=\"DecommissioningNodes\" id=\"title\"> "
			    + " Decommissioning Datanodes : " + decommissioning.size()
                            + "</a><br><br>\n");
	                if (decommissioning.size() > 0) {
                          out.print("<table border=1 cellspacing=0> <tr class=\"headRow\"> "
                              + "<th " + NodeHeaderStr("name")
                              + "> Node <th " + NodeHeaderStr("lastcontact")
                              + "> Last <br>Contact <th "
                              + NodeHeaderStr("underreplicatedblocks")
                              + "> Under Replicated Blocks <th "
                              + NodeHeaderStr("blockswithonlydecommissioningreplicas")
		              + "> Blocks With No <br> Live Replicas <th "
	                      + NodeHeaderStr("underrepblocksinfilesunderconstruction") 
	                      + "> Under Replicated Blocks <br> In Files Under Construction"
	                      + " <th " + NodeHeaderStr("timesincedecommissionrequest")
	                      + "> Time Since Decommissioning Started"
                          );
                          jspHelper.sortNodeList(decommissioning, "name", "ASC");
                          for (int i = 0; i < decommissioning.size(); i++) {
                            generateDecommissioningNodeData(out, decommissioning.get(i),
                                port_suffix, true, nnHttpPort, nnAddr);
                          }
                          out.print("</table>\n");
                        }
                        out.print("</div>");
                  }
	}
}

	private static String getDeadDataNodeUrl(Configuration conf) {
		return conf.get(FSConstants.DEAD_DATANODE_URL);
	}
	
%>

<%
NameNode nn = (NameNode)application.getAttribute("name.node");
FSNamesystem fsn = nn.getNamesystem();
String namenodeLabel = NameNode.getDefaultAddress(nn.getConf());
%>

<html>

<link rel="stylesheet" type="text/css" href="static/hadoop.css">
<title>Hadoop NameNode <%=namenodeLabel%></title>
  
<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>


<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getVersion()%>, r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by <%= VersionInfo.getUser()%>
<tr> <td id="col1"> Upgrades: <td> <%= jspHelper.getUpgradeStatusText()%>
</table></div><br>				      

<b><a href="nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<b><a href="logs/">Namenode Logs</a></b><br>
<b><a href=dfshealth.jsp> Go back to DFS home</a></b>
<hr>
<%
	generateDFSNodesList(out, nn, request); 
%>

<%
out.println(ServletUtil.htmlFooter());
%>

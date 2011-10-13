<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.net.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.io.*"
  import="org.apache.hadoop.conf.*"
  import="org.apache.hadoop.net.DNS"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
%>
<%!
  static JspHelper jspHelper = new JspHelper();
  
  public void generateDirectoryStructure( JspWriter out, 
                                          HttpServletRequest req,
                                          HttpServletResponse resp) 
    throws IOException {
    String dir = req.getParameter("dir");
    if (dir == null || dir.length() == 0) {
      out.print("Invalid input");
      return;
    }
    boolean showUsage = false;
    if (req.getParameter("showUsage") != null) {
      showUsage = true;
    }
    
    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    DFSClient dfs = null;
    try {
      dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
    } catch (IOException ex) {
      // This probably means that the namenode is not out of safemode
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    }
    String target = dir;
    if (!dfs.exists(target)) {
      out.print("<h3>File or directory : " + target + " does not exist</h3>");
      JspHelper.printGotoForm(out, namenodeInfoPort, target);
    }
    else {
      if( !dfs.isDirectory(target) ) { // a file
        List<LocatedBlock> blocks = 
          dfs.namenode.getBlockLocations(dir, 0, 1).getLocatedBlocks();
	      
        LocatedBlock firstBlock = null;
        DatanodeInfo [] locations = null;
        if (blocks.size() > 0) {
          firstBlock = blocks.get(0);
          locations = firstBlock.getLocations();
        }
        if (locations == null || locations.length == 0) {
          out.print("Empty file");
        } else {
          DatanodeInfo chosenNode = jspHelper.bestNode(firstBlock);
          String fqdn = InetAddress.getByName(chosenNode.getHost()).
            getCanonicalHostName();
          String datanodeAddr = chosenNode.getName();
          int datanodePort = Integer.parseInt(
                                              datanodeAddr.substring(
                                                                     datanodeAddr.indexOf(':') + 1, 
                                                                     datanodeAddr.length())); 
          String redirectLocation = "http://"+fqdn+":" +
            chosenNode.getInfoPort() + 
            "/browseBlock.jsp?blockId=" +
            firstBlock.getBlock().getBlockId() +
            "&blockSize=" + firstBlock.getBlock().getNumBytes() +
            "&genstamp=" + firstBlock.getBlock().getGenerationStamp() +
            "&filename=" + URLEncoder.encode(dir, "UTF-8") + 
            "&datanodePort=" + datanodePort + 
            "&namenodeInfoPort=" + namenodeInfoPort;
          resp.sendRedirect(redirectLocation);
        }
        return;
      }
      // directory
      FileStatus[] files = dfs.listPaths(target);
      //generate a table and dump the info
      List<String> headingList = new ArrayList<String>();
      headingList.add("Name");
      headingList.add("Type");
      headingList.add("Size");
      if (showUsage) {
        headingList.add("Space Consumed");
      }
      headingList.add("Replication");
      headingList.add("Block Size");
      headingList.add("Modification Time");
      headingList.add("Permission");
      headingList.add("Owner");
      headingList.add("Group" );
      String[] headings = headingList.toArray(new String[headingList.size()]);
      out.print("<h3>Contents of directory ");
      JspHelper.printPathWithLinks(dir, out, namenodeInfoPort);
      out.print("</h3><hr>");
      JspHelper.printGotoForm(out, namenodeInfoPort, dir);
      out.print("<hr>");
	
      File f = new File(dir);
      String parent;
      if ((parent = f.getParent()) != null)
        out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent +
                  "&namenodeInfoPort=" + namenodeInfoPort +
                  (showUsage ? "&showUsage=1" : "") +
                  "\">Go to parent directory</a><br>");
	
      if (files == null || files.length == 0) {
        out.print("Empty directory");
      }
      else {
        jspHelper.addTableHeader(out);
        int row=0;
        jspHelper.addTableRow(out, headings, row++);
        String cols [] = new String[headings.length];
        for (int i = 0; i < files.length; i++) {
          int colIdx = 0;
          //Get the location of the first block of the file
          if (files[i].getPath().toString().endsWith(".crc")) continue;
          String datanodeUrl = req.getRequestURL()+"?dir="+
              URLEncoder.encode(files[i].getPath().toString(), "UTF-8") + 
              "&namenodeInfoPort=" + namenodeInfoPort +
              (showUsage ? "&showUsage=1" : "");
          cols[colIdx++] = "<a href=\""+datanodeUrl+"\">"+files[i].getPath().getName()+"</a>";
          if (!files[i].isDir()) {
            cols[colIdx++] = "file";
            cols[colIdx++] = StringUtils.byteDesc(files[i].getLen());
            if (showUsage) {
              cols[colIdx++] = StringUtils.byteDesc(files[i].getLen() * files[i].getReplication());
            }
            cols[colIdx++] = Short.toString(files[i].getReplication());
            cols[colIdx++] = StringUtils.byteDesc(files[i].getBlockSize());
          }
          else {
            cols[colIdx++] = "dir";
            if (showUsage) {
              ContentSummary cs = dfs.getContentSummary(files[i].getPath().toUri().getPath());
              cols[colIdx++] = StringUtils.byteDesc(cs.getLength());
              cols[colIdx++] = StringUtils.byteDesc(cs.getSpaceConsumed());
            } else {
              cols[colIdx++] = "";
            }
            cols[colIdx++] = "";
            cols[colIdx++] = "";
          }
          cols[colIdx++] = FsShell.dateForm.format(new Date((files[i].getModificationTime())));
          cols[colIdx++] = files[i].getPermission().toString();
          cols[colIdx++] = files[i].getOwner();
          cols[colIdx++] = files[i].getGroup();
          jspHelper.addTableRow(out, cols, row++);
        }
        jspHelper.addTableFooter(out);
      }
    } 
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

%>

<html>
<head>
<style type=text/css>
<!--
body 
  {
  font-face:sanserif;
  }
-->
</style>
<%JspHelper.createTitle(out, request, request.getParameter("dir")); %>
</head>

<body onload="document.goto.dir.focus()">
<% 
  try {
    generateDirectoryStructure(out,request,response);
  }
  catch(IOException ioe) {
    String msg = ioe.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    out.print("<h3>" + msg + "</h3>");
  }
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>

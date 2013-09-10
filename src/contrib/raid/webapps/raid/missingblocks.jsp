<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.io.InputStreamReader"
  import="java.util.*"
  import="org.apache.hadoop.raid.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.DistributedFileSystem.*"
%>

<%!
  String error = null;
  public BufferedReader runFsck(RaidNode raidNode) throws Exception {
    try {
      String dir = "/";
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(bout, true);
      RaidShell shell = new RaidShell(raidNode.getConf(), ps);
      int res = ToolRunner.run(shell, new String[]{"-fsck", dir, "-count", 
          "-retNumStrpsMissingBlks"});
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      shell.close();
      return new BufferedReader(new InputStreamReader(bin));
    } catch (Exception e) {
      error = e.getMessage();
      return null;
    }
  }
%>
<%
  RaidNode raidNode = (RaidNode) application.getAttribute("raidnode");
  String name = raidNode.getHostName();
  name = name.substring(0, name.indexOf(".")).toUpperCase();
%>

<html>
  <head>
    <title><%=name %> Hadoop RaidNode Administration</title>
    <link rel="stylesheet" type="text/css" href="static/hadoop.css">
  </head>
<body>
<h1><%=name %> Hadoop RaidNode Administration</h1>
<b>Started:</b> <%= new Date(raidNode.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
<hr>
<% 
out.print("<h2>Missing Blocks</h2>");
%>

<%
  BufferedReader reader = runFsck(raidNode);
  if (error != null) {
%>
    <%=error%> <br>
<%
  } else {
    String line = null;
    String numCorruptedFiles = null;
    String numMissingBlkFiles = null;
    String nonRaidedCorruptFiles = null;
    Map<String, long[]> numStrpWithMissingBlksMap = 
        new HashMap<String, long[]> ();
        
    if (reader != null) {
      numCorruptedFiles = reader.readLine();
      numMissingBlkFiles = reader.readLine();
      nonRaidedCorruptFiles = reader.readLine();
      
      for (Codec codec : Codec.getCodecs()) {
        numStrpWithMissingBlksMap.put(codec.id, 
            new long[codec.stripeLength + codec.parityLength]);
      }
      for (int i = 0; i < Codec.getCodecs().size(); i++) {
        String codecId = reader.readLine();
        if (codecId == null) {
          break;
        }
        
        Codec codec = Codec.getCodec(codecId);
        long[] incNumStrpWithMissingBlks = 
            new long[codec.stripeLength + codec.parityLength];
        for(int j = 0; j < incNumStrpWithMissingBlks.length; j++){
          line = reader.readLine();
          if (line == null) {
            throw new IOException("Raidfsck did not print the array " +
                "for number stripes with missing blocks for index " + j);
          }
          incNumStrpWithMissingBlks[j] = Long.parseLong(line);
        }
        numStrpWithMissingBlksMap.put(codec.id, 
            incNumStrpWithMissingBlks);
      }
      reader.close();
      out.print(RaidUtils.getMissingBlksHtmlTable(
          Long.parseLong(nonRaidedCorruptFiles),
          numStrpWithMissingBlksMap));
    }
%>
    <p>
      <%=numCorruptedFiles%> Raid unrecoverable files.
    </p>
    <p>
      <b>Total:</b> <%=numMissingBlkFiles%> files have at least one missing/corrupt blocks.
    </p>
<%  
  }
%>
<%
out.println(ServletUtil.htmlFooter());
%>

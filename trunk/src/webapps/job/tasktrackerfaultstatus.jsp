<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JobTracker.FaultInfo"
  import="org.apache.hadoop.mapred.JobTracker.JobFault"
  import="org.apache.hadoop.util.*"
%>
<%
  JobTracker jt = (JobTracker) application.getAttribute("job.tracker");
  String jtName = 
           StringUtils.simpleHostname(jt.getJobTrackerMachine());
  String ttName = request.getParameter("host");
%>
<%!
  private static void printTitle(JspWriter out, String hostname, 
                                 boolean blacklisted) 
      throws IOException {
      
    out.print("<h1>" + hostname + " status: ");
    
    if (blacklisted) {
      out.print("<font color='#A00000'>blacklisted</font>");
    } else {
      out.print("<font color='#00A000'>not blacklisted</font>");
    }
    
    out.println("</h1><hr><br>\n\n");
  }
  
  private static void printJobFailureTable(JspWriter out, JobFault[] faults) 
      throws IOException {
    
    out.println("<h2>Cause: <u>job failures</u></h2>");
    out.println("<table id=\"JobFailureTable\" border=2 cellpadding=5>");
    out.println("<tr><th>Job</th><th># Failures</th>"
        + "<th colspan=10000 align=left>Task Failures</th></tr>");
    
    int i = 0;
    //for (Map.Entry<String,List<String>> e : trackerFaults.entrySet()) throws IOException {
    for (JobFault fault : faults) {
      String jobName = fault.job;
      int numFailures = fault.taskExceptions.length;
      
      out.println("<tr class=\"failure\">");
      out.println("<td>" + jobName + "</td><td align=right>" + 
          numFailures + "</td>");
      
      for (String ex : fault.taskExceptions) {
        out.println("<td>");
        out.println("<div class=\"swapable open\">" + 
            getEncodedSummaryLine(ex) + "</div>");
        out.println("<div hidden><pre>" + getEncoding(ex) + 
            "</pre><div class=\"swapable close\"><br>(collapse)</div></div>");
        out.println("</td>");
      }
      out.println("</tr>");
    }
    out.println("</table><br>\n\n");
  }
  
  private static void printMarkedUnhealthy(JspWriter out) throws IOException {
    out.println("<h2>Cause: <u>marked unhealthy</u></h2><br>\n\n");
  }
  
  private static String getEncoding(String text) throws IOException {
    // Replace \n by <br> and make JS-friendly
    return text.replace("\n","<br>").replace("'","\\\'");
  }
  
  private static String getEncodedSummaryLine(String text) throws IOException {
    return getEncoding(text.split("\n")[0]);
  }
  
  public static void printHTMLDocument(JspWriter out, String hostname, 
                                       JobTracker jt)
      throws IOException {
    
    FaultInfo fi = 
        jt.faultyTrackers.getFaultInfo(hostname, false);
    
    boolean blacklisted = ((fi != null) ? fi.isBlacklisted() : false);
    boolean healthy = ((fi != null) ? fi.isHealthy() : true);
    
    printTitle(out, hostname, blacklisted);
    if (blacklisted) {
        printJobFailureTable(out, fi.getFaults());
    }
    if (!healthy) {
      printMarkedUnhealthy(out);
    }
    
    out.flush();
  }
%>

<html>
<head>
<link rel="stylesheet" type="text/css" href="http://mrtest.data.facebook.com:50030/static/hadoop.css">

<style type="text/css">
pre { font: 11pt consolas; }
tr.failure { font: 11pt consolas; vertical-align: text-top; }
.swapable { cursor: pointer; }
.swapable:hover { text-decoration: underline; }
.swapable.open { color: #A00000; }
.swapable.close { font: 8pt times; color: #0000FF; }
</style>

<script type="text/javascript">
function getSummaryLine (text) {
  return text.split('<br>')[0];
}
document.onclick = function(event) { 
   cn = event.target.className; 
   if (cn === 'swapable open') { 
       children = event.target.parentNode.children;
       for (i in children) {
           children[i].hidden = !children[i].hidden;
       }
   } else if (cn === 'swapable close') {
       children = event.target.parentNode.parentNode.children;
       for (i in children) {
           children[i].hidden = !children[i].hidden;
       }
   }
}
</script>
<title>Blacklist status: <%=ttName%></title>
</head>
<body>

<%
    try {
        printHTMLDocument(out, ttName, jt);
    } catch (Exception ex) {
        ex.printStackTrace(new PrintWriter(out));
    }
%>

</body>
</html>
package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import java.util.Collection;
import java.util.Arrays;

public final class corrupt_005ffiles_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

//for java.io.Serializable
  private static final long serialVersionUID = 1L;
  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write('\n');

  /*
   * Licensed to the Apache Software Foundation (ASF) under one
   * or more contributor license agreements.  See the NOTICE file
   * distributed with this work for additional information
   * regarding copyright ownership.  The ASF licenses this file
   * to you under the Apache License, Version 2.0 (the
   * "License"); you may not use this file except in compliance
   * with the License.  You may obtain a copy of the License at
   *
   *     http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */

      out.write('\n');
      out.write('\n');
      out.write('\n');

  NameNode nn = (NameNode) application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  String namenodeLabel = nn.getNameNodeAddress().getHostName() + ":"
      + nn.getNameNodeAddress().getPort();
  Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks = 
	fsn.listCorruptFileBlocks("/", null);
  int corruptFileCount = corruptFileBlocks.size();

      out.write("\n\n<html>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>Hadoop ");
      out.print(namenodeLabel);
      out.write("</title>\n<body>\n<h1> '");
      out.print(namenodeLabel);
      out.write("'</h1>\n<br>\n<b><a href=\"/nn_browsedfscontent.jsp\">Browse the filesystem</a></b>\n<br>\n<b><a href=\"/logs/\">Logs</a></b>\n<br>\n<b><a href=/dfshealth.jsp> Go back to DFS home</a></b>\n<hr>\n<h3>Reported Corrupt Files</h3>\n");

  if (corruptFileCount == 0) {

      out.write("\n    <i>No missing blocks found at the moment.</i> <br>\n    Please run fsck for a thorough health analysis.\n");

  } else {
    for (FSNamesystem.CorruptFileBlockInfo c : corruptFileBlocks) {
      String currentFileBlock = c.toString();

      out.write("\n      ");
      out.print(currentFileBlock);
      out.write("<br>\n");

    }

      out.write("\n    <p>\n      <b>Total:</b> At least ");
      out.print(corruptFileCount);
      out.write(" corrupt file(s)\n    </p>\n");

  }

      out.write('\n');
      out.write('\n');

  out.println(ServletUtil.htmlFooter());

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}

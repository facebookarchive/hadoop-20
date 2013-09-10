/**
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
package org.apache.hadoop.http;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.resource.Resource;
import org.mortbay.util.StringUtil;
import org.mortbay.util.URIUtil;

/**
 * Instead of using absolute path in the href link,
 * we use relative path
 */

public class StaticServlet extends DefaultServlet {
  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(StaticServlet.class);
  
  protected void sendDirectory(HttpServletRequest request,
    HttpServletResponse response, Resource resource, boolean parent)
    throws IOException
  {
    byte[] data=null;
    String base = URIUtil.addPaths(request.getRequestURI(),URIUtil.SLASH);
    // Use customized getListHTML
    String dir = getListHTML(resource, base, parent);
    if (dir==null)
    {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "No directory");
      return;
    }
    data=dir.getBytes("UTF-8");
    response.setContentType("text/html; charset=UTF-8");
    response.setContentLength(data.length);
    response.getOutputStream().write(data);
  }
  
  private static String deTag(String raw) 
  {
      return StringUtil.replace( StringUtil.replace(raw,"<","&lt;"), ">", "&gt;");
  }
  
  /**
   * Defang any characters that could break the URI string in an HREF.
   * Such as <a href="/path/to;<script>Window.alert("XSS"+'%20'+"here");</script>">Link</a>
   * 
   * The above example would parse incorrectly on various browsers as the "<" or '"' characters
   * would end the href attribute value string prematurely.
   * 
   * @param raw the raw text to defang.
   * @return the defanged text.
   */
  private static String defangURI(String raw) 
  {
      StringBuffer buf = null;
      
      if (buf==null)
      {
          for (int i=0;i<raw.length();i++)
          {
              char c=raw.charAt(i);
              switch(c)
              {
                  case '\'':
                  case '"':
                  case '<':
                  case '>':
                      buf=new StringBuffer(raw.length()<<1);
                      break;
              }
          }
          if (buf==null)
              return raw;
      }
      
      for (int i=0;i<raw.length();i++)
      {
          char c=raw.charAt(i);       
          switch(c)
          {
            case '"':
                buf.append("%22");
                continue;
            case '\'':
                buf.append("%27");
                continue;
            case '<':
                buf.append("%3C");
                continue;
            case '>':
                buf.append("%3E");
                continue;
            default:
                buf.append(c);
                continue;
          }
      }

      return buf.toString();
  }
  
  public String getListHTML(Resource resource, String base, boolean parent)
      throws IOException
  {
      base=URIUtil.canonicalPath(base);
      if (base==null || !resource.isDirectory())
        return null;
      
      String[] ls = resource.list();
      if (ls==null)
          return null;
      Arrays.sort(ls);
      
      String decodedBase = URIUtil.decodePath(base);
      String title = "Directory: "+deTag(decodedBase);

      StringBuffer buf=new StringBuffer(4096);
      buf.append("<HTML><HEAD><TITLE>");
      buf.append(title);
      buf.append("</TITLE></HEAD><BODY>\n<H1>");
      buf.append(title);
      buf.append("</H1>\n<TABLE BORDER=0>\n");
      
      if (parent)
      {
          buf.append("<TR><TD><A HREF=\"");
          buf.append(URIUtil.addPaths(base,"../"));
          buf.append("\">Parent Directory</A></TD><TD></TD><TD></TD></TR>\n");
      }
      
      String defangedBase = defangURI(base);
      
      DateFormat dfmt=DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
                                                     DateFormat.MEDIUM);
      for (int i=0 ; i< ls.length ; i++)
      {
          Resource item = resource.addPath(ls[i]);
          
          buf.append("\n<TR><TD><A HREF=\"");
          String path=URIUtil.addPaths(defangedBase,URIUtil.encodePath(ls[i]));
          buf.append(URIUtil.encodePath(ls[i]));
          
          if (item.isDirectory() && !path.endsWith("/"))
              buf.append(URIUtil.SLASH);
          
          // URIUtil.encodePath(buf,path);
          buf.append("\">");
          buf.append(deTag(ls[i]));
          buf.append("&nbsp;");
          buf.append("</TD><TD ALIGN=right>");
          buf.append(item.length());
          buf.append(" bytes&nbsp;</TD><TD>");
          buf.append(dfmt.format(new Date(item.lastModified())));
          buf.append("</TD></TR>");
      }
      buf.append("</TABLE>\n");
      buf.append("</BODY></HTML>\n");
      
      return buf.toString();
  }
}

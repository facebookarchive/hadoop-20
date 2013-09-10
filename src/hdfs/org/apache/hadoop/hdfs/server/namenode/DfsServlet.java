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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A base class for the servlets in DFS.
 */
abstract class DfsServlet extends HttpServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;
  
  private ClientProtocol nnProxy = null;

  static final Log LOG = LogFactory.getLog(DfsServlet.class.getCanonicalName());

  /** Get {@link UserGroupInformation} from request */
  protected UnixUserGroupInformation getUGI(HttpServletRequest request) {
    String ugi = request.getParameter("ugi");
    try {
      return new UnixUserGroupInformation(ugi.split(","));
    }
    catch(Exception e) {
      LOG.warn("Invalid ugi (= " + ugi + ")");
    }
    return JspHelper.webUGI;
  }

  /**
   * Create a {@link NameNode} proxy from the current {@link ServletContext}. 
   */
  protected synchronized ClientProtocol createNameNodeProxy(UnixUserGroupInformation ugi
      ) throws IOException {
    if (nnProxy != null) {
      return nnProxy;
    }
    ServletContext context = getServletContext();
    InetSocketAddress nnAddr = (InetSocketAddress)context.getAttribute("name.node.address");
    if (nnAddr == null) {
      throw new IOException("The namenode is not out of safemode yet");
    }
    Configuration conf = new Configuration(
        (Configuration)context.getAttribute("name.conf"));
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    nnProxy = DFSClient.createNamenode(nnAddr, conf);
    return nnProxy;
  }

  /** Create a URI for redirecting request to a datanode */
  protected URI createRedirectUri(String servletpath, UserGroupInformation ugi,
      DatanodeID host, HttpServletRequest request, NameNode nn) throws URISyntaxException {
    final String hostname = host instanceof DatanodeInfo?
        ((DatanodeInfo)host).getHostName(): host.getHost();
    final String scheme = request.getScheme();
    final int port = "https".equals(scheme)?
        (Integer)getServletContext().getAttribute("datanode.https.port")
        : host.getInfoPort();
    // Add namenode address to the URL params
    final String nnAddr = NetUtils.toIpPort(nn.getNameNodeAddress());
    final String filename = request.getPathInfo();
    return new URI(scheme, null, hostname, port, servletpath,
        "filename=" + filename + "&ugi=" + ugi +
        JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr), null);
  }

  /** Get filename from the request */
  protected String getFilename(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    final String filename = request.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      throw new IOException("Invalid filename");
    }
    return filename;
  }
  
  /** Create a URI for streaming a file */
  protected URI createUri(String file,
      DatanodeID[] candidates,  UnixUserGroupInformation ugi,
      HttpServletRequest request) throws URISyntaxException {
    String scheme = request.getScheme();
    final DatanodeID host = candidates[0];
    final String hostname;
    if (host instanceof DatanodeInfo) {
      hostname = ((DatanodeInfo)host).getHostName();
    } else {
      hostname = host.getHost();
    }
    
    // Construct query.
    StringBuilder builder = new StringBuilder();
    builder.append("ugi=" + ugi);
    
    // Populate the rest of parameters.
    Enumeration<?> it = request.getParameterNames();
    while (it.hasMoreElements()) {
      String key = it.nextElement().toString();
      String value = request.getParameter(key);
      builder.append("&" + key + "=" + value);
    }

    // Construct the possible candidates for retry
    if (candidates.length > 1) {
      builder.append("&candidates=");
      appendDatanodeID(builder, candidates[1]);
      for (int j=2; j<candidates.length; j++) {
        builder.append(" ");
        appendDatanodeID(builder, candidates[j]);
      }
    }

    // Add namenode address to the url params
    NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
    String addr = NetUtils.toIpPort(nn.getNameNodeAddress());
    builder.append(JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr));

    return new URI(scheme, null, hostname,
        "https".equals(scheme)
          ? (Integer)getServletContext().getAttribute("datanode.https.port")
          : host.getInfoPort(),
        "/streamFile" + file, builder.toString(), null);
  }

  private static void appendDatanodeID(StringBuilder builder, 
      DatanodeID candidate) {
    builder.append(candidate.getHost());
    builder.append(":");
    builder.append(candidate.getInfoPort());    
  }
}

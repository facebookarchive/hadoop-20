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

import javax.servlet.*;
import javax.servlet.http.*;

import java.io.*;
import java.net.*;
import java.util.List;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.conf.*;

public class StreamFile extends DfsServlet {
  private static final Configuration masterConf = new Configuration();
  
  /** getting a client for connecting to dfs */
  protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {
    Configuration conf = new Configuration(masterConf);
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, getUGI(request));
    return JspHelper.getDFSClient(request, conf);
  }
  
  /** Get the datanode candidates from the request */
  private DatanodeID[] getDatanodes(HttpServletRequest request)
  throws IOException {
    final String datanodes = request.getParameter("candidates");
    if (datanodes == null) {
      return null;
    }
    final String[] datanodeStrs = datanodes.split(" ");
    if (datanodeStrs.length == 0) {
      return null;
    }
    final DatanodeID[] dnIDs = new DatanodeID[datanodeStrs.length];
    for (int i=0; i<dnIDs.length; i++) {
      String hostName = datanodeStrs[i];
      int colon = datanodeStrs[i].indexOf(":");
      if (colon < 0) {
        throw new IOException("Invalid datanode name " + 
            datanodeStrs[i] + ", expecting name:port pair");
      } 
      hostName = datanodeStrs[i].substring(0, colon);
      int infoPort;
      try {
        infoPort = Integer.parseInt(datanodeStrs[i].substring(colon+1));
      } catch (NumberFormatException ne) {
        throw new IOException("Invalid datanode name " + 
            datanodeStrs[i] + ", expecting name:port pair", ne);
      }
      dnIDs[i] = new DatanodeID(hostName, null, infoPort, -1);
    }
    return dnIDs;
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final String filename = request.getParameter("filename") != null ?
                            request.getParameter("filename") :
                            request.getPathInfo();

    String posStr = request.getParameter("seek");
    Long pos = posStr == null ? null : Long.valueOf(posStr);
    if (filename == null || filename.length() == 0) {
      DataNode.LOG.info("Invalid input");
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    DFSClient dfs = null;
    DFSInputStream in = null;
    OutputStream os = null;
    try {
      dfs = getDFSClient(request);
    } catch (IOException e) {
      // Can not connect to NN; redirect to a different datanode
      List<DatanodeID> candidates = JspHelper.bestNode(
          getDatanodes(request), false);
      try {
        response.sendRedirect(
            createUri(filename, 
                      candidates.toArray(new DatanodeID[candidates.size()]),
                      getUGI(request),
                      request).toURL().toString());
        return;
      } catch (URISyntaxException ue) {
        throw new ServletException(ue); 
      }
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
      return;
    }
    in = dfs.open(filename);
    long contentLength = in.getFileLength();
    if (pos != null) {
      contentLength -= pos;
      in.seek(pos);
    }
    
    os = response.getOutputStream();
    response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                       filename + "\"");
    response.setHeader("isUnderConstruction",
                       in.isUnderConstruction() ? "true" : "false");
    response.setContentType("application/octet-stream");
    response.setHeader(
      HftpFileSystem.CONTENT_LENGTH_FIELD, 
      String.valueOf(contentLength)
    );
    
    byte buf[] = new byte[4096];
    try {
      int bytesRead;
      while ((bytesRead = in.read(buf)) != -1) {
        os.write(buf, 0, bytesRead);
      }
    } catch (IOException ioe) {
      DataNode.LOG.warn("Failed to server request: " + request, ioe);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(os);
      if(dfs!=null) dfs.close();
    }
  }
}

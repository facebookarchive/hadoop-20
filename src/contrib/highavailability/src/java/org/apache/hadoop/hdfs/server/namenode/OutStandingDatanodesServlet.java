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

import java.util.*;
import java.io.*;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.StandbySafeMode;
import org.apache.hadoop.util.StringUtils;

public class OutStandingDatanodesServlet extends HttpServlet {

  private static String standbyNull = "<br> <a class= \"warning\" " +
    "name=\"NotStandbyNull\" id=\"title\"> This is the Standby Avatar, " +
    "but safe mode information is null. This could happen because we have " +
    "just left safemode during failover </a><br><br>\n";
  private static String notStandby = "<br> <a class= \"warning\" " +
    "name=\"NotStandby\" id=\"title\"> This is not the Standby Avatar " +
    "</a><br><br>\n";

  @SuppressWarnings("unchecked")
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    ServletContext context = getServletContext();
    AvatarNode nn = (AvatarNode)context.getAttribute("avatar.node");
    FSNamesystem fsn = nn.getNamesystem();
    boolean isStandby = false;
    boolean isFailover = false;
    Set <DatanodeID> heartbeats = null;
    Set <DatanodeID> reports = null;
    String msg = null;
    fsn.writeLock();
    // We need to capture all information under a namesystem lock, since we
    // might leave safemode and the safemode object might be set to null.
    try {
      isStandby = (nn.reportAvatar() == Avatar.STANDBY);
      StandbySafeMode sm = nn.getStandbySafeMode();
      if (sm == null && isStandby) {
        msg = standbyNull;
      } else if (sm == null) {
        msg = notStandby;
      } else {
        isFailover = sm.failoverInProgress();
        heartbeats = sm.getOutStandingHeartbeats();
        reports = sm.getOutStandingReports();
      }
    } finally {
      fsn.writeUnlock();
    }

    // We print the information separately so that we can release the namesystem
    // lock quickly.
    PrintWriter out = null;
    try {
      out = response.getWriter();
      if (msg != null) {
        out.print(msg);
        return;
      }
      if (!isStandby) {
        out.print(notStandby);
        return;
      } else if (!isFailover) {
        out.print("<br> <a class = \"warning\" name=\"NotStandbyFailover\"" +
            " id=\"title\"> The Standby Avatar is not in failover mode" +
            " </a><br><br>\n");
        return;
      }
      out.print("<br> <a name=\"OutStandingHeartbeats\" id=\"title\"> " +
          " Following datanodes have outstanding heartbeats : </a><br><br>\n");
      for (DatanodeID node : heartbeats) {
        out.print("<br>" + node.getName() + "<br>");
      }
      out.print("<br> <a name=\"OutStandingReports\" id=\"title\"> " +
          " Following datanodes have outstanding reports : </a><br><br>\n");
      for (DatanodeID node : reports) {
        out.print("<br>" + node.getName() + "<br>");
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}

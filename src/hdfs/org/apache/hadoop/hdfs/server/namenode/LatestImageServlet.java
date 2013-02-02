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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;

import org.mortbay.util.ajax.JSON;

public class LatestImageServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;


  public void doGet(HttpServletRequest request,
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    final ServletContext context = getServletContext();
    final NameNode nn = (NameNode)context.getAttribute("name.node");
    final FSNamesystem fsn = nn.getNamesystem();
    final FSImage fsimage = fsn.getFSImage();
    long lastCheckpointTxId;

    
    // construct the map of returned images
    // txid -> list of images -> each image described by a property map
    final Map<Long, List<Map<String, String>>> outputMap 
      = new HashMap<Long, List<Map<String, String>>>();
    Map<File, StorageLocationType> images = null;

    fsn.writeLock();
    try {
      // for now, we support only the latest checkpoint
      lastCheckpointTxId = fsimage.storage.getMostRecentCheckpointTxId();
      images = fsimage.storage.getImages(lastCheckpointTxId);
    } finally {
      fsn.writeUnlock();
    }
    
    // for each image, construct its properties
    List<Map<String, String>> imageList = new ArrayList<Map<String, String>>();
    for(File f : images.keySet()) {
      Map<String, String> imageProperties = new HashMap<String, String>();
      imageProperties.put("name", f.getPath());
      imageProperties.put("timestamp", Long.toString(f.lastModified()));
      imageProperties.put("type", images.get(f).toString());
      imageList.add(imageProperties);
    }
    
    // for now, we support only the latest checkpoint
    outputMap.put(lastCheckpointTxId, imageList);

    // construct JSON
    String output = JSON.toString(outputMap);

    // We print the information separately so that we can release the namesystem
    // lock quickly.
    PrintWriter out = null;
    try {
      out = response.getWriter();
      out.write(output);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}

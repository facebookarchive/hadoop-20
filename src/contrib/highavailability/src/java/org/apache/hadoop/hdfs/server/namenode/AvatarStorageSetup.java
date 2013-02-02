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
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode.StartupInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;

public class AvatarStorageSetup {
  
  public static final Log LOG = LogFactory.getLog(AvatarStorageSetup.class.getName());

  static void validate(Configuration conf, 
      Collection<URI> namedirs, Collection<URI> editsdir, 
      URI img0, URI img1, URI edit0, URI edit1) throws IOException {
    
    LOG.info("Avatar conf validation - namedirs: " + namedirs);
    LOG.info("Avatar conf validation - editdirs: " + editsdir);
    LOG.info("Avatar conf validation - shared namedirs: " + img0 + ", " + img1);
    LOG.info("Avatar conf validation - shared editdirs: " + edit0 + ", " + edit1);

    String msg = "";
    
    if (conf == null) {
      msg = "Configuration should not be null. ";
    }
    
    if (img0 == null || img1 == null || edit0 == null || edit1 == null) {
      msg = "Configuration does not contain shared locations for image and edits. ";
    }

    // verify that the shared image dirctories are not specified as dfs.name.dir
    msg += checkContainment(namedirs, img0, "dfs.name.dir",
        "dfs.name.dir.shared0");
    msg += checkContainment(namedirs, img1, "dfs.name.dir",
        "dfs.name.dir.shared1");

    // verify that the shared edits directories are not specified as
    // dfs.name.edits.dir
    msg += checkContainment(editsdir, edit0, "dfs.name.edits.dir",
        "dfs.name.edits.dir.shared0");
    msg += checkContainment(editsdir, edit1, "dfs.name.edits.dir",
        "dfs.name.edits.dir.shared1");
    
    if (conf.get("dfs.name.dir.shared") != null
        || conf.get("dfs.name.edits.dir.shared") != null) {
      msg += " dfs.name.dir.shared and dfs.name.edits.dir.shared should not be set manually";
    }
    checkMessage(msg);
    
    // For now we store images only in NFS
    // but we will download them instead of copying.
    checkFileURIScheme(img0, img1);
  }
  
  private static String checkContainment(Collection<URI> set, URI key,
      String setName, String keyName) {
    if (set.contains(key)) {
      return " The name specified in " + keyName + " is already part of "
          + setName + ". ";
    }
    return "";
  }
  
  private static void checkMessage(String msg) throws IOException {
    if (msg.length() != 0) {
      LOG.fatal(msg);
      throw new IOException(msg);
    }
  }
  
  /*
   * Temporarily we only deal with files...
   */
  private static void checkFileURIScheme(URI... uris) throws IOException {
    for(URI uri : uris)
      if (uri.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) != 0)
        throw new IOException("The specified path is not a file." +
            "Avatar supports file namenode storage only...");
  }
  
  static void updateConf(StartupInfo startInfo, Configuration newconf,
      Collection<URI> dirs, URI dir0, URI dir1, String keyName) {
    StringBuffer buf = new StringBuffer();

    if (startInfo.instance == InstanceId.NODEONE) {
      buf.append(dir1);
      newconf.set(keyName + ".shared", dir1.toString());
    } else if (startInfo.instance == InstanceId.NODEZERO) {
      buf.append(dir0);
      newconf.set(keyName + ".shared", dir0.toString());
    }
    for (URI str : dirs) {
      buf.append(",");
      buf.append(str);
    }
    newconf.set(keyName, buf.toString());
    buf = null;
  }
}

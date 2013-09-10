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
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode.StartupInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.util.FlushableLogger;

public class AvatarStorageSetup {
  
  public static final Log LOG = LogFactory.getLog(AvatarStorageSetup.class.getName());
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);

  static void validate(Configuration conf, 
      Collection<URI> namedirs, Collection<URI> editsdir, 
      URI img0, URI img1, URI edit0, URI edit1) throws IOException {
    
    FLOG.info("Avatar conf validation - namedirs: " + namedirs);
    FLOG.info("Avatar conf validation - editdirs: " + editsdir);
    FLOG.info("Avatar conf validation - shared namedirs: " + img0 + ", " + img1);
    FLOG.info("Avatar conf validation - shared editdirs: " + edit0 + ", " + edit1);
    
    String msg = "";
    
    if (conf == null) {
      msg += "Configuration should not be null. ";
    }
    
    if (img0 == null || img1 == null || edit0 == null || edit1 == null) {
      msg += "Configuration does not contain shared locations for image and edits. ";
      // at this point there we fail
      checkMessage(msg);
    }
    
    if (img0.equals(img1)) {
      msg += "Configuration contains the same image location for dfs.name.dir.shared0 "
          + "and dfs.name.dir.shared1";
      // at this point there we fail
      checkMessage(msg);
    }
    
    if (edit0.equals(edit1)) {
      msg += "Configuration contains the same edits location for dfs.name.edits.dir.shared0 "
          + "and dfs.name.edits.dir.shared1";
      // at this point there we fail
      checkMessage(msg);
    }
    
    // non-shared storage is file based 
    msg += checkFileURIScheme(namedirs);
    msg += checkFileURIScheme(editsdir);

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
    
    // check shared image location
    msg += checkImageStorage(img0, edit0);
    msg += checkImageStorage(img1, edit1);
    
    checkMessage(msg);   
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
      FLOG.fatal(msg);
      throw new IOException(msg);
    }
  }
  
  /**
   * Shared image needs to be in file storage, or QJM providing that QJM also
   * stores edits.
   */
  private static String checkImageStorage(URI sharedImage, URI sharedEdits) {
    if (sharedImage.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      // shared image is stored in file storage
      return "";
    } else if (sharedImage.getScheme().equals(
        QuorumJournalManager.QJM_URI_SCHEME)
        && sharedImage.equals(sharedEdits)) {
      // image is stored in qjm together with edits
      return "";
    }
    return "Shared image uri: " + sharedImage + " must be either file storage"
        + " or be equal to shared edits storage " + sharedEdits + ". ";
  }
  
  /**
   * For non-shared storage, we enforce file uris
   */
  private static String checkFileURIScheme(Collection<URI> uris) {
    for (URI uri : uris)
      if (uri.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) != 0)
        return "The specified path is not a file."
            + "Avatar supports file non-shared storage only... ";
    return "";
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

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
package org.apache.hadoop.hdfs.notifier;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;


public class NotifierUtils {
  
  /**
   * Read version file from the given directory and return
   * the layout stored therein.
   */
  public static int getVersion(URI editsURI) throws IOException {
    if (editsURI.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      StorageDirectory sd = new NNStorage(new StorageInfo()).new StorageDirectory(
          new File(editsURI.getPath()));
      File versionFile = sd.getVersionFile();
      if (!versionFile.exists()) {
        throw new IOException("No VERSION file in: " + editsURI + "version file: " + versionFile );
      }
      Properties props = Storage.getProps(versionFile);
      String layout = props.getProperty(Storage.LAYOUT_VERSION);
      if (layout == null) {
        throw new IOException("No layout version in: " + editsURI);
      }
      return Integer.valueOf(layout);
    } else {
      throw new IOException("Non file journals not supported yet.");
    }
  }
  
  /**
   * Get file associated with the given URI
   */
  public static File uriToFile(URI u) throws IOException {
    if (!u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      throw new IOException("URI does not represent a file");
    }
    return new File(u.getPath());
  }

  public static String asString(NamespaceNotification n) {
    return "[Notification: " + EventType.fromByteValue(n.type).name() + " " +
        n.path + " " + n.txId + "]";
  }
  
  
  public static String asString(NamespaceEvent e) {
    return "[Event: " + EventType.fromByteValue(e.type).name() + " " +
        e.path + "]";
  }
  
  
  public static String asString(NamespaceEventKey e) {
    return asString(e.event);
  }
  
  
  public static String getBasePath(NamespaceNotification notification) {
    String basePath = notification.path.substring(0,
        notification.path.lastIndexOf(Path.SEPARATOR));
    if (basePath.trim().length() == 0) {
      basePath = Path.SEPARATOR;
    }
    
    switch (EventType.fromByteValue(notification.getType())) {
      case FILE_ADDED:
      case FILE_CLOSED:
      case NODE_DELETED:
      case DIR_ADDED:
        return basePath;
      default:
        return null;
    }
  }
  
  /**
   * return all the ancestors of the given path, include itself.
   * @param eventPath
   * @return
   */
  public static List<String> getAllAncestors(String eventPath) {
    // check if the path is valid.
    if (eventPath == null || !eventPath.startsWith(Path.SEPARATOR)) {
      return null;
    }
    
    if (eventPath.equals(Path.SEPARATOR)) {
      return Arrays.asList(Path.SEPARATOR);
    }
    
    List<String> ancestors = new ArrayList<String>();
    while (eventPath.length() > 0) {
      ancestors.add(eventPath);
      eventPath = eventPath.substring(0, eventPath.lastIndexOf(Path.SEPARATOR));
    }
    // add the root directory
    ancestors.add(Path.SEPARATOR);
    
    return ancestors;
  }
  
  public static String getAdditionalPath(NamespaceNotification notification) {
    switch (EventType.fromByteValue(notification.getType())) {
      case FILE_ADDED:
      case FILE_CLOSED:
      case NODE_DELETED:
      case DIR_ADDED:
        return notification.path.substring(
            notification.path.lastIndexOf(Path.SEPARATOR) + 1,
            notification.path.length());
      default:
        return null;
    }
  }
  
  public static boolean compareNotifications(NamespaceNotification n1,
      NamespaceNotification n2) {
    return n1.type == n2.type && n1.path.equals(n2.path) && n1.txId == n2.txId;
  }
}

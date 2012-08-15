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

public class NotifierUtils {

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
    switch (EventType.fromByteValue(notification.getType())) {
      case FILE_ADDED:
        String basePath = notification.path.substring(0,
            notification.path.lastIndexOf('/'));
        if (basePath.trim().length() == 0) {
          return "/";
        }
        return basePath;
      case FILE_CLOSED:
      case NODE_DELETED:
        return notification.path;
      default:
        return null;
    }
  }

  
  public static String getAdditionalPath(NamespaceNotification notification) {
    switch (EventType.fromByteValue(notification.getType())) {
      case FILE_ADDED:
        return notification.path.substring(
            notification.path.lastIndexOf('/') + 1,
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

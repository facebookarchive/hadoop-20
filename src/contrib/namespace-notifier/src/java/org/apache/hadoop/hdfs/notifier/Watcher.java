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

/**
 * Must be implemented by the classes which want to receive namespace
 * notifications.
 */
public interface Watcher {

  /**
   * Called when a notification was received.
   * @param notification
   */
  public void handleNamespaceNotification(NamespaceNotification notification);
  
  
  /**
   * Called when the connection to the server failed and recovering the
   * connection trough all the other servers failed.
   */
  public void connectionFailed();
  
  
  /**
   * Called when the connection to the server is established. This will be
   * called on start-up and after a connectionFailed method call. You can only
   * place watches after this method has been called given that connectionFailed
   * was not called since then.
   */
  public void connectionSuccesful();
}

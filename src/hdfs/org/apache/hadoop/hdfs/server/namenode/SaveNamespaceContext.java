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

import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;


/**
 * Context for an ongoing SaveNamespace operation. This class
 * allows cancellation.
 */
class SaveNamespaceContext {

  /**
   * If the operation has been canceled, set to the reason why
   * it has been canceled (eg standby moving to active)
   */
  private volatile String cancelReason = null;
  private long txid = -1;


  /**
   * Requests that the current saveNamespace operation be
   * canceled if it is still running.
   */
  void cancel(String reason) {
    this.cancelReason = reason;
  }

  void checkCancelled() throws SaveNamespaceCancelledException {
    if (cancelReason != null) {
      InjectionHandler
          .processEvent(InjectionEvent.SAVE_NAMESPACE_CONTEXT_EXCEPTION);
      throw new SaveNamespaceCancelledException(
          cancelReason);
    }
  }

  boolean isCancelled() {
    return cancelReason != null;
  }
  
  public void clear() {
    this.cancelReason = null;
  }
  
  public void setTxId(long txid) {
    this.txid = txid;
  }
  
  long getTxId() {
    return txid;
  }
}

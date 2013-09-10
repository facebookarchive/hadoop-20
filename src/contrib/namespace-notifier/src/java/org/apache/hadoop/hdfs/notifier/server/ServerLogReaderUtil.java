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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;

public class ServerLogReaderUtil {

  private static final Log LOG = LogFactory.getLog(ServerLogReaderUtil.class);
  
  /**
   * Converts a Transaction Log operation into a NamespaceNotification
   * object.
   * @param op the Transaction Log operation.
   * @return the NamespaceNotification object or null if the type of
   *         the operation isn't supported to be transformed into a
   *         NamespaceNotification.
   */
  static NamespaceNotification createNotification(FSEditLogOp op) {
    switch (op.opCode) {
      case OP_ADD:
        return new NamespaceNotification(((AddOp)op).path,
            EventType.FILE_ADDED.getByteValue(), op.getTransactionId());

      case OP_CLOSE:
        return new NamespaceNotification(((CloseOp)op).path,
            EventType.FILE_CLOSED.getByteValue(), op.getTransactionId());
        
      case OP_DELETE:
        return new NamespaceNotification(((DeleteOp)op).path,
            EventType.NODE_DELETED.getByteValue(), op.getTransactionId());
        
      case OP_MKDIR:
        return new NamespaceNotification(((MkdirOp)op).path,
            EventType.DIR_ADDED.getByteValue(), op.getTransactionId());
      default:
        return null;
    }
  }

  /**
   * We would skip the transaction if its id is less than or equal to current
   * transaction id.
   */
  static boolean shouldSkipOp(long currentTransactionId, FSEditLogOp op) {
    if (currentTransactionId == -1 
        || op.getTransactionId() > currentTransactionId) {
      return false;
    }
    
    return true;
  }
  
  /**
   * Asserts the read operation is the expected one.
   * @param op the newly read operation
   * @param expectedTransactionId
   * @return next expected transaction id
   * @throws IOException if the transaction id doesn't match the expected
   *         transaction id. The transaction id's should be consecutive.
   */
  static long checkTransactionId(long currentTransactionId, FSEditLogOp op)
      throws IOException {
    if (currentTransactionId != -1) {
      if (op.getTransactionId() != currentTransactionId + 1) {
        LOG.error("Read invalid txId=" + op.getTransactionId()
            + " expectedTxId=" + (currentTransactionId + 1) + ":" +  op);
        throw new IOException("checkTransactionId failed");
      }
    }
    currentTransactionId = op.getTransactionId();
    return currentTransactionId;
  }
}

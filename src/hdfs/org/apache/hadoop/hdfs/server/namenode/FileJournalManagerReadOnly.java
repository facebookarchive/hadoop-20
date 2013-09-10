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

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;

/**
 * Read-only version of FileJournalManager. It should be used outside of the
 * active NN to gain access to edit log segments, without the risk of corrupting
 * the underlying storage.
 */
public class FileJournalManagerReadOnly extends FileJournalManager {

  public FileJournalManagerReadOnly(StorageDirectory sd) {
    super(sd);
  }

  public FileJournalManagerReadOnly(StorageDirectory sd,
      NameNodeMetrics metrics, StorageErrorReporter errorReporter) {
    super(sd, metrics, errorReporter);
  }
  
  // methods not allowed

  public long getNumberOfTransactions(long fromTxnId) throws IOException,
      CorruptionException {
    throw new IOException("Unsupported operation");
  }

  public void formatJournal(StorageInfo si) throws IOException {
    throw new IOException("Operation not supported");
  }

  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    throw new IOException("Operation not supported");
  }

  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    throw new IOException("Operation not supported");
  }

  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    throw new IOException("Operation not supported");
  }

  public void recoverUnfinalizedSegments() throws IOException {
    throw new IOException("Operation not supported");
  }

  public void close() throws IOException {
    throw new IOException("Operation not supported");
  }
}

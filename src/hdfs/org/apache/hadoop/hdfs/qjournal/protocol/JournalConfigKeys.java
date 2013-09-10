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
package org.apache.hadoop.hdfs.qjournal.protocol;

public class JournalConfigKeys {
  
  // Journal-node related configs. These are read on the JN side.
  public static final String  DFS_JOURNALNODE_DIR_KEY = "dfs.journalnode.dir";
  public static final String  DFS_JOURNALNODE_DIR_DEFAULT = "/tmp/hadoop/dfs/journalnode/";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_KEY = "dfs.journalnode.rpc-address";
  public static final int     DFS_JOURNALNODE_RPC_PORT_DEFAULT = 8485;
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_RPC_PORT_DEFAULT;
  
  public static final String  DFS_JOURNALNODE_HOSTS = "dfs.journalnode.hosts";
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_KEY = "dfs.journalnode.http-address";
  public static final int     DFS_JOURNALNODE_HTTP_PORT_DEFAULT = 8480;
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTP_PORT_DEFAULT;

  // Journal-node related configs for the client side.
  public static final String  DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY = "dfs.qjournal.queued-edits.limit.mb";
  public static final int     DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT = 10;
  
  // Quorum-journal timeouts for various operations. Unlikely to need
  // to be tweaked, but configurable just in case.
  public static final String  DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.start-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.prepare-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.accept-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.finalize-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY = "dfs.qjournal.select-input-streams.timeout.ms";
  public static final String  DFS_QJOURNAL_GET_IMAGE_MANIFEST_TIMEOUT_KEY = "dfs.qjournal.get-image-manifest.timeout.ms";
  public static final String  DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY = "dfs.qjournal.get-journal-state.timeout.ms";
  public static final String  DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY = "dfs.qjournal.new-epoch.timeout.ms";
  public static final String  DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY = "dfs.qjournal.write-txns.timeout.ms";
  public static final String  DFS_QJOURNAL_HTTP_TIMEOUT_KEY = "dfs.qjournal.http.timeout.ms";
  public static final String  DFS_QJOURNAL_CONNECT_TIMEOUT_KEY = "dfs.qjournal.connect.timeout.ms";
  public static final String  DFS_QJOURNAL_IPC_READER_KEY = "dfs.qjournal.ipc.threads";
  public static final String  DFS_QJOURNAL_IMAGE_BUFFER_SIZE_KEY = "dfs.qjournal.image.buffer.size";
  public static final String  DFS_QJOURNAL_IMAGE_MAX_BUFFERED_CHUNKS_KEY = "dfs.qjournal.image.max.buffered.chunks";
  public static final String  DFS_QJOURNAL_FORMAT_TIMEOUT_KEY = "dfs.qjournal.format.timeout.ms";
  public static final String  DFS_QJOURNAL_HAS_DATA_TIMEOUT_KEY = "dfs.qjournal.has.data.timeout.ms";
  
  public static final int     DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_GET_IMAGE_MANIFEST_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_HTTP_TIMEOUT_DEFAULT = 5000;
  public static final long    DFS_QJOURNAL_CONNECT_TIMEOUT_DEFAULT = Long.MAX_VALUE;
  public static final int     DFS_QJOURNAL_IPC_READER_DEFAULT = 16;
  public static final int     DFS_QJOURNAL_IMAGE_BUFFER_SIZE_DEFAULT = 10 * 1024 * 1024;
  public static final int     DFS_QJOURNAL_IMAGE_MAX_BUFFERED_CHUNKS_DEFAULT = 20;
  public static final int     DFS_QJOURNAL_FORMAT_TIMEOUT_DEFAULT = 600000; // 10 mins
  public static final int     DFS_QJOURNAL_HAS_DATA_TIMEOUT_DEFAULT = 600000; // 10 mins
}

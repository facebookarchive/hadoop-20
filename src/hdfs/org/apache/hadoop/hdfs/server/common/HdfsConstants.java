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
package org.apache.hadoop.hdfs.server.common;


/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

public interface HdfsConstants {
  /**
   * Type of the node
   */
  static public enum NodeType {
    NAME_NODE,
    DATA_NODE,
    JOURNAL_NODE;
  }

  public static enum Transition {
    FORMAT("Format"),
    UPGRADE("Upgrade"),
    COMPLETE_UPGRADE("Complete Upgrade"),
    FINALIZE("Finalize"),
    RECOVER("Recover"),
    ROLLBACK("Rollback");

    private String name = null;
    private Transition(String arg) {this.name = arg;}

    @Override
    public String toString() {
      return name;
    }
  }

  // Startup options
  static public enum StartupOption{
    FORMAT  ("-format"),
    REGULAR ("-regular"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT  ("-importCheckpoint"),
    SERVICE ("-service"),
    IGNORETXIDMISMATCH ("-ignoretxidmismatch");
    
    private String name = null;
    private StartupOption(String arg) {this.name = arg;}

    public String getName() {
      return name;
    }
  }

  // socket releated properties
  public static final String DFS_DATANODE_READ_EXTENSION = 
                            "dfs.datanode.socket.read.extension.timeout";
  public static final String DFS_DATANODE_WRITE_EXTENTSION =
                            "dfs.datanode.socket.write.extension.timeout";
  
  // Timeouts for communicating with DataNode for streaming writes/reads
  public static int READ_TIMEOUT = 60 * 1000;
  public static int READ_TIMEOUT_EXTENSION = 3 * 1000;
  public static int WRITE_TIMEOUT = 8 * 60 * 1000;
  public static int WRITE_TIMEOUT_EXTENSION = 5 * 1000; //for write pipeline
  public static int DN_KEEPALIVE_TIMEOUT = 5 * 1000; 
  
  // constants for edits log
  public static long DEFAULT_EDIT_PREALLOCATE_SIZE = 1024 * 1024; // 1 MB
  public static int  DEFAULT_EDIT_BUFFER_SIZE = 512 * 1024; // 0.5 MB
  public static int  DEFAULT_MAX_BUFFERED_TRANSACTIONS = 10000; // ten 

  // property for fsimage compression
  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final boolean DFS_IMAGE_COMPRESS_DEFAULT = false;
  public static final String DFS_IMAGE_COMPRESSION_CODEC_KEY = 
    "dfs.image.compression.codec";
  public static final String DFS_IMAGE_COMPRESSION_CODEC_DEFAULT =
    "org.apache.hadoop.io.compress.DefaultCodec";
  public static final String DFS_IMAGE_TRANSFER_RATE_KEY =
    "dfs.image.transfer.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_RATE_DEFAULT = 0;  // disable
  public static final String DFS_IMAGE_SAVE_ON_START_KEY =
    "dfs.image.save.on.start";
  public static final boolean DFS_IMAGE_SAVE_ON_START_DEFAULT = true;

  // The lease holder for recovery initiated by the NameNode
  public static final String NN_RECOVERY_LEASEHOLDER = "NN_Recovery";
  
  //An invalid transaction ID that will never be seen in a real namesystem.
  public static final long INVALID_TXID = -1;

  // quorum read properties
  public static final String DFS_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS =
                            "dfs.dfsclient.quorum.read.threshold.millis";
  public static final long DEFAULT_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS =
      500;
  
  public static final String DFS_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE = 
                            "dfs.dfsclient.quorum.read.threadpool.size";
  public static final int DEFAULT_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE = 0;

  public static final int DEFAULT_PACKETSIZE = 64 * 1024;
}


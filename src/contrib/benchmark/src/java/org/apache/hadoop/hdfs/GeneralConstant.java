package org.apache.hadoop.hdfs;
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

public interface GeneralConstant {
  public static final long NMAPS = 1;
  public static final long NTHREADS = 1; 

  public static final String ROOT = "genbenchmark/";
  public static final String INPUT = ROOT + "input/";
  public static final String DFS_INPUT = ROOT + "test-input/";
  public static final String DFS_OUTPUT = ROOT + "test-output/";
  public static final String OUTPUT = ROOT + "output/";
  public static enum Counter {
    FILES_CREATED, PROCESSED_SIZE, PROCESSED_TIME, FILES_SCANNED
  }
  public static final String MAP_TASK_ID_KEY = "mapred.task.id";
  public static final String NUMBER_OF_MAPS_KEY = "dfs.nmaps";
  public static final String NUMBER_OF_THREADS_KEY = "dfs.threads";
  public static final String BUFFER_SIZE_KEY = "dfs.buffer.size.write";
  public static final String THREAD_CLASS_KEY = "dfs.thread.class";
  public static final String TEST_TYPE_KEY = "test.type";
  public static final String REPLICATION_KEY = "dfs.replication";
  public static final String WRITER_DATARATE_KEY = "writer.datarate.KB.per.sec";
  public static final String OUTPUT_DIR_KEY = "output.dir";
}

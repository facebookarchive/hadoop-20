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

public class NotifierConfig {
  
  public static final String LOG_READER_STREAM_RETRIES = "notifier.reader.stream.retries";
  public static final int LOG_READER_STREAM_RETRIES_DEFAULT = 30;
  
  public static final String LOG_READER_STREAM_TIMEOUT = "notifier.reader.stream.timeout";
  public static final long LOG_READER_STREAM_TIMEOUT_DEFAULT = 5000;
  
  public static final String LOG_READER_STREAM_ERROR_SLEEP = "notifier.reader.stream.errorsleep";
  public static final long LOG_READER_STREAM_ERROR_SLEEP_DEFAULT = 1000;

}

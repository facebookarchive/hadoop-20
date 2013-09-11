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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * Classes of Implementations that create JDBC connection
 * 
 */
public interface DBConnectionFactory {
  public static final String RAID_DB_OPS_SLEEP_TIME_KEY = 
      "hdfs.raid.db.ops.sleep.time";
  // sleep time between two db ops
  public static final long DEFAULT_DB_OPS_SLEEP_TIME = 2000;
  /**
   * get the JDBC connection url string of database 
   * @param isWrite we want to send write queries or not
   * @return connection url to the database
   * @throws IOException
   */
  String getUrl(boolean isWrite) throws IOException;
  
  /**
   * Get Database connection
   * @param url
   * @return database connection object
   * @throws SQLException
   */
  Connection getConnection(String url) throws SQLException;
  
  // Initialize the config
  void initialize(Configuration conf) throws IOException;
  
  public long getDBOpsSleepTime();
}

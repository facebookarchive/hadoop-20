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
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

public class SimpleDBConnectionFactory implements DBConnectionFactory {
  private Configuration conf;
  private String user;
  private String password;
  private long dbOpsSleepTime = DEFAULT_DB_OPS_SLEEP_TIME;
  public static final String DB_CONNECTION_USER = "hdfs.raid.db.user";
  public static final String DB_CONNECTION_PASSWORD = "hdfs.raid.db.password";
  public static final String DB_CONNECTION_URL = "hdfs.raid.db.connection.url";
  
  @Override
  public void initialize(Configuration conf) throws IOException {
    this.conf = conf;
    this.user = conf.get(DB_CONNECTION_USER, "");
    this.password = conf.get(DB_CONNECTION_PASSWORD, "");
    this.dbOpsSleepTime = conf.getLong(RAID_DB_OPS_SLEEP_TIME_KEY,
        DEFAULT_DB_OPS_SLEEP_TIME);
  }
  
  /**
   * return connection url:
   * example: jdbc:mysql://localhost:3306/db?
   */
  @Override
  public String getUrl(boolean isWrite) throws IOException {
    return conf.get(DB_CONNECTION_URL, "");
  }
  
  @Override
  public Connection getConnection(String url) throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }
  
  @Override
  public long getDBOpsSleepTime() {
    return dbOpsSleepTime;
  }
}

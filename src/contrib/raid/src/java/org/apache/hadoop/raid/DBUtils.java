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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.raid.DBConnectionFactory;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utilities for writing hooks.
 */
public class DBUtils {
  static final private Log LOG = LogFactory.getLog(DBUtils.class);

  // The default value is to retry 20 times with maximum retry interval
  // 60 seconds. The expectation is about 22 minutes. After 7 retries, it
  // reaches 60 seconds.
  static final int DEFAULT_SQL_NUM_RETRIES = 20;
  static final int DEFAULT_RETRY_MAX_INTERVAL_SEC = 60;
  public static final String DB_MAX_RETRY =
      "hdfs.raid.db.max_retries";
  public static final int DEFAULT_DB_MAX_RETRY = 30;
  public static final String RAID_DB_CONNECTION_FACTORY_CLASS_KEY =
      "hdfs.raid.db.connection.factory.class";
  public static final int RETRY_MAX_INTERVAL_SEC = 60;
  public static final List<Object> EMPTY_SQL_PARAMS = new ArrayList<Object>();
  public static Random rand = new Random();
  public static int numDBOpenObjects = 0;
  public static Map<Boolean, String> defaultUrls =
      Collections.synchronizedMap(new HashMap<Boolean, String>()); 

  public static Long selectCount(DBConnectionFactory connectionFactory, 
      String sql, List<Object> sqlParams, int sqlNumRetries, 
      String tblName)
      throws IOException {
    List<List<Object>> results =
        DBUtils.runInsertSelect(connectionFactory, sql, sqlParams, true,
            sqlNumRetries, DBUtils.RETRY_MAX_INTERVAL_SEC, false, false);
    Long count = null;
    // should be the first field in the first element
    if (results != null && !results.isEmpty() && results.get(0).get(0) != null) {
      count = (Long)results.get(0).get(0);
    }
    if (count == null) {
      throw new IOException("You cannot select from " + tblName);
    }
    return count;
  }
  
  public static DBConnectionFactory getDBConnectionFactory(Configuration conf) 
      throws IOException {
      Class<?> DBFactoryClass = null;
      DBFactoryClass = conf.getClass(
          DBUtils.RAID_DB_CONNECTION_FACTORY_CLASS_KEY, 
          SimpleDBConnectionFactory.class);
      if (DBFactoryClass == null) {
        throw new IOException("Connection factory key: " + 
            DBUtils.RAID_DB_CONNECTION_FACTORY_CLASS_KEY + " is not set!");
      }
      DBConnectionFactory connectionFactory =
          (DBConnectionFactory) ReflectionUtils.newInstance(DBFactoryClass,
                                                            conf);
      connectionFactory.initialize(conf);
      return connectionFactory;
    }
  
  public static int getSqlNumRetry(Configuration conf) {
    return conf.getInt(DB_MAX_RETRY, DEFAULT_DB_MAX_RETRY);
  }

  public static void runInsert(DBConnectionFactory connectionFactory, String sql,
                               List<Object> sqlParams, int numRetries)
      throws IOException {
    runInsertSelect(connectionFactory, sql, sqlParams, true, numRetries,
                    DEFAULT_RETRY_MAX_INTERVAL_SEC, true, false);
  }
  
  public static List<List<Object>> getResults(ResultSet result)
      throws SQLException {
    List<List<Object>> results = new ArrayList<List<Object>>();
    int numColumns = result.getMetaData().getColumnCount();
    while (result.next()) {
      List<Object> row = new ArrayList<Object>();
      results.add(row);
      for (int index = 1; index <= numColumns; index++) {
        row.add(result.getObject(index));
      }
    }
    return results;
  }
  
  public static void close(ResultSet generatedKeys,
      PreparedStatement[] pstmts, Connection conn) {
    if (generatedKeys != null) {
      try {
        generatedKeys.close();
      } catch (Exception e) {
        LOG.warn("Error to close ResultSet", e);
      } finally {
        try {
          if (!generatedKeys.isClosed()) {
            LOG.warn("ResultSet is not closed");
            DBUtils.numDBOpenObjects++;
          }
        } catch (Exception ignore) {DBUtils.numDBOpenObjects++;}
      }
    }
    if (pstmts != null && pstmts.length > 0) {
      for (PreparedStatement pstmt: pstmts) {
        if (pstmt == null) {
          continue;
        }
        try {
          pstmt.close();
        } catch (Exception e) {
          LOG.warn("Error to close PreparedStatement", e);
        } finally {
          try {
            if (!pstmt.isClosed()) {
              LOG.warn("PreparedStatement is not closed");
              DBUtils.numDBOpenObjects++;
            }
          } catch (Exception ignore) {DBUtils.numDBOpenObjects++;}
        }
      }
    }
    if (conn!=null) {
      try {
        conn.close();
      } catch (Exception e) {
        LOG.warn("Error to close Connection", e);
      } finally {
        try {
          if (!conn.isClosed()) {
            LOG.warn("Connection is not closed");
            DBUtils.numDBOpenObjects++;
          }
        } catch (Exception ignore) {DBUtils.numDBOpenObjects++;}
      }
    }
  }

  // In the case of a select returns a list of lists, where each inner list represents a row
  // returned by the query.  In the case of an insert, returns null.
  public static List<List<Object>> runInsertSelect(
      DBConnectionFactory connectionFactory, String sql,
    List<Object> sqlParams, boolean isWrite, int numRetries,
    int retryMaxInternalSec, boolean insert, boolean getGeneratedKeys)
        throws IOException {
    int waitMS = 3000; // wait for at least 3s before next retry.
    for (int i = 0; i < numRetries; ++i) {
      Connection conn = null;
      ResultSet generatedKeys = null;
      PreparedStatement pstmt = null;
      String url = null;
      try {
        try {
          url = connectionFactory.getUrl(isWrite);
        } catch (IOException ioe) {
          LOG.warn("Cannot get DB URL, fall back to the default one", ioe);
          url = defaultUrls.get(isWrite);
          if (url == null) {
            throw ioe;
          }
        }
        LOG.info("Attepting connection with URL " + url);
        conn = connectionFactory.getConnection(url);
        defaultUrls.put(isWrite, url);
        pstmt = getPreparedStatement(conn, sql, sqlParams,
            getGeneratedKeys);
        if (insert) {
          int recordsUpdated = pstmt.executeUpdate();
          LOG.info("rows inserted: " + recordsUpdated + " sql: " + sql);
          List<List<Object>> results = null;
          if (getGeneratedKeys) {
            generatedKeys = pstmt.getGeneratedKeys();
            results = getResults(generatedKeys);
          }
          Thread.sleep(connectionFactory.getDBOpsSleepTime() +
              rand.nextInt(1000));
          return results;
        }
        else {
          generatedKeys = pstmt.executeQuery();
          List<List<Object>> results = getResults(generatedKeys);
          pstmt.clearBatch();
          LOG.info("rows selected: " + results.size() + " sql: " + sql);
          Thread.sleep(connectionFactory.getDBOpsSleepTime() +
              rand.nextInt(1000));
          return results;
        }
      } catch (Exception e) {
        // We should catch a better exception than Exception, but since
        // DBConnectionUrlFactory.getUrl() defines throws Exception, it's hard
        // for us to figure out the complete set it can throw. We follow
        // DBConnectionUrlFactory.getUrl()'s definition to catch Exception.
        // It shouldn't be a big problem as after numRetries, we anyway exit.
        LOG.info("Exception " + e + ". Will retry " + (numRetries - i)
            + " times.");
        // Introducing a random factor to the wait time before another retry.
        // The wait time is dependent on # of failures and a random factor.
        // At the first time of getting a SQLException, the wait time
        // is a random number between [0,300] msec. If the first retry
        // still fails, we will wait 300 msec grace period before the 2nd retry.
        // Also at the second retry, the waiting window is expanded to 600 msec
        // alleviating the request rate from the server. Similarly the 3rd retry
        // will wait 600 msec grace period before retry and the waiting window
        // is
        // expanded to 1200 msec.

        waitMS += waitMS;
        if (waitMS > retryMaxInternalSec * 1000) {
          waitMS = retryMaxInternalSec * 1000;
        }
        double waitTime = waitMS + waitMS * rand.nextDouble();
        if (i + 1 == numRetries) {
          LOG.error("Still got Exception after " + numRetries + "  retries.",
              e);
          throw new IOException(e);
        }
        try {
          Thread.sleep((long) waitTime);
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
      } finally {
        DBUtils.close(generatedKeys, new PreparedStatement[]{pstmt}, conn);
      }
    }
    return null;
  }
  
  public static PreparedStatement getPreparedStatement(Connection conn, 
      String sql, List<Object> sqlParams, boolean getGeneratedKeys) 
      throws IOException, SQLException {
    PreparedStatement pstmt = getGeneratedKeys? conn.prepareStatement(sql,
        Statement.RETURN_GENERATED_KEYS) : conn.prepareStatement(sql);
    int pos = 1;
    for (Object param : sqlParams) {
      if (param instanceof Integer) {
        pstmt.setInt(pos++, ((Integer) param).intValue());
      } else {
        pstmt.setString(pos++, (String) param);
      }
    }
    return pstmt;
  }
 
}

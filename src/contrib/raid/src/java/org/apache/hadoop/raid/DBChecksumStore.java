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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.raid.DBConnectionFactory;
import org.apache.hadoop.raid.DBUtils;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Store the mapping between blockId(string) and checksum(Long) into database
 */
public class DBChecksumStore extends ChecksumStore {
  public static final Log LOG = LogFactory.getLog(DBChecksumStore.class);
  public static final String DB_TABLE_NAME = "hdfs.raid.checksum.db.table";
  private DBConnectionFactory connectionFactory;
  private String tblName;
  private int sqlNumRetries = DBUtils.DEFAULT_DB_MAX_RETRY;

  // SQL Queries
  private String CREATE_TABLE_SQL;
  private String DROP_TABLE_SQL;
  private String SELECT_WHERE_SQL;
  private String SELECT_COUNT_SQL;
  private String SELECT_COUNT_WHERE_SQL;
  private String INSERT_IF_NOT_EXISTS_SQL;
  private String REPLACE_SQL;
  private String CLEAR_SQL;

  public void constructSql() {
    CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + tblName + 
        "(block_id BIGINT NOT NULL, gen_stamp BIGINT NOT NULL," +
        " checksum BIGINT NOT NULL, INDEX USING BTREE (block_id)," +
        " PRIMARY KEY(block_id, gen_stamp))";
    DROP_TABLE_SQL = "DROP TABLE " + tblName;
    SELECT_WHERE_SQL = "SELECT checksum FROM " + tblName +
                       " WHERE block_id = ? AND gen_stamp = ? ";
    SELECT_COUNT_SQL = "SELECT COUNT(*) FROM " + tblName;
    SELECT_COUNT_WHERE_SQL = "SELECT COUNT(*) FROM " + tblName +
                             " WHERE block_id = ? and gen_stamp = ?";
    INSERT_IF_NOT_EXISTS_SQL = "INSERT IGNORE INTO " + tblName +
                               " (block_id, gen_stamp, checksum) VALUES " +
                               " (?, ?, ?)";
    REPLACE_SQL = "REPLACE INTO " + tblName +
                  " (block_id, gen_stamp, checksum) VALUES " +
                  " (?, ?, ?)";
    CLEAR_SQL = "TRUNCATE TABLE " + tblName;
  }
  
  private void createTable() throws IOException {
    DBUtils.runInsert(connectionFactory, CREATE_TABLE_SQL,
        DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries);
    LOG.info("Created a table " + tblName);
  }

  public void initialize(Configuration conf, boolean createStore)
      throws IOException {
    connectionFactory = DBUtils.getDBConnectionFactory(conf);
    tblName = conf.get(DB_TABLE_NAME);
    if (tblName == null) {
      throw new IOException("Config key " + DB_TABLE_NAME + " is not defined");
    }
    constructSql();
    sqlNumRetries = DBUtils.getSqlNumRetry(conf);
    if (createStore) {
      createTable();
    }
  }

  @Override
  public int size() throws IOException {
    Long count = DBUtils.selectCount(connectionFactory, 
        SELECT_COUNT_SQL, DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries, tblName);
    return count.intValue();
  }

  @Override
  public boolean isEmpty() throws IOException {
    return size() == 0;
  }

  @Override
  public boolean hasChecksum(Block blk) throws IOException {
    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(Long.toString(blk.getBlockId()));
    sqlParams.add(Long.toString(blk.getGenerationStamp()));
    Long count = DBUtils.selectCount(connectionFactory,
        SELECT_COUNT_WHERE_SQL, sqlParams, sqlNumRetries, tblName);
    return count > 0L;
  }

  @Override
  public Long getChecksum(Block blk) throws IOException {
    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(Long.toString(blk.getBlockId()));
    sqlParams.add(Long.toString(blk.getGenerationStamp()));
    List<List<Object>> results =
        DBUtils.runInsertSelect(connectionFactory, SELECT_WHERE_SQL,
                                sqlParams, true, sqlNumRetries,
                                DBUtils.RETRY_MAX_INTERVAL_SEC, false,
                                false);
    if (results == null) {
      throw new IOException("You cannot select from " + tblName);
    }
    if (results.isEmpty()) {
      //No record is found
      return null;
    }
    if (results.size() > 1) {
      throw new IOException("More than one checksum for the block " + blk);
    }
    Long checksum = (Long)results.get(0).get(0);
    LOG.info("Fetch " + blk + " -> " + checksum);
    return checksum;
  }

  @Override
  public void putChecksum(Block blk, Long newChecksum) throws IOException {
    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(Long.toString(blk.getBlockId()));
    sqlParams.add(Long.toString(blk.getGenerationStamp()));
    sqlParams.add(newChecksum.toString());
    DBUtils.runInsert(connectionFactory, REPLACE_SQL, sqlParams, sqlNumRetries);
    LOG.info("Put " + blk + " -> " + newChecksum);
  }

  @Override
  public Long putIfAbsent(Block blk, Long newChecksum) throws IOException {
    Long oldChecksum = getChecksum(blk);
    if (oldChecksum != null) {
      return oldChecksum;
    }
    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(Long.toString(blk.getBlockId()));
    sqlParams.add(Long.toString(blk.getGenerationStamp()));
    sqlParams.add(newChecksum.toString());
    DBUtils.runInsert(connectionFactory, INSERT_IF_NOT_EXISTS_SQL, sqlParams,
        sqlNumRetries);
    LOG.info("PutIfAbsent " + blk + " -> " + newChecksum);
    return oldChecksum;
  }

  // Only used for testing
  @Override
  public void clear() throws IOException {
    DBUtils.runInsert(connectionFactory, CLEAR_SQL, DBUtils.EMPTY_SQL_PARAMS,
        sqlNumRetries);
    LOG.info("Clear all values");
  }
  
  // Only used for testing
  public void dropTable() throws IOException {
    DBUtils.runInsert(connectionFactory, DROP_TABLE_SQL,
        DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries);
    LOG.info("Drop table " + tblName);
  }
  
  public DBConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }
}

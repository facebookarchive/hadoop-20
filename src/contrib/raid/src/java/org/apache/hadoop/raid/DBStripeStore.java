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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.raid.DBConnectionFactory;
import org.apache.hadoop.raid.DBUtils;

/**
 * Store the relationships between blocks and stripes into database
 */
public class DBStripeStore extends StripeStore {
  public static final Log LOG = LogFactory.getLog(DBStripeStore.class);
  public static final String DB_TABLE_NAME = "hdfs.raid.stripe.db.table";
  // sleep time between two put stripe operations
  public static final String DB_PUT_STRIPE_SLEEP_TIME_KEY =
      "hdfs.raid.stripe.db.sleep";
  public static final long DEFAULT_DB_PUT_STRIPE_SLEEP_TIME = 2000;
  public static final String[] STRIPESTORE_SPECIFIC_KEYS = 
      new String[] {
        DB_TABLE_NAME
      };
  private DBConnectionFactory connectionFactory;
  private String tblName;
  private long putStripeSleepTime = DEFAULT_DB_PUT_STRIPE_SLEEP_TIME;
  private int sqlNumRetries = DBUtils.DEFAULT_DB_MAX_RETRY;
  private String defaultUrl = null;

  // SQL Queries
  private String VALUES_STRING = "(?, ?, ?, ?, ?)";
  private String CREATE_TABLE_SQL;
  private String DROP_TABLE_SQL;
  private String SELECT_WHERE_SQL;
  private String SELECT_COUNT_SQL;
  private String REPLACE_SQL;
  private String CLEAR_SQL;
  // stripe id related
  public String NEW_STRIPE_ID_SQL;

  public void constructSql() {
    CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + tblName + 
                       "(auto_id BIGINT NOT NULL AUTO_INCREMENT," +
                       " block_id BIGINT NOT NULL," +
                       " gen_stamp BIGINT NOT NULL," +
                       " codec_id BIGINT NOT NULL," +
                       " block_order TINYINT NOT NULL," +
                       " stripe_id BIGINT NOT NULL," +
                       " INDEX USING BTREE(block_id)," +
                       " INDEX USING BTREE(stripe_id)," +
                       " PRIMARY KEY(auto_id)," +
                       " UNIQUE KEY `block` (block_id, gen_stamp, codec_id));";
    DROP_TABLE_SQL = "DROP TABLE " + tblName;
    SELECT_WHERE_SQL = "SELECT a.block_id, a.gen_stamp, a.block_order FROM " +
                       tblName + " AS a, " + tblName + " AS b WHERE " +
                       " a.stripe_id = b.stripe_id AND b.block_id = ? " +
                       " AND b.gen_stamp = ? AND b.codec_id = ? " + 
                       " order by a.block_order";
    SELECT_COUNT_SQL = "SELECT COUNT(DISTINCT stripe_id) FROM " + tblName;
    REPLACE_SQL = "REPLACE INTO " + tblName +
                  " (block_id, gen_stamp, codec_id, block_order, stripe_id)" +
                  " VALUES ";
    NEW_STRIPE_ID_SQL = "REPLACE INTO " + tblName + "(block_id, gen_stamp," +
                  " codec_id, block_order, stripe_id) values (?, ?, ?, ?, ?)";
    CLEAR_SQL = "TRUNCATE TABLE " + tblName;
  }
  
  private void createTable() throws IOException {
    DBUtils.runInsert(connectionFactory, CREATE_TABLE_SQL, 
        DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries);
    LOG.info("Created a table " + tblName);
  }
  
  @Override
  public void initialize(Configuration conf, boolean createStore, 
      FileSystem fs) throws IOException {
    connectionFactory = DBUtils.getDBConnectionFactory(conf);   
    Configuration newConf = initializeConf(STRIPESTORE_SPECIFIC_KEYS, conf, fs);
    tblName = newConf.get(DB_TABLE_NAME);
    if (tblName == null) {
      throw new IOException("Config key " + DB_TABLE_NAME + " is not defined");
    }
    constructSql();
    sqlNumRetries = DBUtils.getSqlNumRetry(conf);
    if (createStore) {
      createTable(); 
    }
    putStripeSleepTime = conf.getLong(DB_PUT_STRIPE_SLEEP_TIME_KEY,
        DEFAULT_DB_PUT_STRIPE_SLEEP_TIME);   
  }

  @Override
  public int numStripes() throws IOException {
    Long count = DBUtils.selectCount(connectionFactory, 
        SELECT_COUNT_SQL, DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries, tblName);
    return count.intValue();
  }

  @Override
  public StripeInfo getStripe(Codec codec, Block block) throws IOException {
    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(Long.toString(block.getBlockId()));
    sqlParams.add(Long.toString(block.getGenerationStamp()));
    sqlParams.add(Long.toString(codec.id.hashCode()));
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
    // block_order of the first result should be negative number, its
    // absolute value is the order of the last block
    Block lastBlock = new Block((Long)results.get(0).get(0),
        0L, (Long)results.get(0).get(1));
    Integer lastBlockOrder = (Integer)results.get(0).get(2);
    if (lastBlockOrder >= 0 || (-lastBlockOrder != results.size() - 1) 
        || results.size() <= codec.parityLength) {
      throw new IOException("Wrong result is returned for codec " + codec.id + 
          " last block: " + lastBlock + " last block order: " + lastBlockOrder);
    }
      
    List<Block> parityBlocks = new ArrayList<Block>();
    List<Block> sourceBlocks = new ArrayList<Block>();
    for (int i = 1; i < results.size(); i++) {
      Block curBlock = new Block((Long)results.get(i).get(0), 0L,
          (Long)results.get(i).get(1));
      Integer curBlockOrder = (Integer)results.get(i).get(2);
      if (curBlockOrder != i - 1) {
        throw new IOException("The " + (i-1) + "th block " + curBlock +
            "'s order is " + curBlockOrder);
      }
      if (i - 1 < codec.parityLength) {
        parityBlocks.add(curBlock);
      } else {
        sourceBlocks.add(curBlock);
      }
    }
    // Finally add the last block;
    sourceBlocks.add(lastBlock);
    StripeInfo si = new StripeInfo(codec, block, parityBlocks, sourceBlocks);
    LOG.info("Fetch " + codec.id + ":" + block + " -> " + si);
    return si;
  }
  
  private String getInsertStripeSql(List<Block> parityBlks, 
      List<Block> srcBlks) {
    StringBuilder sb = new StringBuilder(REPLACE_SQL);
    for (int j = 0; j < parityBlks.size() + srcBlks.size(); j++) {
      if (j > 0) {
        sb.append(",");
      }
      sb.append(VALUES_STRING);
    }
    return sb.toString();
  }
  
  private List<Object> constructGetStripeSqlParam(Codec codec,
      List<Block> parityBlks, List<Block> srcBlks) {
    List<Object> sqlParams = new ArrayList<Object>();
    // insert just the first block
    int index = 0;
    sqlParams.add(Long.toString(parityBlks.get(index).getBlockId()));
    sqlParams.add(Long.toString(parityBlks.get(index).getGenerationStamp()));
    sqlParams.add(Long.toString(codec.id.hashCode()));
    sqlParams.add(Long.toString(index));
    // temporary id will be overwritten later
    sqlParams.add(Long.toString(-1L)); 
    return sqlParams;
  }
  
  private List<Object> constructInsertStripeSqlParam(Codec codec,
      List<Block> parityBlks, List<Block> srcBlks, Long stripeId) {
    List<Object> sqlParams = new ArrayList<Object>();
    for (int i = 0; i < parityBlks.size(); i++) {
      sqlParams.add(Long.toString(parityBlks.get(i).getBlockId()));
      sqlParams.add(Long.toString(parityBlks.get(i).getGenerationStamp()));
      sqlParams.add(Long.toString(codec.id.hashCode()));
      sqlParams.add(Long.toString(i));
      sqlParams.add(Long.toString(stripeId));
    }
    int srcBlksSize = srcBlks.size();
    for (int i = 0; i < srcBlksSize; i++) {
      sqlParams.add(Long.toString(srcBlks.get(i).getBlockId()));
      sqlParams.add(Long.toString(srcBlks.get(i).getGenerationStamp()));
      sqlParams.add(Long.toString(codec.id.hashCode()));
      int blockOrder = (i == srcBlksSize - 1)? 
          -(i + codec.parityLength): 
           (i + codec.parityLength);
      sqlParams.add(Long.toString(blockOrder));
      sqlParams.add(Long.toString(stripeId));
    }
    return sqlParams;
  }

  @Override
  public void putStripe(Codec codec, List<Block> parityBlks, List<Block> srcBlks)
      throws IOException {
    
    if (parityBlks.size() != codec.parityLength) {
      throw new IOException("Number of parity blocks " + parityBlks.size() + 
          " doesn't match codec " + codec.id + " (" + codec.parityLength + ")");
    }
    if (srcBlks.size() > codec.stripeLength) {
      throw new IOException("Number of source blocks " + srcBlks.size() + 
          " is greater than codec " + codec.id + " (" + codec.stripeLength + ")"); 
    }
    
    List<Object> getStripeSqlParams = constructGetStripeSqlParam(codec,
            parityBlks, srcBlks);
    String insertStripeSql = getInsertStripeSql(parityBlks, srcBlks);
    
    int waitMS = 3000; // wait for at least 3sec before next retry.
    Random rand = new Random();
    for (int i = 0; i < sqlNumRetries; ++i) {
      Connection conn = null;
      PreparedStatement getStripeStatement = null;
      ResultSet generatedKeys = null;
      PreparedStatement insertStripeStatement = null;
      String url = null;
      try {
        try {
          url = connectionFactory.getUrl(true);
        } catch (IOException ioe) {
          LOG.warn("Cannot get DB URL, fall back to the default one:" + 
              defaultUrl, ioe);
          url = defaultUrl;
          if (url == null) {
            throw ioe;
          }
        }
        LOG.info("Attepting connection with URL " + url);
        conn = connectionFactory.getConnection(url);
        conn.setAutoCommit(false);
        defaultUrl = url;
        getStripeStatement = DBUtils.getPreparedStatement(conn,
            NEW_STRIPE_ID_SQL, getStripeSqlParams, true);
        int recordsUpdated = getStripeStatement.executeUpdate();
        LOG.info("rows inserted: " + recordsUpdated + " sql: " + NEW_STRIPE_ID_SQL);
        generatedKeys = getStripeStatement.getGeneratedKeys();
        List<List<Object>> results = DBUtils.getResults(generatedKeys);
        Long stripeId = (Long)results.get(0).get(0);
        List<Object> insertStripeSqlParams = constructInsertStripeSqlParam(codec,
            parityBlks, srcBlks, stripeId);
        insertStripeStatement = DBUtils.getPreparedStatement(conn,
            insertStripeSql, insertStripeSqlParams, false);
        recordsUpdated = insertStripeStatement.executeUpdate();
        conn.commit();
        LOG.info("rows inserted: " + recordsUpdated + " sql: " + insertStripeSql);
        StripeInfo si = new StripeInfo(codec, null, parityBlks, srcBlks);
        LOG.info("Put " + si + " into stripe store");
        Thread.sleep(putStripeSleepTime + rand.nextInt(1000));
        return;
      } catch (Exception e) {
        // We should catch a better exception than Exception, but since
        // DBConnectionUrlFactory.getUrl() defines throws Exception, it's hard
        // for us to figure out the complete set it can throw. We follow
        // DBConnectionUrlFactory.getUrl()'s definition to catch Exception.
        // It shouldn't be a big problem as after numRetries, we anyway exit.
        LOG.info("Exception " + e + ". Will retry " + (sqlNumRetries - i)
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
        if (conn != null) {
          try {
            conn.rollback();
            LOG.info("putStripe Transaction was rolled back");
          } catch(SQLException excep) {
            LOG.error(excep); 
          }
        }
        waitMS += waitMS;
        if (waitMS > DBUtils.RETRY_MAX_INTERVAL_SEC * 1000) {
          waitMS = DBUtils.RETRY_MAX_INTERVAL_SEC * 1000;
        }
        double waitTime = waitMS + waitMS * rand.nextDouble();
        if (i + 1 == sqlNumRetries) {
          LOG.error("Still got Exception after " + sqlNumRetries + "  retries.",
              e);
          throw new IOException(e);
        }
        try {
          Thread.sleep((long) waitTime);
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
      } finally {
        try {
          if (conn != null) {
            conn.setAutoCommit(true);
          }
        } catch (SQLException sqlExp) {
          LOG.warn("Fail to set AutoCommit to true", sqlExp); 
        }
        DBUtils.close(generatedKeys,
            new PreparedStatement[]{getStripeStatement,
            insertStripeStatement}, conn);
      }
    } 
  }

  // Only used for testing
  @Override
  public void clear() throws IOException {
    DBUtils.runInsert(connectionFactory, CLEAR_SQL, DBUtils.EMPTY_SQL_PARAMS,
        sqlNumRetries);
    LOG.info("Clear all values from table " + tblName);
  }
  
  // Only used for testing
  public void dropTable() throws IOException {
    DBUtils.runInsert(connectionFactory, DROP_TABLE_SQL,
        DBUtils.EMPTY_SQL_PARAMS, sqlNumRetries);
    LOG.info("Drop table " + tblName);
  }
  
  public long getPutStripeSleepTime() {
    return putStripeSleepTime;
  }
  
  public DBConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }  
}

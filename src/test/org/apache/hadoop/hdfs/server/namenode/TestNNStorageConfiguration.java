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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestNNStorageConfiguration {
  
  final static Log LOG = LogFactory.getLog(TestNNStorageConfiguration.class);
  
  private Configuration getConf() {
    Configuration conf = new Configuration();
    // provide a clear configuration
    conf.clear();
    return conf;
  }
  
  @Test
  public void testCheckpointTriggerBasic() {
    Configuration conf = getConf();
    conf.setLong("fs.checkpoint.txns", 200);
    
    long count = NNStorageConfiguration.getCheckpointTxnCount(conf);
    assertEquals(200, count);
  }
  
  @Test
  public void testCheckpointTriggerOld() {
    Configuration conf = getConf();
    conf.setLong("fs.checkpoint.size", 10000);
    
    // divided by 100 - if fs.checkpoint.txns is not present
    long count = NNStorageConfiguration.getCheckpointTxnCount(conf);
    assertEquals(100, count);
  }
  
  @Test
  public void testCheckpointTriggerBoth() {
    Configuration conf = getConf();
    conf.setLong("fs.checkpoint.size", 10000);
    conf.setLong("fs.checkpoint.txns", 200);
    
    // if both are present we prioritize fs.checkpoint.txns
    long count = NNStorageConfiguration.getCheckpointTxnCount(conf);
    assertEquals(200, count);
  }
}

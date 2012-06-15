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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

/**
  * Test the generation of parity blocks for files with different block
  * sizes. Also test that a data block can be regenerated from a raid stripe
  * using the parity block
  */
public class TestRaidUtils extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestRaidUtils.class);

  public void testParseOptions() {
    Configuration conf = new Configuration();

    assertEquals(null, conf.get("key1"));
    assertEquals(null, conf.get("key2"));
    assertEquals(null, conf.get("key3"));
    conf.set("test.condensedoption", "key1:value1,key2:value2,key3:value3");
    RaidUtils.parseAndSetOptions(conf, "test.condensedoption");
    assertEquals("value1", conf.get("key1"));
    assertEquals("value2", conf.get("key2"));
    assertEquals("value3", conf.get("key3"));
  }
}

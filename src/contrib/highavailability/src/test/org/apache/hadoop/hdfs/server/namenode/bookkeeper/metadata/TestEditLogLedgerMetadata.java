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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;

public class TestEditLogLedgerMetadata {

  private static final EditLogLedgerMetadata METADATA_1 =
      new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION,
          1, 1, 100);

  private static final EditLogLedgerMetadata METADATA_2 =
      new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION,
          2, 101, -1);

  @Test
  public void testCompareTo() throws Exception {
    EditLogLedgerMetadata allFieldsMatch = new EditLogLedgerMetadata(
        METADATA_1.getLogVersion(), METADATA_1.getLedgerId(),
        METADATA_1.getFirstTxId(), METADATA_1.getLastTxId());
    assertEquals("Should return 0 if all fields match",
        METADATA_1.compareTo(allFieldsMatch), 0);

    assertEquals("Should return -1 if firstTxId is <",
        METADATA_1.compareTo(METADATA_2), -1);
    assertEquals("Should return 1 if firstTxId is >",
        METADATA_2.compareTo(METADATA_1), 1);

  }

  @Test
  public void testEquals() throws Exception {
    assertEquals("Should be equal to itself", METADATA_1, METADATA_1);

    EditLogLedgerMetadata allFieldsMatch = new EditLogLedgerMetadata(
        METADATA_1.getLogVersion(), METADATA_1.getLedgerId(),
        METADATA_1.getFirstTxId(), METADATA_1.getLastTxId());
    assertEquals("Should be equal if all fields match", METADATA_1, allFieldsMatch);

    assertNotSame("Should not be equal if fields do not match", METADATA_1,
        METADATA_2);
  }
}

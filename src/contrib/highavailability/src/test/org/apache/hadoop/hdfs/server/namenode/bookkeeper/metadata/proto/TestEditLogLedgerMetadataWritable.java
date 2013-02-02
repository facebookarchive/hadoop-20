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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.EditLogLedgerMetadata;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Tests the writable class for {@link EditLogLedgerMetadata} to verify that
 * serialization is correct.
 */
public class TestEditLogLedgerMetadataWritable {
  /**
   * Tests that {@link EditLogLedgerMetadata} can be correctly serialized
   * and deserialized.
   */
  @Test
  public void testReadAndWrite() throws Exception {
    EditLogLedgerMetadata ledgerMetadataIn = new EditLogLedgerMetadata(
        FSConstants.LAYOUT_VERSION, 1, 1, -1);
    EditLogLedgerMetadataWritable ledgerMetadataWritableIn =
        new EditLogLedgerMetadataWritable();
    ledgerMetadataWritableIn.set(ledgerMetadataIn);

    // Calls readWriteFields()
    byte[] editLogLedgerMedataBytes =
        WritableUtil.writableToByteArray(ledgerMetadataWritableIn);

    // Calls readFields()
    EditLogLedgerMetadataWritable ledgerMetadataWritableOut =
        WritableUtil.readWritableFromByteArray(editLogLedgerMedataBytes,
            new EditLogLedgerMetadataWritable());

    // Tests that deserialize(read(write(serialize(deserialize(m)) == m
    EditLogLedgerMetadata ledgerMetadataOut = ledgerMetadataWritableOut.get();
    assertEquals(ledgerMetadataIn, ledgerMetadataOut);
  }
}

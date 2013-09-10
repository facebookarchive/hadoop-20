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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestWriteOptions {

  private void check(int flag, WriteOptions options) {
    options.setSyncFileRange(flag);
    assertEquals(flag, options.getSyncFileRange());
  }

  @Test
  public void testSyncFileRange() {
    WriteOptions options = new WriteOptions();
    int a1 = NativeIO.SYNC_FILE_RANGE_WAIT_AFTER;
    int a2 = NativeIO.SYNC_FILE_RANGE_WAIT_BEFORE;
    int a3 = NativeIO.SYNC_FILE_RANGE_WRITE;

    // Default should be SYNC_FILE_RANGE_WRITE.
    assertEquals(a3, options.getSyncFileRange());

    check(a1, options);
    check(a2, options);
    check(a3, options);
    check(a1 | a2, options);
    check(a1 | a3, options);
    check(a2 | a3, options);
  }

}

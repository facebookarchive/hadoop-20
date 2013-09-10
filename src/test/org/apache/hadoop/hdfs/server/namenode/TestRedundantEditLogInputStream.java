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

import static org.junit.Assert.*;

import java.io.File;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class TestRedundantEditLogInputStream {

  private static final Log LOG = LogFactory
      .getLog(TestRedundantEditLogInputStream.class);

  @Test
  public void testStreamComparator() throws Exception {
    Comparator<EditLogInputStream> comparator = RedundantEditLogInputStream.segmentComparator;

    EditLogInputStream elis1 = new EditLogFileInputStream(new File("/foo/bar"),
        0, 10, true);
    EditLogInputStream elis2 = new EditLogFileInputStream(new File("/foo/bar"),
        0, 11, true);
    
    EditLogInputStream elis3 = new EditLogFileInputStream(new File("/foo/bar"),
        0, 10, false);
    EditLogInputStream elis4 = new EditLogFileInputStream(new File("/foo/bar"),
        0, 11, false);
    
    // finalized is always preferred
    assertTrue(0 > comparator.compare(elis1, elis3));
    assertTrue(0 < comparator.compare(elis3, elis1));
    
    assertTrue(0 > comparator.compare(elis2, elis3));
    assertTrue(0 < comparator.compare(elis3, elis2));
    
    // finalized longer is preferred
    assertTrue(0 < comparator.compare(elis3, elis4));
    assertTrue(0 > comparator.compare(elis4, elis3));
    
    // inprogress longer is preferred
    assertTrue(0 < comparator.compare(elis1, elis2));
    assertTrue(0 > comparator.compare(elis2, elis1));
  }
}

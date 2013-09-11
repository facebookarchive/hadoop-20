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

import org.apache.hadoop.io.DataTransferHeaderOptions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestDataTransferHeaderOptions {

  @Test
  public void testRandom() {
    DataTransferHeaderOptions options = new DataTransferHeaderOptions();
    for (int c = 0; c < 4; c++) {
      for (int data = 0; data < 8; data++) {
        for (int advise = 0; advise < 6; advise++) {
          options.setFadvise(advise);
          options.setIoprio(c, data);
          assertEquals(advise, options.getFadvise());
          assertEquals(c, options.getIoprioClass());
          assertEquals(data, options.getIoprioData());
        }
      }
    }
  }

  @Test
  public void testRandomSerialize() throws Exception {
    DataTransferHeaderOptions options = new DataTransferHeaderOptions();
    for (int c = 0; c < 4; c++) {
      for (int data = 0; data < 8; data++) {
        for (int advise = 0; advise < 6; advise++) {
          options.setFadvise(advise);
          options.setIoprio(c, data);

          // Serialize.
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bos);
          options.write(out);
          byte[] buf = bos.toByteArray();
          out.close();

          // De - Serialize
          DataInputStream in = new DataInputStream(
              new ByteArrayInputStream(buf));
          DataTransferHeaderOptions expectedOptions = new DataTransferHeaderOptions();
          expectedOptions
          .readFields(in);

          assertEquals(advise, expectedOptions.getFadvise());
          assertEquals(c, expectedOptions.getIoprioClass());
          assertEquals(data, expectedOptions.getIoprioData());
        }
      }
    }
  }
}

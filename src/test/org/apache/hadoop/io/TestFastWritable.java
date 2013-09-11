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
package org.apache.hadoop.io;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FastWritableRegister.FastWritableId;
import org.apache.hadoop.io.FastWritableRegister.FastWritable;
import org.junit.Before;
import org.junit.Test;

public class TestFastWritable {

  public static final Log LOG = LogFactory.getLog(TestFastWritable.class
      .getName());

  static class FW1 implements FastWritable {
    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public FastWritable getFastWritableInstance(Configuration conf) {
      return new FW1();
    }

    @Override
    public byte[] getSerializedName() {
      return ObjectWritable
          .prepareCachedNameBytes(FastWritableRegister.FastWritableId.SERIAL_VERSION_ID_1
              .toString());
    }
  }

  static class FW2 implements FastWritable {
    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public FastWritable getFastWritableInstance(Configuration conf) {
      return new FW2();
    }

    @Override
    public byte[] getSerializedName() {
      return ObjectWritable
          .prepareCachedNameBytes(FastWritableRegister.FastWritableId.SERIAL_VERSION_ID_1
              .toString());
    }
  }

  @Before
  public void setUp() {
    FastWritableRegister.clear();
  }

  @Test
  public void testBasic() {
    FW1 fw1 = new FW1();
    FW2 fw2 = new FW2();

    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw1);
    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_2, fw2);
    assertTrue(FastWritableRegister.tryGetInstance(
        FastWritableId.SERIAL_VERSION_ID_1.toString(), new Configuration()) instanceof FW1);
    assertTrue(FastWritableRegister.tryGetInstance(
        FastWritableId.SERIAL_VERSION_ID_2.toString(), new Configuration()) instanceof FW2);
  }

  @Test
  public void testUnique() {
    FW1 fw1 = new FW1();
    FW2 fw2 = new FW2();

    // registering twice the same singleton should be fine
    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw1);
    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw1);

    try {
      FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1,
          new FW1());
      fail("Should not allow to register the same name even with the same type");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }

    try {
      FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw1);
      FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw2);
      fail("Should not allow to register two classes with the same name");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
  }

  @Test
  public void testInstance() {
    FW1 fw1 = new FW1();
    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_1, fw1);
    Set<FastWritable> instances = new HashSet<FastWritable>();
    Configuration conf = new Configuration();
    for (int i = 0; i < 100; i++) {
      instances.add(FastWritableRegister.tryGetInstance(
          FastWritableId.SERIAL_VERSION_ID_1.toString(), conf));
    }
    assertEquals(100, instances.size());
  }

}

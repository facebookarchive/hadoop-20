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
package org.apache.hadoop.ipc;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.FastProtocolRegister.FastProtocolId;
import org.apache.hadoop.ipc.FastProtocolRegister.FastProtocol;
import org.junit.Before;
import org.junit.Test;

public class TestFastProtocol {

  public static final Log LOG = LogFactory.getLog(TestFastProtocol.class
      .getName());

  static class FP1 implements FastProtocol {
    public void method() {
    }
  }

  static class FP2 implements FastProtocol {
    public void method() {
    }
  }

  @Before
  public void setUp() {
    FastProtocolRegister.clear();
  }

  @Test
  public void testBasic() throws Exception {
    FP1 fp1 = new FP1();
    FP2 fp2 = new FP2();

    FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1, fp1
        .getClass().getMethod("method"));
    FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_2, fp2
        .getClass().getMethod("method"));
    assertTrue(FastProtocolRegister.tryGetMethod(
        FastProtocolId.SERIAL_VERSION_ID_1.toString()).equals(
        fp1.getClass().getMethod("method")));
    assertTrue(FastProtocolRegister.tryGetMethod(
        FastProtocolId.SERIAL_VERSION_ID_2.toString()).equals(
        fp2.getClass().getMethod("method")));
  }

  @Test
  public void testUnique() throws Exception {
    FP1 fp1 = new FP1();
    FP2 fp2 = new FP2();

    Method m = fp1.getClass().getMethod("method");
    // registering twice the same singleton should be fine
    FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1, m);
    FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1, m);

    try {
      FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1,
          new FP1().getClass().getMethod("method"));
      fail("Should not allow to register the same name with another method instance");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }

    try {
      FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1, fp2
          .getClass().getMethod("method"));
      fail("Should not allow to register two classes with the same name");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
  }
}

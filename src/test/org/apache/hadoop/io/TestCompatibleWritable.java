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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCompatibleWritable {
  private static final String TEST_LONG_STRING = RandomStringUtils.random(2 * 1024, false, true);

  @Test(timeout = 1000)
  public void test0to0() throws Exception {
    TypeVersion0 a = new TypeVersion0();
    TypeVersion0 b = new TypeVersion0();
    checkedCloneInto(a, b);
    assertFalse(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test0to1() throws Exception {
    TypeVersion0 a = new TypeVersion0();
    TypeVersion1 b = new TypeVersion1();
    checkedCloneInto(a, b);
    assertTrue(b.assignedDefaults1);
    assertFalse(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test0to2() throws Exception {
    TypeVersion0 a = new TypeVersion0();
    TypeVersion2 b = new TypeVersion2();
    checkedCloneInto(a, b);
    assertTrue(b.assignedDefaults1);
    assertTrue(b.assignedDefaults2);
    assertFalse(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test1to0() throws Exception {
    TypeVersion1 a = new TypeVersion1();
    TypeVersion0 b = new TypeVersion0();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertTrue(a.getBufferCalled);
    assertTrue(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test1to1() throws Exception {
    TypeVersion1 a = new TypeVersion1();
    TypeVersion1 b = new TypeVersion1();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertFalse(b.assignedDefaults1);
    assertTrue(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test1to2() throws Exception {
    TypeVersion1 a = new TypeVersion1();
    TypeVersion2 b = new TypeVersion2();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertFalse(b.assignedDefaults1);
    assertTrue(b.assignedDefaults2);
    assertTrue(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test2to0() throws Exception {
    TypeVersion2 a = new TypeVersion2();
    TypeVersion0 b = new TypeVersion0();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertFalse(a.assignedDefaults2);
    assertTrue(a.getBufferCalled);
    assertTrue(b.getBufferCalled);
    assertTrue(a.getBufferCalled);
    assertTrue(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test2to1() throws Exception {
    TypeVersion2 a = new TypeVersion2();
    TypeVersion1 b = new TypeVersion1();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertFalse(a.assignedDefaults2);
    assertFalse(b.assignedDefaults1);
    assertTrue(a.getBufferCalled);
    assertTrue(b.getBufferCalled);
  }

  @Test(timeout = 1000)
  public void test2to2() throws Exception {
    TypeVersion2 a = new TypeVersion2();
    TypeVersion2 b = new TypeVersion2();
    checkedCloneInto(a, b);
    assertFalse(a.assignedDefaults1);
    assertFalse(a.assignedDefaults2);
    assertFalse(b.assignedDefaults1);
    assertFalse(b.assignedDefaults2);
    assertTrue(a.getBufferCalled);
    assertFalse(b.getBufferCalled);
  }

  @Test
  public void test3to3() throws Exception {
    TypeVersion0Enclosing a = new TypeVersion0Enclosing();
    TypeVersion0Enclosing b = new TypeVersion0Enclosing();
    checkedCloneInto(a, b);
  }

  private void checkedCloneInto(Writable src, Writable dst) throws IOException {
    DataOutputBuffer outBuffer = new DataOutputBuffer();
    DataInputBuffer inBuffer = new DataInputBuffer();
    outBuffer.reset();
    src.write(outBuffer);
    inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    dst.readFields(inBuffer);
    assertEquals(inBuffer.getLength(), inBuffer.getPosition());
    assertFalse(inBuffer.canReadNextVersion());
  }

  ////////////////////////////////////////////////
  // Example versioned classes
  ////////////////////////////////////////////////

  /** This is an initial version - a base class. */
  private static class TypeVersion0 extends CompatibleWritable {
    public final String FIELDS0 = "0123";
    /** Not serialized */
    public boolean getBufferCalled = false;

    @Override
    protected int getVersion() {
      return INITIAL_VERSION;
    }

    @Override
    protected byte[] getBuffer(int size) {
      getBufferCalled = true;
      return super.getBuffer(size);
    }

    @Override
    protected void writeCompatible(DataOutput out) throws IOException {
      Text.writeStringOpt(out, FIELDS0);
    }

    @Override
    protected void readCompatible(CompatibleDataInput in) throws IOException {
      assertEquals(FIELDS0, Text.readStringOpt(in));
    }
  }

  /** This is an extended version, which adds a few fields. */
  private static class TypeVersion1 extends TypeVersion0 {
    public final String FIELDS1 = "45678";
    public final String FIELDS1extra = "fawefsd";
    public boolean assignedDefaults1 = false;

    @Override
    protected int getVersion() {
      // Consecutive versions must increment this by one.
      // This could be super.getVersion() + 1, but it's not for efficiency reasons.
      return INITIAL_VERSION + 1;
    }

    @Override
    protected void writeCompatible(DataOutput out) throws IOException {
      // Just like in {@link Writable}, first serialize base class (previous versions).
      super.writeCompatible(out);
      Text.writeStringOpt(out, FIELDS1);
      Text.writeStringOpt(out, FIELDS1extra);
    }

    @Override
    protected void readCompatible(CompatibleDataInput in) throws IOException {
      super.readCompatible(in);
      // This call is not a pure function (predicate in this case),
      // call only once per version increment.
      if (in.canReadNextVersion()) {
        assertEquals(FIELDS1, Text.readStringOpt(in));
        assertEquals(FIELDS1extra, Text.readStringOpt(in));
      } else {
        assignedDefaults1 = true;
      }
    }
  }

  private static class TypeVersion2 extends TypeVersion1 {
    public final String FIELDS2 = "9AB";
    public TypeVersion0Enclosing enclosing = new TypeVersion0Enclosing();
    public boolean assignedDefaults2 = false;

    @Override
    protected int getVersion() {
      return INITIAL_VERSION + 2;
    }

    @Override
    protected void writeCompatible(DataOutput out) throws IOException {
      super.writeCompatible(out);
      Text.writeStringOpt(out, FIELDS2);
      enclosing.write(out);
    }

    @Override
    protected void readCompatible(CompatibleDataInput in) throws IOException {
      super.readCompatible(in);
      if (in.canReadNextVersion()) {
        assertEquals(FIELDS2, Text.readStringOpt(in));
        enclosing.readFields(in);
      } else {
        assignedDefaults2 = true;
      }
    }
  }

  private static class TypeVersion0Enclosing extends CompatibleWritable {
    public final String FIELDS = "zxcvcvx";

    @Override
    protected int getVersion() {
      return INITIAL_VERSION;
    }

    @Override
    protected void writeCompatible(DataOutput out) throws IOException {
      Text.writeStringOpt(out, FIELDS);
    }

    @Override
    protected void readCompatible(CompatibleDataInput in) throws IOException {
      assertEquals(FIELDS, Text.readStringOpt(in));
      assertFalse(in.canReadNextVersion());
    }
  }
}

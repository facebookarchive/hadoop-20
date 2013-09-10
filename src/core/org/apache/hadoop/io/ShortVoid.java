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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FastWritableRegister.FastWritable;
import org.apache.hadoop.io.FastWritableRegister.FastWritableId;

/**
 * An object of this class can be used for fast responses for rpc calls with
 * void return value. Using primitive "void" is translated into
 * ObjectWritable.NullInstance instance. This return type ensures minimal
 * processing overhead of the responses.
 */
public class ShortVoid implements FastWritable {

  // the only object of this class
  public static final ShortVoid instance = new ShortVoid();

  private static final String classId = FastWritableRegister.FastWritableId.SERIAL_VERSION_ID_VOID
      .toString();
  public static final byte[] serializedName = ObjectWritable
      .prepareCachedNameBytes(classId);

  private ShortVoid() {
    // we do not allow to instantiate an object of this class
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // empty
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // empty
  }

  @Override
  public FastWritable getFastWritableInstance(Configuration conf) {
    return instance;
  }

  @Override
  public byte[] getSerializedName() {
    return serializedName;
  }

  // register this class to FastWritable registry
  public static void init() {
    FastWritableRegister.register(FastWritableId.SERIAL_VERSION_ID_VOID,
        instance);
  }

  // for clients which might not explicitly call init()
  // we initialize here
  static {
    init();
  }
}

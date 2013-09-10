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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Metadata for a currently open file
 * @see FileSystem#iterativeGetOpenFiles
 */
@ThriftStruct
public class OpenFileInfo implements Writable {
  
  public OpenFileInfo() {
  }

  public OpenFileInfo(String filePath, long millisOpen) {
    this.filePath = filePath;
    this.millisOpen = millisOpen;
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, filePath);
    out.writeLong(millisOpen);
  }

  public void readFields(DataInput in) throws IOException {
    filePath = Text.readString(in);
    millisOpen = in.readLong();
  }

  @ThriftField(1)
  public String filePath;       // full path of the file
  @ThriftField(2)
  public long millisOpen;       // milliseconds that the file has been held open

  public static void write(DataOutput out, OpenFileInfo elem) throws IOException {
    OpenFileInfo info = new OpenFileInfo(elem.filePath, elem.millisOpen);
    info.write(out);
  }

  public static OpenFileInfo read(DataInput in) throws IOException {
    OpenFileInfo info = new OpenFileInfo();
    info.readFields(in);
    return info;
  }
}


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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HarProperties implements Writable {
  private long modificationTime = 0;
  private long accessTime = 0;
  private FsPermission permission = null;
  private String owner = null;
  private String group = null;
 
  public HarProperties() {}
  
  public HarProperties(long modificationTime, long accessTime, FsPermission permission,
      String owner, String group) {
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.permission = permission;
    this.owner = owner;
    this.group = group;
  }
  
  static public HarProperties fromString(String data) throws UnsupportedEncodingException {
    String[] newsplits = HarFileSystem.decode(data).split(" ");
    long modificationTime = Long.parseLong(newsplits[0]);
    long accessTime = Long.parseLong(newsplits[1]);
    FsPermission permission = new FsPermission(Short.parseShort(newsplits[2]));
    String owner = HarFileSystem.decode(newsplits[3]);
    String group = HarFileSystem.decode(newsplits[4]);
    return new HarProperties(modificationTime, accessTime, permission, owner, group);
  } 
  
  public String serialize() throws UnsupportedEncodingException {
    return HarFileSystem.encode(
        modificationTime + " "
            + accessTime + " "
            + permission.toShort() + " "
            + HarFileSystem.encode(owner) + " "
            + HarFileSystem.encode(group));
  } 
  
  public HarProperties(FileStatus fileStatus) {
    modificationTime = fileStatus.getModificationTime();
    accessTime = fileStatus.getAccessTime();
    permission = fileStatus.getPermission();
    owner = fileStatus.getOwner();
    group = fileStatus.getGroup();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    modificationTime = in.readLong();
    accessTime = in.readLong();
    permission = new FsPermission(in.readShort());
    owner = Text.readString(in);
    group = Text.readString(in);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(modificationTime);
    out.writeLong(accessTime);
    out.writeShort(permission.toShort());
    Text.writeString(out, owner);
    Text.writeString(out, group);
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public FsPermission getPermission() {
    return permission;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }
}

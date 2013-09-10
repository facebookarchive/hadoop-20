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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.hdfs.server.datanode.DatanodeProtocols;

public class AvatarDatanodeCommand extends DatanodeCommand {
  static class Backoff extends DatanodeCommand {
    private Backoff() {super(DatanodeProtocols.DNA_BACKOFF);}
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }
  
  static class ClearPrimary extends DatanodeCommand {
    private ClearPrimary() {
      super(DatanodeProtocols.DNA_CLEARPRIMARY);
    }
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }
  
  static class PrepareFailover extends DatanodeCommand {
    private PrepareFailover() {
      super(DatanodeProtocols.DNA_PREPAREFAILOVER);
    }
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }

  public static final DatanodeCommand BACKOFF = new Backoff();
  public static final DatanodeCommand CLEARPRIMARY = new ClearPrimary();
  public static final DatanodeCommand PREPAREFAILOVER = new PrepareFailover();
}


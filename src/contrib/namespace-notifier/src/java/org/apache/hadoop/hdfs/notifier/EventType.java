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
package org.apache.hadoop.hdfs.notifier;

import java.util.HashMap;
import java.util.Map;


public enum EventType {
  // A file was added. Won't trigger for a directory. 
  FILE_ADDED   ((byte)0),
  // A file was closed. Won't trigger for a directory.
  FILE_CLOSED  ((byte)1),
  // A file or directory was deleted.
  NODE_DELETED ((byte)2),
  // A directory was added.
  DIR_ADDED    ((byte)3);
  
  
  // Caching the association between the byte value and the enum value
  private static final Map<Byte, EventType> byteToEnum = 
      new HashMap<Byte, EventType>();
  
  // The actual byte value for this instance
  private byte byteValue;
  
  EventType(byte value) {
    byteValue = value;
  }

  static {
    // Initialize the mapping from byte to enum
    for(EventType eventType : values())
      byteToEnum.put(eventType.getByteValue(), eventType);
  }

  public byte getByteValue() {
    return byteValue;
  }
  
  public static EventType fromByteValue(byte byteValue) {
    return byteToEnum.get(byteValue);
  }
  
};

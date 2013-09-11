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

/**
 * Wrapper class over NamespaceEvent. It can be used as a key in a map or an
 * entry in a set.
 */
public class NamespaceEventKey {
  
  public NamespaceEvent event;
  
  private int hashCodeValue;
  
  public NamespaceEventKey(NamespaceEvent event) {
    this.event = event;
    hashCodeValue = (int)event.getType() + event.getPath().hashCode();
  }
  
  
  public NamespaceEventKey(NamespaceNotification n) {
    event = new NamespaceEvent(NotifierUtils.getBasePath(n), n.type);
    hashCodeValue = (int)event.getType() + event.getPath().hashCode();
  }
  
  
  public NamespaceEventKey(String path, EventType type) {
    event = new NamespaceEvent(path, type.getByteValue());
    hashCodeValue = (int)event.getType() + event.getPath().hashCode();
  }
  
  
  public NamespaceEventKey(String path, byte type) {
    event = new NamespaceEvent(path, type);
    hashCodeValue = (int)event.getType() + event.getPath().hashCode();
  }
  
  
  public NamespaceEvent getEvent() {
    return event;
  }
  
  
  @Override
  public int hashCode() {
    return hashCodeValue;
  }
  
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NamespaceEventKey)) {
      return false;
    }
    
    NamespaceEvent event2 = ((NamespaceEventKey)o).event;
    return event2.getPath().equals(event.getPath()) &&
        event2.getType() == event.getType();
  }

}

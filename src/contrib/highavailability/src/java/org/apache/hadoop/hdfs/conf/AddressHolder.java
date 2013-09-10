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

package org.apache.hadoop.hdfs.conf;

import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Simple holder object that bundles three things together:
 * 1. Configuration source object.
 * 2. Configuration key that this object will be responsible for
 * 3. The type<T> and parsing logic it needs to do for that object.
 */
public abstract class AddressHolder<T> {
  private Configurable parent;

  public AddressHolder(Configurable theParent) {
    this.parent = theParent;
  }

  protected Configuration getConf() {
    return parent.getConf();
  }

  public abstract T getAddress() throws UnknownHostException;

  public abstract String getKey();
}

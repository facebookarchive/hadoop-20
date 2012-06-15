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
package org.apache.hadoop.hdfs.protocol;

import java.util.Collection;
import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.fs.Path;

/**********************************************************************
 * HighTideProtocol is used by user code 
 * {@link org.apache.hadoop.hdfs.HighTideShell} class to communicate 
 * with the HighTideNode.  User code can manipulate the configured policies.
 *
 **********************************************************************/
public interface HighTideProtocol extends VersionedProtocol {

  /**
   * Compared to the previous version the following changes have been introduced:
   * Only the latest change is reflected.
   * 1: new protocol introduced
   */
  public static final long versionID = 1L;

  /**
   * Get a listing of all configured policies
   * @throws IOException
   * return all categories of configured policies
   */
  public PolicyInfo[] getAllPolicies() throws IOException;
}

/*
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

package org.apache.hadoop.mapred;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * The protocol is used to set and get the clusterManagerSafeMode flag
 * on the ProxyJobTracker
 */
public interface ClusterManagerSafeModeProtocol extends VersionedProtocol {
  /**
   * The versionID is used to compare if the versions are correct
   */
  final int versionID = 1;

  /**
   * Set the clusterManagerSafeMode flag on ProxyJobTracker
   * @param safeMode Is the value of the clusterManagerSafeMode flag that we
   *                 want to be set.
   * @return 0 if the flag was set successfully.
   */
  int setClusterManagerSafeModeFlag(boolean safeMode);

  /**
   * Get the clusterManagerSafeMode flag from the ProxyJobTracker
   * @return A boolean which is the value of the clusterManagerSafeMode flag.
   */
  boolean getClusterManagerSafeModeFlag();
}

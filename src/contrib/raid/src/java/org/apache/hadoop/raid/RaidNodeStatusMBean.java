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
package org.apache.hadoop.raid;

public interface RaidNodeStatusMBean {
  /**
   * Returns how long (in seconds) raidnode doesn't fix a block since
   * last successful fix
   * 0 if no block needs to fix
   * current - lastFixTime if there are some blocks need to be fixed. 
   * @return
   */
  public long getTimeSinceLastSuccessfulFix();
  
  /**
   * Returns the number of under-redundant files failed to increase
   * replication to 3
   */
  public long getNumUnderRedundantFilesFailedIncreaseReplication();
  
  /**
   * Returns the number of under-redundant files succeeded to increase
   * replication to 3
   */
  public long getNumUnderRedundantFilesSucceededIncreaseReplication();
}

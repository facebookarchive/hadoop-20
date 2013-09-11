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
package org.apache.hadoop.hdfs.server.namenode;

/**
 * A pluggabe safe mode interface.
 */
public interface SafeModeInfo {
  
  /**
   * A tip on how safe mode is to be turned off: manually or automatically.
   */
  public String getTurnOffTip();
  
  /**
   * Check if safe mode was entered manually or at startup.
   */
  public boolean isManual();

  /**
   * Check if safe mode is on.
   * 
   * @return true if in safe mode
   */
  public boolean isOn();

  /**
   * Leave safe mode.
   * <p/>
   * Switch to manual safe mode if distributed upgrade is required.<br>
   * Check for invalid, under- & over-replicated blocks in the end of startup.
   * 
   * @param checkForUpgrades
   *          whether or not we need to check for upgrades
   */
  public void leave(boolean checkForUpgrades);

  /**
   * Enter safemode manually.
   */
  public void setManual();

  /**
   * Shutdown the safe mode.
   */
  public void shutdown();

  /**
   * Whether or not we can leave safe mode.
   * @return true if can leave or false otherwise.
   */
  public boolean canLeave();

  /**
   * Check and trigger safe mode if needed.
   */
  public void checkMode();
  
  /**
   * Check if the RBW reports should be processed.
   */
  public boolean shouldProcessRBWReports();
  
  /**
   * Initializes replication queues *without* leaving safemode.
   * This should only be used only through dfsadmin command.
   */
  public void initializeReplicationQueues();
}

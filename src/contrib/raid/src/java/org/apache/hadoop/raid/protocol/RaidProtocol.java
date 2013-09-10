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
package org.apache.hadoop.raid.protocol;

import java.io.IOException;
import org.apache.hadoop.ipc.VersionedProtocol;

/**********************************************************************
 * RaidProtocol is used by user code 
 * {@link org.apache.hadoop.raid.RaidShell} class to communicate 
 * with the RaidNode.  User code can manipulate the configured policies.
 *
 **********************************************************************/
public interface RaidProtocol extends VersionedProtocol {

  /**
   * Compared to the previous version the following changes have been introduced:
   * Only the latest change is reflected.
   * 1: new protocol introduced
   * 2: send recovery lag
   */
  public static final long versionID = 2L;

  /**
   * Get a listing of all configured policies
   * @throws IOException
   * return all categories of configured policies
   */
  public PolicyInfo[] getAllPolicies() throws IOException;

  /**
   * Unraid the specified input path. This is called when the specified file
   * is corrupted. This call will move the specified file to file.old
   * and then recover it from the RAID subsystem.
   *
   * @param inputPath The absolute pathname of the file to be recovered.
   * @param corruptOffset The offset that has the corruption
   */
  public String recoverFile(String inputPath, long corruptOffset) throws IOException;
  
  /*
   * When a file is fixed, the block fixer task will send the recovery time to
   * raidnode for data collection.
   * If the path fails to fix, recoveryTime will be Integer.MAX_VALUE
   * taskId is provided for debugging.
   */
  public void sendRecoveryTime(String path, long recoveryTime, String taskId)
      throws IOException;

  
  /*
   * Ask raidnode to start a smoke test to verify everything is working
   */
  public boolean startSmokeTest() throws Exception;
}

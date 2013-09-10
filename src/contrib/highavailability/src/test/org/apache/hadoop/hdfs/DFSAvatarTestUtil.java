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

package org.apache.hadoop.hdfs;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.namenode.AvatarNode;

public class DFSAvatarTestUtil {

  /**
   * Make sure standby has caught up with primary in reading and applying logs
   * This process should not take more than 5 seconds
   * @param primaryAvatar
   * @param standbyAvatar
   */
  public static void assertTxnIdSync(AvatarNode primaryAvatar, AvatarNode standbyAvatar) {
    long startTime = System.currentTimeMillis();
    long targetTxnId = getCurrentTxId(primaryAvatar);
    while (true) {
      if (System.currentTimeMillis() - startTime > 5000 ||
          getCurrentTxId(standbyAvatar) == targetTxnId) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    assertTrue(getCurrentTxId(standbyAvatar) == targetTxnId);
  }
  
  public static long getCurrentTxId(AvatarNode avatar) {
    return avatar.getFSImage().getEditLog().getCurrentTxId();
  }

}

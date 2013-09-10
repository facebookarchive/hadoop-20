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

import java.util.ArrayList;

public class FSNamesystemDatanodeHelper {

  public static DatanodeStatus getDataNodeStats(FSNamesystem ns) {
    return getDatanodeStats(ns, new ArrayList<DatanodeDescriptor>(),
        new ArrayList<DatanodeDescriptor>());
  }

  /**
   * Get status of the datanodes in the system.
   */
  public static DatanodeStatus getDatanodeStats(FSNamesystem ns,
      ArrayList<DatanodeDescriptor> live, ArrayList<DatanodeDescriptor> dead) {
    ns.DFSNodesStatus(live, dead);
    ArrayList<DatanodeDescriptor> decommissioning = ns
        .getDecommissioningNodesList(live);

    // live nodes
    int numLive = live.size();
    int numLiveExcluded = 0;
    int numLiveDecommissioningInProgress = decommissioning.size();
    int numLiveDecommissioned = 0;

    for (DatanodeDescriptor d : live) {
      numLiveDecommissioned += d.isDecommissioned() ? 1 : 0;
      numLiveExcluded += ns.inExcludedHostsList(d, null) ? 1 : 0;
    }

    // dead nodes
    int numDead = dead.size();
    int numDeadExcluded = 0;
    int numDeadDecommissioningNotCompleted = 0;
    int numDeadDecommissioned = 0;

    for (DatanodeDescriptor d : dead) {
      numDeadDecommissioned += d.isDecommissioned() ? 1 : 0;
      numDeadExcluded += ns.inExcludedHostsList(d, null) ? 1 : 0;
    }

    numDeadDecommissioningNotCompleted = numDeadExcluded - numDeadDecommissioned;

    return new DatanodeStatus(numLive, 
        numLiveExcluded,
        numLiveDecommissioningInProgress, 
        numLiveDecommissioned, 
        numDead,
        numDeadExcluded, 
        numDeadDecommissioningNotCompleted,
        numDeadDecommissioned);
  }

  public static class DatanodeStatus {
    // live nodes
    public final int numLive;
    public final int numLiveExcluded;
    public final int numLiveDecommissioningInProgress;
    public final int numLiveDecommissioned;

    // dead nodes
    public final int numDead;
    public final int numDeadExcluded;
    public final int numDeadDecommissioningNotCompleted;
    public final int numDeadDecommissioned;

    public DatanodeStatus(int numLive, 
        int numLiveExcluded,
        int numLiveDecomissioningInProgress, 
        int numLiveDecommissioned,
        int numDead, 
        int numDeadExcluded,
        int numDeadDecomissioningNotCompleted, 
        int numDeadDecommissioned) {
      this.numLive = numLive;
      this.numLiveExcluded = numLiveExcluded;
      this.numLiveDecommissioningInProgress = numLiveDecomissioningInProgress;
      this.numLiveDecommissioned = numLiveDecommissioned;

      this.numDead = numDead;
      this.numDeadExcluded = numDeadExcluded;
      this.numDeadDecommissioningNotCompleted = numDeadDecomissioningNotCompleted;
      this.numDeadDecommissioned = numDeadDecommissioned;
    }
  }
}

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

package org.apache.hadoop.mapred;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


/**
 * {@link MemBasedLoadManager} implements {@link CapBasedLoadManager} for use by
 * the {@link FairScheduler} that checks the available memory on tasktracker to
 * determine whether to launch a task.
 * 
 * Check the memory usage information of the Jobs and TaskTrackers to
 * determine whether a task can be launched on a particular node.
 *
 * To use this class, set the following configurations
 * 1. mapred.fairscheduler.loadmanager 
 *    org.apache.hadoop.mapred.MemBasedLoadManager
 *    If this is not set up, FairScheduler will use the default LoadManager
 * 2. mapred.membasedloadmanager.reservedvmem.mb
 *    mb of the desired reserved memory (not be used by tasks) on the machine
 *    If this is not set up, the default of 2GB will be used
 * 3. mapred.fairscheduler.membasedloadmanager.affectedusers
 *    The users affected by this policy separated by commas.
 *    If this is not set up, the default is all users.
 * 
 * Other related configurations
 * 1. mapred.job.map.memory.mb
 * 2. mapred.job.reduce.memory.mb
 *    These configurations are with the jobs which will be considered to load.
 *    If these are not set up, the default value zero will be used.
 */
public class MemBasedLoadManager extends CapBasedLoadManager {

  private long reservedPhysicalMemoryOnTT; // in mb
  private static final long DEFAULT_RESERVED_PHYSICAL_MEMORY_ON_TT = 2 * 1024L;
  private boolean affectAllUsers = false;
	private Set<String> affectedUsers = new HashSet<String>();
  public static final Log LOG = LogFactory.getLog(MemBasedLoadManager.class);

  public static final String RESERVED_PHYSICAL_MEMORY_ON_TT_STRING =
          "mapred.membasedloadmanager.reserved.physicalmemory.mb";
  public static final String AFFECTED_USERS_STRING =
          "mapred.membasedloadmanager.affectedusers";

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
    setReservedPhysicalMemoryOnTT(conf.getLong(
            RESERVED_PHYSICAL_MEMORY_ON_TT_STRING,
            DEFAULT_RESERVED_PHYSICAL_MEMORY_ON_TT));
    String [] affectedUsersArray = conf.getStrings(AFFECTED_USERS_STRING);
    if (affectedUsersArray != null) {
      getAffectedUsers().addAll(Arrays.asList(affectedUsersArray));
    }

    if (getAffectedUsers().size() == 0) {
      // if not specified, the policy applies to every user
      setAffectAllUsers(true);
    }
  }

  @Override
  public void start() {
    LOG.info("MemBasedLoadManager started");
  }

  @Override
  public boolean canLaunchTask(TaskTrackerStatus tracker,
      JobInProgress job, TaskType type) {
    // check if this user is affected by the policy
    String user = job.getProfile().getUser();
    if (!isAffectAllUsers()) {
      if (!getAffectedUsers().contains(user)) {
        // this user is not affected
        return super.canLaunchTask(tracker, job, type);
      }
    }

    long availablePhysicalMemory =
            tracker.getResourceStatus().getAvailablePhysicalMemory();

    if (availablePhysicalMemory <
        getReservedPhysicalMemoryOnTT() * 1024 * 1024L) {
      String msg = String.format("Cannot launch %s task. Not enough memory: " +
              "[User: %s, TT:%s, Job:%s, Available:%s, Reserved:%s]",
              type, user, tracker.getHost(), job.getJobID(),
              availablePhysicalMemory,
              getReservedPhysicalMemoryOnTT() * 1024 * 1024L);
      LOG.warn(msg);
      return false;
    }
    String msg = String.format("Launch %s task: " +
            "[User:%s, TT:%s, Job:%s, Available:%s, Reserved:%s]",
            type, user, tracker.getHost(), job.getJobID(),
            availablePhysicalMemory,
            getReservedPhysicalMemoryOnTT() * 1024 * 1024L);
    LOG.debug(msg);
    return true;
  }

  /**
   * Get reserved physical memory on TaskTracker in mb
   * @return reservedPhysicalMemoryOnTT
   */
  public long getReservedPhysicalMemoryOnTT() {
    return reservedPhysicalMemoryOnTT;
  }

  /**
   * Set reserved physical memory on TaskTracker in mb
   * @param reservedPhyscialMemoryOnTT the reservedPhysicalMemoryOnTT to set
   */
  public void setReservedPhysicalMemoryOnTT(long reservedPhysicalMemoryOnTT) {
    this.reservedPhysicalMemoryOnTT = reservedPhysicalMemoryOnTT;
  }

  /**
   * Get the users who will affected by this loader
   * @return the affectedUsers
   */
  public Set<String> getAffectedUsers() {
    return affectedUsers;
  }

  /**
   * Set the users who will be affected by this loader
   * @param affectedUsers the affectedUsers to set
   */
  public void setAffectedUsers(Set<String> affectedUsers) {
    this.affectedUsers = affectedUsers;
  }

  /**
   * Is this loader affects all users?
   * @return the affectAllUsers
   */
  public boolean isAffectAllUsers() {
    return affectAllUsers;
  }

  /**
   * Make this loader affects all users or not.
   * @param affectAllUsers the affectAllUsers to set
   */
  public void setAffectAllUsers(boolean affectAllUsers) {
    this.affectAllUsers = affectAllUsers;
  }
}
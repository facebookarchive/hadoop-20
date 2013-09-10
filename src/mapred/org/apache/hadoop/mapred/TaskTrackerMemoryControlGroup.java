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

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ControlGroup.MemoryControlGroup;
import org.apache.hadoop.syscall.LinuxSystemCall;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Limits memory usages of a TaskTracker and its Task through a Linux memory
 * control group.
 *
 * Requirement:
 * <ul>
 * <li>A memory sub-system control group is available</li>
 * <li>A path to a target control group is configured
 * (mapred.tasktracker.cgroup.mem.root) or the default path will be used
 * (/cgroup/memory/tasktrackers)</li>
 * <li>A user launching Task has a permission to modify a control group (i.e.
 * using chown)</li>
 * <li>A memory limit property is set at the target control group</li>
 * </ul>
 *
 * Current limitation of this implementation:
 * <ul>
 * <li>Support only a single TaskTracker per server</li>
 * </ul>
 */
public class TaskTrackerMemoryControlGroup {
  private static Log LOG = LogFactory.getLog(TaskTrackerMemoryControlGroup.class);

  public static final String CGROUP_MEM_ROOT_PROPERTY = "mapred.container.cgroup.mem.root";
  public static final String DEFAULT_CGROUP_MEM_ROOT = "/cgroup/memory/task_container";
  public static final String CGROUP_MEM_JT_ROOT = "mapred.jobtracker.cgroup.mem.root";
  public static final String DEFAULT_JT_ROOT = "/cgroup/memory/jobtrackers";
  public static final String CGROUP_MEM_TT_ROOT = "mapred.tasktracker.cgroup.mem.root";
  public static final String DEFAULT_TT_ROOT = "/cgroup/memory/tasktrackers";
  // conf to control if we shall setup CGroup memory limit for individual tasks.
  // The default is false
  public static final String CGROUP_MEM_TASK_LIMIT= "mapred.tasktracker.cgroup.mem.tasklimit";

  public static final String CGROUP_TRASH_GROUP_NAME = "trash";

  private boolean isAvailable;
  private MemoryControlGroup ttcgp;
  private MemoryControlGroup jtcgp;
  private MemoryControlGroup containercgp;
  private MemoryControlGroup trashcgp;
  private boolean isTaskLimitOn = false;
  private String rootpath;
  
  private AtomicInteger numFailedToAddTask = new AtomicInteger();

  public TaskTrackerMemoryControlGroup(Configuration conf) {
    if (!MemoryControlGroup.isAvailable()) {
      LOG.warn("TaskMemoryControlGroup is disabled because a memory sub-system is not available");
      isAvailable = false;
      return;
    }

    String jtRootpath = conf.get(CGROUP_MEM_JT_ROOT, DEFAULT_JT_ROOT);
    jtcgp = new MemoryControlGroup(jtRootpath);
    jtcgp.enableMoveChargeAtImmigrate();

    if (!jtcgp.canControl()) {
      LOG.warn("TaskMemoryControlGroup is disabled because jtgroup doesn't have appropriate permission for "
          + jtRootpath);
      isAvailable = false;
      return;
    }

    String ttRootpath = conf.get(CGROUP_MEM_TT_ROOT, DEFAULT_TT_ROOT);
    ttcgp = new MemoryControlGroup(ttRootpath);
    ttcgp.enableMoveChargeAtImmigrate();

    if (!ttcgp.canControl()) {
      LOG.warn("TaskMemoryControlGroup is disabled because ttgroup doesn't have appropriate permission for "
          + ttRootpath);
      isAvailable = false;
      return;
    }
    if (getPID().equals("")) {
      LOG.warn("TaskMemoryControlGroup is disabled because JVM_PID is not set for TaskTracker");
      isAvailable = false;
      return;
    }
    ttcgp.addToGroup(getPID());

    rootpath = conf.get(CGROUP_MEM_ROOT_PROPERTY, DEFAULT_CGROUP_MEM_ROOT);
    containercgp = new MemoryControlGroup(rootpath);
    if (!containercgp.canControl()) {
      LOG.warn("TaskMemoryControlGroup is disabled because TaskTracker does not have appropriate permission for "
          + rootpath);
      isAvailable = false;
      return;
    }

    if (containercgp.getMemoryUsageLimit() <= 0) {
      LOG.warn("TaskMemoryControlGroup is disabled because memory.limit_in_bytes is not set up");
      isAvailable = false;
      return;
    }

    containercgp.enableMoveChargeAtImmigrate();
    containercgp.enableUseHierarchy();

    isAvailable = true;
    isTaskLimitOn = conf.getBoolean(CGROUP_MEM_TASK_LIMIT, false);

    trashcgp = containercgp.createSubGroup(CGROUP_TRASH_GROUP_NAME);
    trashcgp.disableMoveChargeAtImmigrate();
    // delete the old container group. Some tasks are failed to be removed when 
    // the task tracker exited.
    File containDir = new File(rootpath);
    for (String child: containDir.list()) {
      if (child.startsWith("attempt")) {
        LOG.info("Remove " + child);
        try {
          BufferedReader reader = new BufferedReader(new FileReader(
            rootpath + "/" +child + "/tasks"));
          String thread = "";
          while( ( thread = reader.readLine() ) != null) {
            LOG.info(" kill " + thread);
            LinuxSystemCall.killProcessGroup(Integer.parseInt(thread));
          }
          reader.close();
        } catch (java.io.IOException e) {
          LOG.info("Exception in killing tasks");
        }
        removeTask(child);    
      }
    }
    LOG.info("TaskTrackerMemoryControlGroup is created with memory = " +
      containercgp.getMemoryUsageLimit());
  }
  
  public int getAndResetNumFailedToAddTask() {
    return this.numFailedToAddTask.getAndSet(0);
  }

  public void addTask(String taskname, String pid, long memoryLimit) {
    if (!isAvailable) {
      this.numFailedToAddTask.incrementAndGet();
      return ;
    }

    MemoryControlGroup taskcgp = containercgp.createSubGroup(taskname);
    taskcgp.enableMoveChargeAtImmigrate();
    if (isTaskLimitOn) {
      taskcgp.setMemoryUsageLimit(memoryLimit);
      LOG.info("Task " + taskname + " is added to control group with memory = " +
        memoryLimit );
    } else {
      LOG.info("Task " + taskname + " is added to control group without limit");
    }
    taskcgp.addToGroup(pid);
  }

  public void removeTask(String taskname) {
    if (!isAvailable)
      return ;

    MemoryControlGroup taskcgp = containercgp.getSubGroup(taskname);
    trashcgp.addToGroup(taskcgp.getThreadGroupList());
    taskcgp.deleteGroup();
  }

  private static String getPID() {
    return System.getenv().get("JVM_PID");
  }
  
  public boolean getTaskLimitOn(){
    return isTaskLimitOn;
  }
  
  public String getRootPath(){
    return rootpath;
  }
  
  public MemoryControlGroup getContainerMemoryControlGroup() {
    return containercgp;
  }

  public MemoryControlGroup getJTMemoryControlGroup() {
    return jtcgp;
  }

  public MemoryControlGroup getTTMemoryControlGroup() {
    return ttcgp;
  }

  public boolean checkAvailable() {
    return isAvailable;
  }
  
}

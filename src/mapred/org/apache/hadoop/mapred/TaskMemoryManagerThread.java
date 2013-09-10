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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.syscall.LinuxSystemCall;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages memory usage of tasks running under this TT. Kills any task-trees
 * that overflow and over-step memory limits.
 */
class TaskMemoryManagerThread extends Thread {

  public static final int TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT = 5 * 1024;

  private static Log LOG = LogFactory.getLog(TaskMemoryManagerThread.class);

  private TaskTracker taskTracker;
  private ResourceCalculatorPlugin resourceCalculator;
  private final long monitoringInterval;
  private long sleepInterval;

  // The amout of memory are all in bytes
  private final long maxMemoryAllowedForAllTasks;
  private long reservedRssMemory;
  private long maxRssMemoryAllowedForAllTasks;
  private int maxRssMemoryAllowedUpdateCounter;
  static private boolean doUpdateReservedPhysicalMemory = true;
  static public final String HIGH_MEMORY_KEYWORD = "high-memory";
  static public final String TT_MEMORY_MANAGER_MONITORING_INTERVAL =
          "mapred.tasktracker.taskmemorymanager.monitoring-interval";
  // The amount of memory which will not be used for running tasks
  // If this is violated, task with largest memory will be killed.
  static public final String TT_RESERVED_PHYSICAL_MEMORY_MB =
          "mapred.tasktracker.reserved.physicalmemory.mb";
  // The maximum amount of memory that can be used for running task.
  // If this is violated, task with largest memory will be killed.
  static public final String TT_MAX_RSS_MEMORY_MB =
          "mapred.tasktracker.tasks.max.rssmemory.mb";

  private final Map<TaskAttemptID, ProcessTreeInfo> processTreeInfoMap;
  private final Map<TaskAttemptID, ProcessTreeInfo> tasksToBeAdded;
  private final List<TaskAttemptID> tasksToBeRemoved;

  private volatile boolean running = true;
  
  public TaskMemoryManagerThread(TaskTracker taskTracker) {

    this(taskTracker.getTotalMemoryAllottedForTasksOnTT() * 1024 * 1024L,
	       taskTracker.getJobConf().getLong(TT_MEMORY_MANAGER_MONITORING_INTERVAL,
                                          5000L));

    this.taskTracker = taskTracker;
    this.resourceCalculator = taskTracker.resourceCalculatorPlugin;
    loadMaxRssMemoryConfig(taskTracker.getJobConf());
  }

  // mainly for test purposes. note that the tasktracker variable is
  // not set here.
  TaskMemoryManagerThread(long maxMemoryAllowedForAllTasks,
                          long monitoringInterval) {
    setName(this.getClass().getName());

    processTreeInfoMap = new ConcurrentHashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeAdded = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeRemoved = new ArrayList<TaskAttemptID>();

    this.maxMemoryAllowedForAllTasks = maxMemoryAllowedForAllTasks > 0 ?
         maxMemoryAllowedForAllTasks : JobConf.DISABLED_MEMORY_LIMIT;

    this.monitoringInterval = monitoringInterval;
  }

  public void addTask(TaskAttemptID tid, long memLimit) {
    synchronized (tasksToBeAdded) {
      LOG.debug("Add " + tid);
      ProcessTreeInfo ptInfo = new ProcessTreeInfo(tid, null, null, memLimit);
      tasksToBeAdded.put(tid, ptInfo);
    }
  }

  public List<ProcessTreeInfo> getTasks() {
    List<ProcessTreeInfo> taskList =
      new ArrayList<ProcessTreeInfo>();
    synchronized (tasksToBeAdded) {
      taskList.addAll(tasksToBeAdded.values());
      tasksToBeAdded.clear();
    }
    taskList.addAll(processTreeInfoMap.values());
    synchronized (tasksToBeRemoved) {
      for (TaskAttemptID tid : tasksToBeRemoved) {
        taskList.remove(tid);
      }
      tasksToBeRemoved.clear();
    }
    return taskList;
  }

  public void setSleepInterval(long sleepInterval) {
    this.sleepInterval = sleepInterval;
  }

  public void resetSleepInterval() {
    this.sleepInterval = this.monitoringInterval;
  }

  public void removeTask(TaskAttemptID tid) {
    synchronized (tasksToBeRemoved) {
      LOG.debug("Remove " + tid);
      tasksToBeRemoved.add(tid);
    }
  }

  public static class ProcessTreeInfo {
    private final TaskAttemptID tid;
    private String pid;
    private ProcfsBasedProcessTree pTree;
    private final long memLimit;
    private String pidFile;

    public ProcessTreeInfo(TaskAttemptID tid, String pid,
        ProcfsBasedProcessTree pTree, long memLimit) {
      this.tid = tid;
      this.pid = pid;
      this.pTree = pTree;
      this.memLimit = memLimit;
    }

    public TaskAttemptID getTID() {
      return tid;
    }

    public String getPID() {
      return pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    public ProcfsBasedProcessTree getProcessTree() {
      return pTree;
    }

    public void setProcessTree(ProcfsBasedProcessTree pTree) {
      this.pTree = pTree;
    }

    public long getMemLimit() {
      return memLimit;
    }
  }

  @Override
  public void run() {

    LOG.info("Starting thread: " + this.getClass());

    while (running) {
      try {
        // Print the processTrees for debugging.
        if (LOG.isDebugEnabled()) {
          StringBuffer tmp = new StringBuffer("[ ");
          for (ProcessTreeInfo p : processTreeInfoMap.values()) {
            tmp.append(p.getPID());
            tmp.append(" ");
          }
          LOG.debug("Current ProcessTree list : "
              + tmp.substring(0, tmp.length()) + "]");
        }

        //Add new Tasks
        synchronized (tasksToBeAdded) {
          processTreeInfoMap.putAll(tasksToBeAdded);
          tasksToBeAdded.clear();
        }

        //Remove finished Tasks
        synchronized (tasksToBeRemoved) {
          for (TaskAttemptID tid : tasksToBeRemoved) {
            processTreeInfoMap.remove(tid);
          }
          tasksToBeRemoved.clear();
        }

        long memoryStillInUsage = 0;
        long rssMemoryStillInUsage = 0;
        taskTracker.setTaskTrackerRSSMem(resourceCalculator.getProcResourceValues().getPhysicalMemorySize());
        
        // Now, check memory usage and kill any overflowing tasks
        for (Iterator<Map.Entry<TaskAttemptID, ProcessTreeInfo>> it = processTreeInfoMap
            .entrySet().iterator(); it.hasNext();) {
          Map.Entry<TaskAttemptID, ProcessTreeInfo> entry = it.next();
          TaskAttemptID tid = entry.getKey();
          ProcessTreeInfo ptInfo = entry.getValue();
          try {
            String pId = ptInfo.getPID();

            // Initialize any uninitialized processTrees
            if (pId == null) {
              // get pid from taskAttemptId
              pId = taskTracker.getPid(ptInfo.getTID());
              if (pId != null) {
                // PID will be null, either if the pid file is yet to be created
                // or if the tip is finished and we removed pidFile, but the TIP
                // itself is still retained in runningTasks till successful
                // transmission to JT

                // create process tree object
                long sleeptimeBeforeSigkill = taskTracker.getJobConf().getLong(
                    JvmManager.SLEEPTIME_BEFORE_SIGKILL_KEY,
                    ProcessTree.DEFAULT_SLEEPTIME_BEFORE_SIGKILL);

                ProcfsBasedProcessTree pt = new ProcfsBasedProcessTree(
                    pId,ProcessTree.isSetsidAvailable, sleeptimeBeforeSigkill);
                LOG.debug("Tracking ProcessTree " + pId + " for the first time");

                ptInfo.setPid(pId);
                ptInfo.setProcessTree(pt);
              }
            }
            // End of initializing any uninitialized processTrees

            if (pId == null) {
              continue; // processTree cannot be tracked
            }

            LOG.debug("Constructing ProcessTree for : PID = " + pId + " TID = "
                + tid);
            ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
            pTree = pTree.getProcessTree(); // get the updated process-tree
            ptInfo.setProcessTree(pTree); // update ptInfo with process-tree of
            // updated state
            long currentMemUsage = pTree.getCumulativeVmem();
            long currentRssMemUsage = pTree.getCumulativeRssmem();
            // as processes begin with an age 1, we want to see if there 
            // are processes more than 1 iteration old.
            long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
            long limit = ptInfo.getMemLimit();
            String user = taskTracker.getUserName(ptInfo.tid);
            if (user == null) {
              // If user is null the task is deleted from the TT memory
              continue;
            }
            // Log RSS and virtual memory usage of all tasks
            LOG.debug((String.format("Memory usage of ProcessTree %s : " +
                               "[USER,TID,RSS,VMEM,VLimit,TotalRSSLimit]"
                     + "=[%s,%s,%s,%s,%s,%s]",
                     pId, user, ptInfo.tid, currentRssMemUsage,
                     currentMemUsage, limit, maxRssMemoryAllowedForAllTasks)));

            if (doCheckVirtualMemory() &&
                isProcessTreeOverLimit(tid.toString(), currentMemUsage,
                                        curMemUsageOfAgedProcesses, limit)) {
              // Task (the root process) is still alive and overflowing memory.
               // Dump the process-tree and then clean it up.
              String msg =
                  "TaskTree [pid=" + pId + ",tipID=" + tid
                      + "] is running beyond memory-limits. Current usage : "
                      + currentMemUsage + "bytes. Limit : " + limit
                      + "bytes. Killing task. \nDump of the process-tree for "
                      + tid + " : \n" + pTree.getProcessTreeDump();
              LOG.warn(msg);
              taskTracker.cleanUpOverMemoryTask(tid, true, msg);

              LinuxSystemCall.killProcessGroup(Integer.parseInt(pId));
              it.remove();
              LOG.info("Removed ProcessTree with root " + pId);
            } else {
              // Accounting the total memory in usage for all tasks that are still
              // alive and within limits.
              memoryStillInUsage += currentMemUsage;
              rssMemoryStillInUsage += currentRssMemUsage;
            }
          } catch (Exception e) {
            // Log the exception and proceed to the next task.
            LOG.warn("Uncaught exception in TaskMemoryManager "
                + "while managing memory of " + tid, e);
          }
        }
        long availableRssMemory =
            resourceCalculator.getAvailablePhysicalMemorySize();

        long phyTotal = resourceCalculator.getPhysicalMemorySize();
        long unaccountedMemory = phyTotal -
            availableRssMemory - rssMemoryStillInUsage;
        taskTracker.getTaskTrackerInstrumentation().unaccountedMemory(
            unaccountedMemory);
        LOG.info("phyTotal:" + phyTotal +
            " unaccounted:" + unaccountedMemory +
            " vMemory:" + memoryStillInUsage +
            " rssMemory:" + rssMemoryStillInUsage +
            " rssMemoryLimit:" + maxRssMemoryAllowedForAllTasks +
            " rssMemoryAvailable:" + availableRssMemory +
            " rssMemoryReserved:" + reservedRssMemory +
            " totalTasks:" + processTreeInfoMap.size());

        if (doCheckVirtualMemory() &&
            memoryStillInUsage > maxMemoryAllowedForAllTasks) {
          LOG.warn("The total memory in usage " + memoryStillInUsage
              + " is overflowing TTs limits "
              + maxMemoryAllowedForAllTasks
              + ". Trying to kill a few tasks with the least progress.");
          killTasksWithLeastProgress(memoryStillInUsage);
        }

        updateMaxRssMemory();
        if (doCheckPhysicalMemory() &&
            (rssMemoryStillInUsage > maxRssMemoryAllowedForAllTasks ||
             availableRssMemory < reservedRssMemory)) {
          LOG.warn("The total physical memory in usage " + rssMemoryStillInUsage
              + " is overflowing TTs limits "
              + maxRssMemoryAllowedForAllTasks
              + ". Trying to kill a few tasks with the highest memory.");
          failTasksWithMaxRssMemory(rssMemoryStillInUsage, availableRssMemory);
        }

        // Sleep for some time before beginning next cycle
        LOG.debug(this.getClass() + " : Sleeping for " + sleepInterval
            + " ms");
        Thread.sleep(sleepInterval);
      } catch (InterruptedException iex) {
        if (running) {
          LOG.error("Class " + this.getClass() + " was interrupted", iex);
        }
      } catch (Throwable t) {
        LOG.error("Class " + this.getClass() + " encountered error", t);
      }
    }
  }

  /**
   * Is the total physical memory check enabled?
   * @return true if total physical memory check is enabled.
   */
  private boolean doCheckPhysicalMemory() {
    return !(maxRssMemoryAllowedForAllTasks == JobConf.DISABLED_MEMORY_LIMIT);
  }

  /**
   * Is the total virtual memory check enabled?
   * @return true if total virtual memory check is enabled.
   */
  private boolean doCheckVirtualMemory() {
    return !(maxMemoryAllowedForAllTasks == JobConf.DISABLED_MEMORY_LIMIT);
  }

  /**
   * Disable updating the reserved physical memory. Used only for tests.
   */
  static public void disableUpdateReservedPhysicalMemory() {
    doUpdateReservedPhysicalMemory = false;
  }
    
  /**
   * Read the reserved physical memory configuration and update the maximum
   * physical memory allowed periodically. This allows us to change the 
   * physcial memory limit configuration without starting TaskTracker
   */
  private void updateMaxRssMemory() {
    if (!doUpdateReservedPhysicalMemory) {
      return;
    }
    final int MEM_CONFIGURATION_READ_PERIOD = 100;
    maxRssMemoryAllowedUpdateCounter++;
    if (maxRssMemoryAllowedUpdateCounter > MEM_CONFIGURATION_READ_PERIOD) {
      maxRssMemoryAllowedUpdateCounter = 0;
      loadMaxRssMemoryConfig(new Configuration());
    }
  }

  private void loadMaxRssMemoryConfig(Configuration conf) {
    long reservedRssMemoryMB =
      conf.getLong(TaskMemoryManagerThread.TT_RESERVED_PHYSICAL_MEMORY_MB,
          JobConf.DISABLED_MEMORY_LIMIT);
    long maxRssMemoryAllowedForAllTasksMB =
      conf.getLong(TaskMemoryManagerThread.TT_MAX_RSS_MEMORY_MB,
          JobConf.DISABLED_MEMORY_LIMIT);
    if (reservedRssMemoryMB == JobConf.DISABLED_MEMORY_LIMIT) {
      reservedRssMemory = JobConf.DISABLED_MEMORY_LIMIT;
      maxRssMemoryAllowedForAllTasks = JobConf.DISABLED_MEMORY_LIMIT;
    } else {
      reservedRssMemory = reservedRssMemoryMB * 1024 * 1024L;
      if (maxRssMemoryAllowedForAllTasksMB == JobConf.DISABLED_MEMORY_LIMIT) {
        maxRssMemoryAllowedForAllTasks =
          taskTracker.getTotalPhysicalMemoryOnTT() - reservedRssMemory;
      } else {
        maxRssMemoryAllowedForAllTasks = maxRssMemoryAllowedForAllTasksMB * 1024 * 1024L;
      }
    }
  }

  /**
   * Check whether a task's process tree's current memory usage is over limit.
   * 
   * When a java process exec's a program, it could momentarily account for
   * double the size of it's memory, because the JVM does a fork()+exec()
   * which at fork time creates a copy of the parent's memory. If the 
   * monitoring thread detects the memory used by the task tree at the same
   * instance, it could assume it is over limit and kill the tree, for no
   * fault of the process itself.
   * 
   * We counter this problem by employing a heuristic check:
   * - if a process tree exceeds the memory limit by more than twice, 
   * it is killed immediately
   * - if a process tree has processes older than the monitoring interval
   * exceeding the memory limit by even 1 time, it is killed. Else it is given
   * the benefit of doubt to lie around for one more iteration.
   * 
   * @param tId Task Id for the task tree
   * @param currentMemUsage Memory usage of a task tree
   * @param curMemUsageOfAgedProcesses Memory usage of processes older than
   *                                    an iteration in a task tree
   * @param limit The limit specified for the task
   * @return true if the memory usage is more than twice the specified limit,
   *              or if processes in the tree, older than this thread's 
   *              monitoring interval, exceed the memory limit. False, 
   *              otherwise.
   */
  boolean isProcessTreeOverLimit(String tId, 
                                  long currentMemUsage, 
                                  long curMemUsageOfAgedProcesses, 
                                  long limit) {
    boolean isOverLimit = false;
    
    if (currentMemUsage > (2*limit)) {
      LOG.warn("Process tree for task: " + tId + " running over twice " +
                "the configured limit. Limit=" + limit + 
                ", current usage = " + currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > limit) {
      LOG.warn("Process tree for task: " + tId + " has processes older than 1 " +
          "iteration running over the configured limit. Limit=" + limit + 
          ", current usage = " + curMemUsageOfAgedProcesses);
      isOverLimit = true;
    }

    return isOverLimit; 
  }

  // method provided just for easy testing purposes
  boolean isProcessTreeOverLimit(ProcfsBasedProcessTree pTree, 
                                    String tId, long limit) {
    long currentMemUsage = pTree.getCumulativeVmem();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
    return isProcessTreeOverLimit(tId, currentMemUsage, 
                                  curMemUsageOfAgedProcesses, limit);
  }

  private void killTasksWithLeastProgress(long memoryStillInUsage) {

    List<TaskAttemptID> tasksToKill = new ArrayList<TaskAttemptID>();
    List<TaskAttemptID> tasksToExclude = new ArrayList<TaskAttemptID>();
    // Find tasks to kill so as to get memory usage under limits.
    while (memoryStillInUsage > maxMemoryAllowedForAllTasks) {
      // Exclude tasks that are already marked for
      // killing.
      TaskInProgress task = taskTracker.findTaskToKill(tasksToExclude);
      if (task == null) {
        break; // couldn't find any more tasks to kill.
      }

      TaskAttemptID tid = task.getTask().getTaskID();
      if (processTreeInfoMap.containsKey(tid)) {
        ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
        ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
        memoryStillInUsage -= pTree.getCumulativeVmem();
        tasksToKill.add(tid);
      }
      // Exclude this task from next search because it is already
      // considered.
      tasksToExclude.add(tid);
    }

    // Now kill the tasks.
    if (!tasksToKill.isEmpty()) {
      for (TaskAttemptID tid : tasksToKill) {
        String msg =
            "Killing one of the least progress tasks - " + tid
                + ", as the cumulative memory usage of all the tasks on "
                + "the TaskTracker exceeds virtual memory limit "
                + maxMemoryAllowedForAllTasks + ".";
        LOG.warn(msg);
        killTask(tid, msg, false);
      }
    } else {
      LOG.info("The total memory usage is overflowing TTs limits. "
          + "But found no alive task to kill for freeing memory.");
    }
  }
  
  /**
   * Return the cumulative rss memory used by a task
   * @param tid the task attempt ID of the task
   * @return rss memory usage in bytes. 0 if the process tree is not available
   */
  private long getTaskCumulativeRssmem(TaskAttemptID tid) {
	    ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
	    ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
	    return pTree == null ? 0 : pTree.getCumulativeVmem();
  }

  /**
   * Starting from the tasks use the highest amount of RSS memory,
   * fail the tasks until the RSS memory meets the requirement
   * @param rssMemoryInUsage RSS memory used by all tasks
   * @param availableRssMemory The free and cache memory in the system
   */
  private void failTasksWithMaxRssMemory(
      long rssMemoryInUsage, long availableRssMemory) {
    
    List<TaskAttemptID> tasksToKill = new ArrayList<TaskAttemptID>();
    List<TaskAttemptID> allTasks = new ArrayList<TaskAttemptID>();
    allTasks.addAll(processTreeInfoMap.keySet());
    // Sort the tasks descendingly according to RSS memory usage 
    Collections.sort(allTasks, new Comparator<TaskAttemptID>() {
      @Override
      public int compare(TaskAttemptID tid1, TaskAttemptID tid2) {
        return  getTaskCumulativeRssmem(tid2) > getTaskCumulativeRssmem(tid1) ?
                1 : -1;
      }});
    
    long rssMemoryStillInUsage = rssMemoryInUsage;
    long availableRssMemoryAfterKilling = availableRssMemory;
    // Fail the tasks one by one until the memory requirement is met
    while ((rssMemoryStillInUsage > maxRssMemoryAllowedForAllTasks ||
           availableRssMemoryAfterKilling < reservedRssMemory) &&
           !allTasks.isEmpty()) {
      TaskAttemptID tid = allTasks.remove(0);
      if (!isKillable(tid)) {
        continue;
      }
      long rssmem = getTaskCumulativeRssmem(tid);
      if (rssmem == 0) {
        break; // Skip tasks without process tree information currently
      }
      tasksToKill.add(tid);
	    rssMemoryStillInUsage -= rssmem;
	    availableRssMemoryAfterKilling += rssmem;
    }

    // Now kill the tasks.
    if (!tasksToKill.isEmpty()) {
      for (TaskAttemptID tid : tasksToKill) {
        long taskMemoryLimit = getTaskMemoryLimit(tid);
        long taskMemory = getTaskCumulativeRssmem(tid);
        String pid = processTreeInfoMap.get(tid).getPID();
        String msg = HIGH_MEMORY_KEYWORD + " task:" + tid +
            " pid:" + pid +
            " taskMemory:" + taskMemory +
            " taskMemoryLimit:" + taskMemoryLimit +
            " availableMemory:" + availableRssMemory +
            " totalMemory:" + rssMemoryInUsage +
            " totalMemoryLimit:" + maxRssMemoryAllowedForAllTasks;
        if (taskMemory > taskMemoryLimit) {
          msg = "Failing " + msg;
          LOG.warn(msg);
          killTask(tid, msg, true);
        } else {
          msg = "Killing " + msg;
          LOG.warn(msg);
          killTask(tid, msg, false);
        }
      }
    } else {
      LOG.error("The total physical memory usage is overflowing TTs limits. "
          + "But found no alive task to kill for freeing memory.");
    }
  }

  private long getTaskMemoryLimit(TaskAttemptID tid) {
    JobConf conf;
    synchronized (this.taskTracker) {
      conf = this.taskTracker.tasks.get(tid).getJobConf();
    }
    long taskMemoryLimit = tid.isMap() ?
        conf.getInt(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY,
            TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT) :
        conf.getInt(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY,
            TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT);
    return taskMemoryLimit * 1024 * 1024L;
  }

  /**
   * Kill the task and clean up ProcessTreeInfo
   * @param tid task attempt ID of the task to be killed.
   * @param msg diagonostic message
   * @param wasFailure if true, fail the task
   */
  private void killTask(TaskAttemptID tid, String msg, boolean wasFailure) {
    // Kill the task and mark it as killed.
    taskTracker.cleanUpOverMemoryTask(tid, wasFailure, msg);
    // Now destroy the ProcessTree, remove it from monitoring map.
    ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
    ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
    try {
      LinuxSystemCall.killProcessGroup(Integer.parseInt(ptInfo.getPID()));
    } catch (java.io.IOException e) {
      LOG.error("Could not kill process group " + ptInfo.getPID(), e);
    }
    processTreeInfoMap.remove(tid);
    LOG.info("Removed ProcessTree with root " + ptInfo.getPID());
  }

  /**
   * Check if a task can be killed to increase free memory
   * @param tid task attempt ID
   * @return true if the task can be killed
   */
  private boolean isKillable(TaskAttemptID tid) {
      TaskInProgress tip = taskTracker.runningTasks.get(tid);
      return tip != null && !tip.wasKilled() &&
             (tip.getRunState() == TaskStatus.State.RUNNING ||
              tip.getRunState() == TaskStatus.State.COMMIT_PENDING);
  }

  public void shutdown() {
    this.running = false;
    this.interrupt();
  }
}

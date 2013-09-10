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
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.syscall.LinuxSystemCall;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.mapred.ControlGroup.MemoryControlGroup;

/**
 * Manages memory usage of tasks running under this TT. Kills task-trees
 * which uses the most memory when CGroup memory usage gets too close to
 * the limit.
 */
class CGroupMemoryWatcher extends Thread {

  private static Log LOG = LogFactory.getLog(CGroupMemoryWatcher.class);

  static public final String CGROUPHIGH_MEMORY_KEYWORD = "CGroup_high_memory";
  private final TaskTracker taskTracker;
  // if the diff for a task's memory usage and its max is less than this, 
  // it will be killed
  private final long taskLowMemoryThreshold;
  // if the diff for the contianer's memory usage and its max is less than this, 
  // tasks in the group will be killed
  private long lowMemoryThreshold;
  // If a task has been in the CGroup for a long time (deafult:2 days),
  // it will be removed
  private final long oldTaskThreshold;
  private final long oldTagThreshold;
  private final long emptyTaskThreshold;
  // The sleep interval. When tasks need to be killed, set the sleep interval
  // to the min one. 
  private final long monitoringInterval;
  private final long minMonitoringInterval;
  private final String rootpath;
  private final String tagpath;

  private final Map<TaskAttemptID, CGroupProcessTreeInfo> processTreeInfoMap;
  private final Map<TaskAttemptID, CGroupProcessTreeInfo> tasksToBeAdded;
  private final List<TaskAttemptID> tasksToBeRemoved;
  private final List<TaskAttemptID> tasksToKill;
  private boolean isTaskLimitOn;
  // If the CGroup is setup correctly
  private Boolean isWatching;
  private boolean isInitialized;
  private MemoryControlGroup ttMemoryGroup;
  private MemoryControlGroup jtMemoryGroup;
  private MemoryControlGroup containerGroup;
  private TaskTrackerMemoryControlGroup ttMemoryCGroup;
  private CGroupMemStat memStat;
  private int lowMemoryThreadoldUpdateCounter;
  private Integer oomNo;
  private final TaskMemoryManagerThread taskMemoryManager;

  private volatile boolean running = true;
  
  private AtomicInteger aliveTaskNum = new AtomicInteger();
  private long killTimeStamp = 0;
  private AtomicLong aliveTasksCPUMSecs = new AtomicLong();
  
  private static final long TT_RSS_MEM_THRESHOLD = 2*1024*1024*1024L;
  
  public CGroupMemoryWatcher(TaskTracker taskTracker) {
    JobConf jobConf = taskTracker.getJobConf();
    this.monitoringInterval= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.interval", 2000L);
    this.minMonitoringInterval = jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.mininterval", 200L);
    this.taskLowMemoryThreshold= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.task.threshold", 64 * 1024);
    this.lowMemoryThreshold= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.low.threshold", 512 * 1024 * 1024);
    this.oldTaskThreshold= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.oldtask.threshold", 86400 * 1000 * 10);
    this.emptyTaskThreshold= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.emptytask.threshold",  86400 * 1000 );
    int tasksKilledSize = jobConf.getInt(
      "mapred.tasktracker.cgroup.memory.historysize", 50);;
    this.rootpath = taskTracker.getJobConf().get(
      TaskTrackerMemoryControlGroup.CGROUP_MEM_TT_ROOT,
      TaskTrackerMemoryControlGroup.DEFAULT_TT_ROOT);
    this.oldTagThreshold= jobConf.getLong(
      "mapred.tasktracker.cgroup.memory.oldtag.threshold",  7200 * 1000 );
    this.tagpath = taskTracker.getJobConf().get(
      "mapred.tasktracker.cgroup.memory.tagdir",
      "/usr/local/hadoop/EventListener/tags"
      );

    this.taskTracker = taskTracker;
    processTreeInfoMap = new ConcurrentHashMap<TaskAttemptID, CGroupProcessTreeInfo>();
    tasksToBeAdded = new HashMap<TaskAttemptID, CGroupProcessTreeInfo>();
    tasksToBeRemoved = new ArrayList<TaskAttemptID>();
    tasksToKill =  new ArrayList<TaskAttemptID>();
    isTaskLimitOn = false;
    isWatching = false;
    isInitialized = false;
    ttMemoryGroup = null;
    jtMemoryGroup = null;
    ttMemoryCGroup = null;
    containerGroup = null;
    memStat = new CGroupMemStat(tasksKilledSize);
    lowMemoryThreadoldUpdateCounter = 0;
    oomNo = 0;
    if ( taskTracker.isTaskMemoryManagerEnabled()) {
      taskMemoryManager = taskTracker.getTaskMemoryManager();
    } else {
      taskMemoryManager = null;
    }

    if (initialize()) {
       isInitialized = true;
    }
    setName("MemoryWatcher");
  }

  public void addTask(TaskAttemptID tid, long limit) {
    synchronized (isWatching) {
      if (!isWatching){
        if (taskMemoryManager != null) {
          taskMemoryManager.addTask(tid, limit);
        }
        return;
      }
      LOG.debug("Add " + tid);
      synchronized (tasksToBeAdded) {
        CGroupProcessTreeInfo ptInfo = new CGroupProcessTreeInfo(tid, null, limit);
        tasksToBeAdded.put(tid, ptInfo);
      }
    }
  }

  public void removeTask(TaskAttemptID tid) {
    synchronized (isWatching) {
      if (!isWatching){
        if (taskMemoryManager != null) {
          taskMemoryManager.removeTask(tid);
        }
        return;
      }
      LOG.debug("remove " + tid);
      synchronized (tasksToBeRemoved) {
        tasksToBeRemoved.add(tid);
      }
    }
  }

  private List<CGroupProcessTreeInfo> getTasks() {
    List<CGroupProcessTreeInfo> taskList =
      new ArrayList<CGroupProcessTreeInfo>();
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


  public int getOOMNo() {
    synchronized (oomNo) {
      return oomNo;
    }
  }

  public void resetOOMNo() {
    synchronized (oomNo) {
      oomNo = 0;
    }
  }
  
  public void incOOMNo() {
    synchronized (oomNo) {
      // always set oomNo to 1
      oomNo = 1;
    }
  }
  
  public static class CGroupMemStat {
    private long memoryLimit;
    private long maxMemoryUsed;
    private long memoryUsage;
    private long lowMemoryThreshold;
    private long memoryUsedByTasks;
    private final LinkedList<CGroupProcessTreeInfo> tasksKilledRecently;
    // The number of killed tasks to keep
    private final int tasksKilledSize;
    private long jtMaxMemoryUsed;
    private long jtMemoryUsage;
    private long ttMaxMemoryUsed;
    private long ttMemoryUsage;

    public CGroupMemStat(int listSize) {
      this.tasksKilledSize = listSize;
      this.maxMemoryUsed = 0;
      this.memoryUsage = 0;
      this.lowMemoryThreshold = 0;
      this.memoryLimit = 0;
      this.memoryUsedByTasks = 0;
      this.jtMaxMemoryUsed = 0;
      this.jtMemoryUsage = 0;
      this.ttMaxMemoryUsed = 0;
      this.ttMemoryUsage = 0;
      tasksKilledRecently = new LinkedList<CGroupProcessTreeInfo>();
    }
    public CGroupMemStat(CGroupMemStat inStat) {
      this.memoryLimit = inStat.memoryLimit;
      this.tasksKilledSize = inStat.tasksKilledSize;
      this.maxMemoryUsed = inStat.maxMemoryUsed;
      this.memoryUsage = inStat.memoryUsage;
      this.lowMemoryThreshold = inStat.lowMemoryThreshold;
      this.memoryUsedByTasks = inStat.memoryUsedByTasks;
      this.jtMaxMemoryUsed= inStat.jtMaxMemoryUsed;
      this.jtMemoryUsage = inStat.jtMemoryUsage;
      this.ttMaxMemoryUsed= inStat.ttMaxMemoryUsed;
      this.ttMemoryUsage = inStat.ttMemoryUsage;
      this.tasksKilledRecently = new LinkedList<CGroupProcessTreeInfo>();
      this.tasksKilledRecently.addAll(inStat.tasksKilledRecently);
    }
    
    public void setMemoryUsedByTasks(long value) {
      memoryUsedByTasks = value;
    }

    public long getMemoryUsedByTasks() {
      return memoryUsedByTasks;
    }

    public void setMemoryLimit(long value) {
      memoryLimit = value;
    }

    public long getMemoryLimit() {
      return memoryLimit;
    }

    public void setMaxMemoryUsed(long value) {
      if (value > maxMemoryUsed) {
        maxMemoryUsed = value;
      }
    }

    public long getMaxMemoryUsed() {
      return maxMemoryUsed;
    }
    
    public void setMemoryUsage(long value) {
      memoryUsage = value;
    }

    public long getMemoryUsage() {
      return memoryUsage;
    }

    public void setJTMaxMemoryUsed(long value) {
      if (value > jtMaxMemoryUsed) {
        jtMaxMemoryUsed = value;
      }
    }

    public long getJTMaxMemoryUsed() {
      return jtMaxMemoryUsed;
    }
    
    public void setJTMemoryUsage(long value) {
      jtMemoryUsage = value;
    }

    public long getJTMemoryUsage() {
      return jtMemoryUsage;
    }

    public void setTTMaxMemoryUsed(long value) {
      if (value > ttMaxMemoryUsed) {
        ttMaxMemoryUsed = value;
      }
    }

    public long getTTMaxMemoryUsed() {
      return ttMaxMemoryUsed;
    }
    
    public void setTTMemoryUsage(long value) {
      ttMemoryUsage = value;
    }

    public long getTTMemoryUsage() {
      return ttMemoryUsage;
    }
    
    public void setLowMemoryThreshold(long value) {
      lowMemoryThreshold = value;
    }

    public long getLowMemoryThreshold() {
      return lowMemoryThreshold;
    }

    public void addTask(CGroupProcessTreeInfo ptInfo) {
      if (tasksKilledRecently.size() >=  tasksKilledSize) {
        tasksKilledRecently.removeLast();
      }
      tasksKilledRecently.addFirst(ptInfo);
    }

    public List<CGroupProcessTreeInfo> getTasks() {
      return tasksKilledRecently;
    }
  }

  public static class CGroupProcessTreeInfo {
    private final TaskAttemptID tid;
    private String pid;
    private String pidFile;
    private long memoryUsed;
    private long maxMemoryUsed;
    private final long creationTime;
    private long killTime;
    private long limit;

    public CGroupProcessTreeInfo(TaskAttemptID tid, String pid, long limit) {
      this.tid = tid;
      this.pid = pid;
      this.memoryUsed = 0;
      this.maxMemoryUsed = 0;
      this.creationTime = System.currentTimeMillis();
      this.killTime = 0;
      this.limit = limit;
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

    public void setMemoryUsed(long memoryUsed) {
      this.memoryUsed = memoryUsed;
    }

    public long getMemoryUsed() {
      return memoryUsed;
    }

    public void setMaxMemoryUsed(long maxMemoryUsed) {
      if (maxMemoryUsed > this.maxMemoryUsed) {
        this.maxMemoryUsed = maxMemoryUsed;
      }
    }

    public long getMaxMemoryUsed() {
      return maxMemoryUsed;
    }

    public long getCreationTime() {
      return creationTime;
    }

    public long getKillTime() {
      return killTime;
    }

    public void setKillTime(long value) {
      killTime = value;
    }

    public long getLimit() {
      return limit;
    }

    public void setLimit(long value) {
      limit = value;
    }
  }

  private boolean isGroupLimitOn() {
    if (containerGroup == null) {
       return false;
    }
    if (containerGroup.getMemoryUsageLimit() <= 0 ||
        containerGroup.getMemoryUsageLimit() == Long.MAX_VALUE) {
      return false;
    }
    return true;
  }
  
  private boolean initialize() {
    ttMemoryCGroup = taskTracker.getTaskTrackerMemoryControlGroup();
    if (ttMemoryCGroup == null || !ttMemoryCGroup.checkAvailable()) {
      return false;
    }
    containerGroup = ttMemoryCGroup.getContainerMemoryControlGroup();
    ttMemoryGroup = ttMemoryCGroup.getTTMemoryControlGroup();
    jtMemoryGroup = ttMemoryCGroup.getJTMemoryControlGroup();
    if (containerGroup == null) {
      LOG.warn("There is no container memory CGroup");
      return false;
    }
    isTaskLimitOn = ttMemoryCGroup.getTaskLimitOn();
    return true;
  }
  
  public int getAndResetAliveTaskNum () {
    return aliveTaskNum.getAndSet(0);
  }
  
  public long getAndResetAliveTasksCPUMSecs() {
    return aliveTasksCPUMSecs.getAndSet(0);
  }
  
  @Override
  public void run() {
    long memoryToRelease = 0;
    long sleepTime = monitoringInterval;
    long memoryLeft = 0;
    long memoryUsedByTasks = 0;
    boolean taskKilled = false;
    int activeTasks = 0;

    LOG.info("Starting thread: " + this.getClass());
    if (!isInitialized) {
      LOG.error("CGroup is not initialized, exiting");
      return;
    }

    while (running) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
      }

      if (isGroupLimitOn()) {
        synchronized (isWatching) {
          if (isWatching == false) {
            isWatching = true;
            LOG.info("The CGroup limit is turned on");
            if (taskMemoryManager != null) {
              taskMemoryManager.setSleepInterval(monitoringInterval * 60);
              sleepTime = monitoringInterval;
              List<TaskMemoryManagerThread.ProcessTreeInfo> taskList = taskMemoryManager.getTasks();
              for (TaskMemoryManagerThread.ProcessTreeInfo pinfo: taskList) {
                LOG.info("Move " + pinfo.getTID() + " to Watcher");
                synchronized (tasksToBeAdded) {
                  CGroupProcessTreeInfo ptInfo = new CGroupProcessTreeInfo(
                    pinfo.getTID(), null, pinfo.getMemLimit());
                  tasksToBeAdded.put(pinfo.getTID(), ptInfo);
                }
                taskMemoryManager.removeTask(pinfo.getTID());
              }
            }
          }
        }
      } else {
        // no cgroup limit will turn off the watcher
        LOG.info("no tasks:" + processTreeInfoMap.size());
        synchronized (isWatching) {
          if (isWatching == true) {
            isWatching = false;
            LOG.info("The CGroup limit is turned off");
            if (taskMemoryManager != null) {
              taskMemoryManager.resetSleepInterval();
              sleepTime = monitoringInterval * 60;
              List<CGroupProcessTreeInfo> taskList = getTasks();
              for (CGroupProcessTreeInfo pinfo: taskList) {
                LOG.info("Move " + pinfo.getTID() + " out of Watcher");
                taskMemoryManager.addTask(pinfo.getTID(), pinfo.getLimit());
                processTreeInfoMap.remove(pinfo.getTID());
              }
            }
          }
        }
        continue;
      }
      synchronized (tasksToBeAdded) {
        processTreeInfoMap.putAll(tasksToBeAdded);
        tasksToBeAdded.clear();
      }
      synchronized (tasksToBeRemoved) {
        for (TaskAttemptID tid : tasksToBeRemoved) {
          processTreeInfoMap.remove(tid);
        }
        tasksToBeRemoved.clear();
      }

      buildProcessTree();
      tasksToKill.clear();
      memoryLeft = 0;
      taskKilled = false;
      memoryToRelease = 0;
      
      long ttRssMem = ttMemoryGroup.getRSSMemoryUsage();
      if (ttRssMem >= TT_RSS_MEM_THRESHOLD) {
        LOG.info("The RSS memory used by this task tracker is very high:" + ttRssMem);
      }
      taskTracker.setTaskTrackerRSSMem(ttRssMem);

      // going through all the task attempts, find the ones hit limit
      // and check the memory usage
      memoryUsedByTasks = 0;
      for (Iterator<Map.Entry<TaskAttemptID, CGroupProcessTreeInfo>> it =
        processTreeInfoMap.entrySet().iterator(); it.hasNext();) {
        Map.Entry<TaskAttemptID, CGroupProcessTreeInfo> entry = it.next();
        TaskAttemptID attempt = entry.getKey();
        CGroupProcessTreeInfo pInfo = entry.getValue();
        pInfo.setMemoryUsed(0);
        MemoryControlGroup taskGroup = 
          containerGroup.getSubGroup(attempt.toString());
        if (taskGroup == null) {
          LOG.warn(attempt + " has no memory CGroup");
          continue;
        }
        
        long memoryUsed = taskGroup.getRSSMemoryUsage();
        pInfo.setMaxMemoryUsed(memoryUsed);
        memoryUsedByTasks += memoryUsed;
        pInfo.setMemoryUsed(memoryUsed);
        
        if (isTaskLimitOn && isKillable(attempt) && 
            taskGroup.getMemoryUsageLimit() > 0 && 
           (taskGroup.getMemoryUsageLimit() - memoryUsed
            < taskLowMemoryThreshold)) {
          LOG.warn(attempt + " hits the memory threshold " + memoryUsed);
          tasksToKill.add(attempt);
          memoryToRelease -= memoryUsed;
        }
      }
      // kill the tasks which was at the limit
      if (tasksToKill.size() > 0) {
        killTasks();
        taskKilled = true;
      }
      // check container
      long containerMemoryUsed = containerGroup.getRSSMemoryUsage() + memoryUsedByTasks;
      memoryLeft = containerGroup.getMemoryUsageLimit() - containerMemoryUsed;
      if  ((memoryLeft < lowMemoryThreshold) &&
            containerGroup.getMemoryUsageLimit() > 0) {
        LOG.warn("container hits the threshold for RSS memory " + containerMemoryUsed);
        memoryToRelease += lowMemoryThreshold;
      }
      // release memory ASAP
      if (memoryToRelease > 0) {
        LOG.warn("Need to release " + memoryToRelease + " memory");
        failTasksWithMaxMemory(memoryToRelease);
        incOOMNo();
        sleepTime = minMonitoringInterval;
        logAliveTasks();
        continue;
      } 
      synchronized (memStat) {
        memStat.setMaxMemoryUsed(containerMemoryUsed);
        memStat.setMemoryUsage(containerMemoryUsed);
        memStat.setMemoryLimit(containerGroup.getMemoryUsageLimit());
        memStat.setLowMemoryThreshold(lowMemoryThreshold);
        memStat.setMemoryUsedByTasks(memoryUsedByTasks);
        long jtMemoryUsed = jtMemoryGroup.getRSSMemoryUsage();
        memStat.setJTMaxMemoryUsed(jtMemoryUsed);
        memStat.setJTMemoryUsage(jtMemoryUsed);
        long ttMemoryUsed = ttMemoryGroup.getRSSMemoryUsage();
        memStat.setTTMaxMemoryUsed(ttMemoryUsed);
        memStat.setTTMemoryUsage(ttMemoryUsed);
      }
      sleepTime = monitoringInterval;
      if (taskKilled) {
        logAliveTasks();  
      }
      // remove the very old attempts
      long current = System.currentTimeMillis();
      List<TaskAttemptID> oldTasks = new ArrayList<TaskAttemptID>();
      oldTasks.addAll(processTreeInfoMap.keySet());
      for (TaskAttemptID attempt: oldTasks) {
        CGroupProcessTreeInfo pInfo = processTreeInfoMap.get(attempt);
        if (pInfo == null) {
          continue;
        }
        if (current - pInfo.getCreationTime() >=  oldTaskThreshold) {
          LOG.warn("Remove old entry " + attempt);
          ttMemoryCGroup.removeTask(attempt.toString());
          processTreeInfoMap.remove(attempt);
        } else if (current - pInfo.getCreationTime() >= emptyTaskThreshold) {
          MemoryControlGroup taskGroup = 
            containerGroup.getSubGroup(attempt.toString());
          if (taskGroup == null || taskGroup.getMaxMemoryUsage() == 0) {
            LOG.warn("Remove empty entry " + attempt);
            processTreeInfoMap.remove(attempt);
          }
        }
      }
    }
    for (TaskAttemptID attempt: processTreeInfoMap.keySet()) {
      ttMemoryCGroup.removeTask(attempt.toString());
    }
  }

  private void buildProcessTree() {
    for (Iterator<Map.Entry<TaskAttemptID, CGroupProcessTreeInfo>> it = processTreeInfoMap
      .entrySet().iterator(); it.hasNext();) {
      Map.Entry<TaskAttemptID, CGroupProcessTreeInfo> entry = it.next();
      TaskAttemptID tid = entry.getKey();
      CGroupProcessTreeInfo ptInfo = entry.getValue();
      String pId = ptInfo.getPID();

      // Initialize any uninitialized processTrees
      if (pId == null) {
        // get pid from taskAttemptId
        pId = taskTracker.getPid(ptInfo.getTID());
        if (pId != null) {
          ptInfo.setPid(pId);
          if (pId == null) {
            continue; // processTree cannot be tracked
          }

        }
      }
    }
  }

  /**
   * Starting from the tasks use the highest amount of memory,
   * fail the tasks until the memory released meets the requirement
   * @param memoryToRelease the mix memory to get released
   */
  private void failTasksWithMaxMemory(long memoryToRelease) {
    
    List<TaskAttemptID> allTasks = new ArrayList<TaskAttemptID>();
    allTasks.addAll(processTreeInfoMap.keySet());
    // Sort the tasks descendingly according to RSS memory usage 
    Collections.sort(allTasks, new Comparator<TaskAttemptID>() {
      @Override
      public int compare(TaskAttemptID tid1, TaskAttemptID tid2) {
        return  processTreeInfoMap.get(tid2).getMemoryUsed() >
                processTreeInfoMap.get(tid1).getMemoryUsed() ?
                1 : -1;
      }});
    
    long memoryReleased = 0;
    // Fail the tasks one by one until the memory requirement is met
    while (memoryReleased < memoryToRelease && !allTasks.isEmpty()) {
      TaskAttemptID tid = allTasks.remove(0);
      if (!isKillable(tid)) {
        continue;
      }
      long memoryUsed = processTreeInfoMap.get(tid).getMemoryUsed();
      if (memoryUsed == 0) {
        break; // Skip tasks without process tree information currently
      }
      tasksToKill.add(tid);
      memoryReleased += memoryUsed;
    }
    if (tasksToKill.isEmpty()) {
      LOG.error("The total memory usage is over CGroup limits. "
          + "But found no alive task to kill for freeing memory.");
    } else if (memoryReleased < memoryToRelease) {
      LOG.error("The total memory usage is over CGroup limits. "
          + "But uanble to find enough tasks to kill for freeing memory.");
    }
    killTasks();
  }  

  private void killTasks() {
    long killTime = System.currentTimeMillis();
    synchronized (memStat) {
      for (TaskAttemptID tid : tasksToKill) {
        CGroupProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
        ptInfo.setKillTime(killTime);
        memStat.addTask(ptInfo);
      }
    }
    // Now kill the tasks.
    for (TaskAttemptID tid : tasksToKill) {
      CGroupProcessTreeInfo processTreeInfo = processTreeInfoMap.get(tid);
      String pid = processTreeInfo.getPID();
      String msg = "Killing " + CGROUPHIGH_MEMORY_KEYWORD + " task:" + tid +
        " pid:" + pid +
        " taskMemory:" + processTreeInfo.getMemoryUsed();
      LOG.warn(msg);
      // book keeping the task killed
      taskTracker.addKilledTaskToRssBuckets(processTreeInfo.getMemoryUsed());
      if (processTreeInfo.getCreationTime() < killTimeStamp) {
        this.aliveTaskNum.decrementAndGet();
        this.aliveTasksCPUMSecs.addAndGet(-taskTracker.getTaskCPUMSecs(tid));
      }
      long taskMemoryLimit = getTaskMemoryLimit(tid);
      if (taskMemoryLimit < processTreeInfo.getMemoryUsed()) {
        killTask(tid, msg, true);
      } else {
        killTask(tid, msg, false);
      }
    }
    tasksToKill.clear();
  }
  
  private void logAliveTasks() {
    if (processTreeInfoMap.size() > 0) {
      LOG.info("After killing high memory task, the following tasks is still alive:");
    }
    for (TaskAttemptID tid : processTreeInfoMap.keySet()) {
      CGroupProcessTreeInfo processTreeInfo = processTreeInfoMap.get(tid);
      if (processTreeInfo.getKillTime() > 0) {
        continue; 
      }
      String pid = processTreeInfo.getPID();
      String msg = "Task:" + tid +
        " pid:" + pid +
        " taskMemory:" + processTreeInfo.getMemoryUsed();
      LOG.info(msg);
      if (processTreeInfo.getCreationTime() > killTimeStamp) {
        this.aliveTaskNum.incrementAndGet();
        this.aliveTasksCPUMSecs.addAndGet(taskTracker.getTaskCPUMSecs(tid));
      }
    }
    
    killTimeStamp = System.currentTimeMillis();
  }

  private void logAliveTasks(TaskAttemptID tidKilled) {
    long killTime = System.currentTimeMillis();
    CGroupProcessTreeInfo ptInfo = processTreeInfoMap.get(tidKilled);
    if (ptInfo == null) {
      LOG.info(tidKilled + " doesn't exist");
      return;
    }
    if (ptInfo.getKillTime() > 0) {
      LOG.info(tidKilled + " has been killed");
      return;
    }
    ptInfo.setKillTime(killTime);
    if (ptInfo.getCreationTime() < killTimeStamp) {
      this.aliveTaskNum.decrementAndGet();
      this.aliveTasksCPUMSecs.addAndGet(-taskTracker.getTaskCPUMSecs(tidKilled));
    }
    synchronized (memStat) {
      memStat.addTask(ptInfo);
    }
    logAliveTasks();
  }
  /**
   * Kill the task and clean up CGroupProcessTreeInfo
   * @param tid task attempt ID of the task to be killed.
   * @param msg diagonostic message
   * @param wasFailure if true, fail the task
   */
  private void killTask(TaskAttemptID tid, String msg, boolean wasFailure) {
    // Kill the task and mark it as killed.
    taskTracker.cleanUpOverMemoryTask(tid, wasFailure, msg);
    // Now destroy the ProcessTree, remove it from monitoring map.
    CGroupProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
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

  public boolean checkWatchable() {
    synchronized (isWatching) {
      return isWatching;
    }
  }

  public CGroupMemStat getCGroupMemStat() {
    CGroupMemStat tmpStat;
    synchronized(memStat) {
      memStat = new CGroupMemStat(memStat);
    }
    return memStat;
  }

  private long getTaskMemoryLimit(TaskAttemptID tid) {
    JobConf conf;
    synchronized (this.taskTracker) {
      conf = this.taskTracker.tasks.get(tid).getJobConf();
    }
    long taskMemoryLimit = tid.isMap() ?
        conf.getInt(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY,
            TaskMemoryManagerThread.TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT) :
        conf.getInt(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY,
            TaskMemoryManagerThread.TASK_MAX_PHYSICAL_MEMORY_MB_DEFAULT);
    return taskMemoryLimit * 1024 * 1024L;
  }

  public boolean isKilledByCGroup(TaskAttemptID tid) {
    File cgroupFlagFile = new File(tagpath + "/" + tid);
    try {
      if (cgroupFlagFile.exists()) {
        LOG.info(tid + " is killed by CGroup");
        String msg = "Killing " + CGROUPHIGH_MEMORY_KEYWORD;
        boolean wasFailure = false;
        long rssUsed = 0;
        long taskMemoryLimit = getTaskMemoryLimit(tid);
        BufferedReader reader = new BufferedReader(new FileReader(cgroupFlagFile));
        String rssLine = reader.readLine();
        if (rssLine != null) {
          rssUsed = Long.parseLong(rssLine);
          if (taskMemoryLimit < rssUsed) {
            wasFailure  = true;
          }
          msg += " rss:" + rssUsed;
          // book keeping the task killed 
          taskTracker.addKilledTaskToRssBuckets(rssUsed);
        }
        reader.close();
        synchronized (this.taskTracker) {
          taskTracker.cleanUpOverMemoryTask(tid, wasFailure, msg);
        }
        incOOMNo();
        if (isWatching) {
          logAliveTasks(tid);
        }
        if (!cgroupFlagFile.delete()) {
          LOG.error("Failed to delete tag file:" + tid);
        }
        return true;      
      }
    } catch (java.io.IOException e) {
      LOG.error("Exception in deleting " + cgroupFlagFile);
    }
    return false;
  }
}

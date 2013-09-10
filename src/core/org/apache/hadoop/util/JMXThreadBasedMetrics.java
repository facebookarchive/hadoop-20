package org.apache.hadoop.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class JMXThreadBasedMetrics {
  Map<Long, Long> threadInfoMap;
  Map<String, Set<Long>> taskToThreadIds;
  
  private static final ThreadMXBean threadMXBean = 
      ManagementFactory.getThreadMXBean();
  private static final long NANO_TO_MILLI = 
      1000000L;
  
  public JMXThreadBasedMetrics () {
    threadInfoMap = new ConcurrentHashMap<Long, Long> ();
    taskToThreadIds = new ConcurrentHashMap<String, Set<Long>> ();
  }
  
  public void updateCPUInfo () {
    long [] threadIds = threadMXBean.getAllThreadIds();
    for (long threadId: threadIds) {
      long jvmCPUTime = threadMXBean.getThreadCpuTime(threadId)/NANO_TO_MILLI;
      threadInfoMap.put(threadId, jvmCPUTime);
    }
  }
  
  public long getCumulativeCPUTime() {
    updateCPUInfo ();

    long totalCPU = 0L;
    for (long threadId : threadInfoMap.keySet()) {
      totalCPU += threadInfoMap.get(threadId);
    }
    
    return totalCPU;
  }
  
  private long threadIdToCPU(long threadId) {
    Long cpu = threadInfoMap.get(threadId);
    return (cpu == null ? 0L : cpu.longValue());
  }
  
  public long getTaskCPUTime(String taskName) {
    updateCPUInfo ();

    Set<Long> threadIds = taskToThreadIds.get(taskName);
    if (threadIds == null) {
      // for local job runner, there will be no threads
      // registered to the task
      return 0L;
    }
    
    long cpu = 0L;
    for (long threadId : threadIds) {
      cpu += threadIdToCPU (threadId); 
    }
   
    return cpu;
  }
  
  public void registerThreadToTask(String taskName, long threadId) {
    Set<Long> threadIds = taskToThreadIds.get(taskName);
    if (threadIds == null) {
      threadIds = new HashSet<Long> ();
      taskToThreadIds.put(taskName, threadIds);
    }
    
    threadIds.add(threadId);
  }
  
  public long getCurrentThreadCPUTime() {
    updateCPUInfo ();
    
    return threadIdToCPU(Thread.currentThread().getId());
  }
}

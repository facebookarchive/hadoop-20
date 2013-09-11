package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ControlGroup.MemoryControlGroup;
import org.apache.hadoop.util.ResourceCalculatorPlugin;

public class CGroupResourceTracker {
  private MemoryControlGroup memControlGroup = null;
  private ResourceCalculatorPlugin resourceCalculaotr = null;
  
  private static Log LOG = LogFactory.getLog(CGroupResourceTracker.class);
  
  public static enum RESOURCE_TRAKCER_TYPE {
    JOB_TRACKER,
    TASK_TRACKER,
    TASK
  }
  
  public CGroupResourceTracker(
           JobConf conf, RESOURCE_TRAKCER_TYPE trackerType, 
           String target,
           ResourceCalculatorPlugin plugin) {
    this.resourceCalculaotr = plugin;
    
    boolean taskMemoryControlGroupEnabled = conf.getBoolean(
        TaskTracker.MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY,
        TaskTracker.DEFAULT_MAPRED_TASKTRACKER_CGROUP_MEM_ENABLE_PROPERTY);
    
    if (taskMemoryControlGroupEnabled) {
      if (MemoryControlGroup.isAvailable()) {
        switch (trackerType) {
          case JOB_TRACKER:
            String jtRootpath = conf.get(
                TaskTrackerMemoryControlGroup.CGROUP_MEM_JT_ROOT, 
                TaskTrackerMemoryControlGroup.DEFAULT_JT_ROOT);
            memControlGroup= new MemoryControlGroup(jtRootpath);
            
            if (isMemTrackerAvailable()) {
              LOG.info("A CGroupResourceTracker for JOB_TRACKER created.");
            }
            break;
          case TASK_TRACKER:
            String ttRootpath = conf.get(
                TaskTrackerMemoryControlGroup.CGROUP_MEM_TT_ROOT, 
                TaskTrackerMemoryControlGroup.DEFAULT_TT_ROOT);
              memControlGroup = new MemoryControlGroup(ttRootpath);
              
            if (isMemTrackerAvailable()) {
              LOG.info("A CGroupResourceTracker for TASK_TRACKER created.");
            }
            break;
          case TASK:
            String rootpath = conf.get(
              TaskTrackerMemoryControlGroup.CGROUP_MEM_ROOT_PROPERTY, 
              TaskTrackerMemoryControlGroup.DEFAULT_CGROUP_MEM_ROOT);
            MemoryControlGroup container = new MemoryControlGroup(rootpath);
            memControlGroup = 
              container.getSubGroup(target);
            
            if (isMemTrackerAvailable()) {
              LOG.info("A CGroupResourceTracker for TASK:" + 
                target + " created.");
            }
            break;
        }
      }
    }
  }
  
  public boolean isMemTrackerAvailable() {
    return (memControlGroup != null);
  }
  
  public long getMaxMemoryUsage() {
    if (!isMemTrackerAvailable()) {
      if (this.resourceCalculaotr != null) {
        return 
          this.resourceCalculaotr.getProcResourceValues().getVirtualMemorySize();
      }
      return 0;
    }
    
    return memControlGroup.getMaxMemoryUsage();
  }
  
  public long getMemoryUsage() {
    if (!isMemTrackerAvailable()) {
      if (this.resourceCalculaotr != null) {
        return 
          this.resourceCalculaotr.getProcResourceValues().getPhysicalMemorySize();
      }
      return 0;
    }
    
    return memControlGroup.getMemoryUsage();
  }
  
  public long getRSSMemoryUsage() {
    if (!isMemTrackerAvailable()) {
      if (this.resourceCalculaotr != null) {
        return 
          this.resourceCalculaotr.getProcResourceValues().getPhysicalMemorySize();
      }
      return 0;
    }
    
    return memControlGroup.getRSSMemoryUsage();
  }
  
}

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ControlGroup.CPUControlGroup;

/**
 * Limits CPU usages of each Task through a Linux cpu control group.
 * 
 * Requirement:
 * <ul>
 * <li>A cpu sub-system control group is available</li>
 * <li>A path to a target control group is configured
 * (mapred.tasktracker.cgroup.cpu.root) or the default path will be used
 * (/cgroup/cpu)</li>
 * <li>A user launching Task has a permission to modify a control group (i.e.
 * using chown)</li>
 * </ul>
 */
public class TaskTrackerCPUControlGroup {
  private static Log LOG = LogFactory.getLog(TaskTrackerCPUControlGroup.class);
  
  public static final String CGROUP_CPU_ROOT_PROPERTY = "mapred.tasktracker.cgroup.cpu.root";
  public static final String DEFAULT_CGROUP_CPU_ROOT = "/cgroup/cpu/tasktrackers";

  public static final String CGROUP_TRASH_GROUP_NAME = "trash";

  private boolean isAvailable;
  private long baseShare;
  private CPUControlGroup ttcgp;
  private CPUControlGroup trashcgp;
  
  public TaskTrackerCPUControlGroup(Configuration conf, int slots) {
    String rootpath = conf.get(CGROUP_CPU_ROOT_PROPERTY, DEFAULT_CGROUP_CPU_ROOT);
    ttcgp = new CPUControlGroup(rootpath);
    
    if (!CPUControlGroup.isAvailable()) {
      LOG.warn("TaskTrackerCPUControlGroup is disabled because a cpu sub-system is not available");
      isAvailable = false;
      return;
    }
    if (!ttcgp.canControl()) {
      LOG.warn("TaskTrackerCPUControlGroup is disabled because TaskTracker does not have an appropriate permission for "
          + rootpath);
      isAvailable = false;
      return;
    }
    
    isAvailable = true;
    
    baseShare = CPUControlGroup.CPU_SHARES_DEFAULT;
    ttcgp.setCPUShares(baseShare*slots);
    trashcgp = ttcgp.createSubGroup(CGROUP_TRASH_GROUP_NAME);
    trashcgp.setCPUShares(baseShare);
    LOG.info("TaskTrackerCPUControlGroup is created with " + slots + " slots");
  }
  
  public void addTask(String taskname, String pid) {
    if (!isAvailable)
      return ;
 
    CPUControlGroup taskcgp = ttcgp.createSubGroup(taskname);
    taskcgp.setCPUShares(baseShare);
    taskcgp.addToGroup(pid);    
  }
  
  public void removeTask(String taskname) {
    if (!isAvailable)
      return ;

    CPUControlGroup taskcgp = ttcgp.getSubGroup(taskname);
    trashcgp.addToGroup(taskcgp.getThreadGroupList());
    taskcgp.deleteGroup();
  }
}

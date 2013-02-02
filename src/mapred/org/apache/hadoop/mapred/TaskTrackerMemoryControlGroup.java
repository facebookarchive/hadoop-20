package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ControlGroup.MemoryControlGroup;

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

  public static final String CGROUP_MEM_ROOT_PROPERTY = "mapred.tasktracker.cgroup.mem.root";
  public static final String DEFAULT_CGROUP_MEM_ROOT = "/cgroup/memory/tasktrackers";

  public static final String CGROUP_TRASH_GROUP_NAME = "trash";
  public static final String CGROUP_CONTAINER_GROUP_NAME = "container";

  private boolean isAvailable;
  private MemoryControlGroup ttcgp;
  private MemoryControlGroup containercgp;
  private MemoryControlGroup trashcgp;

  public TaskTrackerMemoryControlGroup(Configuration conf, long memoryReserved) {
    String rootpath = conf.get(CGROUP_MEM_ROOT_PROPERTY, DEFAULT_CGROUP_MEM_ROOT);
    ttcgp = new MemoryControlGroup(rootpath);

    if (!MemoryControlGroup.isAvailable()) {
      LOG.warn("TaskMemoryControlGroup is disabled because a memory sub-system is not available");
      isAvailable = false;
      return;
    }
    if (!ttcgp.canControl()) {
      LOG.warn("TaskMemoryControlGroup is disabled because TaskTracker does not have an appropriate permission for "
          + rootpath);
      isAvailable = false;
      return;
    }

    if (ttcgp.getMemoryUsageLimit() <= 0) {
      LOG.warn("TaskMemoryControlGroup is disabled because memory.limit_in_bytes is not set up ");
      isAvailable = false;
      return;
    }

    if (getPID().equals("")) {
      LOG.warn("TaskMemoryControlGroup is disabled because JVM_PID is not set for TaskTracker");
      isAvailable = false;
      return;
    }

    isAvailable = true;

    ttcgp.enableMoveChargeAtImmigrate();
    ttcgp.enableUseHierarchy();
    ttcgp.addToGroup(getPID());

    containercgp = ttcgp.createSubGroup(CGROUP_CONTAINER_GROUP_NAME);
    containercgp.enableMoveChargeAtImmigrate();
    containercgp.setMemoryUsageLimit(ttcgp.getMemoryUsageLimit()-memoryReserved);

    trashcgp = containercgp.createSubGroup(CGROUP_TRASH_GROUP_NAME);
    trashcgp.disableMoveChargeAtImmigrate();
    LOG.info("TaskTrackerMemoryControlGroup is created with memory = " +
      ttcgp.getMemoryUsageLimit());
  }

  public void addTask(String taskname, String pid, long memoryLimit) {
    if (!isAvailable)
      return ;

    MemoryControlGroup taskcgp = containercgp.createSubGroup(taskname);
    taskcgp.enableMoveChargeAtImmigrate();
    taskcgp.setMemoryUsageLimit(memoryLimit);
    taskcgp.addToGroup(pid);
    LOG.info("Task " + taskname + " is added to control group with memory = " +
      memoryLimit );
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
}

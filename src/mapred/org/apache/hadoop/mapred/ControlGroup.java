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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An interface of Linux control groups through a cgroup virtual file system.
 * A root privilege is not required to manage a control group as long as
 * a user has an appropriate permission for a target virtual file.
 */
public abstract class ControlGroup {
  private static Log LOG = LogFactory.getLog(ControlGroup.class);

  /**
   * An interface to a memory sub-system of Linux control groups. To use a
   * memory cgroup, create a new group and set the memory allocated to the 
   * members of the group. By default, both use_hierarchy and 
   * move_charge_at_immigrate are disable.
   */
  public static class MemoryControlGroup extends ControlGroup {
    /*
     * memory.usage_in_bytes           # show current res_counter usage for memory
     * memory.limit_in_bytes           # set/show limit of memory usage
     * memory.failcnt                  # show the number of memory usage hits limits
     * memory.max_usage_in_bytes       # show max memory usage recorded
     * memory.soft_limit_in_bytes      # set/show soft limit of memory usage
     * memory.stat                     # show various statistics
     * memory.use_hierarchy            # set/show hierarchical account enabled
     * memory.force_empty              # trigger forced move charge to parent
     * memory.swappiness               # set/show swappiness parameter of vmscan
     * memory.move_charge_at_immigrate # set/show controls of moving charges
     * memory.oom_control              # set/show oom controls.
     * memory.numa_stat                # show the number of memory usage per numa node
     *
     * Reference: https://www.kernel.org/doc/Documentation/cgroups/memory.txt
     */
    public static final String MEM_USAGE_IN_BYTES = "memory.usage_in_bytes";
    public static final String MEM_LIMIT_IN_BYTES = "memory.limit_in_bytes";
    public static final String MEM_FAILCNT = "memory.failcnt";
    public static final String MEM_MAX_USAGE_IN_BYTES = "memory.max_usage_in_bytes";
    public static final String MEM_SOFT_LIMIT_IN_BYTES = "memory.soft_limit_in_bytes";
    public static final String MEM_STAT = "memory.stat";
    public static final String MEM_USE_HIERARCHY = "memory.use_hierarchy";
    public static final String MEM_FORCE_EMPTY = "memory.force_empty";
    public static final String MEM_SWAPPINESS = "memory.swappiness";
    public static final String MEM_MOVE_CHARGE_AT_IMMIGRATE = "memory.move_charge_at_immigrate";
    public static final String MEM_OOM_CONTROL = "memory.oom_control";
    public static final String MEM_NUMA_STAT = "memory.numa_stat";
    public static final String MEM_RSS_IN_BYTES = "rss";
    
    public static final long MEM_LIMIT_IN_BYTES_DISABLE = -1;
    
    public static final long MEM_USE_HIERARCHY_ENABLE = 1;
    public static final long MEM_USE_HIERARCHY_DISABLE = 0;

    public static final long MEM_MOVE_CHARGE_AT_IMMIGRATE_ENABLE = 3;
    public static final long MEM_MOVE_CHARGE_AT_IMMIGRATE_DISABLE = 0;
    
    public static final long MEM_MAX_LIMIT_IN_BYTES = 9223372036854771712L;

    public static final String SUBSYS_MEMORY = "memory";

    public static boolean isAvailable() {
      return isSubsystemAvailable(SUBSYS_MEMORY);
    }
    
    public MemoryControlGroup(String path) {
      super(path);
    }
   
    public MemoryControlGroup getSubGroup(String name) {
      return new MemoryControlGroup(getSubDirectory(name));
    }

    public MemoryControlGroup createSubGroup(String name) {
      return new MemoryControlGroup(createSubDirectory(name));
    }

    public long getMemoryUsage() {
      return getLongParameter(MEM_USAGE_IN_BYTES);
    }
    
    public long getMaxMemoryUsage() {
      return getLongParameter(MEM_MAX_USAGE_IN_BYTES);
    }
   
    public long getMemoryUsageLimit() {
      return getLongParameter(MEM_LIMIT_IN_BYTES);
    }
    
    public long getRSSMemoryUsage() {
      String [] kvPairs = this.getListParameter(MEM_STAT);
      if (kvPairs == null) {
        return 0;
      }
      
      for (String kvPair: kvPairs) {
        String [] kv = kvPair.split("\\s+");
        long ret;
        if (kv.length >= 2 &&
            kv[0].trim().compareToIgnoreCase(MEM_RSS_IN_BYTES) == 0) {
          try {
            ret = Long.parseLong(kv[1].trim());
          }
          catch (NumberFormatException ne) {
            LOG.debug("Error reading parameter "+kv[1]
                +": \"java.lang.NumberFormatException: "+ne.getMessage()+"\"");
            ret = 0;
          }
          return ret;
        }
      }
      
      return 0;
    }
   
    public void setMemoryUsageLimit(long value) {
      if (value > MEM_MAX_LIMIT_IN_BYTES)
        value = MEM_LIMIT_IN_BYTES_DISABLE;
      setLongParameter(MEM_LIMIT_IN_BYTES, value);
    }
    
    public void disableMemoryUsageLimit() {
      setLongParameter(MEM_LIMIT_IN_BYTES, MEM_LIMIT_IN_BYTES_DISABLE);
    }
    
    public void enableMoveChargeAtImmigrate() {
      setLongParameter(MEM_MOVE_CHARGE_AT_IMMIGRATE, MEM_MOVE_CHARGE_AT_IMMIGRATE_ENABLE);
    }
    
    public void disableMoveChargeAtImmigrate() {
      setLongParameter(MEM_MOVE_CHARGE_AT_IMMIGRATE, MEM_MOVE_CHARGE_AT_IMMIGRATE_DISABLE);
    }
    
    public void enableUseHierarchy() {
      if (getLongParameter(MEM_USE_HIERARCHY) != 1)
        setLongParameter(MEM_USE_HIERARCHY, MEM_USE_HIERARCHY_ENABLE);
    }
    
    public void disableUseHierarchy() {
      setLongParameter(MEM_USE_HIERARCHY, MEM_USE_HIERARCHY_DISABLE);
    }
    
    public boolean canControl() {
      return super.canControl() && canWrite(MEM_LIMIT_IN_BYTES) && canMkdir("");
    }
  }
  
  /**
   * An interface to a cpu sub-system of Linux control groups. Currently, this
   * implementation controls only CPU shares of each group. To use a cpu cgroup,
   * create a new group and set the cpu share allocated to the members of the
   * group. 
   */
  public static class CPUControlGroup extends ControlGroup {
    /*
     * cpu.cfs_period_us  # the length of a period (in microseconds)
     * cpu.cfs_quota_us   # the total available run-time within a period
     *                      (in microseconds)
     * cpu.shares         # contains an integer value that specifies a relative
     *                      share of CPU time available to the tasks in a cgroup
     * cpu.stat           # exports throttling statistics
     *
     * Reference: http://www.mjmwired.net/kernel/Documentation/scheduler/sched-bwc.txt
     * https://access.redhat.com/knowledge/docs/en-US/Red_Hat_Enterprise_Linux/6/html-single/Resource_Management_Guide/index.html#sec-cpu
     */
    public static final String CPU_CFS_PERIOD_US = "cpu.cfs_period_us";
    public static final String CPU_CFS_QUOTA_US = "cpu.cfs_quota_us";
    public static final String CPU_SHARES = "cpu.shares";
    public static final String CPU_STAT = "cpu.stat";
    
    public static final long CPU_SHARES_DEFAULT = 1024;

    public static final String SUBSYS_CPU = "cpu";

    public static boolean isAvailable() {
      return isSubsystemAvailable(SUBSYS_CPU);
    }
    
    public CPUControlGroup(String path) {
      super(path);
    }
   
    public CPUControlGroup getSubGroup(String name) {
      return new CPUControlGroup(getSubDirectory(name));
    }

    public CPUControlGroup createSubGroup(String name) {
      return new CPUControlGroup(createSubDirectory(name));
    }

    public long getCPUShares() {
      return getLongParameter(CPU_SHARES);
    }
   
    public void setCPUShares(long value) {
      setLongParameter(CPU_SHARES, value);
    }
    
    public boolean canControl() {
      return super.canControl() && canWrite(CPU_SHARES) && canMkdir("");
    }
  }
  
  /*
   * tasks                   # attach a task(thread) and show list of threads
   * cgroup.procs            # show list of processes
   * cgroup.event_control    # an interface for event_fd()
   *
   * Reference: https://www.kernel.org/doc/Documentation/cgroups/memory.txt
   */
  public static final String CG_TASKS = "tasks";
  public static final String CG_PROCS = "cgroup.procs";
  public static final String CG_EVENT_CONTROL = "cgroup.event_control";
  
  public static final String PROC_CGROUP_PATH = "/proc/cgroups";  
  
  private String path;

  private static boolean isSubsystemAvailable(String tag) {
    tag = tag + "\t";
    String str;
    BufferedReader buReader = null;
    try {
      buReader = new BufferedReader(new FileReader(new File(PROC_CGROUP_PATH)));
      
      str = buReader.readLine();
      while (str != null) {
        if (str.startsWith(tag)) {
          buReader.close();
          return true;
        }
        str = buReader.readLine();
      }
      buReader.close();
    }
    catch (FileNotFoundException fnfe) {
      return false;
    }
    catch (IOException e) {
    	  LOG.warn("Unable to get control group infomation", e);
    }
    
    return false;
  }

  public ControlGroup(String path) {
    this.path = path;
  }
  
  public void addToGroup(String pgrp) {
    setStringParameter(CG_PROCS, pgrp);
  }
    
  public void addToGroup(String[] pgrpList) {
    for(String pgrp: pgrpList) {
      addToGroup(pgrp);
    }
  }
    
  public String[] getThreadGroupList() {
    return getListParameter(CG_PROCS);
  }

  public boolean deleteGroup() {
    File target = new File(path);
    return target.delete();
  }

  public boolean deleteSubGroup(String name) {
    File target = new File(path, name);
    return target.delete();
  }

  protected String getSubDirectory(String name) {
    File target = new File(path, name);
    return target.getPath();
  }
  
  protected String createSubDirectory(String name) {
    File target = new File(path, name);
    if(!target.mkdir()) {
      // It is okay only if it fails because of the directory is already existed.
      if(!target.exists()) {
        LOG.error("Fail to create sub-directory " + target);
        return "";
      }
    }
    return target.getPath();
  }
  
  protected long getLongParameter(String parameter) {
    long ret = 0;
    try {
      ret = Long.parseLong(getStringParameter(parameter).trim());
    }
    catch (NumberFormatException ne) {
      LOG.debug("Error reading parameter "+parameter
          +": \"java.lang.NumberFormatException: "+ne.getMessage()+"\"");
      ret = 0;
    }
    return ret;
  }

  protected void setLongParameter(String parameter, long value) {
    setStringParameter(parameter, String.valueOf(value));
  }
 
  protected String[] getListParameter(String parameter) {
    return StringUtils.split(getStringParameter(parameter), '\n');
  }

  protected String getStringParameter(String parameter) {
    String ret = "";
    try {
      ret = FileUtils.readFileToString(new File(this.path, parameter));
    } catch (IOException e) {
      LOG.debug("Could not retrieve a parameter (" + parameter + ") @ "+path+": \""+e.getMessage()+"\"");
      ret = "";
    }
    return ret;
  }
  
  protected void setStringParameter(String parameter, String data) {
    try {
      FileUtils.writeStringToFile(new File(this.path, parameter), data);
    } catch (IOException e) {
      LOG.warn("Could not set a parameter (" + parameter + "/" + data +") @ "+path+": \""+e.getMessage()+"\"");
    }
  }
  
  protected boolean canRead(String parameter){
    return (new File(this.getSubDirectory(parameter))).canRead();
  }

  protected boolean canWrite(String parameter){
    return (new File(this.getSubDirectory(parameter))).canWrite();
  }

  protected boolean canExecute(String parameter){
    return (new File(this.getSubDirectory(parameter))).canExecute();
  }

  protected boolean canMkdir(String parameter){
    return canWrite(parameter) && canExecute(parameter);
  }
  
  public boolean canControl() {
    return canWrite(CG_PROCS);
  }
}

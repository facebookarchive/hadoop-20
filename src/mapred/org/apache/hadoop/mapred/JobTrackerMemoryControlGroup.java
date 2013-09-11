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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ControlGroup.MemoryControlGroup;

/**
 * Add jobtrackers to job trakcer memory control group so that
 * it will be moved out of the task tracker control group
 *
 */
public class JobTrackerMemoryControlGroup {
  private static Log LOG = LogFactory.getLog(JobTrackerMemoryControlGroup.class);

  public static final String CGROUP_MEM_JT_ROOT = "mapred.jobtracker.cgroup.mem.root";
  public static final String DEFAULT_JT_ROOT = "/cgroup/memory/jobtrackers";

  private boolean isAvailable;
  private MemoryControlGroup jtcgp;

  public JobTrackerMemoryControlGroup(Configuration conf) {
    String jtRootpath = conf.get(CGROUP_MEM_JT_ROOT, DEFAULT_JT_ROOT);
    jtcgp = new MemoryControlGroup(jtRootpath);
    jtcgp.enableMoveChargeAtImmigrate();

    if (!jtcgp.canControl()) {
      LOG.warn("MemoryControlGroup is disabled because jtgroup doesn't have appropriate permission for "
          + jtRootpath);
      isAvailable = false;
      return;
    }

    isAvailable = true;
  }

  public void addJobTracker(String jtname, String pid) {
    if (!isAvailable)
      return ;

    LOG.info("Remote JT " + jtname + " is added to control group without limit");
    jtcgp.addToGroup(pid);
  }

}

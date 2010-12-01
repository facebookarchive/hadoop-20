/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.hadoop.mapred;


import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
/**
 *
 */
public class TestMemBasedLoadManager extends TestCase {

  public void testCanLaunchTask() {
    // Prepare MemBasedLoadManager to test
    MemBasedLoadManager memLoadMgr = new MemBasedLoadManager();
    memLoadMgr.start();
    Configuration conf = new Configuration();
    String[] affectedUsers = {"user1", "user2", "user3"};
    conf.setStrings(MemBasedLoadManager.AFFECTED_USERS_STRING, affectedUsers);
    conf.setLong(MemBasedLoadManager.RESERVED_PHYSICAL_MEMORY_ON_TT_STRING,
                 4 * 1024L);
    memLoadMgr.setConf(conf);
    assertFalse("MemBasedLoadManager should not affect all user if the list of "
            + "affected users is given", memLoadMgr.isAffectAllUsers());

    // Prepare job to test
    JobConf jobConf = new JobConf();
    JobInProgress job = new JobInProgress(
            JobID.forName("job_200909090000_0001"), jobConf, null);
    job.profile = new JobProfile();
    job.profile.user = "user1";

    // Prepare TaskTracker to test
    String launchMsg = "Memory is under limit. Task should be able to launch.";
    String failedMsg = "Memory exceeds limit. Task should not launch.";

    // Not enough free memory on the TaskTracker
    TaskTrackerStatus tracker = new TaskTrackerStatus();
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(3 * 1024 * 1024 * 1024L);
    assertFalse(failedMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                    TaskType.MAP));
    assertFalse(failedMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                    TaskType.REDUCE));

    // Enough memory on the TaskTracker
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(5 * 1024 * 1024 * 1024L);
    assertTrue(launchMsg, memLoadMgr.canLaunchTask(tracker, job, TaskType.MAP));
    assertTrue(failedMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                    TaskType.REDUCE));

    // Switch to a user that is not affected
    job.profile.user = "user6";
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(1 * 1024 * 1024 * 1024L);
    assertTrue(launchMsg, memLoadMgr.canLaunchTask(tracker, job, TaskType.MAP));
    assertTrue(launchMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                   TaskType.REDUCE));

    // Set the affect everyone property
    memLoadMgr.setAffectAllUsers(true);
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(1 * 1024 * 1024 * 1024L);
    assertFalse(failedMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                    TaskType.MAP));
    assertFalse(failedMsg, memLoadMgr.canLaunchTask(tracker, job,
                                                    TaskType.REDUCE));
  }
}

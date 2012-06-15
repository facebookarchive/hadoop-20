package org.apache.hadoop.mapred;

import java.util.Date;

import org.apache.hadoop.mapred.FairSchedulerMetricsInst.AdmissionControlData;

/**
 * Information about a job that has not been admitted into the scheduler.
 */
public class NotAdmittedJobInfo {
  /** Job submission date */
  private final Date startDate;
  /** Name of the job */
  private final String jobName;
  /** User that submitted the job */
  private final String user;
  /** Pool that the job is running in */
  private final String pool;
  /** Priority of the job */
  private final String priority;
  /** Initial reason the job was not admitted */
  private final String reason;
  /** If the cluster is in hard admission control, note the position */
  private final int hardAdmissionPosition;
  /**
   * Estimated seconds to enter the job based on the last hard
   * job admission averages.
   */
  private final long estimatedHardAdmissionEntranceSecs;

  public NotAdmittedJobInfo(final long startTime,
      final String jobName, final String user,
      final String pool, final String priority,
      final BlockedAdmissionReason reason,
      final int reasonLimit, final int reasonActualValue,
      final int hardAdmissionPosition,
      final float averageWaitMsecsPerHardAdmissionJob) {
    this.startDate = new Date(startTime);
    this.jobName = jobName;
    this.user = user;
    this.pool = pool;
    this.priority = priority;
    this.reason = getReasoning(
        reason, reasonLimit, reasonActualValue, hardAdmissionPosition, null);
    this.hardAdmissionPosition = hardAdmissionPosition;
    if (hardAdmissionPosition >= 0) {
      estimatedHardAdmissionEntranceSecs = (long)
          (averageWaitMsecsPerHardAdmissionJob * (hardAdmissionPosition + 1) /
              1000);
    } else {
      estimatedHardAdmissionEntranceSecs = -1;
    }
  }

  /**
   * Compose the reason message.
   *
   * @param reason Reason why not admitted
   * @param reasonLimit Limit exceeded
   * @param reasonActualValue Actual value that exceed limit
   * @param hardAdmissionPosition If in hard admission control, position in
   *        the queue
   * @return String explaining the issue
   */
  public static String getReasoning(final BlockedAdmissionReason reason,
      final int reasonLimit, final int reasonActualValue,
      final int hardAdmissionPosition,
      JobAdmissionWaitInfo jobAdmissionWaitInfo) {
    if (reason == BlockedAdmissionReason.HARD_CLUSTER_WIDE_MAX_TASKS_EXCEEDED) {
      if (jobAdmissionWaitInfo == null) {
        return reason.toString() + ".";
      } else {
        StringBuffer sb = new StringBuffer();
        sb.append(reason.toString() + ".  In order to protect the jobtracker " +
        		"from exceeding hard memory limits based on the number of " +
        		"total tracked tasks, the cluster is now in cluster-wide " +
        		"hard admission control and accepts jobs on a first come, " +
        		"first served (FIFO) basis.  Your job will be admitted " +
        		"according to this policy.");
        if (jobAdmissionWaitInfo.getAverageCount() > 0) {
          sb.append(" The past " + jobAdmissionWaitInfo.getAverageCount() +
              " jobs admitted while in hard admission control were " +
              "added in an average of " +
              jobAdmissionWaitInfo.getAverageWaitMsecsPerHardAdmissionJob() +
              " msecs, giving this job a rough estimated wait time of " +
              (jobAdmissionWaitInfo.getAverageWaitMsecsPerHardAdmissionJob() *
                  (hardAdmissionPosition + 1)) + " msecs.");
        }
        return sb.toString();
      }
    } else if (reason ==
        BlockedAdmissionReason.SOFT_CLUSTER_WIDE_MAX_TASKS_EXCEEEDED) {
      return reason.toString() + ".";
    } else {
      return reason.toString() + " " + reasonActualValue +
        " exceeds " + reasonLimit + ".";
    }
  }

  public Date getStartDate() {
    return startDate;
  }

  public String getJobName() {
    return jobName;
  }

  public String getUser() {
    return user;
  }

  public String getPool() {
    return pool;
  }

  public String getPriority() {
    return priority;
  }

  public String getReason() {
    return reason;
  }

  public int getHardAdmissionPosition() {
    return hardAdmissionPosition;
  }

  public long getEstimatedHardAdmissionEntranceSecs() {
    return estimatedHardAdmissionEntranceSecs;
  }
}

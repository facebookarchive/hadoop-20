package org.apache.hadoop.mapred;

/**
 * All the possible reasons for a job to not be admitted.
 */
public enum BlockedAdmissionReason {
  /** Should be admitted */
  NONE,
  /** User maximum number of jobs exceeded */
  USER_MAX_JOBS_EXCEEDED,
  /** Pool max jobs exceeded */
  POOL_MAX_JOBS_EXCEEDED,
  /** Pool max inited tasks exceeded */
  POOL_MAX_INITED_TASKS_EXCEEDED,
  /** Pool max map tasks exceeded */
  POOL_MAX_MAP_TASKS_EXCEEDED,
  /** Pool max reduce tasks exceeded */
  POOL_MAX_REDUCE_TASKS_EXCEEDED,
  /** Pool is not a system pool and is in soft cluster admission control */
  SOFT_CLUSTER_WIDE_MAX_TASKS_EXCEEEDED,
  /** Cluster is in hard admission control */
  HARD_CLUSTER_WIDE_MAX_TASKS_EXCEEDED;

  /**
   * Check if the cluster is under some sort of admission control.
   *
   * @param softLimit Soft task limit
   * @param hardLimit Hard task limit
   * @param current Current number of tasks
   * @param usesSoftLimit True if uses the soft limit, false if hard limit
   * @return A clusterwide exceeded reason or none
   */
  public static BlockedAdmissionReason underClusterwideAdmissionControl(
      int softLimit, int hardLimit, int current, boolean usesSoftLimit) {
    if (current > hardLimit) {
      return HARD_CLUSTER_WIDE_MAX_TASKS_EXCEEDED;
    } else if (usesSoftLimit && (current > softLimit)) {
      return SOFT_CLUSTER_WIDE_MAX_TASKS_EXCEEEDED;
    } else {
      return NONE;
    }
  }
}

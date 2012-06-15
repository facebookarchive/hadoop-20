package org.apache.hadoop.mapred;

/**
 * Details about one's jobs wait to be admitted.
 */
public class JobAdmissionWaitInfo {
  /** Was the hard task limit exceeded */
  private final boolean beyondHardTaskLimit;
  /**
   * What is the position in the queue? -1, means not in the queue,
   * not admitted yet
   */
  private final int positionInQueue;
  /** Number of jobs in the queue */
  private final int queueSize;
  /** Average wait per hard admission control job entrance */
  private final float averageWaitMsecsPerHardAdmissionJob;
  /** Number of jobs averageWaitMsecsPerHardAdmissionJob is averaged over. */
  private final int averageCount;

  public JobAdmissionWaitInfo(
      final boolean beyondHardTaskLimit,
      final int positionInQueue,
      final int queueSize,
      final float averageWaitMsecsPerHardAdmissionJob,
      final int averageCount) {
    this.beyondHardTaskLimit = beyondHardTaskLimit;
    this.positionInQueue = positionInQueue;
    this.queueSize = queueSize;
    this.averageWaitMsecsPerHardAdmissionJob =
        averageWaitMsecsPerHardAdmissionJob;
    this.averageCount = averageCount;
  }

  public boolean isBeyondHardTaskLimit() {
    return beyondHardTaskLimit;
  }

  public int getPositionInQueue() {
    return positionInQueue;
  }

  public int getQueueSize() {
    return queueSize;
  }

  public float getAverageWaitMsecsPerHardAdmissionJob() {
    return averageWaitMsecsPerHardAdmissionJob;
  }

  public int getAverageCount() {
    return averageCount;
  }
}

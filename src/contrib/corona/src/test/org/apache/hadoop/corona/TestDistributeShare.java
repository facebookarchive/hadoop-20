package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class TestDistributeShare extends TestCase {
  final double ERROR = 0.01;
  final int MAX = Integer.MAX_VALUE;

  public void testDistributeFifoShare() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 0, MAX, 1.0, 1L));
    schedulables.add(new SchedulableForTest("s2", 5, 0, 0, MAX, 1.0, 2L));
    schedulables.add(new SchedulableForTest("s3", 2, 0, 0, MAX, 1.0, 3L));
    schedulables.add(new SchedulableForTest("s4", 1, 0, 0, MAX, 1.0, 4L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FIFO);
    assertEquals(10, schedulables.get(0).getShare(), ERROR);
    assertEquals(5, schedulables.get(1).getShare(), ERROR);
    assertEquals(1, schedulables.get(2).getShare(), ERROR);
    assertEquals(0, schedulables.get(3).getShare(), ERROR);
  }

  public void testDistributeFairShareWithWeight() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 0, MAX, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", 10, 0, 0, MAX, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 0, MAX, 3.0, 0L));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 0, MAX, 4.0, 0L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FAIR);
    assertEquals(1.0 / 10 * 16, schedulables.get(0).getShare(), ERROR);
    assertEquals(2.0 / 10 * 16, schedulables.get(1).getShare(), ERROR);
    assertEquals(3.0 / 10 * 16, schedulables.get(2).getShare(), ERROR);
    assertEquals(4.0 / 10 * 16, schedulables.get(3).getShare(), ERROR);
  }

  public void testDistributeFairShareWithMaxCap() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 0, 1, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", 10, 0, 0, 2, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 0, MAX, 3.0, 0L));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 0, MAX, 4.0, 0L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FAIR);
    assertEquals(1.0, schedulables.get(0).getShare(), ERROR);
    assertEquals(2.0, schedulables.get(1).getShare(), ERROR);
    assertEquals(3.0 / 7 * (16 - 3), schedulables.get(2).getShare(), ERROR);
    assertEquals(4.0 / 7 * (16 - 3), schedulables.get(3).getShare(), ERROR);
  }

  public void testDistributeFairShareWithRequestCap() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 1, 0, 0, MAX, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", 2, 0, 0, MAX, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 0, MAX, 3.0, 0L));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 0, MAX, 4.0, 0L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FAIR);
    assertEquals(1.0, schedulables.get(0).getShare(), ERROR);
    assertEquals(2.0, schedulables.get(1).getShare(), ERROR);
    assertEquals(3.0 / 7 * (16 - 3), schedulables.get(2).getShare(), ERROR);
    assertEquals(4.0 / 7 * (16 - 3), schedulables.get(3).getShare(), ERROR);
  }

  public void testDistributeFairShareWithMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 3, MAX, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", 10, 0, 7, MAX, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 0, MAX, 3.0, 0L));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 0, MAX, 4.0, 0L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FAIR);
    assertEquals(3.0, schedulables.get(0).getShare(), ERROR);
    assertEquals(7.0, schedulables.get(1).getShare(), ERROR);
    assertEquals(3.0 / 7 * (16 - 10), schedulables.get(2).getShare(), ERROR);
    assertEquals(4.0 / 7 * (16 - 10), schedulables.get(3).getShare(), ERROR);
  }

  public void testDistributeFairShareWithNotActiveLimits() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 1, 4, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", 10, 0, 2, 5, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 3, 6, 3.0, 0L));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 4, 7, 4.0, 0L));
    Schedulable.distributeShare(16, schedulables, ScheduleComparator.FAIR);
    assertEquals(1.0 / 10 * 16, schedulables.get(0).getShare(), ERROR);
    assertEquals(2.0 / 10 * 16, schedulables.get(1).getShare(), ERROR);
    assertEquals(3.0 / 10 * 16, schedulables.get(2).getShare(), ERROR);
    assertEquals(4.0 / 10 * 16, schedulables.get(3).getShare(), ERROR);
  }

  /**
   * Total share < sum(min) && Total share > sum(demand)
   */
  public void testDistributeMinFairShareWayUnderAllocatedOptimistic() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 5, 0, 10, 40, 1.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s2", 5, 0, 20, 50, 2.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s3", 5, 0, 30, 60, 3.0, 0L, 2));
    schedulables.add(new SchedulableForTest("s4", 5, 0, 40, 70, 4.0, 0L, 1));
    Schedulable.distributeShare(30, schedulables, ScheduleComparator.PRIORITY);
    assertEquals(10, schedulables.get(0).getShare(), ERROR);
    assertEquals(20, schedulables.get(1).getShare(), ERROR);
    assertEquals(30, schedulables.get(2).getShare(), ERROR);
    assertEquals(40, schedulables.get(3).getShare(), ERROR);
  }

  /**
   * Total share < sum(min) && Total share < sum(demand)
   */
  public void testDistributeMinFairShareWayUnderAllocatedPessimistic() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 10, 0, 10, 40, 1.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s2", 10, 0, 20, 50, 2.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s3", 10, 0, 30, 60, 3.0, 0L, 2));
    schedulables.add(new SchedulableForTest("s4", 10, 0, 40, 70, 4.0, 0L, 1));
    Schedulable.distributeShare(30, schedulables, ScheduleComparator.PRIORITY);
    assertEquals(10, schedulables.get(0).getShare(), ERROR);
    assertEquals(20, schedulables.get(1).getShare(), ERROR);
    assertEquals(30, schedulables.get(2).getShare(), ERROR);
    assertEquals(40, schedulables.get(3).getShare(), ERROR);
  }

  /**
   * Total share > sum(max or demand)
   */
  public void testDistributeMinFairShareWayOverAllocated() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 15, 0, 10, 40, 1.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s2", 25, 0, 20, 50, 2.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s3", 35, 0, 30, 60, 3.0, 0L, 2));
    schedulables.add(new SchedulableForTest("s4", 45, 0, 40, 70, 4.0, 0L, 1));
    Schedulable.distributeShare(250, schedulables,
                                ScheduleComparator.PRIORITY);
    assertEquals(15, schedulables.get(0).getShare(), ERROR);
    assertEquals(25, schedulables.get(1).getShare(), ERROR);
    assertEquals(35, schedulables.get(2).getShare(), ERROR);
    assertEquals(45, schedulables.get(3).getShare(), ERROR);
  }

  /**
   * Total share < sum(demand) && Total > sum(min)
   */
  public void testDistributeMinFairShareOverAllocated() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s1", 15, 0, 10, 40, 1.0, 0L, 3));
    schedulables.add(new SchedulableForTest("s2", 25, 0, 20, 50, 2.0, 0L, 2));
    schedulables.add(new SchedulableForTest("s3", 35, 0, 30, 60, 3.0, 0L, 2));
    schedulables.add(new SchedulableForTest("s4", 45, 0, 40, 70, 4.0, 0L, 1));
    Schedulable.distributeShare(110, schedulables,
                                ScheduleComparator.PRIORITY);
    assertEquals(15, schedulables.get(0).getShare(), ERROR);
    assertEquals(22, schedulables.get(1).getShare(), ERROR);
    assertEquals(33, schedulables.get(2).getShare(), ERROR);
    assertEquals(40, schedulables.get(3).getShare(), ERROR);
  }

  private class SchedulableForTest extends Schedulable {
    final int requested, granted, min, max;
    final long startTime;
    final double weight;
    final int priority;

    SchedulableForTest(String name, int requested, int granted,
        int min, int max, double weight, long startTime) {
      super(name, ResourceType.MAP);
      this.requested = requested;
      this.granted = granted;
      this.min = min;
      this.max = max;
      this.weight = weight;
      this.startTime = startTime;
      this.priority = 0;
    }

    SchedulableForTest(String name, int requested, int granted,
        int min, int max, double weight, long startTime, int priority) {
      super(name, ResourceType.MAP);
      this.requested = requested;
      this.granted = granted;
      this.min = min;
      this.max = max;
      this.weight = weight;
      this.startTime = startTime;
      this.priority = priority;
    }

    @Override
    public int getGranted() {
      return granted;
    }

    @Override
    public double getWeight() {
      return weight;
    }

    @Override
    public int getMinimum() {
      return min;
    }

    @Override
    public int getMaximum() {
      return max;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public long getDeadline() {
      return -1L;
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public int getRequested() {
      return requested;
    }

    @Override
    public void snapshot() {
    }
  }

}

package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestScheduleComparator extends TestCase {

  public void testFairComparatorWithGrants() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // grants:1,2
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 0, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithWeights() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // weights:2.0,1.0
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 0, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // min:10,0
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 9, 10, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0, 100.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithBothUnderMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // grant:1,2 Min:2,3
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 3, 100.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithTie() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // startTime:0,1
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2, 0.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 2, 0.0, 1L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testMinFairComparatorWithGrants() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // grants:1,2
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 2));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 0,
                                            1.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // Commutativity.
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 0,
                                            1.0, 0L, 0, 2));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s2", schedulables.get(0).getName());
    Assert.assertEquals("s1", schedulables.get(1).getName());

    schedulables.clear();
    // Fair if equal priority
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 0,
                                            1.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  /**
   * Neither of them needy. Different weights.
   */
  public void testMinFairComparatorWithWeights() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // priority wins over weights
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            2.0, 0L, 0, 2));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // commutative
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            2.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 2));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s2", schedulables.get(0).getName());
    Assert.assertEquals("s1", schedulables.get(1).getName());

    schedulables.clear();
    // Fair if equal priority.
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0,
                                            2.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 0,
                                            1.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  /**
   * One of the schedulables is needy.
   */
  public void testMinFairComparatorWithMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // min:10,0 prty:1,2
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 9, 10,
                                            1.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0,
                                            100.0, 0L, 0, 2));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // min:10,0 prty:2,1
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 9, 10,
                                            1.0, 0L, 0, 2));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0,
                                            100.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // min:10,0 prty:1,1
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 9, 10,
                                            1.0, 0L, 0, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0,
                                            100.0, 0L, 0, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  /**
   * Both schedulables are needy.
   */
  public void testMinFairComparatorWithBothUnderMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // grant:1,2 Min:2,3, prty:1,2
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2,
                                            1.0, 0L, 1, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 3,
                                            100.0, 0L, 1, 2));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // grant:1,2 Min:2,3, prty: 2,1, same result
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2,
                                            1.0, 0L, 1, 2));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 3,
                                            100.0, 0L, 1, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());

    schedulables.clear();
    // grant:1,2 Min:2,3, prty: 1,1, same result
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2,
                                            1.0, 0L, 1, 1));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 2, 3,
                                            100.0, 0L, 1, 1));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testMinFairComparatorWithTie() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // startTime:0,1
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2, 0.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 2, 0.0, 0));
    Collections.sort(schedulables, ScheduleComparator.PRIORITY);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFifoComparator() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // startTime:0,1
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 0, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 0, 2.0, 1L));
    Collections.sort(schedulables, ScheduleComparator.FIFO);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFifoComparatorWithTie() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // sorted by pool names in this case
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 1, 2, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 1, 2, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FIFO);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testDeadlineComparator() {
    // s2 has a deadline of 1 which is earler than s1, so it should run first
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0, 0, 2, 1, 0));
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 0, 0, 0, 1, 2, 0));
    Collections.sort(schedulables, ScheduleComparator.DEADLINE);
    Assert.assertEquals("s2", schedulables.get(0).getName());
    Assert.assertEquals("s1", schedulables.get(1).getName());
  }

  public void testDeadlineComparatorWithPriority() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // S1 has priority of 2 so it will be scheduled first
    schedulables.add(new SchedulableForTest("s2", ResourceType.MAP, 0, 0, 0, 2, 1, 1));
    schedulables.add(new SchedulableForTest("s1", ResourceType.MAP, 0, 0, 0, 1, 2, 2));
    Collections.sort(schedulables, ScheduleComparator.DEADLINE);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  private class SchedulableForTest extends Schedulable {
    final int granted, min, priority;
    final long startTime, deadline;
    final double weight;
    SchedulableForTest(String name, ResourceType type, int granted,
        int min, double weight, long startTime, long deadline, int priority) {
      super(name, type);
      this.granted = granted;
      this.min = min;
      this.weight = weight;
      this.startTime = startTime;
      this.deadline = deadline;
      this.priority = priority;
    }

    SchedulableForTest(String name, ResourceType type, int granted,
        int min, double weight, long startTime) {
      this(name, type, granted, min, weight, startTime, 0, 0);
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
    public long getStartTime() {
      return startTime;
    }

    @Override
    public int getRequested() {
      return 0;
    }

    @Override
    public void snapshot() {
    }

    @Override
    public long getDeadline() {
      return deadline;
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

}

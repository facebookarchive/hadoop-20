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
    schedulables.add(new SchedulableForTest("s1", "M", 1, 0, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 2, 0, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithWeights() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // weights:2.0,1.0
    schedulables.add(new SchedulableForTest("s1", "M", 1, 0, 2.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 1, 0, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // min:10,0
    schedulables.add(new SchedulableForTest("s1", "M", 9, 10, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 0, 0, 100.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithBothUnderMin() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // grant:1,2 Min:2,3
    schedulables.add(new SchedulableForTest("s1", "M", 1, 2, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 2, 3, 100.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFairComparatorWithTie() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // startTime:0,1
    schedulables.add(new SchedulableForTest("s1", "M", 1, 2, 0.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 1, 2, 0.0, 1L));
    Collections.sort(schedulables, ScheduleComparator.FAIR);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFifoComparator() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // startTime:0,1
    schedulables.add(new SchedulableForTest("s1", "M", 1, 0, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s2", "M", 1, 0, 2.0, 1L));
    Collections.sort(schedulables, ScheduleComparator.FIFO);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  public void testFifoComparatorWithTie() {
    List<Schedulable> schedulables = new ArrayList<Schedulable>();
    // sorted by pool names in this case
    schedulables.add(new SchedulableForTest("s2", "M", 1, 2, 1.0, 0L));
    schedulables.add(new SchedulableForTest("s1", "M", 1, 2, 1.0, 0L));
    Collections.sort(schedulables, ScheduleComparator.FIFO);
    Assert.assertEquals("s1", schedulables.get(0).getName());
    Assert.assertEquals("s2", schedulables.get(1).getName());
  }

  private class SchedulableForTest extends Schedulable {
    final int granted, min;
    final long startTime;
    final double weight;

    SchedulableForTest(String name, String type, int granted,
        int min, double weight, long startTime) {
      super(name, type);
      this.granted = granted;
      this.min = min;
      this.weight = weight;
      this.startTime = startTime;
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
  }

}

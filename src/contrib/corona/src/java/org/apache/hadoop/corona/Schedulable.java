package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Maintain parameters for sorting
 */
public abstract class Schedulable {

  final private String name;
  final private String type;

  protected int requested;
  protected int granted;
  private double share;

  public Schedulable(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  /**
   * Take a snapshot of requested and granted
   */
  abstract public void snapshot();

  /**
   * Number of current requested resource at the last snapshot
   */
  public int getRequested() {
    return requested;
  }

  /**
   * Number of current granted resource at the last snapshot
   */
  public int getGranted() {
    return granted;
  }

  /**
   * Change the number of granted resource since last snapshot
   */
  public void incGranted(int diff) {
    granted += diff;
  }

  /**
   * Proportional to the number of resource this schedulable should get
   */
  public double getWeight() {
    return 1.0;
  }

  /**
   * The minimum number of resource this schedulable should get
   */
  public int getMinimum() {
    return 0;
  }

  /**
   * The maximum number of resource this schedulable should get
   */
  public int getMaximum() {
    return Integer.MAX_VALUE;
  }

  /**
   * Start time of this schedulable. Can be used for FIFO sort
   */
  public long getStartTime() {
    return -1L;
  }

  /**
   * The target number of granted resource should get in equilibrium
   */
  public double getShare() {
    return share;
  }

  /**
   * Set the share of the schedulables based on the total
   */
  public static void distributeShare(
      double total, final Collection<? extends Schedulable> schedulables,
      ScheduleComparator comparator) {
    switch (comparator) {
    case FIFO:
      Schedulable.distributeShareFifo(total, schedulables);
      break;
    case FAIR:
      Schedulable.distributeShareFair(total, schedulables);
      break;
    default:
      throw new IllegalArgumentException("Unknown comparator");
    }
  }

  private static void distributeShareFifo(
      double total, final Collection<? extends Schedulable> schedulables) {
    List<Schedulable> sches = new ArrayList<Schedulable>(schedulables);
    Collections.sort(sches, ScheduleComparator.FIFO);
    for (Schedulable schedulable : sches) {
      int max = Math.min(schedulable.getRequested(), schedulable.getMaximum());
      if (total > max) {
        schedulable.share = max;
        total -= max;
      } else {
        schedulable.share = total;
        return;
      }
    }
  }

  private static void distributeShareFair(
      double total, final Collection<? extends Schedulable> schedulables) {
    BinarySearcher searcher = new BinarySearcher() {
      @Override
      protected double targetFunction(double x) {
        return totalShareWithRatio(schedulables, x);
      }
    };
    double ratio = searcher.getSolution(total);
    for (Schedulable schedulable : schedulables) {
      schedulable.share = shareWithRatio(schedulable, ratio);
    }
  }

  private static double totalShareWithRatio(
      Collection<? extends Schedulable> schedulables,
      double weightToShareRatio) {
    double totalShare = 0;
    for (Schedulable schedulable : schedulables) {
      totalShare += shareWithRatio(schedulable, weightToShareRatio);
    }
    return totalShare;
  }

  private static double shareWithRatio(
      Schedulable schedulable, double weightToShareRatio) {
    double share = schedulable.getWeight() * weightToShareRatio;
    int min = schedulable.getMinimum();
    int max = schedulable.getMaximum();
    int requested = schedulable.getRequested();
    share = Math.max(min, share);
    share = Math.min(max, share);
    share = Math.min(requested, share);
    return share;
  }

}

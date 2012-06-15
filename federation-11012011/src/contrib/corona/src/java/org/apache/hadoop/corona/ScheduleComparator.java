package org.apache.hadoop.corona;

import java.util.Comparator;

/**
 * Compares the {@link Schedulable} instances
 */
public enum ScheduleComparator implements Comparator<Schedulable> {

  FAIR {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      boolean s1Needy = s1.getGranted() < s1.getMinimum();
      boolean s2Needy = s2.getGranted() < s2.getMinimum();
      if (s1Needy && !s2Needy) {
        return -1;
      }
      if (!s1Needy && s2Needy) {
        return 1;
      }
      double s1Ratio, s2Ratio;
      if (s1Needy && s2Needy) {
        s1Ratio = (double)(s1.getGranted()) / s1.getMinimum();
        s2Ratio = (double)(s2.getGranted()) / s2.getMinimum();
      } else {
        // both not needy
        s1Ratio = s1.getGranted() / s1.getWeight();
        s2Ratio = s2.getGranted() / s2.getWeight();
      }
      if (s1Ratio == s2Ratio) {
        return FIFO.compare(s1, s2);
      }
      // will sort in ascending order
      return s1Ratio < s2Ratio ? -1 : 1;
    }
  },

  FIFO {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      if (s1.getStartTime() < s2.getStartTime()) {
        return -1;
      }
      if (s1.getStartTime() > s2.getStartTime()) {
        return 1;
      }
      return s1.getName().compareTo(s2.getName());
    }
  },

  FAIR_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * FAIR.compare(s1, s2);
    }
  },

  FIFO_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * FIFO.compare(s1, s2);
    }
  };

  abstract public int compare(Schedulable s1, Schedulable s2);
}

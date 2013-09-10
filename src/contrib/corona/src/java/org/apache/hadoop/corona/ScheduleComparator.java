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

package org.apache.hadoop.corona;

import java.util.Comparator;

/**
 * Compares the {@link Schedulable} instances
 */
public enum ScheduleComparator implements Comparator<Schedulable> {
  /** Fair scheduler comparator */
  FAIR {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      /**
       * Priorities are not accounted for when computing the shares
       * for preemption.
       * This code relies on the fact that preemption will never be happening
       * within the pool, and priorities only work within the pool, so we don't
       * need to handle the case when they are both present
       */
      int pri = comparePriorities(s1, s2);
      if (pri != 0) {
        return pri;
      }
      boolean s1Needy = s1.getGranted() < s1.getMinimum();
      boolean s2Needy = s2.getGranted() < s2.getMinimum();
      if (s1Needy && !s2Needy) {
        return -1;
      }
      if (!s1Needy && s2Needy) {
        return 1;
      }
      double s1Ratio;
      double s2Ratio;
      if (s1Needy && s2Needy) {
        s1Ratio = (double) (s1.getGranted()) / s1.getMinimum();
        s2Ratio = (double) (s2.getGranted()) / s2.getMinimum();
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

  /**
   * Fair until min allocation and for same priorities.
   * Over the mins, use strict priority.
   */
  PRIORITY {
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
      double s1Ratio;
      double s2Ratio;
      if (s1Needy && s2Needy) {
        s1Ratio = (double) (s1.getGranted()) / s1.getMinimum();
        s2Ratio = (double) (s2.getGranted()) / s2.getMinimum();
      } else {
        int pri = comparePriorities(s1, s2);
        if (pri != 0) {
          return pri;
        }
        // else fall back to fair allocation.
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

  /** FIFO scheduler comparator */
  FIFO {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      int pri = comparePriorities(s1, s2);
      if (pri != 0) {
        return pri;
      }
      if (s1.getStartTime() < s2.getStartTime()) {
        return -1;
      }
      if (s1.getStartTime() > s2.getStartTime()) {
        return 1;
      }
      return s1.getName().compareTo(s2.getName());
    }
  },

  /** Scheduler comparator that sorts the jobs based on their deadline*/
  DEADLINE {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      int pri = comparePriorities(s1, s2);
      if (pri != 0) {
        return pri;
      }
      long d1 = s1.getDeadline();
      long d2 = s2.getDeadline();
      if (d1 == d2) {
        return FIFO.compare(s1, s2);
      }
      return d1 < d2 ? -1 : 1;
    }
  },

  /** Fair scheduler comparator with preemption */
  FAIR_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * FAIR.compare(s1, s2);
    }
  },

  /** Priority scheduler comparator with preemption */
  PRIORITY_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * PRIORITY.compare(s1, s2);
    }
  },

  /** FIFO scheduler comparator with preemption */
  FIFO_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * FIFO.compare(s1, s2);
    }
  },
  /** Deadline based scheduler preemption comparison */
  DEADLINE_PREEMPT {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      return -1 * DEADLINE.compare(s1, s2);
    }
  };

  /**
   * Compare two schedulables.
   *
   * @param s1 First schedulable
   * @param s2 Second schedulable
   * @return -1 if s1 > s2, 0 if s1 == s2, 1 if s1 < s2
   */
  public abstract int compare(Schedulable s1, Schedulable s2);

  /**
   * Compares schedulables based on their priorities. The higher the priority
   * the higher the position in the sorted order
   * @param s1 First schedulable
   * @param s2 Second schedulable
   * @return -1 if the priority of s1 is higher than s2, 0 if they are same
   * 1 if s2 has a higher priority
   */
  private static int comparePriorities(Schedulable s1, Schedulable s2) {
    int p1 = s1.getPriority();
    int p2 = s2.getPriority();
    if (p1 != p2) {
      return p1 > p2 ? -1 : 1;
    }
    return 0;
  }
}

package org.apache.hadoop.mapred;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.TaskTrackerLoadInfo.TaskInfo;

public class WastedTimeTTLIIterator extends TaskTrackerLoadInfoIterator {

  // Sort the first time
  private Iterator<TaskTrackerLoadInfo> iterator;

  private Iterator<TaskTrackerLoadInfo> getIterator() {
    Collections.sort(trackers, new WastedTimeComparator());
    return trackers.iterator();
  }

  public boolean hasNext() {
    if (iterator == null) {
      iterator = getIterator();
    }
    return iterator.hasNext();
  }

  public TaskTrackerLoadInfo next() {
    if (iterator == null) {
      iterator = getIterator();
    }
    return iterator.next();
  }

  public void remove() {
    if (iterator == null) {
      iterator = getIterator();
    }
    iterator.remove();
  }

  public static class WastedTimeComparator
          implements Comparator<TaskTrackerLoadInfo> {

    public final int THRESHOLD = 10;

    public int compare(TaskTrackerLoadInfo tt1, TaskTrackerLoadInfo tt2) {
      if (tt1.isActive() != tt2.isActive()) {
        return tt1.isActive() ? -1 : 1;
      }

      // Running and finished maps wasted time
      long totalWastedFirst = tt1.getTotalWastedTime();
      long totalWastedSecond = tt2.getTotalWastedTime();

      long totalWastedDiff = totalWastedFirst - totalWastedSecond;
      if ((totalWastedDiff != 0) &&
              ((totalWastedFirst / Math.abs(totalWastedDiff) < THRESHOLD) ||
              (totalWastedSecond / Math.abs(totalWastedDiff) < THRESHOLD))) {
        return (int) Math.signum(totalWastedDiff);
      }

      List<TaskInfo> tasksFirst = tt1.getLocalTasksInfo();
      List<TaskInfo> tasksSecond = tt2.getLocalTasksInfo();

      int tasksDiff = tasksFirst.size() - tasksSecond.size();
      if (tasksDiff > 0 &&
              ((tasksFirst.size() / Math.abs(tasksDiff) < THRESHOLD) ||
              (tasksSecond.size() / Math.abs(tasksDiff) < THRESHOLD))) {
        return tasksDiff;
      }

      int jobEffectFirst = getJobEffectCoefficient(tasksFirst);
      int jobEffectSecond = getJobEffectCoefficient(tasksSecond);
      return jobEffectFirst - jobEffectSecond;
    }

    private int getJobEffectCoefficient(List<TaskInfo> tasks) {
      Map<Integer, Integer> jobsEffect = new HashMap<Integer, Integer>();

      for (TaskInfo task : tasks) {
        Integer jobEffect = jobsEffect.get(task.getJobId());
        if (jobEffect == null) {
          jobEffect = 0;
        }
        jobEffect++;
        jobsEffect.put(task.getJobId(), jobEffect);
      }

      int max = 0;
      for (Integer effect : jobsEffect.values()) {
        if (effect > max) {
          max = effect;
        }
      }
      return max * jobsEffect.size();
    }
  }
}

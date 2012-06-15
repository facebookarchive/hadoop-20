package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.util.StringUtils;

public class MultiTaskTracker {
  private static final Log LOG = LogFactory.getLog(MultiTaskTracker.class);

  public static class MultiTaskTrackerMetrics implements Updater {
    /** Registry of a subset of metrics */
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final List<TaskTracker> trackerList;
    /** Aggregates the metrics between task trackers for task launch */
    private final MetricsTimeVaryingRate aggTaskLaunchMsecs =
        new MetricsTimeVaryingRate("taskLaunchMsecs", registry,
            "Msecs to launch a task after getting request.", true);

    public MultiTaskTrackerMetrics(List<TaskTracker> trackerList) {
      this.trackerList = trackerList;
      MetricsContext context = MetricsUtil.getContext("mapred");
      metricsRecord = MetricsUtil.createRecord(context, "multitasktracker");
      context.registerUpdater(this);
    }

    @Override
    public void doUpdates(MetricsContext context) {
      LOG.info("Updating metrics");
      int numTrackers = trackerList.size();
      long totalMapRefill = 0;
      long totalReduceRefill = 0;
      int totalRunningMaps = 0;
      int totalRunningReduces = 0;
      int totalMapSlots = 0;
      int totalReduceSlots = 0;
      for (TaskTracker tracker : trackerList) {
        totalMapRefill += tracker.getAveMapSlotRefillMsecs();
        totalReduceRefill += tracker.getAveReduceSlotRefillMsecs();
        totalRunningMaps += tracker.getRunningMaps();
        totalRunningReduces += tracker.getRunningReduces();
        totalMapSlots += tracker.getMaxActualMapTasks();
        totalReduceSlots += tracker.getMaxActualReduceTasks();

        // If the metrics exists, aggregate the task launch msecs for all
        // trackers
        TaskTrackerInstrumentation instrumentation =
            tracker.getTaskTrackerInstrumentation();
        if (instrumentation != null) {
          MetricsTimeVaryingRate taskLaunchMsecs =
              instrumentation.getTaskLaunchMsecs();
          if (taskLaunchMsecs != null) {
            taskLaunchMsecs.pushMetric(null);
            aggTaskLaunchMsecs.inc(
                taskLaunchMsecs.getPreviousIntervalAverageTime());
          }
        }
      }
      long avgMapRefill = totalMapRefill / numTrackers;
      long avgReduceRefill = totalReduceRefill / numTrackers;
      metricsRecord.setMetric("aveMapSlotRefillMsecs", avgMapRefill);
      metricsRecord.setMetric("aveReduceSlotRefillMsecs", avgReduceRefill);
      metricsRecord.setMetric("maps_running", totalRunningMaps);
      metricsRecord.setMetric("reduces_running", totalRunningReduces);
      metricsRecord.setMetric("mapTaskSlots", totalMapSlots);
      metricsRecord.setMetric("reduceTaskSlots", totalReduceSlots);

      for (MetricsBase metricsBase : registry.getMetricsList()) {
        metricsBase.pushMetric(metricsRecord);
      }

      metricsRecord.update();
    }
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(MultiTaskTracker.class, args, LOG);
    LOG.debug("MultiTaskTracker starting");
    int numTaskTrackers = Integer.parseInt(args[0]);
    String ttType = "mapred";
    Class ttClass = TaskTracker.class;
    if (args.length > 1) {
      ttType = args[1];
    }
    if (ttType.equals("corona")) {
      ttClass = Class.forName("org.apache.hadoop.mapred.CoronaTaskTracker");
    }
    List<TaskTrackerRunner> runners = new ArrayList<TaskTrackerRunner>();
    List<TaskTracker> trackerList = new ArrayList<TaskTracker>();
    LOG.info("Will start " + numTaskTrackers + " " + ttType + " trackers");
    for (int i = 0; i < numTaskTrackers; i++) {
      Configuration conf = new Configuration();
      JobConf jConf = new JobConf(conf);
      jConf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      jConf.setBoolean("mapred.tasktracker.simulated.tasks", true);
      jConf.setBoolean("mapred.tasktracker.task.completion.event.store", true);
      // Dummy instrumentation.
      jConf.set("mapred.tasktracker.instrumentation",
        TaskTrackerInstrumentation.class.getName());
      String[] baseLocalDirs = jConf.getLocalDirs();
      List<String> localDirs = new LinkedList<String>();
      localDirs.clear();
      for (String localDir : baseLocalDirs) {
        File baseLocalDir = new File(localDir);
        File localDirFile = new File(baseLocalDir, "TT_" + i);
        localDirFile.mkdirs();
        localDirs.add(localDirFile.getAbsolutePath());
      }
      jConf.setStrings("mapred.local.dir",
              localDirs.toArray(new String[localDirs.size()]));
      TaskTracker tracker = createTaskTracker(jConf, ttClass);
      trackerList.add(tracker);
      TaskTrackerRunner runner = new TaskTrackerRunner(i, tracker);
      runner.setDaemon(true);
      runners.add(runner);
      runner.start();
      LOG.info("Started tracker# " + i);
    }
    MultiTaskTrackerMetrics metrics = new MultiTaskTrackerMetrics(trackerList);
    for (TaskTrackerRunner runner : runners) {
      try {
        runner.join();
      } catch (InterruptedException iex) {
      }
    }
  }

  public static TaskTracker createTaskTracker(JobConf jConf, Class ttClass)
      throws IOException {
    try {
      Constructor<?> constructor =
        ttClass.getDeclaredConstructor(new Class[]{JobConf.class});
      TaskTracker tt =
        (TaskTracker) constructor.newInstance(jConf);
      return tt;
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  private static class TaskTrackerRunner extends Thread {

    private TaskTracker ttToRun = null;
    private int id;
    
    public TaskTrackerRunner(int id, TaskTracker tt) {
      super();
      this.ttToRun = tt;
      this.id = id;
    }

    @Override
    public void run() {
      LOG.debug("Running TT #" + id);
      ttToRun.run();
    }
  }
}

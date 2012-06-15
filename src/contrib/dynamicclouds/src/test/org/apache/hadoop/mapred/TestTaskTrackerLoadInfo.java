package org.apache.hadoop.mapred;

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;

public class TestTaskTrackerLoadInfo extends TestCase {

  public void testWIthRunningTasks() {
    Map<String, Object> trackerInfo = new HashMap<String, Object>();

    trackerInfo.put("active", true);
    long lastSeen = System.currentTimeMillis();

    trackerInfo.put("last_seen", lastSeen);
    trackerInfo.put("map_tasks_max", 8L);
    trackerInfo.put("reduce_tasks_max", 8L);
    
    Object[] tasks = new Object[4];

    Map<String, Object> firstMap = new HashMap<String, Object>();

    firstMap.put("job_id", 1L);
    firstMap.put("task_id", 20L);
    firstMap.put("attempt", 0L);
    firstMap.put("type", "map");
    firstMap.put("state", "SUCCEEDED");
    firstMap.put("phase", "MAP");
    firstMap.put("progress", 1.0);
    firstMap.put("start_time", lastSeen);
    firstMap.put("running_time", 10L);

    tasks[0] = firstMap;

    Map<String, Object> secondMap = new HashMap<String, Object>();

    secondMap.put("job_id", 1L);
    secondMap.put("task_id", 21L);
    secondMap.put("attempt", 0L);
    secondMap.put("type", "map");
    secondMap.put("state", "RUNNING");
    secondMap.put("phase", "MAP");
    secondMap.put("progress", 0.5);
    secondMap.put("start_time", lastSeen);
    secondMap.put("running_time", 5L);

    tasks[1] = secondMap;

    Map<String, Object> firstReduce = new HashMap<String, Object>();

    firstReduce.put("job_id", 1L);
    firstReduce.put("task_id", 22L);
    firstReduce.put("attempt", 0L);
    firstReduce.put("type", "reduce");
    firstReduce.put("state", "RUNNING");
    firstReduce.put("phase", "SHUFFLE");
    firstReduce.put("progress", 0.0);
    firstReduce.put("start_time", lastSeen);
    firstReduce.put("running_time", 100L);

    tasks[2] = firstReduce;

    Map<String, Object> secondReduce = new HashMap<String, Object>();

    secondReduce.put("job_id", 1L);
    secondReduce.put("task_id", 23L);
    secondReduce.put("attempt", 0L);
    secondReduce.put("type", "reduce");
    secondReduce.put("state", "RUNNING");
    secondReduce.put("phase", "SORT");
    secondReduce.put("progress", 0.5);
    secondReduce.put("start_time", lastSeen);
    secondReduce.put("running_time", 50L);

    tasks[3] = secondReduce;

    trackerInfo.put("tasks", tasks);

    TaskTrackerLoadInfo info = new TaskTrackerLoadInfo("testTracker");
    info.parseMap(trackerInfo);

    assertEquals(65, info.getTotalWastedTime());
    assertEquals(55, info.getRunningTimeWasted());
    assertEquals(2, info.getTotalMapTasks());
    assertEquals(1, info.getRunningMapTasks());
    assertEquals(2, info.getRunningReduceTasks());

  }

  public void testEmptyReport() {
    Map<String, Object> trackerInfo = new HashMap<String, Object>();

    trackerInfo.put("active", true);
    long lastSeen = System.currentTimeMillis();

    trackerInfo.put("last_seen", lastSeen);
    trackerInfo.put("map_tasks_max", 8L);
    trackerInfo.put("map_tasks_running", 0L);
    trackerInfo.put("reduce_tasks_max", 8L);
    trackerInfo.put("reduce_tasks_running", 0L);
    trackerInfo.put("tasks", new Object[0]);

    TaskTrackerLoadInfo info = new TaskTrackerLoadInfo("testTracker");
    info.parseMap(trackerInfo);

    assertEquals(lastSeen, info.getLastSeen());
    assertEquals(8, info.getMaxMapTasks());
    assertEquals(0, info.getRunningMapTasks());

    assertEquals(8, info.getMaxReduceTasks());
    assertEquals(0, info.getRunningReduceTasks());

    assertEquals(0, info.getTotalWastedTime());
    assertEquals(0, info.getTotalMapTasks());
    assertEquals(0, info.getRunningTimeWasted());

  }
}

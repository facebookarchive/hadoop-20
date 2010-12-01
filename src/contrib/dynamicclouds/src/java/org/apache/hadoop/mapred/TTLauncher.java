package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

class TTLauncher implements Runnable {

  private static final long TIMEOUT = 5000;
  private SortedMap<Long, TTLaunchTask> trackerLaunchers = Collections.synchronizedSortedMap(new TreeMap<Long, TTLaunchTask>());
  private final Object lock = new Object();
  private boolean shutdown = false;

  @Override
  public void run() {
    TTLaunchTask exec = null;
    // On shutdown - launch all trackers int the queue
    while (!shutdown || trackerLaunchers.size() > 0) {
      try {
        try {
          synchronized (lock) {
            // Only wait for new if we are not in shutdown mode
            if (trackerLaunchers.size() == 0 && !shutdown) {
              DynamicCloudsDaemon.LOG.info("Queue is empty - waiting");
              lock.wait();
              DynamicCloudsDaemon.LOG.info("Woken up frorm wait on the queue");
              if (trackerLaunchers.size() == 0 && shutdown) {
                DynamicCloudsDaemon.LOG.info("Shutting down and the queue is empty");
                return;
              }
            }
          }
          // The last time this was launched
          long launchTime = trackerLaunchers.firstKey();
          long currentTime = System.currentTimeMillis();
          long timeSinceLaunch = currentTime - launchTime;
          DynamicCloudsDaemon.LOG.info("Time since last tried  to launch" + timeSinceLaunch);
          if (timeSinceLaunch < TIMEOUT) {
            synchronized (lock) {
              // Wait for time to pass, or for a new item to come in
              lock.wait(TIMEOUT - timeSinceLaunch);
            }
          }
          launchTime = trackerLaunchers.firstKey();
          exec = trackerLaunchers.get(launchTime);
          trackerLaunchers.remove(launchTime);
          DynamicCloudsDaemon.LOG.info("Executing launcher " + exec.toString());
          exec.execute();
        } catch (IOException ex) {
          DynamicCloudsDaemon.LOG.error("Launch of task tracker failed");
          // Start of the tasktracker failed.
          if (!exec.hasAttempts()) {
            continue;
          }
          trackerLaunchers.put(System.currentTimeMillis(), exec);
        }
      } catch (InterruptedException iex) {
        DynamicCloudsDaemon.LOG.error("Interrupted in TTLauncher", iex);
      }
    }
  }

  public int getTasksInQueue(String hostName) {
    int count = 0;
    synchronized (trackerLaunchers) {
      for (TTLaunchTask task : trackerLaunchers.values()) {
        if (task.getClusterName().equals(hostName)) {
          count++;
        }
      }
    }
    return count;
  }

  public void addTTForLaunch(TTLaunchTask exec) {
    synchronized (lock) {
      exec.getClusterName();
      DynamicCloudsDaemon.LOG.info("Added executor " + exec.toString());
      trackerLaunchers.put(0L, exec);
      lock.notifyAll();
    }
  }

  public synchronized void shutdown() {

    synchronized (lock) {
      shutdown = true;
      // Wake up the thread if it is waiting for new stuff to come in
      lock.notifyAll();
    }
  }
}

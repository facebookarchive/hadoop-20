package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Scheduler {

  public static final Log LOG = LogFactory.getLog(Scheduler.class);

  final private Map<String, SchedulerForType> schedulersForTypes;
  final private ConfigManager configManager;
  final private Collection<String> types;

  private CoronaConf conf;

  public Scheduler(NodeManager nodeManager, SessionManager sessionManager,
      SessionNotifier sessionNotifier, Collection<String> types) {
    this(nodeManager, sessionManager, sessionNotifier, types,
        new ConfigManager(types));
  }

  /**
   * Used by unit test to fake the ConfigManager
   */
  public Scheduler(NodeManager nodeManager, SessionManager sessionManager,
      SessionNotifier sessionNotifier, Collection<String> types,
      ConfigManager configManager) {
    this.configManager = configManager;
    this.schedulersForTypes = new HashMap<String, SchedulerForType>();
    this.types = types;
    for (String type : types) {
      SchedulerForType schedulerForType = new SchedulerForType(
          type, sessionManager, sessionNotifier, nodeManager, configManager);
      schedulerForType.setDaemon(true);
      schedulerForType.setName("Scheduler-" + type);
      schedulersForTypes.put(type, schedulerForType);
    }
  }

  public void addSession(String id, Session session) {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      scheduleThread.addSession(id, session);
    }
  }

  public void start() {
    for (Thread schedulerForType : schedulersForTypes.values()) {
      LOG.info("Starting " + schedulerForType.getName());
      schedulerForType.start();
    }
    configManager.start();
  }

  public void setConf(CoronaConf conf) {
    this.conf = conf;
  }

  public CoronaConf getConf() {
    return conf;
  }

  public void close() {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      scheduleThread.close();
    }
    for (Thread scheduleThread : schedulersForTypes.values()) {
      Utilities.waitThreadTermination(scheduleThread);
    }
    configManager.close();
  }

  public void notifyScheduler() {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      synchronized (scheduleThread) {
        scheduleThread.notifyAll();
      }
    }
  }

  public Map<String, PoolMetrics> getPoolMetrics(String type) {
    return schedulersForTypes.get(type).getPoolMetrics();
  }

  public List<String> getPoolNames() {
    Set<String> poolNames = new HashSet<String>();
    for (String type : types) {
      poolNames.addAll(getPoolMetrics(type).keySet());
    }
    List<String> result = new ArrayList<String>();
    result.addAll(poolNames);
    Collections.sort(result);
    return result;
  }
}

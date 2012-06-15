/**
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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Maintains a hierarchy of pools.
 */
public class PoolManager {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.mapred.PoolManager");

  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL = 10 * 1000;

  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT = 5 * 1000;

  // Map and reduce minimum allocations for each pool
  private Map<String, Integer> mapAllocs = new HashMap<String, Integer>();
  private Map<String, Integer> reduceAllocs = new HashMap<String, Integer>();

  // Sharing weights for each pool
  private Map<String, Double> poolWeights = new HashMap<String, Double>();
  // Can the slots in this pool be preempted
  private Map<String, Boolean> canBePreempted = new HashMap<String, Boolean>();
  // Do we boost the weight of the older jobs in the pool?
  private Map<String, Boolean> poolFifoWeight = new HashMap<String, Boolean>();

  // Redirects from one pool to another
  private Map<String, String> poolRedirectMap = new HashMap<String, String>();
  // List of blacklisted pools set that will then get the implicit pool
  private Set<String> poolBlacklistedSet = new HashSet<String>();

  private Set<String> poolNamesInAllocFile = new HashSet<String>();
  private Set<String> systemPoolNames = new HashSet<String>();

  // Max concurrent running jobs for each pool and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  private Map<String, Integer> poolMaxJobs = new HashMap<String, Integer>();
  private Map<String, Integer> userMaxJobs = new HashMap<String, Integer>();
  private Map<String, Integer> poolMaxInitedTasks =
      new HashMap<String, Integer>();
  private Map<String, Integer> poolMaxMaps = new HashMap<String, Integer>();
  private Map<String, Integer> poolMaxReduces = new HashMap<String, Integer>();
  private Map<String, Integer> poolRunningMaps = new HashMap<String, Integer>();
  private Map<String, Integer> poolRunningReduces =
    new HashMap<String, Integer>();
  private int userMaxJobsDefault = Integer.MAX_VALUE;
  private int poolMaxJobsDefault = Integer.MAX_VALUE;
  // Min share preemption timeout for each pool in seconds. If a job in the pool
  // waits this long without receiving its guaranteed share, it is allowed to
  // preempt other jobs' tasks.
  private Map<String, Long> minSharePreemptionTimeouts =
    new HashMap<String, Long>();

  // Default min share preemption timeout for pools where it is not set
  // explicitly.
  private long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;

  // Default maximum number of tasks can be initialized in a pool.
  private int defaultMaxTotalInitedTasks = Integer.MAX_VALUE;

  // Preemption timeout for jobs below fair share in seconds. If a job remains
  // below half its fair share for this long, it is allowed to preempt tasks.
  private long fairSharePreemptionTimeout = Long.MAX_VALUE;

  private final boolean strictPoolsMode;
  private String allocFile; // Path to XML file containing allocations
  private String poolNameProperty; // Jobconf property to use for determining a
                                   // job's pool name (default: user.name)

  /** Set the pool name, all pool names will be lowercased */
  public static final String EXPLICIT_POOL_PROPERTY =
    "mapred.fairscheduler.pool";

  private Map<String, Pool> pools = new HashMap<String, Pool>();

  private long lastReloadAttempt; // Last time we tried to reload the pools file
  private long lastSuccessfulReload; // Last time we successfully reloaded pools
  private boolean lastReloadAttemptFailed = false;

  private Set<String> oversubscribePools;

  public PoolManager(Configuration conf) throws IOException, SAXException,
      AllocationConfigurationException, ParserConfigurationException {
    this.poolNameProperty = conf.get(
        "mapred.fairscheduler.poolnameproperty", "user.name");
    this.allocFile = conf.get("mapred.fairscheduler.allocation.file");
    if (allocFile == null) {
      LOG.warn("No mapred.fairscheduler.allocation.file given in jobconf - " +
          "the fair scheduler will not use any queues.");
    }
    this.strictPoolsMode = conf.getBoolean(
      "mapred.fairscheduler.strictpoolsmode.enabled", false);
    reloadAllocs();
    lastSuccessfulReload = System.currentTimeMillis();
    lastReloadAttempt = System.currentTimeMillis();
    // Create the default pool so that it shows up in the web UI
    getPool(Pool.DEFAULT_POOL_NAME);
  }

  /**
   * Get a pool by name, creating it if necessary.
   *
   * @name Name of the pool to get.  (Should be lower-cased already)
   */
  public synchronized Pool getPool(String name) {
    Pool pool = pools.get(name);
    if (pool == null) {
      boolean isConfiguredPool = poolNamesInAllocFile.contains(name);
      pool = new Pool(name, isConfiguredPool);
      pools.put(name, pool);
    }
    return pool;
  }

  /**
   * Reload allocations file if it hasn't been loaded in a while
   * return true if reloaded
   */
  public boolean reloadAllocsIfNecessary() {
    if (allocFile == null) {
      // A warning has been logged when allocFile is null.
      // We should just return here.
      return false;
    }
    long time = System.currentTimeMillis();
    boolean reloaded = false;
    if (time > lastReloadAttempt + ALLOC_RELOAD_INTERVAL) {
      lastReloadAttempt = time;
      try {
        File file = new File(allocFile);
        long lastModified = file.lastModified();
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + ALLOC_RELOAD_WAIT) {
          reloadAllocs();
          reloaded = true;
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
        }
      } catch (Exception e) {
        // Throwing the error further out here won't help - the RPC thread
        // will catch it and report it in a loop. Instead, just log it and
        // hope somebody will notice from the log.
        // We log the error only on the first failure so we don't fill up the
        // JobTracker's log with these messages.
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload allocations file - " +
              "will use existing allocations.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
    return reloaded;
  }

  /**
   * Updates the allocation list from the allocation config file. This file is
   * expected to be in the following whitespace-separated format:
   *
   * <code>
   * poolName1 mapAlloc reduceAlloc
   * poolName2 mapAlloc reduceAlloc
   * ...
   * </code>
   *
   * Blank lines and lines starting with # are ignored.
   *
   * @throws IOException if the config file cannot be read.
   * @throws AllocationConfigurationException if allocations are invalid.
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   */
  public void reloadAllocs() throws IOException, ParserConfigurationException,
      SAXException, AllocationConfigurationException {
    if (allocFile == null) return;
    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    Map<String, Integer> mapAllocs = new HashMap<String, Integer>();
    Map<String, Integer> reduceAllocs = new HashMap<String, Integer>();
    Map<String, Integer> poolMaxMaps = new HashMap<String, Integer>();
    Map<String, Integer> poolMaxReduces = new HashMap<String, Integer>();
    Map<String, Integer> poolMaxJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolMaxInitedTasks = new HashMap<String, Integer>();
    Map<String, Integer> userMaxJobs = new HashMap<String, Integer>();
    Map<String, Double> poolWeights = new HashMap<String, Double>();
    Map<String, Boolean> canBePreempted = new HashMap<String, Boolean>();
    Map<String, Boolean> poolFifoWeight = new HashMap<String, Boolean>();
    Map<String, Long> minSharePreemptionTimeouts = new HashMap<String, Long>();
    Map<String, String> poolRedirectMap = new HashMap<String, String>();
    Set<String> poolBlacklistedSet = new HashSet<String>();
    Set<String> oversubscribePools = new HashSet<String>();
    long fairSharePreemptionTimeout = Long.MAX_VALUE;
    long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    int defaultMaxTotalInitedTasks = Integer.MAX_VALUE;
    int userMaxJobsDefault = Integer.MAX_VALUE;
    int poolMaxJobsDefault = Integer.MAX_VALUE;

    // Remember all pool names so we can display them on web UI, etc.
    Set<String> poolNamesInAllocFile = new HashSet<String>();
    Set<String> systemPoolNames = new HashSet<String>();

    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new File(allocFile));
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName()))
      throw new AllocationConfigurationException("Bad allocations file: " +
          "top-level element not <allocations>");
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (!(node instanceof Element))
        continue;
      Element element = (Element)node;
      if ("pool".equals(element.getTagName())) {
        String poolName = element.getAttribute("name").toLowerCase();
        poolNamesInAllocFile.add(poolName);
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("minMaps".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            mapAllocs.put(poolName, val);
          } else if ("minSharePreemptionTimeout".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            long val = Long.parseLong(text) * 1000L;
            minSharePreemptionTimeouts.put(poolName, val);
          } else if ("minReduces".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            reduceAllocs.put(poolName,val);
          } else if ("maxMaps".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxMaps.put(poolName, val);
          } else if ("maxReduces".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxReduces.put(poolName, val);
          } else if ("maxRunningJobs".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxJobs.put(poolName, val);
          } else if ("maxTotalInitedTasks".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxInitedTasks.put(poolName, val);
          } else if ("weight".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            double val = Double.parseDouble(text);
            poolWeights.put(poolName, val);
          } else if ("canBePreempted".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            boolean val = Boolean.parseBoolean(text);
            canBePreempted.put(poolName, val);
          } else if ("fifo".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            boolean val = Boolean.parseBoolean(text);
            poolFifoWeight.put(poolName, val);
          } else if ("redirect".equals(field.getTagName())) {
            String redirect =
                ((Text)field.getFirstChild()).getData().trim().toLowerCase();
            poolRedirectMap.put(poolName, redirect);
            LOG.info("Redirecting " + poolName + " to " + redirect +
              ", configured properties for " + poolName + " will be ignored");
          } else if ("blacklisted".equals(field.getTagName())) {
            poolBlacklistedSet.add(poolName);
            LOG.info("Pool " + poolName + " has been blacklisted");
          } else if("overinitialize".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            boolean val = Boolean.parseBoolean(text);
            if (val) {
              oversubscribePools.add(poolName);
            }
          }
        }
        if (poolMaxMaps.containsKey(poolName) && mapAllocs.containsKey(poolName)
            && poolMaxMaps.get(poolName) < mapAllocs.get(poolName)) {
          LOG.warn(String.format(
              "Pool %s has max maps %d less than min maps %d",
              poolName, poolMaxMaps.get(poolName), mapAllocs.get(poolName)));
        }
        if(poolMaxReduces.containsKey(poolName) &&
           reduceAllocs.containsKey(poolName) &&
           poolMaxReduces.get(poolName) < reduceAllocs.get(poolName)) {
          LOG.warn(String.format(
              "Pool %s has max reduces %d less than min reduces %d",
              poolName, poolMaxReduces.get(poolName),
              reduceAllocs.get(poolName)));
        }
      } else if ("user".equals(element.getTagName())) {
        String userName = element.getAttribute("name");
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("maxRunningJobs".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            userMaxJobs.put(userName, val);
          }
        }
      } else if ("userMaxJobsDefault".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        userMaxJobsDefault = val;
      } else if ("poolMaxJobsDefault".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        poolMaxJobsDefault = val;
      } else if ("fairSharePreemptionTimeout".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L; // We use seconds in conf
        fairSharePreemptionTimeout = val;
      } else if ("defaultMinSharePreemptionTimeout".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L; // We use seconds in conf
        defaultMinSharePreemptionTimeout = val;
      } else if ("defaultMaxTotalInitedTasks".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        defaultMaxTotalInitedTasks = val;
      } else if ("systemPools".equals(element.getTagName())) {
        String[] pools = ((Text)element.getFirstChild()).getData().
          trim().split(",");
        for (String pool : pools) {
          systemPoolNames.add(pool);
        }
      } else {
        LOG.warn("Bad element in allocations file: " + element.getTagName());
      }
    }

    // Commit the reload; also create any pool defined in the alloc file
    // if it does not already exist, so it can be displayed on the web UI.
    synchronized(this) {
      this.mapAllocs = mapAllocs;
      this.reduceAllocs = reduceAllocs;
      this.poolMaxMaps = poolMaxMaps;
      this.poolMaxInitedTasks = poolMaxInitedTasks;
      this.poolMaxReduces = poolMaxReduces;
      this.poolMaxJobs = poolMaxJobs;
      this.userMaxJobs = userMaxJobs;
      this.userMaxJobsDefault = userMaxJobsDefault;
      this.poolMaxJobsDefault = poolMaxJobsDefault;
      this.poolWeights = poolWeights;
      this.canBePreempted = canBePreempted;
      this.poolFifoWeight = poolFifoWeight;
      this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
      this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
      this.defaultMaxTotalInitedTasks = defaultMaxTotalInitedTasks;
      this.defaultMinSharePreemptionTimeout = defaultMinSharePreemptionTimeout;
      this.poolRedirectMap = poolRedirectMap;
      this.poolBlacklistedSet = poolBlacklistedSet;
      this.poolNamesInAllocFile = poolNamesInAllocFile;
      this.systemPoolNames = systemPoolNames;
      this.oversubscribePools = oversubscribePools;
      for (String name: poolNamesInAllocFile) {
        getPool(name);
      }
    }
  }

  /**
   * Get the allocation for a particular pool
   */
  public int getMinSlots(String pool, TaskType taskType) {
    Map<String, Integer> allocationMap = (taskType == TaskType.MAP ?
        mapAllocs : reduceAllocs);
    Integer alloc = allocationMap.get(pool);
    return (alloc == null ? 0 : alloc);
  }

  /**
   * Add a job in the appropriate pool
   */
  public synchronized void addJob(JobInProgress job) {
    String poolName = getPoolName(job);
    LOG.info("Adding job " + job.getJobID() + " to pool " + poolName +
             ", originally from pool " +
             job.getJobConf().get(EXPLICIT_POOL_PROPERTY));
    getPool(poolName).addJob(job);
  }

  /**
   * Remove a job
   */
  public synchronized void removeJob(JobInProgress job) {
    if (getPool(getPoolName(job)).removeJob(job)) {
      return;
    }

    // Job wasn't found in this pool.  Search for the job in all the pools
    // (the pool may have been created after the job started).
    for (Pool pool : getPools()) {
      if (pool.removeJob(job)) {
        LOG.info("Removed job " + job.jobId + " from pool " + pool.getName() +
            " instead of pool " + getPoolName(job));
        return;
      }
    }

    LOG.error("removeJob: Couldn't find job " +
        job.jobId + " in any pool, should have been in pool " +
        getPoolName(job));
  }

  /**
   * Change the pool of a particular job
   */
  public synchronized void setPool(JobInProgress job, String pool) {
    removeJob(job);
    job.getJobConf().set(EXPLICIT_POOL_PROPERTY, pool);
    addJob(job);
  }

  /**
   * Get a collection of all pools
   */
  public synchronized Collection<Pool> getPools() {
    return pools.values();
  }

  /**
   * Get the pool name for a JobInProgress from its configuration. This uses
   * the "project" property in the jobconf by default, or the property set with
   * "mapred.fairscheduler.poolnameproperty".
   */
  public synchronized String getPoolName(JobInProgress job) {
    String name = getExplicitPoolName(job).trim();
    String redirect = poolRedirectMap.get(name);

    if (redirect == null) {
      return name;
    } else {
      return redirect;
    }
  }

  /**
   * A quick check for to ensure that if the pool name is set and we are in a
   * strict pools mode, then the pool must exist to run this job.
   *
   * @param job Job to check
   * @throws InvalidJobConfException
   */
  public synchronized void checkValidPoolProperty(JobInProgress job)
      throws InvalidJobConfException {
    if (!strictPoolsMode) {
      return;
    }

    JobConf conf = job.getJobConf();
    // Pool name needs to be lower cased.
    String poolName = conf.get(EXPLICIT_POOL_PROPERTY);
    if (poolName == null) {
      return;
    } else {
      poolName = poolName.toLowerCase();
    }

    if (poolNamesInAllocFile.contains(poolName)) {
      return;
    }

    throw new InvalidJobConfException(
        "checkValidPoolProperty: Pool name " +
        conf.get(EXPLICIT_POOL_PROPERTY) + " set with Hadoop property " +
        EXPLICIT_POOL_PROPERTY + " does not exist.  " +
        "Please check for typos in the pool name.");
  }

  private synchronized String getExplicitPoolName(JobInProgress job) {
    JobConf conf = job.getJobConf();

    // Pool name needs to be lower cased.
    String poolName = conf.get(EXPLICIT_POOL_PROPERTY);
    if (poolName != null) {
      poolName = poolName.toLowerCase();
    }

    String redirect = poolRedirectMap.get(poolName);
    if (redirect != null) {
      poolName = redirect;
    }

    if (poolName == null || poolBlacklistedSet.contains(poolName)) {
      return getImplicitPoolName(job);
    }

    if (!poolNamesInAllocFile.contains(poolName) && strictPoolsMode) {
      return getImplicitPoolName(job);
    }

    if (!isLegalPoolName(poolName)) {
      LOG.warn("Explicit pool name " + poolName + " for job " + job.getJobID() +
          " is not legal. Falling back.");
      return getImplicitPoolName(job);
    }

    return poolName;
  }

  private String getImplicitPoolName(JobInProgress job) {
    JobConf conf = job.getJobConf();

    // Pool name needs to be lower cased.
    String poolName = conf.get(poolNameProperty);
    if (poolName != null) {
      poolName = poolName.toLowerCase();
    }

    String redirect = poolRedirectMap.get(poolName);
    if (redirect != null) {
      poolName = redirect;
    }

    if (poolName == null || poolBlacklistedSet.contains(poolName)) {
      return Pool.DEFAULT_POOL_NAME;
    }

    if (!isLegalPoolName(poolName)) {
      LOG.warn("Implicit pool name " + poolName + " for job " + job.getJobID() +
          " is not legal. Falling back.");
      return Pool.DEFAULT_POOL_NAME;
    }

    return poolName;
  }

  public boolean canOversubscribePool(String name) {
    return oversubscribePools.contains(name);
  }

  public boolean isSystemPool(String name) {
    return systemPoolNames.contains(name);
  }

  /**
   * Returns whether or not the given pool name is legal.
   *
   * Legal pool names are of nonzero length and are formed only of alphanumeric
   * characters, underscores (_), and hyphens (-).
   */
  private static boolean isLegalPoolName(String poolName) {
    return !poolName.matches(".*[^0-9a-z\\-\\_].*")
            && (poolName.length() > 0);

  }

  /**
   * Get all pool names that have been seen either in the allocation file or in
   * a MapReduce job.
   */
  public synchronized Collection<String> getPoolNames() {
    List<String> list = new ArrayList<String>();
    for (Pool pool: getPools()) {
      list.add(pool.getName());
    }
    Collections.sort(list);
    return list;
  }

  public int getUserMaxJobs(String user) {
    if (userMaxJobs.containsKey(user)) {
      return userMaxJobs.get(user);
    } else {
      return userMaxJobsDefault;
    }
  }

  public int getPoolMaxJobs(String pool) {
    if (poolMaxJobs.containsKey(pool)) {
      return poolMaxJobs.get(pool);
    } else {
      return poolMaxJobsDefault;
    }
  }

  public int getPoolMaxInitedTasks(String pool) {
    if (poolMaxInitedTasks.containsKey(pool)) {
      return poolMaxInitedTasks.get(pool);
    } else {
      return defaultMaxTotalInitedTasks;
    }
  }

  public double getPoolWeight(String pool) {
    if (poolWeights.containsKey(pool)) {
      return poolWeights.get(pool);
    } else {
      return 1.0;
    }
  }

  /**
   * Can we take slots from this pool when preempting tasks?
   */
  public boolean canBePreempted(String pool) {
    Boolean result = canBePreempted.get(pool);
    return result == null ? true : result; // Default is true
  }

  /**
   * Do we boost the weight for the older jobs?
   */
  public boolean fifoWeight(String pool) {
    Boolean result = poolFifoWeight.get(pool);
    return result == null ? false : result; // Default is false
  }

  /**
   * Get a pool's min share preemption timeout, in milliseconds. This is the
   * time after which jobs in the pool may kill other pools' tasks if they
   * are below their min share.
   */
  public long getMinSharePreemptionTimeout(String pool) {
    if (minSharePreemptionTimeouts.containsKey(pool)) {
      return minSharePreemptionTimeouts.get(pool);
    } else {
      return defaultMinSharePreemptionTimeout;
    }
  }

  /**
   * Get the fair share preemption, in milliseconds. This is the time
   * after which any job may kill other jobs' tasks if it is below half
   * its fair share.
   */
  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  /**
   * Get the maximum map or reduce slots for the given pool.
   * @return the cap set on this pool, or Integer.MAX_VALUE if not set.
   */
  int getMaxSlots(String poolName, TaskType taskType) {
    Map<String, Integer> maxMap = (taskType == TaskType.MAP ?
                                   poolMaxMaps : poolMaxReduces);
    if (maxMap.containsKey(poolName)) {
      return maxMap.get(poolName);
    } else {
      return Integer.MAX_VALUE;
    }
  }

  int getMaxSlots(Pool pool, TaskType taskType) {
    return getMaxSlots(pool.getName(), taskType);
  }

  /**
   * Set the number of running tasks in all pools to zero
   * @param type type of task to be set
   */
  public void resetRunningTasks(TaskType type) {
    Map<String, Integer> runningMap = (type == TaskType.MAP ?
                                       poolRunningMaps : poolRunningReduces);
    for (String poolName : runningMap.keySet()) {
      runningMap.put(poolName, 0);
    }
  }

  /**
   * Set the number of running tasks in a pool
   * @param poolName name of the pool
   * @param type type of task to be set
   * @param runningTasks number of current running tasks
   */
  public void incRunningTasks(String poolName, TaskType type, int inc) {
    Map<String, Integer> runningMap = (type == TaskType.MAP ?
                                       poolRunningMaps : poolRunningReduces);
    if (!runningMap.containsKey(poolName)) {
      runningMap.put(poolName, 0);
    }
    int runningTasks = runningMap.get(poolName) + inc;
    runningMap.put(poolName, runningTasks);
  }

  /**
   * Get the number of running tasks in a pool
   * @param poolName name of the pool
   * @param type type of task to be set
   * @return number of running tasks
   */
  public int getRunningTasks(String poolName, TaskType type) {
    Map<String, Integer> runningMap = (type == TaskType.MAP ?
                                       poolRunningMaps : poolRunningReduces);
    return (runningMap.containsKey(poolName) ?
            runningMap.get(poolName) : 0);
  }

  /**
   * Is the pool task limit exceeded?
   * @param poolName name of the pool
   * @param type type of the task to be check
   * @return true if the limit is exceeded
   */
  public boolean isMaxTasks(String poolName, TaskType type) {
    return getRunningTasks(poolName, type ) >= getMaxSlots(poolName, type);
  }

  /**
   * Check if the minimum slots set in the configuration is feasible
   * @param clusterStatus The current status of the JobTracker
   * @param type The type of the task to check
   * @return true if the check is passed
   */
  public boolean checkMinimumSlotsAvailable(
      ClusterStatus clusterStatus, TaskType type) {
    Map<String, Integer> poolToMinSlots = (type == TaskType.MAP) ?
        mapAllocs : reduceAllocs;
    int totalSlots = (type == TaskType.MAP) ?
        clusterStatus.getMaxMapTasks() : clusterStatus.getMaxReduceTasks();
    int totalMinSlots = 0;
    for (int minSlots : poolToMinSlots.values()) {
      totalMinSlots += minSlots;
    }
    if (totalMinSlots > totalSlots) {
      LOG.warn(String.format(
          "Bad minimum %s slot configuration. cluster:%s totalMinSlots:%s",
          type, totalSlots, totalMinSlots));
      return false;
    }
    LOG.info(String.format(
        "Minimum %s slots checked. cluster:%s totalMinSlots:%s",
        type, totalSlots, totalSlots));
    return true;
  }
}

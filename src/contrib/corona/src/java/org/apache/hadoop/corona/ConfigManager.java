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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Reloads corona scheduling parameters periodically. Generates the pools
 * config if the {@link PoolsConfigDocumentGenerator} class is set.
 * Needs to be thread safe
 *
 * The following is a corona.xml example:
 *
 * <?xml version="1.0"?>
 * <configuration>
 *   <defaultSchedulingMode>FAIR</defaultSchedulingMode>
 *   <nodeLocalityWaitMAP>0</nodeLocalityWaitMAP>
 *   <rackLocalityWaitMAP>5000</rackLocalityWaitMAP>
 *   <preemptedTaskMaxRunningTime>60000</preemptedTaskMaxRunningTime>
 *   <shareStarvingRatio>0.9</shareStarvingRatio>
 *   <starvingTimeForShare>60000</starvingTimeForShare>
 *   <starvingTimeForMinimum>30000</starvingTimeForMinimum>
 *   <group name="group_a">
 *     <minMAP>200</minMAP>
 *     <minMAP>100</minMAP>
 *     <minREDUCE>100</minREDUCE>
 *     <maxMAP>200</maxMAP>
 *     <maxREDUCE>200</maxREDUCE>
 *     <pool name="pool_sla">
 *       <minMAP>100</minMAP>
 *       <minREDUCE>100</minREDUCE>
 *       <maxMAP>200</maxMAP>
 *       <maxREDUCE>200</maxREDUCE>
 *       <weight>2.0</weight>
 *       <schedulingMode>FIFO</schedulingMode>
 *     </pool>
 *     <pool name="pool_nonsla">
 *     </pool>
 *   </group>
 *   <group name ="group_b">
 *     <maxMAP>200</maxMAP>
 *     <maxREDUCE>200</maxREDUCE>
 *     <weight>3.0</weight>
 *   </group>
 * </configuration>
 *
 * Note that the type strings "MAP" and "REDUCE" must be
 * defined in {@link CoronaConf}
 */
public class ConfigManager {
  /** Configuration xml tag name */
  public static final String CONFIGURATION_TAG_NAME = "configuration";
  /** Redirect xml tag name */
  public static final String REDIRECT_TAG_NAME = "redirect";
  /** Group xml tag name */
  public static final String GROUP_TAG_NAME = "group";
  /** Pool xml tag name */
  public static final String POOL_TAG_NAME = "pool";
  /** Scheduling mode xml tag name */
  public static final String SCHEDULING_MODE_TAG_NAME = "schedulingMode";
  /** Preemptability xml tag name */
  public static final String PREEMPTABILITY_MODE_TAG_NAME = "preemptable";
  /** Request maximum xml tag name */
  public static final String REQUEST_MAX_MODE_TAG_NAME = "requestMax";

  /** Weight xml tag name */
  public static final String WEIGHT_TAG_NAME = "weight";
  /** Priority xml tag name */
  public static final String PRIORITY_TAG_NAME = "priority";
  /** Whitelist xml tag name */
  public static final String WHITELIST_TAG_NAME = "whitelist";
  /** Min xml tag name prefix */
  public static final String MIN_TAG_NAME_PREFIX = "min";
  /** Max xml tag name prefix */
  public static final String MAX_TAG_NAME_PREFIX = "max";
  public static final String REDIRECT_JOB_WITH_LIMIT = "redirectJobWithLimit";
  /** Name xml attribute */
  public static final String NAME_ATTRIBUTE = "name";
  /** Source xml attribute (for redirect) */
  public static final String SOURCE_ATTRIBUTE = "source";
  /** Destination xml attribute (for redirect) */
  public static final String DESTINATION_ATTRIBUTE = "destination";
  public static final String JOB_INPUT_SIZE_LIMIT_ATTRIBUTE = "inputSizeLimit";

  /** Logger */
  private static final Log LOG = LogFactory.getLog(ConfigManager.class);

  /** The default behavior to schedule from nodes to sessions. */
  private static final boolean DEFAULT_SCHEDULE_FROM_NODE_TO_SESSION = false;
  /** The default max running time to consider for preemption */
  private static final long DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME =
    5 * 60 * 1000L;
  /** The default number of attempts to preempt */
  private static final int DEFAULT_PREEMPTION_ROUNDS = 10;
  /** The default ratio to consider starvation */
  private static final double DEFAULT_SHARE_STARVING_RATIO = 0.7;
  /** The minimum amount of time to wait between preemptions */
  public static final long DEFAULT_MIN_PREEMPT_PERIOD = 60 * 1000L;
  /**
   * The default number of grants to give out on each iteration of
   * the Scheduler.
   */
  public static final int DEFAULT_GRANTS_PER_ITERATION = 5000;
  /** The default allowed starvation time for a share */
  public static final long DEFAULT_STARVING_TIME_FOR_SHARE = 5 * 60 * 1000L;
  /** The default allowed starvation time for a minimum allocation */
  public static final long DEFAULT_STARVING_TIME_FOR_MINIMUM = 3 * 60 * 1000L;
  /** The comparator to use by default within any pool group */
  private static final ScheduleComparator DEFAULT_POOL_GROUP_COMPARATOR =
      ScheduleComparator.PRIORITY;
  /** The comparator to use by default within any pool */
  private static final ScheduleComparator DEFAULT_POOL_COMPARATOR =
    ScheduleComparator.FIFO;

  /** If defined, generate the pools config document periodically */
  private PoolsConfigDocumentGenerator poolsConfigDocumentGenerator;
  /** The types this configuration is initialized for */
  private final Collection<ResourceType> TYPES;
  /** The classloader to use when looking for resource in the classpath */
  private final ClassLoader classLoader;
  /** Set of configured pool group names */
  private Set<String> poolGroupNameSet;
  /** Set of configured pool info names */
  private Set<PoolInfo> poolInfoSet;
  /** The Map of max allocations for a given type and group */
  private TypePoolGroupNameMap<Integer> typePoolGroupNameToMax =
      new TypePoolGroupNameMap<Integer>();
  /** The Map of min allocations for a given type and group */
  private TypePoolGroupNameMap<Integer> typePoolGroupNameToMin =
      new TypePoolGroupNameMap<Integer>();
  /** The Map of max allocations for a given type, group, and pool */
  private TypePoolInfoMap<Integer> typePoolInfoToMax =
      new TypePoolInfoMap<Integer>();
  /** The Map of min allocations for a given type, group, and pool */
  private TypePoolInfoMap<Integer> typePoolInfoToMin =
      new TypePoolInfoMap<Integer>();
  /** The set of pools that can't be preempted */
  private Set<PoolInfo> nonPreemptablePools = new HashSet<PoolInfo>();
  /**
   * The set of pools that limit the number of resource requests by the
   * pool maximum.
   */
  private Set<PoolInfo> requestMaxPools = new HashSet<PoolInfo>();
  /** The Map of node locality wait times for different resource types */
  private Map<ResourceType, Long> typeToNodeWait;
  /** The Map of rack locality wait times for different resource types */
  private Map<ResourceType, Long> typeToRackWait;
  /** The Map of comparators configurations for the pools */
  private Map<PoolInfo, ScheduleComparator> poolInfoToComparator;
  /** The Map of the weights configuration for the pools */
  private Map<PoolInfo, Double> poolInfoToWeight;
  /** The Map of redirections (source -> target) for PoolInfo objects */
  private Map<PoolInfo, PoolInfo> poolInfoToRedirect;
  /** The Map of priorities for the pools */
  private Map<PoolInfo, Integer> poolInfoToPriority;
  /** The Map of whitelist for the pools */
  private Map<PoolInfo, String> poolInfoToWhitelist;
  /** The default comparator for the schedulables within the pool */
  private ScheduleComparator defaultPoolComparator;
  /** The ratio of the share to consider starvation */
  private double shareStarvingRatio;
  /** The allowed starvation time for the share */
  private long starvingTimeForShare;
  /** The minimum period between preemptions. */
  private long minPreemptPeriod;
  /** The maximum Job size for a pool */
  private Map<PoolInfo, Long> poolJobSizeLimit;
  /** The Map of redirections when job exceeds limit for Pool */
  private Map<PoolInfo, PoolInfo> jobExceedsLimitPoolRedirect;

  /** The default FIFO pool info for large query to redirect to */
  private PoolInfo defaultFifoPoolInfo; 
  /** Tasks to schedule in one iteration of the scheduler */
  private volatile int grantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
  /** The allowed starvation time for the min allocation */
  private long starvingTimeForMinimum;
  /** The max time of the task to consider for preemption */
  private long preemptedTaskMaxRunningTime;
  /** The number of times to iterate over pools trying to preempt */
  private int preemptionRounds;
  /** Match nodes to session */
  private boolean scheduleFromNodeToSession;
  /** The flag for the reload thread */
  private volatile boolean running = true;
  /** The thread that monitors and reloads the config file */
  private ReloadThread reloadThread;
  /** The last timestamp when the config was successfully loaded */
  private long lastSuccessfulReload = -1L;
  /** Corona conf used to get static config */
  private final CoronaConf conf;
  /** Pools reload period ms */
  private final long poolsReloadPeriodMs;
  /** Config reload period ms */
  private final long configReloadPeriodMs;
  /** The name of the general config file to use */
  private volatile String configFileName;
  /** The name of the pools config file to use */
  private volatile String poolsConfigFileName;

  /**
   * The main constructor for the config manager given the types and the name
   * of the config file to use
   * @param types the types to initialize the configuration for
   */
  public ConfigManager(
      Collection<ResourceType> types, CoronaConf conf) {
    this.TYPES = types;
    this.conf = conf;

    Class<?> poolsConfigDocumentGeneratorClass =
        conf.getPoolsConfigDocumentGeneratorClass();
    if (poolsConfigDocumentGeneratorClass != null) {
      try {
        this.poolsConfigDocumentGenerator = (PoolsConfigDocumentGenerator)
            poolsConfigDocumentGeneratorClass.newInstance();
        poolsConfigDocumentGenerator.initialize(conf);
      } catch (InstantiationException e) {
        LOG.warn("Failed to instantiate " +
            poolsConfigDocumentGeneratorClass, e);
      } catch (IllegalAccessException e) {
        LOG.warn("Failed to instantiate " +
            poolsConfigDocumentGeneratorClass, e);
      }
    } else {
      poolsConfigDocumentGenerator = null;
    }
    poolsReloadPeriodMs = conf.getPoolsReloadPeriodMs();
    configReloadPeriodMs = conf.getConfigReloadPeriodMs();

    LOG.info("ConfigManager: PoolsConfigDocumentGenerator class = " +
        poolsConfigDocumentGeneratorClass + ", poolsReloadPeriodMs = " +
        		poolsReloadPeriodMs + ", configReloadPeriodMs = " +
        configReloadPeriodMs);

    typeToNodeWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    typeToRackWait = new EnumMap<ResourceType, Long>(ResourceType.class);

    defaultPoolComparator = DEFAULT_POOL_COMPARATOR;
    shareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    minPreemptPeriod = DEFAULT_MIN_PREEMPT_PERIOD;
    grantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
    starvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    starvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    preemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
    preemptionRounds = DEFAULT_PREEMPTION_ROUNDS;
    scheduleFromNodeToSession = DEFAULT_SCHEDULE_FROM_NODE_TO_SESSION;

    reloadThread = new ReloadThread();
    reloadThread.setName("Config reload thread");
    reloadThread.setDaemon(true);

    for (ResourceType type : TYPES) {
      typeToNodeWait.put(type, 0L);
      typeToRackWait.put(type, 0L);
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = ConfigManager.class.getClassLoader();
    }
    classLoader = cl;

    if (poolsConfigDocumentGenerator != null) {
      if (generatePoolsConfigIfClassSet() == null) {
        throw new IllegalStateException("Failed to generate the pools " +
            "config.  Must succeed on initialization of ConfigManager.");
      }
    }
    try {
      findConfigFiles();
      reloadAllConfig(true);
    } catch (IOException e) {
      LOG.error("Failed to load " + configFileName, e);
    } catch (SAXException e) {
      LOG.error("Failed to load " + configFileName, e);
    } catch (ParserConfigurationException e) {
      LOG.error("Failed to load " + configFileName, e);
    } catch (JSONException e) {
      LOG.error("Failed to load " + configFileName, e);
    }
  }

  /**
   * Used for unit tests
   */
  public ConfigManager() {
    TYPES = null;
    conf = new CoronaConf(new Configuration());
    poolsReloadPeriodMs = conf.getPoolsReloadPeriodMs();
    configReloadPeriodMs = conf.getConfigReloadPeriodMs();
    classLoader = ConfigManager.class.getClassLoader();
  }

  /**
   * Find the configuration files as set file names or in the classpath.
   */
  private void findConfigFiles() {
    // Find the materialized_JSON configuration file.
    if (configFileName == null) {
      String jsonConfigFileString = conf.getConfigFile().replace(
                                         CoronaConf.DEFAULT_CONFIG_FILE,
                                         Configuration.MATERIALIZEDJSON);
      File jsonConfigFile = new File(jsonConfigFileString);
      String jsonConfigFileName = null;
      if (jsonConfigFile.exists()) {
        jsonConfigFileName = jsonConfigFileString;
      } else {
          URL u = classLoader.getResource(jsonConfigFileString);
          jsonConfigFileName = (u != null) ? u.getPath() : null;
      }
      // Check that the materialized_JSON contains the resources
      // of corona.xml
      if (jsonConfigFileName != null) {
        try {
          jsonConfigFile = new File(jsonConfigFileName);
          InputStream in = new BufferedInputStream(new FileInputStream(
        		                                           jsonConfigFile));
          JSONObject json = conf.instantiateJsonObject(in);
          if (json.has(conf.xmlToThrift(CoronaConf.DEFAULT_CONFIG_FILE))) {
            configFileName = jsonConfigFileName;
            LOG.info("Attempt to find config file " + jsonConfigFileString +
                     " as a file and in class loader returned " + configFileName);
          }
        } catch (IOException e) {
            LOG.warn("IOException: " + "while parsing corona JSON configuration");
        } catch (JSONException e) {
            LOG.warn("JSONException: " + "while parsing corona JSON configuration");
        }
      }
    }
    if (configFileName == null) {
      // Parsing the JSON configuration failed.  Look for
      // the xml configuration.
      String configFileString = conf.getConfigFile();
      File configFile = new File(configFileString);
      if (configFile.exists()) {
        configFileName = configFileString;
      } else {
        URL u = classLoader.getResource(configFileString);
        configFileName = (u != null) ? u.getPath() : null;
      }
      LOG.info("Attempt to find config file " + configFileString +
          " as a file and in class loader returned " + configFileName);
    }
    if (poolsConfigFileName == null) {
      String poolsConfigFileString = conf.getPoolsConfigFile();
      File poolsConfigFile = new File(poolsConfigFileString);
      if (poolsConfigFile.exists()) {
        poolsConfigFileName = poolsConfigFileString;
      } else {
        URL u = classLoader.getResource(poolsConfigFileString);
        poolsConfigFileName = (u != null) ? u.getPath() : null;
      }
      LOG.info("Attempt to find pools config file " + poolsConfigFileString +
          " as a file and in class loader returned " + poolsConfigFileName);
    }
  }

  /**
   * Start monitoring the configuration for the updates
   */
  public void start() {
    reloadThread.start();
  }

  /**
   * Stop monitoring and reloading of the configuration
   */
  public void close() {
    running = false;
    reloadThread.interrupt();
  }

  /**
   * Is this a configured pool group?
   * @param poolGroup Name of the pool group to check
   * @return True if configured, false otherwise
   */
  public synchronized boolean isConfiguredPoolGroup(String poolGroup) {
    if (poolGroupNameSet == null) {
      return false;
    }
    return poolGroupNameSet.contains(poolGroup);
  }

  /**
   * Is this a configured pool info?
   * @param poolInfo Pool info to check
   * @return True if configured, false otherwise
   */
  public synchronized boolean isConfiguredPoolInfo(PoolInfo poolInfo) {
    if (poolInfoSet == null) {
      return false;
    }
    return poolInfoSet.contains(poolInfo);
  }

  /**
   * Get the configured pool infos so that the PoolGroupManager can make sure
   * they are created for stats and cm.jsp.
   */
  public synchronized Collection<PoolInfo> getConfiguredPoolInfos() {
    return poolInfoSet;
  }

  /**
   * Get the configured maximum allocation for a given {@link ResourceType}
   * in a given pool group
   * @param poolGroupName the name of the pool group
   * @param type the type of the resource
   * @return the maximum allocation for the resource in a pool group
   */
  public synchronized int getPoolGroupMaximum(String poolGroupName,
                                              ResourceType type) {
    Integer max = (typePoolGroupNameToMax == null) ? null :
        typePoolGroupNameToMax.get(type, poolGroupName);
    return max == null ? Integer.MAX_VALUE : max;
  }

  /**
   * Get the configured minimum allocation for a given {@link ResourceType}
   * in a given pool group
   * @param poolGroupName the name of the pool group
   * @param type the type of the resource
   * @return the minimum allocation for the resource in a pool group
   */
  public synchronized int getPoolGroupMinimum(String poolGroupName,
                                              ResourceType type) {
    Integer min = (typePoolGroupNameToMin == null) ? null :
        typePoolGroupNameToMin.get(type, poolGroupName);
    return min == null ? 0 : min;
  }

  /**
   * Get the configured ScheduleComparator.
   * in a given pool group
   * @param poolGroupName the name of the pool group
   * @return the configured comparator. (returns DEFAULT_POOL_GROUP_COMPARATOR
   *         by default.
   */
  public ScheduleComparator getPoolGroupComparator(String poolGroupName) {
    // TODO: If it uses a shared data structure, this needs to be thread safe!!
    // TODO: Currently unconfigurable.
    return DEFAULT_POOL_GROUP_COMPARATOR;
  }

  /**
   * Get the configured maximum allocation for a given {@link ResourceType}
   * in a given pool
   * @param poolInfo Pool info to check
   * @param type the type of the resource
   * @return the maximum allocation for the resource in a pool
   */
  public synchronized int getPoolMaximum(PoolInfo poolInfo, ResourceType type) {
    Integer max = (typePoolInfoToMax == null) ? null :
        typePoolInfoToMax.get(type, poolInfo);
    return max == null ? Integer.MAX_VALUE : max;
  }

  /**
   * Get the configured minimum allocation for a given {@link ResourceType}
   * in a given pool
   * @param poolInfo Pool info to check
   * @param type the type of the resource
   * @return the minimum allocation for the resource in a pool
   */
  public synchronized int getPoolMinimum(PoolInfo poolInfo, ResourceType type) {
    Integer min = (typePoolInfoToMin == null) ? null :
        typePoolInfoToMin.get(type, poolInfo);
    return min == null ? 0 : min;
  }

  public synchronized boolean isPoolPreemptable(PoolInfo poolInfo) {
    return !nonPreemptablePools.contains(poolInfo);
  }

  /**
   * Should this pool use the request max to limit job submission?
   * @param poolInfo Pool info to check
   * @return True if using the request max to limit jobs
   */
  public synchronized boolean useRequestMax(PoolInfo poolInfo) {
    return requestMaxPools.contains(poolInfo);
  }

  /**
   * Get the weight for the pool
   * @param poolInfo Pool info to check
   * @return the weight for the pool
   */
  public synchronized double getWeight(PoolInfo poolInfo) {
    Double weight = (poolInfoToWeight == null) ? null :
        poolInfoToWeight.get(poolInfo);
    return weight == null ? 1.0 : weight;
  }

  /**
   * Get the priority for the pool
   * @param poolInfo Pool info to check
   * @return the priority for the pool
   */
  public synchronized int getPriority(PoolInfo poolInfo) {
    Integer priority = (poolInfoToPriority == null) ? null :
        poolInfoToPriority.get(poolInfo);
    return priority == null ? 0 : priority;
  }

  /**
   * Get a redirected PoolInfo (destination) from the source.  Only supports
   * one level of redirection.
   * @param poolInfo Pool info to check for a destination
   * @return Destination pool info if one exists, else return the input
   */
  public synchronized PoolInfo getRedirect(PoolInfo poolInfo) {
    PoolInfo destination = (poolInfoToRedirect == null) ? poolInfo :
        poolInfoToRedirect.get(poolInfo);
    if (destination == null) {
      return poolInfo;
    }
    return destination;
  }

  private synchronized boolean jobExceedsSizeInfoLimit(Long jobSizeInfo,
                                               Long sizeInfoLimit) {
    return jobSizeInfo >= sizeInfoLimit;
  }

  public synchronized PoolInfo getRedirect(PoolInfo poolInfo, Long jobSizeInfo) {
    // Check if the pool specified has a redirect, use result to determine
    // if we should redirect based on size info limit
    PoolInfo actualPoolInfo =  getRedirect(poolInfo);
    Long sizeInfoLimit = poolJobSizeLimit.get(actualPoolInfo);
    
    if(sizeInfoLimit != null && jobExceedsSizeInfoLimit(jobSizeInfo, sizeInfoLimit)) {
      return jobExceedsLimitPoolRedirect.get(actualPoolInfo);
    }
    return actualPoolInfo;
  }
  
  public synchronized String getWhitelist(PoolInfo poolInfo) {
    return poolInfoToWhitelist.get(poolInfo);
  }

  /**
   * Get a copy of the map of redirects (used for cm.jsp)
   *
   * @return Map of redirects otherwise null if none exists
   */
  public synchronized Map<PoolInfo, PoolInfo> getRedirects() {
    return (poolInfoToRedirect == null) ? null :
        new HashMap<PoolInfo, PoolInfo>(poolInfoToRedirect);
  }

  /**
   * Get the comparator to use for scheduling sessions within a pool
   * @param poolInfo Pool info to check
   * @return the scheduling comparator to use for the pool
   */
  public synchronized ScheduleComparator getPoolComparator(PoolInfo poolInfo) {
    ScheduleComparator comparator =
        (poolInfoToComparator == null) ? null :
            poolInfoToComparator.get(poolInfo);
    return comparator == null ? defaultPoolComparator : comparator;
  }

  public synchronized long getPreemptedTaskMaxRunningTime() {
    return preemptedTaskMaxRunningTime;
  }

  public synchronized boolean getScheduleFromNodeToSession() {
    return scheduleFromNodeToSession;
  }

  public synchronized int getPreemptionRounds() {
    return preemptionRounds;
  }

  public synchronized double getShareStarvingRatio() {
    return shareStarvingRatio;
  }

  public synchronized long getStarvingTimeForShare() {
    return starvingTimeForShare;
  }

  public synchronized long getStarvingTimeForMinimum() {
    return starvingTimeForMinimum;
  }

  /**
   * Get the locality wait to be used by the scheduler for a given
   * ResourceType on a given LocalityLevel
   * @param type the type of the resource
   * @param level the locality level
   * @return the time in milliseconds to use as a locality wait for a resource
   * of a given type on a given level
   */
  public synchronized long getLocalityWait(ResourceType type,
                                           LocalityLevel level) {
    if (level == LocalityLevel.ANY) {
      return 0L;
    }
    Long wait = level == LocalityLevel.NODE ?
        typeToNodeWait.get(type) : typeToRackWait.get(type);
    if (wait == null) {
      throw new IllegalArgumentException("Unknown type:" + type);
    }
    return wait;
  }

  public long getMinPreemptPeriod() {
    return minPreemptPeriod;
  }

  public int getGrantsPerIteration() {
    return grantsPerIteration;
  }
  
  /**
   * The thread that monitors the config file and reloads the configuration
   * when the file is updated
   */
  private class ReloadThread extends Thread {
    @Override
    public void run() {
      long lastReloadAttempt = -1L;
      long lastGenerationAttempt = -1L;
      while (running) {
        try {
          boolean reloadAllConfig = false;
          long now = ClusterManager.clock.getTime();
          if ((poolsConfigDocumentGenerator != null) &&
              (now - lastGenerationAttempt > poolsReloadPeriodMs)) {
            lastGenerationAttempt = now;
            generatePoolsConfigIfClassSet();
            reloadAllConfig = true;
          }
          if (now - lastReloadAttempt > configReloadPeriodMs) {
            lastReloadAttempt = now;
            reloadAllConfig = true;
          }

          if (reloadAllConfig) {
            findConfigFiles();
            try {
              reloadAllConfig(false);
            } catch (IOException e) {
              LOG.error("Failed to load " + configFileName, e);
            } catch (SAXException e) {
              LOG.error("Failed to load " + configFileName, e);
            } catch (ParserConfigurationException e) {
              LOG.error("Failed to load " + configFileName, e);
            } catch (JSONException e) {
              LOG.error("Failed to load " + configFileName, e);
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to reload config because of " +
              "an unknown exception", e);
        }
        try {
          Thread.sleep(
              Math.min(poolsReloadPeriodMs, configReloadPeriodMs) / 10);
        } catch (InterruptedException e) {
          LOG.warn("run: Interrupted", e);
        }
      }
    }
  }

  /**
   * Generate the new pools configuration using the configuration generator.
   * The generated configuration is written to a temporary file and then
   * atomically renamed to the specified destination file.
   * This function may be called concurrently and it is safe to do so because
   * of the atomic rename to the destination file.
   *
   * @return Md5 of the generated file or null if generation failed.
   */
  public String generatePoolsConfigIfClassSet() {
    if (poolsConfigDocumentGenerator == null) {
      return null;
    }
    Document document = poolsConfigDocumentGenerator.generatePoolsDocument();
    if (document == null) {
      LOG.warn("generatePoolsConfig: Did not generate a valid pools xml file");
      return null;
    }

    // Write the content into a temporary xml file and rename to the
    // expected file.
    File tempXmlFile;
    try {
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setAttribute("indent-number", new Integer(2));

      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(
          "{http://xml.apache.org/xslt}indent-amount", "2");
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(document);
      tempXmlFile = File.createTempFile("tmpPoolsConfig", "xml");

      if (LOG.isDebugEnabled()) {
        StreamResult stdoutResult = new StreamResult(System.out);
        transformer.transform(source, stdoutResult);
      }

      StreamResult result = new StreamResult(tempXmlFile);
      transformer.transform(source, result);
      String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(
          new FileInputStream(tempXmlFile));
      File destXmlFile = new File(conf.getPoolsConfigFile());
      boolean success = tempXmlFile.renameTo(destXmlFile);
      LOG.info("generatePoolConfig: Renamed generated file " +
          tempXmlFile.getAbsolutePath() + " to " +
          destXmlFile.getAbsolutePath() + " returned " + success +
          " with md5sum " + md5);
      return md5;
    } catch (TransformerConfigurationException e) {
      LOG.warn("generatePoolConfig: Failed to write file", e);
    } catch (IOException e) {
      LOG.warn("generatePoolConfig: Failed to write file", e);
    } catch (TransformerException e) {
      LOG.warn("generatePoolConfig: Failed to write file", e);
    }

    return null;
  }
  
  /**
   * Helper function to reloadJsonConfig(). Parses the JSON Object
   * corresponding to the key "TYPES".
   * @throws JSONException
   */
  private void loadLocalityWaits(JSONObject json, Map<ResourceType, Long> 
               newTypeToNodeWait, Map<ResourceType, Long> newTypeToRackWait) 
               throws JSONException {
    Iterator<String> keys = json.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (!json.isNull(key)) {
        for (ResourceType type : TYPES) {
          if (key.equals("nodeLocalityWait" + type)) {
            long val = Long.parseLong(json.getString(key));
            newTypeToNodeWait.put(type, val);
          }
          if (key.equals("rackLocalityWait" + type)) {
            long val = Long.parseLong(json.getString(key));
            newTypeToRackWait.put(type, val);
          }
        }
      }
    }
  }
 
  private boolean loadPoolInfoToRedirect(String source, String destination,
                                        Map<PoolInfo, PoolInfo> newPoolInfoToRedirect) {
    PoolInfo sourcePoolInfo = PoolInfo.createPoolInfo(source);
    PoolInfo destPoolInfo = PoolInfo.createPoolInfo(destination);
    if (sourcePoolInfo == null || destPoolInfo == null) {
      LOG.error("Illegal redirect source " + sourcePoolInfo + " or destination " +
          destPoolInfo);
      return false;
    } else {
      newPoolInfoToRedirect.put(sourcePoolInfo, destPoolInfo);
      return true;
    }
  }

  /**
   * Helper function to reloadJsonConfig().  Parses the JSON Array
   * corresponding to the key REDIRECT_TAG_NAME.
   * @throws JSONException
   */
  private void loadPoolInfoToRedirect(JSONArray json, Map<PoolInfo, PoolInfo> 
               newPoolInfoToRedirect) throws JSONException {
    for (int i=0; i < json.length(); i++) {
      JSONObject jsonObj = json.getJSONObject(i);
      loadPoolInfoToRedirect(jsonObj.getString(SOURCE_ATTRIBUTE),
                                  jsonObj.getString(DESTINATION_ATTRIBUTE),
                                  newPoolInfoToRedirect);
    }
  }
  
  private void loadPoolInfoToRedirect(String poolGroupName, String source, String destination,
              String specifiedJobInputSizeLimit, 
              Map<PoolInfo, PoolInfo> newPoolInfoToRedirectWithLimit, 
              Map<PoolInfo, Long> newPoolInfoJobSizeLimit) {

    String validSource = PoolInfo.createValidString(poolGroupName, source);
    String validDestination = PoolInfo.createValidString(poolGroupName, destination);
    boolean loadedRedirect = loadPoolInfoToRedirect(validSource, validDestination, newPoolInfoToRedirectWithLimit);
    if (!loadedRedirect) { return; }
    long jobInputSizeLimit = Long.parseLong(specifiedJobInputSizeLimit);
    if (jobInputSizeLimit <= 0) {
      LOG.error("Illegal redirect limit " + jobInputSizeLimit + ". Limit must be > 0.");
    } else {
      PoolInfo sourcePoolInfo = PoolInfo.createPoolInfo(validSource);
      newPoolInfoJobSizeLimit.put(sourcePoolInfo, jobInputSizeLimit);
    }
  }

  /**
   * Reload the general configuration and update all in-memory values. Should
   * be invoked under synchronization.
   * 
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   * @throws JSONException
   */
  private void reloadJsonConfig() throws
      IOException, SAXException, ParserConfigurationException, JSONException {
    Map<ResourceType, Long> newTypeToNodeWait;
    Map<ResourceType, Long> newTypeToRackWait;
    ScheduleComparator newDefaultPoolComparator = DEFAULT_POOL_COMPARATOR;
    double newShareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    long newMinPreemptPeriod = DEFAULT_MIN_PREEMPT_PERIOD;
    int newGrantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
    long newStarvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    long newStarvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    long newPreemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
    int newPreemptionRounds = DEFAULT_PREEMPTION_ROUNDS;
    boolean newScheduleFromNodeToSession = DEFAULT_SCHEDULE_FROM_NODE_TO_SESSION;

    newTypeToNodeWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    newTypeToRackWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    Map<PoolInfo, PoolInfo> newPoolInfoToRedirect =
            new HashMap<PoolInfo, PoolInfo>();
    for (ResourceType type : TYPES) {
      newTypeToNodeWait.put(type, 0L);
      newTypeToRackWait.put(type, 0L);
    }
    
    // All the configuration files for a cluster are placed in one large	
    // json object. This large json object has keys that map to smaller	
    // json objects which hold the same resources as xml configuration 	
    // files. Here, we try to parse the json object that corresponds to	
    // corona.xml
    File jsonConfigFile = new File(configFileName);
    InputStream in = new BufferedInputStream(new FileInputStream(jsonConfigFile));
    JSONObject json = conf.instantiateJsonObject(in);
    json = json.getJSONObject(conf.xmlToThrift(CoronaConf.DEFAULT_CONFIG_FILE));
    Iterator<String> keys = json.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (!json.isNull(key)) {
        if (key.equals("localityWaits")) {
          JSONObject jsonTypes = json.getJSONObject(key);
          loadLocalityWaits(jsonTypes, newTypeToNodeWait, newTypeToRackWait);
        }
        if (key.equals("defaultSchedulingMode")) {
          newDefaultPoolComparator =
              ScheduleComparator.valueOf(json.getString(key));
        }
        if (key.equals("shareStarvingRatio")) {
          newShareStarvingRatio = json.getDouble(key);
          if (newShareStarvingRatio < 0 || newShareStarvingRatio > 1.0) {
            LOG.error("Illegal shareStarvingRatio:" + newShareStarvingRatio);
            newShareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
          }
        }
        if (key.equals("grantsPerIteration")) {
          newGrantsPerIteration = json.getInt(key);
          if (newMinPreemptPeriod < 0) {
            LOG.error("Illegal grantsPerIteration: " + newGrantsPerIteration);
            newGrantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
          }
        }
        if (key.equals("minPreemptPeriod")) {
          newMinPreemptPeriod = json.getLong(key);
          if (newMinPreemptPeriod < 0) {
            LOG.error("Illegal minPreemptPeriod: " + newMinPreemptPeriod);
            newMinPreemptPeriod = DEFAULT_MIN_PREEMPT_PERIOD;
          }
        }
        if (key.equals("starvingTimeForShare")) {
          newStarvingTimeForShare = json.getLong(key);
          if (newStarvingTimeForShare < 0) {
            LOG.error("Illegal starvingTimeForShare:" + newStarvingTimeForShare);
            newStarvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
          }
        }
        if (key.equals("starvingTimeForMinimum")) {
          newStarvingTimeForMinimum = json.getLong(key);
          if (newStarvingTimeForMinimum < 0) {
            LOG.error("Illegal starvingTimeForMinimum:" +
                      newStarvingTimeForMinimum);
            newStarvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
          }
        }
        if (key.equals("preemptedTaskMaxRunningTime")) {
          newPreemptedTaskMaxRunningTime = json.getLong(key);
          if (newPreemptedTaskMaxRunningTime < 0) {
            LOG.error("Illegal preemptedTaskMaxRunningTime:" +
                      newPreemptedTaskMaxRunningTime);
            newPreemptedTaskMaxRunningTime =
              DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
          }
        }
        if (key.equals("preemptionRounds")) {
          newPreemptionRounds = json.getInt(key);
          if (newPreemptionRounds < 0) {
            LOG.error("Illegal preemptedTaskMaxRunningTime:" +
                      newPreemptionRounds);
            newPreemptionRounds = DEFAULT_PREEMPTION_ROUNDS;
          }
        }
        if (key.equals("scheduleFromNodeToSession")) {
          newScheduleFromNodeToSession = json.getBoolean(key);
        }
        if (key.equals(REDIRECT_TAG_NAME)) {
          JSONArray jsonPoolInfoToRedirect = json.getJSONArray(key);
          loadPoolInfoToRedirect(jsonPoolInfoToRedirect, 
                                 newPoolInfoToRedirect);
        }
      }
    }
    synchronized (this) {
      this.typeToNodeWait = newTypeToNodeWait;
      this.typeToRackWait = newTypeToRackWait;
      this.defaultPoolComparator = newDefaultPoolComparator;
      this.shareStarvingRatio = newShareStarvingRatio;
      this.minPreemptPeriod = newMinPreemptPeriod;
      this.grantsPerIteration = newGrantsPerIteration;
      this.starvingTimeForMinimum = newStarvingTimeForMinimum;
      this.starvingTimeForShare = newStarvingTimeForShare;
      this.preemptedTaskMaxRunningTime = newPreemptedTaskMaxRunningTime;
      this.preemptionRounds = newPreemptionRounds;
      this.scheduleFromNodeToSession = newScheduleFromNodeToSession;
      this.poolInfoToRedirect = newPoolInfoToRedirect;
    }
  }
  
  /**
   * Reload the general configuration and update all in-memory values. Should
   * be invoked under synchronization.
   *
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  private void reloadConfig() throws
      IOException, SAXException, ParserConfigurationException, JSONException {
    // Loading corona configuration as JSON.
    if (configFileName != null && configFileName.endsWith(
                                  Configuration.MATERIALIZEDJSON)) {
      reloadJsonConfig();
      return;
    }
    // Loading corona configuration as XML.  XML configurations are
    // deprecated.  We intend to remove all XML configurations and 
    // transition entirely into JSON.
    Map<ResourceType, Long> newTypeToNodeWait;
    Map<ResourceType, Long> newTypeToRackWait;
    ScheduleComparator newDefaultPoolComparator = DEFAULT_POOL_COMPARATOR;
    double newShareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    long newMinPreemptPeriod = DEFAULT_MIN_PREEMPT_PERIOD;
    int newGrantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
    long newStarvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    long newStarvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    long newPreemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
    int newPreemptionRounds = DEFAULT_PREEMPTION_ROUNDS;
    boolean newScheduleFromNodeToSession = DEFAULT_SCHEDULE_FROM_NODE_TO_SESSION;
    newTypeToNodeWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    newTypeToRackWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    Map<PoolInfo, PoolInfo> newPoolInfoToRedirect =
        new HashMap<PoolInfo, PoolInfo>();

    for (ResourceType type : TYPES) {
      newTypeToNodeWait.put(type, 0L);
      newTypeToRackWait.put(type, 0L);
    }

    Element root = getRootElement(configFileName);
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); ++i) {
      Node node = elements.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element element = (Element) node;
      for (ResourceType type : TYPES) {
        if (matched(element, "nodeLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          newTypeToNodeWait.put(type, val);
        }
        if (matched(element, "rackLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          newTypeToRackWait.put(type, val);
        }
      }
      if (matched(element, "defaultSchedulingMode")) {
        newDefaultPoolComparator =
            ScheduleComparator.valueOf(getText(element));
      }
      if (matched(element, "shareStarvingRatio")) {
        newShareStarvingRatio = Double.parseDouble(getText(element));
        if (newShareStarvingRatio < 0 || newShareStarvingRatio > 1.0) {
          LOG.error("Illegal shareStarvingRatio:" + newShareStarvingRatio);
          newShareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
        }
      }
      if (matched(element, "grantsPerIteration")) {
        newGrantsPerIteration = Integer.parseInt(getText(element));
        if (newMinPreemptPeriod < 0) {
          LOG.error("Illegal grantsPerIteration: " + newGrantsPerIteration);
          newGrantsPerIteration = DEFAULT_GRANTS_PER_ITERATION;
        }
      }
      if (matched(element, "minPreemptPeriod")) {
        newMinPreemptPeriod = Long.parseLong(getText(element));
        if (newMinPreemptPeriod < 0) {
          LOG.error("Illegal minPreemptPeriod: " + newMinPreemptPeriod);
          newMinPreemptPeriod = DEFAULT_MIN_PREEMPT_PERIOD;
        }
      }
      if (matched(element, "starvingTimeForShare")) {
        newStarvingTimeForShare = Long.parseLong(getText(element));
        if (newStarvingTimeForShare < 0) {
          LOG.error("Illegal starvingTimeForShare:" + newStarvingTimeForShare);
          newStarvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
        }
      }
      if (matched(element, "starvingTimeForMinimum")) {
        newStarvingTimeForMinimum = Long.parseLong(getText(element));
        if (newStarvingTimeForMinimum < 0) {
          LOG.error("Illegal starvingTimeForMinimum:" +
              newStarvingTimeForMinimum);
          newStarvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
        }
      }
      if (matched(element, "preemptedTaskMaxRunningTime")) {
        newPreemptedTaskMaxRunningTime = Long.parseLong(getText(element));
        if (newPreemptedTaskMaxRunningTime < 0) {
          LOG.error("Illegal preemptedTaskMaxRunningTime:" +
              newPreemptedTaskMaxRunningTime);
          newPreemptedTaskMaxRunningTime =
            DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
        }
      }
      if (matched(element, "preemptionRounds")) {
        newPreemptionRounds = Integer.parseInt(getText(element));
        if (newPreemptionRounds < 0) {
          LOG.error("Illegal preemptedTaskMaxRunningTime:" +
              newPreemptionRounds);
          newPreemptionRounds = DEFAULT_PREEMPTION_ROUNDS;
        }
      }
      if (matched(element, "scheduleFromNodeToSession")) {
        newScheduleFromNodeToSession = Boolean.parseBoolean(getText(element));
      }
      if (matched(element, REDIRECT_TAG_NAME)) {
        loadPoolInfoToRedirect(
            element.getAttribute(SOURCE_ATTRIBUTE),
            element.getAttribute(DESTINATION_ATTRIBUTE),
            newPoolInfoToRedirect);
      }
    }
    synchronized (this) {
      this.typeToNodeWait = newTypeToNodeWait;
      this.typeToRackWait = newTypeToRackWait;
      this.defaultPoolComparator = newDefaultPoolComparator;
      this.shareStarvingRatio = newShareStarvingRatio;
      this.minPreemptPeriod = newMinPreemptPeriod;
      this.grantsPerIteration = newGrantsPerIteration;
      this.starvingTimeForMinimum = newStarvingTimeForMinimum;
      this.starvingTimeForShare = newStarvingTimeForShare;
      this.preemptedTaskMaxRunningTime = newPreemptedTaskMaxRunningTime;
      this.preemptionRounds = newPreemptionRounds;
      this.scheduleFromNodeToSession = newScheduleFromNodeToSession;
      this.poolInfoToRedirect = newPoolInfoToRedirect;
    }
  }

  /**
   * Reload the pools config and update all in-memory values
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  private void reloadPoolsConfig()
      throws IOException, SAXException, ParserConfigurationException {
    if (poolsConfigFileName == null) {
      return;
    }
    Set<String> newPoolGroupNameSet = new HashSet<String>();
    Set<PoolInfo> newPoolInfoSet = new HashSet<PoolInfo>();
    TypePoolGroupNameMap<Integer> newTypePoolNameGroupToMax =
        new TypePoolGroupNameMap<Integer>();
    TypePoolGroupNameMap<Integer> newTypePoolNameGroupToMin =
        new TypePoolGroupNameMap<Integer>();
    TypePoolInfoMap<Integer> newTypePoolInfoToMax =
        new TypePoolInfoMap<Integer>();
    TypePoolInfoMap<Integer> newTypePoolInfoToMin =
        new TypePoolInfoMap<Integer>();
    Map<PoolInfo, ScheduleComparator> newPoolInfoToComparator =
        new HashMap<PoolInfo, ScheduleComparator>();
    Map<PoolInfo, Double> newPoolInfoToWeight =
        new HashMap<PoolInfo, Double>();
    Map<PoolInfo, Integer> newPoolInfoToPriority =
        new HashMap<PoolInfo, Integer>();
    Map<PoolInfo, String> newPoolInfoToWhitelist = 
        new HashMap<PoolInfo, String>();

    Map<PoolInfo, PoolInfo> newJobExceedsLimitPoolRedirect = new HashMap<PoolInfo, PoolInfo>();
    Map<PoolInfo, Long> newPoolJobSizeLimit = new HashMap<PoolInfo, Long>();

    Element root = getRootElement(poolsConfigFileName);
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); ++i) {
      Node node = elements.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element element = (Element) node;
      if (matched(element, GROUP_TAG_NAME)) {
        String groupName = element.getAttribute(NAME_ATTRIBUTE);
        if (!newPoolGroupNameSet.add(groupName)) {
          LOG.debug("Already added group " + groupName);
        }
        NodeList groupFields = element.getChildNodes();
        for (int j = 0; j < groupFields.getLength(); ++j) {
          Node groupNode = groupFields.item(j);
          if (!(groupNode instanceof Element)) {
            continue;
          }
          Element field = (Element) groupNode;
          for (ResourceType type : TYPES) {
            if (matched(field, MIN_TAG_NAME_PREFIX + type)) {
              int val = Integer.parseInt(getText(field));
              newTypePoolNameGroupToMin.put(type, groupName, val);
            }
            if (matched(field, MAX_TAG_NAME_PREFIX + type)) {
              int val = Integer.parseInt(getText(field));
              newTypePoolNameGroupToMax.put(type, groupName, val);
            }
          }
          if (matched(field,REDIRECT_JOB_WITH_LIMIT)){
            loadPoolInfoToRedirect(groupName,
                                  field.getAttribute(SOURCE_ATTRIBUTE),
                                  field.getAttribute(DESTINATION_ATTRIBUTE),
                                  field.getAttribute(JOB_INPUT_SIZE_LIMIT_ATTRIBUTE),
                                  newJobExceedsLimitPoolRedirect,
                                  newPoolJobSizeLimit);
          }
          if (matched(field, POOL_TAG_NAME)) {
            PoolInfo poolInfo = new PoolInfo(groupName,
                                             field.getAttribute("name"));
            if (!newPoolInfoSet.add(poolInfo)) {
              LOG.warn("Already added pool info " + poolInfo);
            }
            NodeList poolFields = field.getChildNodes();
            for (int k = 0; k < poolFields.getLength(); ++k) {
              Node poolNode = poolFields.item(k);
              if (!(poolNode instanceof Element)) {
                continue;
              }
              Element poolField = (Element) poolNode;
              for (ResourceType type : TYPES) {
                if (matched(poolField, MIN_TAG_NAME_PREFIX + type)) {
                  int val = Integer.parseInt(getText(poolField));
                  newTypePoolInfoToMin.put(type, poolInfo, val);
                }
                if (matched(poolField, MAX_TAG_NAME_PREFIX + type)) {
                  int val = Integer.parseInt(getText(poolField));
                  newTypePoolInfoToMax.put(type, poolInfo, val);
                }
              }
              if (matched(poolField, PREEMPTABILITY_MODE_TAG_NAME)) {
                boolean val = Boolean.parseBoolean(getText(poolField));
                if (!val) {
                  nonPreemptablePools.add(poolInfo);
                }
              }
              if (matched(poolField, REQUEST_MAX_MODE_TAG_NAME)) {
                boolean val = Boolean.parseBoolean(getText(poolField));
                if (val) {
                  requestMaxPools.add(poolInfo);
                }
              }
              if (matched(poolField, SCHEDULING_MODE_TAG_NAME)) {
                ScheduleComparator val =
                    ScheduleComparator.valueOf(getText(poolField));
                newPoolInfoToComparator.put(poolInfo, val);
              }
              if (matched(poolField, WEIGHT_TAG_NAME)) {
                double val = Double.parseDouble(getText(poolField));
                newPoolInfoToWeight.put(poolInfo, val);
              }
              if (matched(poolField, PRIORITY_TAG_NAME)) {
                int val = Integer.parseInt(getText(poolField));
                newPoolInfoToPriority.put(poolInfo, val);
              }
              if (matched(poolField, WHITELIST_TAG_NAME)) {
                newPoolInfoToWhitelist.put(poolInfo, getText(poolField));
              }
            }
          }
        }
      }
    }
    synchronized (this) {
      this.poolGroupNameSet = newPoolGroupNameSet;
      this.poolInfoSet = Collections.unmodifiableSet(newPoolInfoSet);
      this.typePoolGroupNameToMax = newTypePoolNameGroupToMax;
      this.typePoolGroupNameToMin = newTypePoolNameGroupToMin;
      this.typePoolInfoToMax = newTypePoolInfoToMax;
      this.typePoolInfoToMin = newTypePoolInfoToMin;
      this.poolInfoToComparator = newPoolInfoToComparator;
      this.poolInfoToWeight = newPoolInfoToWeight;
      this.poolInfoToPriority = newPoolInfoToPriority;
      this.poolInfoToWhitelist = newPoolInfoToWhitelist;
      this.jobExceedsLimitPoolRedirect = newJobExceedsLimitPoolRedirect;
      this.poolJobSizeLimit = newPoolJobSizeLimit;
    }
  }

  /**
   * Reload all the configuration files if the config changed and
   * set the last successful reload time.  Synchronized due to potential
   * conflict from a fetch pools config http request.
   *
   * @return true if the config was reloaded, false otherwise
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   * @param init true when the config manager is being initialized.
   *             false on reloads
   */
  public synchronized boolean reloadAllConfig(boolean init)
      throws IOException, SAXException, ParserConfigurationException, JSONException {
    if (!isConfigChanged(init)) {
      return false;
    }
    reloadConfig();
    reloadPoolsConfig();
    this.lastSuccessfulReload = ClusterManager.clock.getTime();
    return true;
  }

  /**
   * Check if the config files have changed since they were last read
   * @return true if the modification time of the file is greater
   * than that of the last successful reload, false otherwise
   * @param init true when the config manager is being initialized.
   *             false on reloads
   */
  private boolean isConfigChanged(boolean init)
      throws IOException {
    if (init &&
        (configFileName == null ||
            (poolsConfigFileName == null &&
                conf.onlyAllowConfiguredPools()))) {
      throw new IOException("ClusterManager needs a config and a " +
          "pools file to start");
    }
    if (configFileName == null && poolsConfigFileName == null) {
      return false;
    }

    boolean configChanged = false;

    if (configFileName != null) {
      File file = new File(configFileName);
      configChanged |= (file.lastModified() == 0 ||
        file.lastModified() > lastSuccessfulReload);
    }


    if (poolsConfigFileName != null) {
      File file = new File(poolsConfigFileName);
      configChanged |= (file.lastModified() == 0 ||
          file.lastModified() > lastSuccessfulReload);
    }

    return configChanged;
  }

  /**
   * Get the root element of the XML document
   * @return the root element of the XML document
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  private Element getRootElement(String fileName) throws
      IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new File(fileName));
    Element root = doc.getDocumentElement();
    if (!matched(root, CONFIGURATION_TAG_NAME)) {
      throw new IOException("Bad " + fileName);
    }
    return root;
  }

  /**
   * Check if the element name matches the tagName provided
   * @param element the xml element
   * @param tagName the name to check against
   * @return true if the name of the element matches tagName, false otherwise
   */
  private static boolean matched(Element element, String tagName) {
    return tagName.equals(element.getTagName());
  }

  /**
   * Get the text inside of the Xml element
   * @param element xml element
   * @return the text inside of the xml element
   */
  private static String getText(Element element) {
    if (element.getFirstChild() == null) {
      return "";
    }
    return ((Text) element.getFirstChild()).getData().trim();
  }

}

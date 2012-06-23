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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
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
  /** Weight xml tag name */
  public static final String WEIGHT_TAG_NAME = "weight";
  /** Min xml tag name prefix */
  public static final String MIN_TAG_NAME_PREFIX = "min";
  /** Max xml tag name prefix */
  public static final String MAX_TAG_NAME_PREFIX = "max";
  /** Name xml attribute */
  public static final String NAME_ATTRIBUTE = "name";
  /** Source xml attribute (for redirect) */
  public static final String SOURCE_ATTRIBUTE = "source";
  /** Destination xml attribute (for redirect) */
  public static final String DESTINATION_ATTRIBUTE = "destination";

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
  /** The comparator to use by default */
  private static final ScheduleComparator DEFAULT_COMPARATOR =
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
  /** The default comparator for the schedulables within the pool */
  private ScheduleComparator defaultComparator;
  /** The ratio of the share to consider starvation */
  private double shareStarvingRatio;
  /** The allowed starvation time for the share */
  private long starvingTimeForShare;
  /** The minimum period between preemptions. */
  private long minPreemptPeriod;
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

    defaultComparator = DEFAULT_COMPARATOR;
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
      reloadAllConfig();
    } catch (IOException e) {
      LOG.error("Failed to load " + configFileName, e);
    } catch (SAXException e) {
      LOG.error("Failed to load " + configFileName, e);
    } catch (ParserConfigurationException e) {
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
    if (configFileName == null) {
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
  public synchronized ScheduleComparator getComparator(PoolInfo poolInfo) {
    ScheduleComparator comparator =
        (poolInfoToComparator == null) ? null :
            poolInfoToComparator.get(poolInfo);
    return comparator == null ? defaultComparator : comparator;
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
            reloadAllConfig();
          } catch (IOException e) {
            LOG.error("Failed to load " + configFileName, e);
          } catch (SAXException e) {
            LOG.error("Failed to load " + configFileName, e);
          } catch (ParserConfigurationException e) {
            LOG.error("Failed to load " + configFileName, e);
          }
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
   * Reload the general configuration and update all in-memory values. Should
   * be invoked under synchronization.
   *
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  private void reloadConfig() throws
      IOException, SAXException, ParserConfigurationException {
    Map<ResourceType, Long> newTypeToNodeWait;
    Map<ResourceType, Long> newTypeToRackWait;
    ScheduleComparator newDefaultComparator = DEFAULT_COMPARATOR;
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
        newDefaultComparator = ScheduleComparator.valueOf(getText(element));
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
        PoolInfo source = PoolInfo.createPoolInfo(
            element.getAttribute(SOURCE_ATTRIBUTE));
        PoolInfo destination = PoolInfo.createPoolInfo(
            element.getAttribute(DESTINATION_ATTRIBUTE));
        if (source == null || destination == null) {
          LOG.error("Illegal redirect source " + source + " or destination " +
              destination);
        } else {
          newPoolInfoToRedirect.put(source, destination);
        }
      }
    }
    synchronized (this) {
      this.typeToNodeWait = newTypeToNodeWait;
      this.typeToRackWait = newTypeToRackWait;
      this.defaultComparator = newDefaultComparator;
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
              if (matched(poolField, SCHEDULING_MODE_TAG_NAME)) {
                ScheduleComparator val =
                    ScheduleComparator.valueOf(getText(poolField));
                newPoolInfoToComparator.put(poolInfo, val);
              }
              if (matched(poolField, WEIGHT_TAG_NAME)) {
                double val = Double.parseDouble(getText(poolField));
                newPoolInfoToWeight.put(poolInfo, val);
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
   */
  public synchronized boolean reloadAllConfig()
      throws IOException, SAXException, ParserConfigurationException {
    if (!isConfigChanged()) {
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
   */
  private boolean isConfigChanged() {
    if (configFileName == null && poolsConfigFileName == null) {
      return false;
    }

    File file = new File(configFileName);
    boolean configChanged = (file.lastModified() == 0 ||
        file.lastModified() > lastSuccessfulReload);


    file = new File(poolsConfigFileName);
    boolean poolsConfigChanged = (file.lastModified() == 0 ||
        file.lastModified() > lastSuccessfulReload);

    return configChanged || poolsConfigChanged;
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
    return ((Text) element.getFirstChild()).getData().trim();
  }

}

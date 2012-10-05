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

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;


/**
 * Reloads corona scheduling parameters periodically. Needs to be thread safe
 *
 * The following is a corona.xml example:
 *
 * <?xml version="1.0"?>
 * <configuration>
 *   <defaultSchedulingMode>FAIR</defaultSchedulingMode>
 *   <nodeLocalityWaitM>0</nodeLocalityWaitM>
 *   <rackLocalityWaitM>5000</rackLocalityWaitM>
 *   <preemptedTaskMaxRunningTime>60000</preemptedTaskMaxRunningTime>
 *   <shareStarvingRatio>0.9</shareStarvingRatio>
 *   <starvingTimeForShare>60000</starvingTimeForShare>
 *   <starvingTimeForMinimum>30000</starvingTimeForMinimum>
 *   <pool name="poolA">
 *     <minM>100</minM>
 *     <minR>100</minR>
 *     <maxM>200</maxM>
 *     <maxR>200</maxR>
 *     <weight>2.0</weight>
 *     <schedulingMode>FIFO</schedulingMode>
 *   <pool name="poolB">
 *     <maxM>200</maxM>
 *     <maxR>200</maxR>
 *     <weight>3.0</weight>
 * </configuration>
 *
 * Note that the type string "M" and "R" must be defined in {@link CoronaConf}
 */
public class ConfigManager {

  /** Logger */
  public static final Log LOG = LogFactory.getLog(ConfigManager.class);
  /** The interval to check for the updates in the config file */
  private static final long CONFIG_RELOAD_PERIOD = 3 * 60 * 1000L; // 3 minutes

  /** The default max running time to consider for preemption */
  private static final long DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME =
    5 * 60 * 1000L;
  /** The default number of attempts to preempt */
  private static final int DEFAULT_PREEMPTION_ROUNDS = 10;
  /** The default ratio to consider starvation */
  private static final double DEFAULT_SHARE_STARVING_RATIO = 0.7;
  /** The default allowed starvation time for a share */
  private static final long DEFAULT_STARVING_TIME_FOR_SHARE = 5 * 60 * 1000L;
  /** The default allowed starvation time for a minimum allocation */
  private static final long DEFAULT_STARVING_TIME_FOR_MINIMUM = 3 * 60 * 1000L;
  /** The comparator to use by default */
  private static final ScheduleComparator DEFAULT_COMPARATOR = 
    ScheduleComparator.FIFO;

  /** The types this configuration is initialized for */
  private final Collection<ResourceType> TYPES;
  /** The classloader to use when looking for resource in the classpath */
  private final ClassLoader classLoader;
  /** Set of configured pool names */
  private Set<String> poolNames;
  /** The Map of max allocations for a given type and a given pool */
  private Map<ResourceType, Map<String, Integer>> typeToPoolToMax;
  /** The Map of min allocations for a given type and a given pool */
  private Map<ResourceType, Map<String, Integer>> typeToPoolToMin;
  /** The Map of node locality wait times for different resource types */
  private Map<ResourceType, Long> typeToNodeWait;
  /** The Map of rack locality wait times for different resource types */
  private Map<ResourceType, Long> typeToRackWait;
  /** The Map of comparators configurations for the pools */
  private Map<String, ScheduleComparator> poolToComparator;
  /** The Map of the weights configuration for the pools */
  private Map<String, Double> poolToWeight;
  /** The default comparator for the schedulables within the pool */
  private ScheduleComparator defaultComparator;
  /** The ratio of the share to consider starvation */
  private double shareStarvingRatio;
  /** The allowed starvation time for the share */
  private long starvingTimeForShare;
  /** The allowed starvation time for the min allocation */
  private long starvingTimeForMinimum;
  /** The max time of the task to consider for preemption */
  private long preemptedTaskMaxRunningTime;
  /** The number of times to iterate over pools trying to preempt */
  private int preemptionRounds;
  /** The flag for the reload thread */
  private volatile boolean running = true;
  /** The thread that monitors and reloads the config file */
  private ReloadThread reloadThread;
  /** The last timestamp when the config was successfully loaded */
  private long lastSuccessfulReload = -1L;
  /** The name of the config file to use */
  private String configFileName = null;

  /**
   * Construct the config manager for a given list of resource types
   * @param types the types to initialize the configuration for
   */
  public ConfigManager(Collection<ResourceType> types) {
    this(types, null);
  }

  /**
   * The main constructor for the config manager given the types and the name
   * of the config file to use
   * @param types the types to initialize the configuration for
   * @param configFileName the name of the configuration file
   */
  public ConfigManager(Collection<ResourceType> types, String configFileName) {
    this.TYPES = types;
    typeToPoolToMax =
        new EnumMap<ResourceType, Map<String, Integer>>(ResourceType.class);
    typeToPoolToMin =
        new EnumMap<ResourceType, Map<String, Integer>>(ResourceType.class);
    typeToNodeWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    typeToRackWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    poolToComparator = new HashMap<String, ScheduleComparator>();
    poolToWeight = new HashMap<String, Double>();

    defaultComparator = DEFAULT_COMPARATOR;
    shareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    starvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    starvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    preemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
    preemptionRounds = DEFAULT_PREEMPTION_ROUNDS;

    reloadThread = new ReloadThread();
    reloadThread.setName("Config reload thread");
    reloadThread.setDaemon(true);

    for (ResourceType type : TYPES) {
      typeToPoolToMax.put(type, new HashMap<String, Integer>());
      typeToPoolToMin.put(type, new HashMap<String, Integer>());
      typeToNodeWait.put(type, 0L);
      typeToRackWait.put(type, 0L);
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = ConfigManager.class.getClassLoader();
    }
    classLoader = cl;

    this.configFileName = configFileName;

    try {
      findConfigFile();
      reloadConfig();
    } catch (Exception e) {
      LOG.error("Failed to load " + configFileName, e);
    }
  }

  /**
   * Used for unit tests
   */
  public ConfigManager() {
    TYPES = null;
    classLoader = ConfigManager.class.getClassLoader();
  }

  /**
   * Find the configuration file in the classpath
   */
  private void findConfigFile() {
    if (configFileName != null) {
      return;
    }

    URL u = classLoader.getResource(CoronaConf.POOL_CONFIG_FILE);
    LOG.info("Found config file: " + (u != null ? u.getPath() : ""));
    configFileName = (u != null) ? u.getPath() : null;
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
   * Is this a configured pool
   * @param poolName Name of the pool to check
   * @return True if configured, false otherwise
   */
  public synchronized boolean isConfiguredPool(String poolName) {
    if (poolNames == null) {
      return false;
    }
    return poolNames.contains(poolName);
  }

  /**
   * Get the configured maximum allocation for a given {@link ResourceType}
   * in a given pool
   * @param name the name of the pool
   * @param type the type of the resource
   * @return the maximum allocation for the resource in a pool
   */
  public synchronized int getMaximum(String name, ResourceType type) {
    Map<String, Integer> poolToMax = typeToPoolToMax.get(type);
    if (poolToMax == null) {
      throw new IllegalArgumentException("Unknown type:" + type);
    }
    Integer max = poolToMax.get(name);
    return max == null ? Integer.MAX_VALUE : max;
  }

  /**
   * Get the configured minimum allocation for a given {@link ResourceType}
   * in a given pool
   * @param name the name of the pool
   * @param type the type of the resource
   * @return the minimum allocation of the resources
   */
  public synchronized int getMinimum(String name, ResourceType type) {
    Map<String, Integer> poolToMin = typeToPoolToMin.get(type);
    if (poolToMin == null) {
      throw new IllegalArgumentException("Unknown type:" + type);
    }
    Integer min = poolToMin.get(name);
    return min == null ? 0 : min;
  }

  /**
   * Get the weight for the pool
   * @param name the name of the pool
   * @return the weight for the pool
   */
  public synchronized double getWeight(String name) {
    Double weight = poolToWeight.get(name);
    return weight == null ? 1.0 : weight;
  }

  /**
   * Get the comparator to use for scheduling sessions within a pool
   * @param name the name of the pool
   * @return the scheduling comparator to use for the pool
   */
  public synchronized ScheduleComparator getComparator(String name) {
    ScheduleComparator comparator = poolToComparator.get(name);
    return comparator == null ? defaultComparator : comparator;
  }

  public synchronized long getPreemptedTaskMaxRunningTime() {
    return preemptedTaskMaxRunningTime;
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

  /**
   * The thread that monitors the config file and reloads the configuration
   * when the file is updated
   */
  private class ReloadThread extends Thread {
    @Override
    public void run() {
      long lastReloadAttempt = -1L;
      while (running) {
        long now = ClusterManager.clock.getTime();
        if (lastReloadAttempt - now > CONFIG_RELOAD_PERIOD) {
          lastReloadAttempt = now;

          findConfigFile();

          try {
            reloadConfig();
          } catch (Throwable e) {
            LOG.error("Failed to reload " + configFileName, e);
          }
        }
        try {
          Thread.sleep(CONFIG_RELOAD_PERIOD / 10);
        } catch (InterruptedException e) {
        }
      }
    }

  }

  /**
   * Reload the configuration and update all in-memory values
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  void reloadConfig() throws
      IOException, SAXException, ParserConfigurationException {

    if (!isConfigChanged()) {
      return;
    }
    Set<String> newPoolNames = new HashSet<String>();
    Map<ResourceType, Map<String, Integer>> newTypeToPoolToMax;
    Map<ResourceType, Map<String, Integer>> newTypeToPoolToMin;
    Map<ResourceType, Long> newTypeToNodeWait;
    Map<ResourceType, Long> newTypeToRackWait;
    Map<String, ScheduleComparator> newPoolToComparator;
    Map<String, Double> newPoolToWeight;
    ScheduleComparator newDefaultComparator = DEFAULT_COMPARATOR;
    double newShareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    long newStarvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    long newStarvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    long newPreemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
    int newPreemptionRounds = DEFAULT_PREEMPTION_ROUNDS;

    newTypeToPoolToMax =
        new EnumMap<ResourceType, Map<String, Integer>>(ResourceType.class);
    newTypeToPoolToMin =
        new EnumMap<ResourceType, Map<String, Integer>>(ResourceType.class);
    newTypeToNodeWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    newTypeToRackWait = new EnumMap<ResourceType, Long>(ResourceType.class);
    newPoolToComparator = new HashMap<String, ScheduleComparator>();
    newPoolToWeight = new HashMap<String, Double>();

    for (ResourceType type : TYPES) {
      newTypeToPoolToMax.put(type, new HashMap<String, Integer>());
      newTypeToPoolToMin.put(type, new HashMap<String, Integer>());
      newTypeToNodeWait.put(type, 0L);
      newTypeToRackWait.put(type, 0L);
    }

    Element root = getRootElement();
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); ++i) {
      Node node = elements.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element element = (Element) node;
      for (ResourceType type : TYPES) {
        // Note that the type string is separately defined in CoronaConf
        if (matched(element, "nodeLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          newTypeToNodeWait.put(type, val);
        }
        if (matched(element, "rackLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          newTypeToRackWait.put(type, val);
        }
      }
      if (matched(element, "pool")) {
        String poolName = element.getAttribute("name");
        if (!newPoolNames.add(poolName)) {
          LOG.warn("Already added " + poolName);
        }
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); ++j) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }
          Element field = (Element) fieldNode;
          for (ResourceType type : TYPES) {
            // Note that the type string is separately defined in CoronaConf
            if (matched(field, "min" + type)) {
              int val = Integer.parseInt(getText(field));
              Map<String, Integer> poolToMin = newTypeToPoolToMin.get(type);
              poolToMin.put(poolName, val);
            }
            if (matched(field, "max" + type)) {
              int val = Integer.parseInt(getText(field));
              Map<String, Integer> poolToMax = newTypeToPoolToMax.get(type);
              poolToMax.put(poolName, val);
            }
          }
          if (matched(field, "schedulingMode")) {
            ScheduleComparator val = ScheduleComparator.valueOf(getText(field));
            newPoolToComparator.put(poolName, val);
          }
          if (matched(field, "weight")) {
            double val = Double.parseDouble(getText(field));
            newPoolToWeight.put(poolName, val);
          }
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
    }
    synchronized (this) {
      this.poolNames = newPoolNames;
      this.typeToPoolToMax = newTypeToPoolToMax;
      this.typeToPoolToMin = newTypeToPoolToMin;
      this.typeToNodeWait = newTypeToNodeWait;
      this.typeToRackWait = newTypeToRackWait;
      this.poolToComparator = newPoolToComparator;
      this.poolToWeight = newPoolToWeight;
      this.defaultComparator = newDefaultComparator;
      this.lastSuccessfulReload = ClusterManager.clock.getTime();
      this.shareStarvingRatio = newShareStarvingRatio;
      this.starvingTimeForMinimum = newStarvingTimeForMinimum;
      this.starvingTimeForShare = newStarvingTimeForShare;
      this.preemptedTaskMaxRunningTime = newPreemptedTaskMaxRunningTime;
      this.preemptionRounds = newPreemptionRounds;
    }
  }

  /**
   * Check if the config file has changed since it was last read
   * @return true if the modification time of the file is greater
   * than that of the last successful reload, false otherwise
   */
  private boolean isConfigChanged() {
    if (configFileName == null) {
      return false;
    }

    File file = new File(configFileName);
    return file.lastModified() == 0 ||
           file.lastModified() > lastSuccessfulReload;
  }

  /**
   * Get the root element of the XML document
   * @return the root element of the XML document
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  private Element getRootElement() throws
      IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new File(configFileName));
    Element root = doc.getDocumentElement();
    if (!matched(root, "configuration")) {
      throw new IOException("Bad " + configFileName);
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

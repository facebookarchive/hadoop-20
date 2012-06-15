package org.apache.hadoop.corona;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

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

  static final public Log LOG = LogFactory.getLog(ConfigManager.class);
  static final private long CONFIG_RELOAD_PERIOD = 3 * 60 * 1000L; // 3 minutes

  final private Collection<String> TYPES;
  final private ScheduleComparator DEFAULT_COMPARATOR = ScheduleComparator.FIFO;
  final private long DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME = 5 * 60 * 1000L;
  final private double DEFAULT_SHARE_STARVING_RATIO = 0.7;
  final private long DEFAULT_STARVING_TIME_FOR_SHARE = 5 * 60 * 1000L;
  final private long DEFAULT_STARVING_TIME_FOR_MINIMUM = 3 * 60 * 1000L;
  final private ClassLoader classLoader;


  private Map<String, Map<String, Integer>> typeToPoolToMax;
  private Map<String, Map<String, Integer>> typeToPoolToMin;
  private Map<String, Long> typeToNodeWait;
  private Map<String, Long> typeToRackWait;
  private Map<String, ScheduleComparator> poolToComparator;
  private Map<String, Double> poolToWeight;
  private ScheduleComparator defaultComparator;
  private double shareStarvingRatio;
  private long starvingTimeForShare;
  private long starvingTimeForMinimum;
  private long preemptedTaskMaxRunningTime;
  private volatile boolean running;
  private ReloadThread reloadThread;
  private long lastSuccessfulReload = -1L;
  private String configFileName = null;

  private void findConfigFile() {
    if (configFileName != null)
      return;

    URL u = classLoader.getResource(CoronaConf.POOL_CONFIG_FILE);
    LOG.info ("Found config file: " + (u != null ? u.getPath() : ""));
    configFileName = (u != null) ? u.getPath() : null;
  }

  public ConfigManager(Collection<String> types) {
    this(types, null);
  }

  public ConfigManager(Collection<String> types, String configFileName) {
    this.TYPES = types;
    typeToPoolToMax = new IdentityHashMap<String, Map<String, Integer>>();
    typeToPoolToMin = new IdentityHashMap<String, Map<String, Integer>>();
    typeToNodeWait = new IdentityHashMap<String, Long>();
    typeToRackWait = new IdentityHashMap<String, Long>();
    poolToComparator = new HashMap<String, ScheduleComparator>();
    poolToWeight = new HashMap<String, Double>();

    defaultComparator = DEFAULT_COMPARATOR;
    shareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    starvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    starvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    preemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;

    reloadThread = new ReloadThread();
    reloadThread.setName("Config reload thread");
    reloadThread.setDaemon(true);

    for (String type : TYPES) {
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


  public void start() {
    reloadThread.start();
  }

  public void close() {
    running = false;
    reloadThread.interrupt();
  }

  public synchronized int getMaximum(String name, String type) {
    Map<String, Integer> poolToMax = typeToPoolToMax.get(type);
    if (poolToMax == null) {
      throw new IllegalArgumentException("Unknown type:" + type);
    }
    Integer max = poolToMax.get(name);
    return max == null ? Integer.MAX_VALUE : max;
  }

  public synchronized int getMinimum(String name, String type) {
    Map<String, Integer> poolToMin = typeToPoolToMin.get(type);
    if (poolToMin == null) {
      throw new IllegalArgumentException("Unknown type:" + type);
    }
    Integer min = poolToMin.get(name);
    return min == null ? 0 : min;
  }

  public synchronized double getWeight(String name) {
    Double weight = poolToWeight.get(name);
    return weight == null ? 1.0 : weight;
  }

  public synchronized ScheduleComparator getComparator(String name) {
    ScheduleComparator comparator = poolToComparator.get(name);
    return comparator == null ? defaultComparator : comparator;
  }

  public synchronized long getPreemptedTaskMaxRunningTime() {
    return preemptedTaskMaxRunningTime;
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

  public synchronized long getLocalityWait(String type, LocalityLevel level) {
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
          } catch (Exception e) {
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

  void reloadConfig() throws
      IOException, SAXException, ParserConfigurationException {

    if (!isConfigChanged()) {
      return;
    }
    Map<String, Map<String, Integer>> typeToPoolToMax;
    Map<String, Map<String, Integer>> typeToPoolToMin;
    Map<String, Long> typeToNodeWait;
    Map<String, Long> typeToRackWait;
    Map<String, ScheduleComparator> poolToComparator;
    Map<String, Double> poolToWeight;
    ScheduleComparator defaultComparator = DEFAULT_COMPARATOR;
    double shareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
    long starvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
    long starvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
    long preemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;

    typeToPoolToMax = new IdentityHashMap<String, Map<String, Integer>>();
    typeToPoolToMin = new IdentityHashMap<String, Map<String, Integer>>();
    typeToNodeWait = new IdentityHashMap<String, Long>();
    typeToRackWait = new IdentityHashMap<String, Long>();
    poolToComparator = new HashMap<String, ScheduleComparator>();
    poolToWeight = new HashMap<String, Double>();

    for (String type : TYPES) {
      typeToPoolToMax.put(type, new HashMap<String, Integer>());
      typeToPoolToMin.put(type, new HashMap<String, Integer>());
      typeToNodeWait.put(type, 0L);
      typeToRackWait.put(type, 0L);
    }

    Element root = getRootElement();
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); ++i) {
      Node node = elements.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element element = (Element)node;
      for (String type : TYPES) {
        // Note that the type string is separately defined in CoronaConf
        if (matched(element, "nodeLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          typeToNodeWait.put(type, val);
        }
        if (matched(element, "rackLocalityWait" + type)) {
          long val = Long.parseLong(getText(element));
          typeToRackWait.put(type, val);
        }
      }
      if (matched(element, "pool")) {
        String poolName = element.getAttribute("name");
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); ++j) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }
          Element field = (Element) fieldNode;
          for (String type : TYPES) {
            // Note that the type string is separately defined in CoronaConf
            if (matched(field, "min" + type)) {
              int val = Integer.parseInt(getText(field));
              Map<String, Integer> poolToMin = typeToPoolToMin.get(type);
              poolToMin.put(poolName, val);
            }
            if (matched(field, "max" + type)) {
              int val = Integer.parseInt(getText(field));
              Map<String, Integer> poolToMax = typeToPoolToMax.get(type);
              poolToMax.put(poolName, val);
            }
          }
          if (matched(field, "schedulingMode")) {
            ScheduleComparator val = ScheduleComparator.valueOf(getText(field));
            poolToComparator.put(poolName, val);
          }
          if (matched(field, "weight")) {
            double val = Double.parseDouble(getText(field));
            poolToWeight.put(poolName, val);
          }
        }
      }
      if (matched(element, "defaultSchedulingMode")) {
        defaultComparator = ScheduleComparator.valueOf(getText(element));
      }
      if (matched(element, "shareStarvingRatio")) {
        shareStarvingRatio = Double.parseDouble(getText(element));
        if (shareStarvingRatio < 0 || shareStarvingRatio > 1.0) {
          LOG.error("Illegal shareStarvingRatio:" + shareStarvingRatio);
          shareStarvingRatio = DEFAULT_SHARE_STARVING_RATIO;
        }
      }
      if (matched(element, "starvingTimeForShare")) {
        starvingTimeForShare = Long.parseLong(getText(element));
        if (starvingTimeForShare < 0) {
          LOG.error("Illegal starvingTimeForShare:" + starvingTimeForShare);
          starvingTimeForShare = DEFAULT_STARVING_TIME_FOR_SHARE;
        }
      }
      if (matched(element, "starvingTimeForMinimum")) {
        starvingTimeForMinimum = Long.parseLong(getText(element));
        if (starvingTimeForMinimum < 0) {
          LOG.error("Illegal starvingTimeForMinimum:" + starvingTimeForMinimum);
          starvingTimeForMinimum = DEFAULT_STARVING_TIME_FOR_MINIMUM;
        }
      }
      if (matched(element, "preemptedTaskMaxRunningTime")) {
        preemptedTaskMaxRunningTime = Long.parseLong(getText(element));
        if (preemptedTaskMaxRunningTime < 0) {
          LOG.error("Illegal preemptedTaskMaxRunningTime:" +
              preemptedTaskMaxRunningTime);
          preemptedTaskMaxRunningTime = DEFAULT_PREEMPT_TASK_MAX_RUNNING_TIME;
        }
      }
    }
    synchronized (this) {
      this.typeToPoolToMax = typeToPoolToMax;
      this.typeToPoolToMin = typeToPoolToMin;
      this.typeToNodeWait = typeToNodeWait;
      this.typeToRackWait = typeToRackWait;
      this.poolToComparator = poolToComparator;
      this.poolToWeight = poolToWeight;
      this.defaultComparator = defaultComparator;
      this.lastSuccessfulReload = ClusterManager.clock.getTime();
      this.shareStarvingRatio = shareStarvingRatio;
      this.starvingTimeForMinimum = starvingTimeForMinimum;
      this.starvingTimeForShare = starvingTimeForShare;
      this.preemptedTaskMaxRunningTime = preemptedTaskMaxRunningTime;
    }
  }

  private boolean isConfigChanged() {
    if (configFileName == null)
      return false;

    File file = new File(configFileName);
    return file.lastModified() == 0 ||
           file.lastModified() > lastSuccessfulReload;
  }

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

  private static boolean matched(Element element, String tagName) {
    return tagName.equals(element.getTagName());
  }

  private static String getText(Element element) {
    return ((Text)element.getFirstChild()).getData().trim();
  }

}

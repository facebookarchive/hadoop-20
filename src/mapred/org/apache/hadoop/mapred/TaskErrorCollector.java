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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

public class TaskErrorCollector implements Updater {
  
  private static final String ERROR_XML = "error.xml";
  public static final String NUM_WINDOWS_KEY = "mapred.taskerrorcollector.window.number";
  public static final String WINDOW_LENGTH_KEY = "mapred.taskerrorcollector.window.milliseconds";
  public static final String CONFIG_FILE_KEY = "mapred.taskerrorcollector.error.file";
  public static final String COUNTER_GROUP_NAME = "TaskError";
  
  public static final Log LOG = LogFactory.getLog(TaskErrorCollector.class);
  
  private static final String METRICS_KEY_PREFIX = "task_error_";
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry;
  private final Map<String, TaskError> knownErrors;
  private final TaskError UNKNOWN_ERROR = new TaskError("UNKNOWN", "",
      "Task diagnostic info does not match any pattern defined in xml file"); 
  
  // Used by UI
  static final private int WINDOW_LENGTH = 10 * 60 * 1000; // 10 minute
  static final private int NUM_WINDOWS = 3 * 86400 * 1000 / WINDOW_LENGTH; // 3 days
  private long lastWindowIndex = 0;
  private final int windowLength;
  private final int numWindows;
  private final LinkedList<Map<TaskError, Integer>> errorCountsQueue =
    new LinkedList<Map<TaskError, Integer>>();
  private final LinkedList<Long> startTimeQueue = new LinkedList<Long>();
  private final Map<TaskError, Integer> sinceStartErrorCounts;

  // Used by metrics
  private final Map<TaskError, MetricsTimeVaryingLong> errorCountsMetrics =
    new HashMap<TaskError, MetricsTimeVaryingLong>();
  // Cumulative counters
  private final Counters errorCounters = new Counters();

  public TaskErrorCollector(Configuration conf) {
    this(
      conf,
      conf.getInt(WINDOW_LENGTH_KEY, WINDOW_LENGTH),
      conf.getInt(NUM_WINDOWS_KEY, NUM_WINDOWS));
  }

  public TaskErrorCollector(
    Configuration conf, int windowLength, int numWindows) {
    this.windowLength = windowLength;
    this.numWindows = numWindows;

    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "taskerror");
    registry = new MetricsRegistry();

    context.registerUpdater(this);

    URL configURL = null;
    String configFilePath = conf.get(CONFIG_FILE_KEY);
    if (configFilePath == null) {
      // Search the class path if it is not configured
      configURL =
        TaskErrorCollector.class.getClassLoader().getResource(ERROR_XML);
    } else {
      try {
        configURL = new URL("file://" +
          new File(configFilePath).getAbsolutePath());
      } catch (MalformedURLException e) {
        LOG.error("Error in creating config URL", e);
      }
    }

    if (configURL == null) {
      LOG.warn("Could not get error collector configuration. " +
           TaskErrorCollector.class.getSimpleName() +
           " will see every error as UNKNOWN_ERROR.");
      knownErrors = Collections.emptyMap();
    } else {
      LOG.info("Parsing configuration from " + configURL);
      knownErrors = parseConfigFile(configURL);
    }
    createMetrics();
    sinceStartErrorCounts = createErrorCountsMap();
  }

  private void createMetrics() {
    for (TaskError error : knownErrors.values()) {
      LOG.info("metricsKey:" + error.metricsKey);
      errorCountsMetrics.put(error, new MetricsTimeVaryingLong(
          error.metricsKey, registry, error.description));
    }
    errorCountsMetrics.put(UNKNOWN_ERROR, new MetricsTimeVaryingLong(
          UNKNOWN_ERROR.metricsKey, registry, UNKNOWN_ERROR.description));
  }

  private Map<TaskError, Integer> createErrorCountsMap() {
    Map<TaskError, Integer> errorCountsMap =
        new LinkedHashMap<TaskError, Integer>();
    Counters.Group grp = errorCounters.getGroup(COUNTER_GROUP_NAME);
    for (TaskError error : knownErrors.values()) {
      errorCountsMap.put(error, 0);
      // Make sure counter is present with value 0.
      grp.getCounterForName(error.name).increment(0);
    }
    errorCountsMap.put(UNKNOWN_ERROR, 0);
    return errorCountsMap;
  }

  public synchronized void collect(TaskInProgress tip, TaskAttemptID taskId,
      long now) {
    List<String> diagnostics = tip.getDiagnosticInfo(taskId);
    if (diagnostics == null || diagnostics.isEmpty()) {
      incErrorCounts(UNKNOWN_ERROR, now);
      return;
    }
    String latestDiagnostic = diagnostics.get(diagnostics.size() - 1);
    latestDiagnostic = latestDiagnostic.replace("\n", " ");
    boolean found = false;
    for (TaskError error : knownErrors.values()) {
      String p = error.pattern.toString();
      if (error.pattern.matcher(latestDiagnostic).matches()) {
        incErrorCounts(error, now);
        found = true;
        break;
      }
    }
    if (!found) {
      LOG.info("Undefined diagnostic info:" + latestDiagnostic);
      incErrorCounts(UNKNOWN_ERROR, now);
    }
  }

  /**
   */
  /**
   * Get recent TaskError counts within the given window
   * @param timeWindow Window size in milliseconds.
   *        Ex: 24 * 60 * 60 * 1000 gives you last day error counts
   * @return Counts for each TaskError
   */
  public synchronized Map<TaskError, Integer> getRecentErrorCounts(long timeWindow) {
    long start = System.currentTimeMillis() - timeWindow;
    Map<TaskError, Integer> errorCounts = createErrorCountsMap();
    Iterator<Map<TaskError, Integer>> errorCountsIter = errorCountsQueue.iterator();
    Iterator<Long> startTimeIter = startTimeQueue.iterator();
    while (errorCountsIter.hasNext() && start < startTimeIter.next()) {
      Map<TaskError, Integer> windowErrorCounts = errorCountsIter.next();
      for (Map.Entry<TaskError, Integer> entry : windowErrorCounts.entrySet()) {
        errorCounts.put(entry.getKey(),
            errorCounts.get(entry.getKey()) + entry.getValue());
      }
    }
    return errorCounts;
  }

  public synchronized Map<TaskError, Integer> getErrorCounts() {
    return Collections.unmodifiableMap(sinceStartErrorCounts);
  }

  private void incErrorCounts(TaskError error, long now) {

    Map<TaskError, Integer> current = getCurrentErrorCounts(now);
    current.put(error, current.get(error) + 1); 

    errorCountsMetrics.get(error).inc();
    Counters.Group grp = errorCounters.getGroup(COUNTER_GROUP_NAME);
    Counters.Counter ctr = grp.getCounterForName(error.name);
    ctr.increment(1);

    sinceStartErrorCounts.put(error, sinceStartErrorCounts.get(error) + 1);

  }

  private Map<TaskError, Integer> getCurrentErrorCounts(long now) {
    long windowIndex = now / windowLength;
    if (windowIndex != lastWindowIndex || errorCountsQueue.isEmpty()) {
      lastWindowIndex = windowIndex;
      errorCountsQueue.addFirst(createErrorCountsMap());
      startTimeQueue.addFirst(windowIndex * windowLength);
      if (errorCountsQueue.size() > numWindows) {
        errorCountsQueue.removeLast();
        startTimeQueue.removeLast();
      }
    }
    return errorCountsQueue.getFirst();
  }

  public Counters getErrorCountsCounters() {
    return errorCounters;
  }

  public class TaskError {
    final String name;
    final Pattern pattern;
    final String metricsKey;
    final String description;
    TaskError(String name, String patternString, String description) {
      this.name = name;
      this.metricsKey = toMetricName(name);
      this.pattern = Pattern.compile(patternString);
      this.description = description;
    }
    private String toMetricName(String name) {
      return METRICS_KEY_PREFIX + name.toLowerCase().replaceAll("\\s+", "_");
    }
    @Override
    public String toString() {
      return "name:" + name + " pattern:" + pattern.toString() +
          " metricsKey:" + metricsKey + " description:" + description;
    }
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }


/**
 * Parse the error.xml file which contains the error 
 * 
 * The following is an example of the error.xml
 * 
 * <?xml version="1.0"?>
 * <configuration>
 *   <error name="Map output lost">
 *     <pattern>Map output lost</pattern>
 *     <description>TaskTracker cannot find requested map output</description>
 *   </error>
 *   <error name="Memory killing">
 *     <pattern>Killing the top memory-consuming tasks.*</pattern>
 *     <description>When TaskTracker has no enough memory, it kills the task with highest memory</description>
 *   </error>
 *   <error name="Memory failing">
 *     <pattern>Failing the top memory-consuming tasks.*</pattern>
 *     <description>When TaskTracker has no enough memory, it check the task with highest memory. If it used more than configured memory, the task fails.</description>
 *   </error>
 *   <error name="Preemption">
 *     <pattern>Killed for preemption.*</pattern>
 *     <description>Task killed because of preemption</description>
 *   </error>
 *   <error name="Killed from JSP">
 *     <pattern>Killed from JSP page.*</pattern>
 *     <description>Someone kill the task from Web UI</description>
 *   </error>
 *   <error name="No space">
 *     <pattern>No space left on device.*</pattern>
 *     <description>Cannot find disk space on the TaskTracker</description>
 *   </error>
 * </configuration>
 * @param configURL
 * @throws IOException 
 * 
 */
  private Map<String, TaskError> parseConfigFile(URL configURL) {
    Map<String, TaskError> knownErrors = new LinkedHashMap<String, TaskError>();
    try {
      Element root = getRootElement(configURL);
      NodeList elements = root.getChildNodes();
      for (int i = 0; i < elements.getLength(); ++i) {
        Node node = elements.item(i);
        if (!(node instanceof Element)) {
          continue;
        }
        Element element = (Element)node;
        if (matched(element, "error")) {
          String name = element.getAttribute("name");
          String pattern = "";
          String description = "";
          NodeList fields = element.getChildNodes();
          for (int j = 0; j < fields.getLength(); ++j) {
            Node fieldNode = fields.item(j);
            if (!(fieldNode instanceof Element)) {
              continue;
            }
            Element field = (Element)fieldNode;
            if (matched(field, "pattern")) {
              pattern = getText(field);
            } else if (matched(field, "description")) {
              description = getText(field);
            }
          }
          TaskError taskError = new TaskError(name, pattern, description);
          LOG.info("Adding TaskError " + taskError);
          knownErrors.put(name, taskError);
        }
      }
    } catch (IOException ie) {
      LOG.error("Error parsing config file " + configURL, ie);
    }
    return knownErrors;
  }

  private Element getRootElement(URL configURL) throws IOException {
    Element root = null;
    try {
      DocumentBuilderFactory docBuilderFactory =
          DocumentBuilderFactory.newInstance();
      docBuilderFactory.setIgnoringComments(true);
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = builder.parse(configURL.openStream());
      root = doc.getDocumentElement();
      if (!matched(root, "configuration")) {
        throw new IOException("Bad task error config at " + configURL);
      }
    } catch (SAXException se) {
      throw new IOException(se);
    } catch (ParserConfigurationException pe) {
      throw new IOException(pe);
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

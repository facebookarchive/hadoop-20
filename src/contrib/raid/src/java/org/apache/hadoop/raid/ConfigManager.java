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

package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.fs.Path;

/**
 * Maintains the configuration xml file that is read into memory.
 */
class ConfigManager {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.raid.ConfigManager");

  /** Time to wait between checks of the config file */
  public static final long RELOAD_INTERVAL = 10 * 1000;

  /** Time to wait between successive runs of all policies */
  public static final long RESCAN_INTERVAL = 3600 * 1000;
  
  public static final long HAR_PARTFILE_SIZE = 4 * 1024 * 1024 * 1024l;

  public static final int DISTRAID_MAX_JOBS = 10;

  public static final int DISTRAID_MAX_FILES = 5000;
  
  public static final String DIRRAID_BLOCK_LIMIT_KEY =
      "hdfs.raid.dir.raid.block.limit";
  
  public static final int DEFAULT_DIRRAID_BLOCK_LIMIT = 5000;

  /**
   * Time to wait after the config file has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long RELOAD_WAIT = 5 * 1000;

  private static final String DEFAULT_CONFIG_FILE = "raid.xml"; 
  
  private Configuration conf;    // Hadoop configuration
  private String configFileName; // Path to config XML file
  
  private long lastReloadAttempt; // Last time we tried to reload the config file
  private long lastSuccessfulReload; // Last time we successfully reloaded config
  private boolean lastReloadAttemptFailed = false;
  private long reloadInterval = RELOAD_INTERVAL;
  private long periodicity; // time between runs of all policies
  private long harPartfileSize;
  private int maxJobsPerPolicy; // Max no. of jobs running simultaneously for
                                // a job.
  private int maxFilesPerJob; // Max no. of files raided by a job.
  private int maxBlocksPerDirRaidJob; // Max no. of blocks raided by a dir-raid job
  // the url of the read reconstruction metrics
  private String readReconstructionMetricsUrl;

  // Reload the configuration
  private boolean doReload;
  private Thread reloadThread;
  private volatile boolean running = false;

  // Collection of all configured policies.
  Collection<PolicyInfo> allPolicies = new ArrayList<PolicyInfo>();

  // For unit test
  ConfigManager() { }

  public ConfigManager(Configuration conf) throws IOException, SAXException,
      RaidConfigurationException, ClassNotFoundException, ParserConfigurationException,
      JSONException {
    this.conf = conf;
    this.configFileName = conf.get("raid.config.file");
    if (this.configFileName == null) {
      this.configFileName = DEFAULT_CONFIG_FILE;
    }
    this.doReload = conf.getBoolean("raid.config.reload", true);
    this.reloadInterval = conf.getLong("raid.config.reload.interval", RELOAD_INTERVAL);
    this.periodicity = conf.getLong("raid.policy.rescan.interval",  RESCAN_INTERVAL);
    this.harPartfileSize = conf.getLong("raid.har.partfile.size", HAR_PARTFILE_SIZE);
    this.maxJobsPerPolicy = conf.getInt("raid.distraid.max.jobs",
                                        DISTRAID_MAX_JOBS);
    this.maxFilesPerJob = conf.getInt("raid.distraid.max.files",
                                      DISTRAID_MAX_FILES);
    this.maxBlocksPerDirRaidJob = conf.getInt(DIRRAID_BLOCK_LIMIT_KEY,
        DEFAULT_DIRRAID_BLOCK_LIMIT);
    this.readReconstructionMetricsUrl = conf.get(
                                "raid.read.reconstruction.metrics.url", "");
    reloadConfigs();
    lastSuccessfulReload = RaidNode.now();
    lastReloadAttempt = RaidNode.now();
    running = true;
  }
  
  /**
   * Reload config file if it hasn't been loaded in a while
   * Returns true if the file was reloaded.
   */
  public synchronized boolean reloadConfigsIfNecessary() {
    long time = RaidNode.now();
    if (time > lastReloadAttempt + reloadInterval) {
      lastReloadAttempt = time;
      try {
        File file = new File(configFileName);
        long lastModified = file.lastModified();
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + RELOAD_WAIT) {
          reloadConfigs();
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
          return true;
        }
      } catch (Exception e) {
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload config file - " +
              "will use existing configuration.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
    return false;
  }
  
  /**
   * Updates the in-memory data structures from the config file. This file is
   * expected to be in the following whitespace-separated format:
   * 
   <configuration>
      <policy name = RaidScanWeekly>
        <srcPath prefix="hdfs://hadoop.myhost.com:9000/user/warehouse/u_full/*"/>
        <parentPolicy> RaidScanMonthly</parentPolicy>
        <property>
          <name>targetReplication</name>
          <value>2</value>
          <description> after RAIDing, decrease the replication factor of the file to 
                        this value.
          </description>
        </property>
        <property>
          <name>metaReplication</name>
          <value>2</value>
          <description> the replication factor of the RAID meta file
          </description>
        </property>
      </policy>
   </configuration>
   *
   * Blank lines and lines starting with # are ignored.
   *  
   * @throws IOException if the config file cannot be read.
   * @throws RaidConfigurationException if configuration entries are invalid.
   * @throws ClassNotFoundException if user-defined policy classes cannot be loaded
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   * @throws JSONException 
   * @returns A new set of policy categories.
   */
  void reloadConfigs() throws IOException, ParserConfigurationException,
      SAXException, ClassNotFoundException, RaidConfigurationException, JSONException {
    JSONObject json = (JSONObject) conf.getJsonConfig(configFileName);
    if (json != null) {
      reloadJSONConfigs(json);
    } else {
      reloadXmlConfigs();
    }
  }
  
  void reloadXmlConfigs() throws IOException, ParserConfigurationException, 
      SAXException, ClassNotFoundException, RaidConfigurationException {

    if (configFileName == null) {
       return;
    }
    
    File file = new File(configFileName);
    if (!file.exists()) {
      throw new RaidConfigurationException("Configuration file " + configFileName +
                                           " does not exist.");
    }

    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    List<PolicyInfo> all = new ArrayList<PolicyInfo>();
    long periodicityValue = periodicity;
    
    
    // Read and parse the configuration file.
    // allow include files in configuration file
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for raid parser "
                + docBuilderFactory + ":" + e, e);
    }
    LOG.info("Reloading config file " + file);

    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(file);
    Element root = doc.getDocumentElement();
    if (!"configuration".equalsIgnoreCase(root.getTagName()))
      throw new RaidConfigurationException("Bad configuration file: " + 
          "top-level element not <configuration>");
    NodeList policies = root.getChildNodes();

    Map<String, PolicyInfo> existingPolicies =
      new HashMap<String, PolicyInfo>();
    // loop through all the configured policies.
    for (int i = 0; i < policies.getLength(); i++) {
      Node node = policies.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element policy = (Element)node;
      if ("policy".equalsIgnoreCase(policy.getTagName())) {
        String policyName = policy.getAttribute("name");
        PolicyInfo curr = new PolicyInfo(policyName, conf);
        PolicyInfo parent = null;
        NodeList policyElements = policy.getChildNodes();
        for (int j = 0; j < policyElements.getLength(); j++) {
          Node node1 = policyElements.item(j);
          if (!(node1 instanceof Element)) {
            continue;
          }
          Element property = (Element) node1;
          String propertyName = property.getTagName();
          if ("srcPath".equalsIgnoreCase(propertyName)) {
            String srcPathPrefix = property.getAttribute("prefix");
            if (srcPathPrefix != null && srcPathPrefix.length() > 0) {
              curr.setSrcPath(srcPathPrefix);
            }
          } else if ("fileList".equalsIgnoreCase(propertyName)) {
            String text = ((Text)property.getFirstChild()).getData().trim();
            LOG.info(policyName + ".fileList = " + text);
            curr.setFileListPath(new Path(text));
          } else if ("codecId".equalsIgnoreCase(propertyName)) {
            String text = ((Text)property.getFirstChild()).getData().trim();
            LOG.info(policyName + ".codecId = " + text);
            curr.setCodecId(text);
          } else if ("shouldRaid".equalsIgnoreCase(propertyName)) {
            String text = ((Text)property.getFirstChild()).getData().trim();
            curr.setShouldRaid(Boolean.parseBoolean(text));
          } else if ("description".equalsIgnoreCase(propertyName)) {
            String text = ((Text)property.getFirstChild()).getData().trim();
            curr.setDescription(text);
          } else if ("parentPolicy".equalsIgnoreCase(propertyName)) {
            String text = ((Text)property.getFirstChild()).getData().trim();
            parent = existingPolicies.get(text);
          } else if ("property".equalsIgnoreCase(propertyName)) {
            NodeList nl = property.getChildNodes();
            String pname=null,pvalue=null;
            for (int l = 0; l < nl.getLength(); l++){
              Node node3 = nl.item(l);
              if (!(node3 instanceof Element)) {
                continue;
              }
              Element item = (Element) node3;
              String itemName = item.getTagName();
              if ("name".equalsIgnoreCase(itemName)){
                pname = ((Text)item.getFirstChild()).getData().trim();
              } else if ("value".equalsIgnoreCase(itemName)){
                pvalue = ((Text)item.getFirstChild()).getData().trim();
              }
            }
            if (pname != null && pvalue != null) {
              LOG.info(policyName + "." + pname + " = " + pvalue);
              curr.setProperty(pname,pvalue);
            }
          } else {
            LOG.info("Found bad property " + propertyName +
                     " policy name " + policyName +
                     ". Ignoring.");
          }
        }  // done with all properties of this policy
        PolicyInfo pinfo;
        if (parent != null) {
          pinfo = new PolicyInfo(policyName, conf);
          pinfo.copyFrom(parent);
          pinfo.copyFrom(curr);
        } else {
          pinfo = curr;
        }
        if (pinfo.getSrcPath() != null || pinfo.getFileListPath() != null) {
          all.add(pinfo);
        }
        existingPolicies.put(policyName, pinfo);
      }
    }
    setAllPolicies(all);
    periodicity = periodicityValue;
    return;
  }

  private void reloadJSONConfigs(JSONObject json) throws JSONException, IOException {
    if (json == null) {
      json = (JSONObject) conf.getJsonConfig(configFileName);
    }
    ArrayList<PolicyInfo> newAllPolicies = new ArrayList<PolicyInfo>();
    JSONArray policyArray = json.getJSONArray("fileListPolicies");
    for (int i = 0; i < policyArray.length(); i++) {
      loadPolicyInfoFromJSON(policyArray.getJSONObject(i), newAllPolicies);
    }
    policyArray = json.getJSONArray("srcPathPolicies");
    for (int i = 0; i < policyArray.length(); i++) {
      loadPolicyInfoFromJSON(policyArray.getJSONObject(i), newAllPolicies);
    }
    setAllPolicies(newAllPolicies);
  }
  
  private void loadPolicyInfoFromJSON(JSONObject json,
      Collection<PolicyInfo> policies) throws JSONException, IOException {
    PolicyInfo policyInfo = new PolicyInfo(json.getString("name"), conf);
    String key = null;
    String stringVal = null;
    Object value = null;
    for (Iterator<?> keys = json.keys(); keys.hasNext();) {
      key = (String) keys.next();
      if (key == null || key.equals("")) continue;
      value = json.get(key);

      if (value instanceof String) {
        stringVal = (String)value;
      } else if (value instanceof Integer) {
        stringVal = new Integer((Integer)value).toString();
      } else if (value instanceof Long) {
        stringVal = new Long((Long)value).toString();
      } else if (value instanceof Double) {
        stringVal = new Double((Double)value).toString();
      } else if (value instanceof Boolean) {
        stringVal = new Boolean((Boolean)value).toString();
      } else {
        LOG.warn("unsupported value in json object: " + value);
      }

      if (key.equals("description")) {
        policyInfo.setDescription(stringVal);
      } else if (key.equals("codecId")) {
        policyInfo.setCodecId(stringVal);
      } else if (key.equals("fileListPath")) {
        policyInfo.setFileListPath(new Path(stringVal));
      } else if (key.equals("srcPath")) {
        policyInfo.setSrcPath(stringVal);
      } else if (key.equals("shouldRaid")) {
        policyInfo.setShouldRaid((Boolean)value);
      } else {
        policyInfo.setProperty(key, stringVal);
      }
    }
    policies.add(policyInfo);
  }

  public synchronized long getPeriodicity() {
    return periodicity;
  }
  
  public synchronized long getHarPartfileSize() {
    return harPartfileSize;
  }

  public synchronized int getMaxJobsPerPolicy() {
    return maxJobsPerPolicy;
  }

  public synchronized int getMaxFilesPerJob() {
    return maxFilesPerJob;
  }
  
  public synchronized int getMaxBlocksPerDirRaidJob() {
    return maxBlocksPerDirRaidJob;
  }
  
  public synchronized String getReadReconstructionMetricsUrl() {
    return readReconstructionMetricsUrl;
  }

  /**
   * Get a collection of all policies
   */
  public synchronized Collection<PolicyInfo> getAllPolicies() {
    return new ArrayList<PolicyInfo>(allPolicies);
  }
  
  /**
   * Set a collection of all policies
   */
  protected synchronized void setAllPolicies(Collection<PolicyInfo> value) {
    this.allPolicies = value;
  }

  /**
   * Start a background thread to reload the config file
   */
  void startReload() {
    if (doReload) {
      reloadThread = new UpdateThread();
      reloadThread.start();
    }
  }

  /**
   * Stop the background thread that reload the config file
   */
  void stopReload() throws InterruptedException {
    if (reloadThread != null) {
      running = false;
      reloadThread.interrupt();
      reloadThread.join();
      reloadThread = null;
    }
  }

  /**
   * A thread which reloads the config file.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("Raid update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(reloadInterval);
          reloadConfigsIfNecessary();
        } catch (InterruptedException e) {
          // do nothing 
        } catch (Exception e) {
          LOG.error("Failed to reload config file ", e);
        }
      }
    }
  }
  
  /**
   * Find the PolicyInfo corresponding to a given policy name
   * @param policyName the name of a policy
   * @return PolicyInfo if there is a matched policy; null otherwise
   */
  PolicyInfo getPolicy(String policyName) {
    for (PolicyInfo policy : allPolicies) {
      if (policyName.equals(policy.getName())) {
        return policy;
      }
    }
    return null;
  }
}

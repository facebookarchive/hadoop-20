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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.MalformedObjectNameException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.apache.hadoop.hdfs.server.namenode.NameNodeMXBean;

/**
 * This class generates the data that is needed to be displayed on cluster web 
 * console by connecting to each namenode through http.
 */
class ClusterJspHelper {
  public final static String TOTAL_FILES_AND_DIRECTORIES = "TotalFilesAndDirectories";
  public final static String TOTAL = "Total";
  public final static String FREE = "Free";
  public final static String NAMESPACE_USED = "NamespaceUsed";
  public final static String NON_DFS_USEDSPACE = "NonDfsUsedSpace";
  public final static String TOTAL_BLOCKS = "TotalBlocks";
  public final static String NUMBER_MISSING_BLOCKS = "NumberOfMissingBlocks";
  public final static String SAFE_MODE_TEXT = "SafeModeText";
  public final static String LIVE_NODES = "LiveNodes";
  public final static String DEAD_NODES = "DeadNodes";
  public final static String DECOM_NODES = "DecomNodes";
  public final static String NNSPECIFIC_KEYS = "NNSpecificKeys";
  public final static String IS_PRIMARY = "IsPrimary";
  
  public static class NameNodeKey 
    implements Comparable<NameNodeKey> {

    public static final int ACTIVE = 0;
    public static final int BOTH = 1;
    public static final int STANDBY = 2;
    public static final String DELIMITER = "\n";
    
    String key;
    int type;
    
    public NameNodeKey(){ }
    
    public NameNodeKey(String input) {
      String[] splits = input.split(DELIMITER);
      key = splits[0];
      type = Integer.parseInt(splits[1]);
    }
    
    public NameNodeKey(String s, int t){
      key = s;
      type = t;
    }

    @Override
    public int compareTo(NameNodeKey o) {
      if (type == o.type)
        return key.compareTo(o.key);
        
      return new Integer(type).compareTo(o.type);
    }
    
    @Override
    public boolean equals(Object o){
      NameNodeKey other = (NameNodeKey)o;
      return key.equals(other.key) && 
          type == other.type;
    }
    
    public String getKey() {
      return key;
    }
    
    public int getType() {
      return type;
    }
    
    public void setKey(String key) {
      this.key = key;
    }
    
    public void setType(int type) {
      this.type = type;
    }
    
    public int hashCode() {
      return key.hashCode();
    }

    public String toString() {
      return this.key + DELIMITER + this.type;
    }
  }
  
  final static public String WEB_UGI_PROPERTY_NAME = "dfs.web.ugi";
  private static final Log LOG = LogFactory.getLog(ClusterJspHelper.class);
  public static final String OVERALL_STATUS = "overall-status";
  public static final String DEAD = "Dead";
  public static Configuration conf = new Configuration();
  private static final boolean isAvatar =
      conf.get("fs.default.name0", "").length() > 0;

  public NameNode localnn;
  public static final UnixUserGroupInformation webUGI
  = UnixUserGroupInformation.createImmutable(
      conf.getStrings(WEB_UGI_PROPERTY_NAME));
  
  public ClusterJspHelper(NameNode nn) {
    localnn = nn;
  }
  
  private NamenodeMXBeanHelper getNNHelper(InetSocketAddress isa) 
      throws IOException, MalformedObjectNameException, 
      URISyntaxException {
    if (localnn != null) {
      Configuration runningConf = localnn.getConf();
      InetSocketAddress nameNodeAddr = NameNode.getClientProtocolAddress(runningConf); 
      if (nameNodeAddr.equals(isa)) {
        return new NamenodeMXBeanHelper(isa, conf, localnn);
      }
    }
    return new NamenodeMXBeanHelper(isa, conf);
  }
  
  public class NameNodeStatusFetcher extends Thread {
    InetSocketAddress isa;
    Exception e = null;
    NamenodeStatus nn = null;
    public NameNodeStatusFetcher(InetSocketAddress isa) {
      this.isa = isa;
    }
    
    public void run() {
      NamenodeMXBeanHelper nnHelper = null;
      LOG.info("connect to " + isa.toString());
      long starttime = System.currentTimeMillis();
      try {
        nnHelper = getNNHelper(isa);
        nn = nnHelper.getNamenodeStatus();
      } catch ( Exception exception ) {
        // track exceptions encountered when connecting to namenodes
        this.e = exception;
        LOG.error(isa.toString() + " has the exception:", e);
        nn = new NamenodeStatus();
      } finally {
        LOG.info("Take time " + isa.toString() + " : " + 
            (System.currentTimeMillis() - starttime));
      }
    }
  }
  
  public class DecommissionStatusFetcher extends Thread {
    InetSocketAddress isa;
    Exception e = null;
    Map<String, Map<String, String>> statusMap = null;
    
    public DecommissionStatusFetcher(InetSocketAddress isa,
        Map<String, Map<String, String>> statusMap) {
      this.isa = isa;
      this.statusMap = statusMap;
    }
    
    public void run() {
      NamenodeMXBeanHelper nnHelper = null;
      try {
        nnHelper = getNNHelper(isa);
        synchronized(statusMap) {
          nnHelper.getDecomNodeInfoForReport(statusMap);
        }
      } catch (Exception exception) {
        this.e = exception;
        LOG.error(isa.toString() + " has the exception:", e);
      }
    }
  }
  
  /**
   * JSP helper function that generates cluster health report.  When 
   * encountering exception while getting Namenode status, the exception will 
   * be listed in the page with corresponding stack trace.
   */
  ClusterStatus generateClusterHealthReport() {
    ClusterStatus cs = new ClusterStatus();
    List<InetSocketAddress> isas = null;
    ArrayList<String> suffixes = null;
    if (isAvatar) {
      suffixes = new ArrayList<String>();
      suffixes.add("0"); suffixes.add("1");
    }
    try {
      cs.nnAddrs = isas = DFSUtil.getClientRpcAddresses(conf, suffixes);
    } catch (Exception e) {
      // Could not build cluster status
      cs.setError(e);
      LOG.error(e);
      return cs;
    }
    
    sort(isas);
    
    // Process each namenode and add it to ClusterStatus in parallel
    NameNodeStatusFetcher[] threads = new NameNodeStatusFetcher[isas.size()];
    for (int i = 0; i < isas.size(); i++) {
      threads[i] = new NameNodeStatusFetcher(isas.get(i));
      threads[i].start();
    }
    for (NameNodeStatusFetcher thread : threads) {
      try {
        thread.join();
        if (thread.e != null) {
          cs.addException(thread.isa.toString(), thread.e);
        }
        cs.addNamenodeStatus(thread.nn);
      } catch (InterruptedException ex) {
        LOG.warn(ex);
      }
    }
    return cs;
  }

  private void sort(List<InetSocketAddress> isas) {
    Collections.sort(isas,
        new Comparator<InetSocketAddress>()
        {
            @Override
            public int compare(InetSocketAddress o1, InetSocketAddress o2) {
              return o1.toString().compareTo(o2.toString());
            }        
        }); 
  }

  /**
   * Helper function that generates the decommissioning report.
   */
  DecommissionStatus generateDecommissioningReport() {
    List<InetSocketAddress> isas = null;
    ArrayList<String> suffixes = null;
    if (isAvatar) {
      suffixes = new ArrayList<String>();
      suffixes.add("0"); suffixes.add("1");
    }
    try {
      isas = DFSUtil.getClientRpcAddresses(conf, suffixes);
      sort(isas);
    } catch (Exception e) {
      // catch any exception encountered other than connecting to namenodes
      DecommissionStatus dInfo = new DecommissionStatus(e);
      LOG.error(e);
      return dInfo;
    }
    
    // Outer map key is datanode. Inner map key is namenode and the value is 
    // decom status of the datanode for the corresponding namenode
    Map<String, Map<String, String>> statusMap = 
      new HashMap<String, Map<String, String>>();
    
    // Map of exceptions encountered when connecting to namenode
    // key is namenode and value is exception
    Map<String, Exception> decommissionExceptions = 
      new HashMap<String, Exception>();
    
    List<String> unreportedNamenode = new ArrayList<String>();
    DecommissionStatusFetcher[] threads = new DecommissionStatusFetcher[isas.size()];
    for (int i = 0; i < isas.size(); i++) {
      threads[i] = new DecommissionStatusFetcher(isas.get(i), statusMap);
      threads[i].start();
    }
    for (DecommissionStatusFetcher thread : threads) {
      try {
        thread.join();
        if (thread.e != null) {
          // catch exceptions encountered while connecting to namenodes
          decommissionExceptions.put(thread.isa.toString(), thread.e);
          unreportedNamenode.add(thread.isa.toString());
        }
      } catch (InterruptedException ex) {
        LOG.warn(ex);
      }
    }
    updateUnknownStatus(statusMap, unreportedNamenode);
    getDecommissionNodeClusterState(statusMap);
    return new DecommissionStatus(statusMap, isas,
        getDatanodeHttpPort(conf), decommissionExceptions);
  }
  
  /**
   * Based on the state of the datanode at each namenode, marks the overall
   * state of the datanode across all the namenodes, to one of the following:
   * <ol>
   * <li>{@link DecommissionStates#DECOMMISSIONED}</li>
   * <li>{@link DecommissionStates#DECOMMISSION_INPROGRESS}</li>
   * <li>{@link DecommissionStates#PARTIALLY_DECOMMISSIONED}</li>
   * <li>{@link DecommissionStates#UNKNOWN}</li>
   * </ol>
   * 
   * @param statusMap
   *          map whose key is datanode, value is an inner map with key being
   *          namenode, value being decommission state.
   */
  private void getDecommissionNodeClusterState(
      Map<String, Map<String, String>> statusMap) {
    if (statusMap == null || statusMap.isEmpty()) {
      return;
    }
    
    // For each datanodes
    Iterator<Entry<String, Map<String, String>>> it = 
      statusMap.entrySet().iterator();
    while (it.hasNext()) {
      // Map entry for a datanode:
      // key is namenode, value is datanode status at the namenode
      Entry<String, Map<String, String>> entry = it.next();
      Map<String, String> nnStatus = entry.getValue();
      if (nnStatus == null || nnStatus.isEmpty()) {
        continue;
      }
      
      boolean isUnknown = false;
      int unknown = 0;
      int decommissioned = 0;
      int decomInProg = 0;
      int inservice = 0;
      int dead = 0;
      DecommissionStates overallState = DecommissionStates.UNKNOWN;
      // Process a datanode state from each namenode
      for (Map.Entry<String, String> m : nnStatus.entrySet()) {
        String status = m.getValue();
        if (status.equals(DecommissionStates.UNKNOWN.toString())) {
          isUnknown = true;
          unknown++;
        } else 
          if (status.equals(AdminStates.DECOMMISSION_INPROGRESS.toString())) {
          decomInProg++;
        } else if (status.equals(AdminStates.DECOMMISSIONED.toString())) {
          decommissioned++;
        } else if (status.equals(AdminStates.NORMAL.toString())) {
          inservice++;
        } else if (status.equals(DEAD)) {
          // dead
          dead++;
        }
      }
      
      // Consolidate all the states from namenode in to overall state
      int nns = nnStatus.keySet().size();
      if ((inservice + dead + unknown) == nns) {
        // Do not display this data node. Remove this entry from status map.  
        it.remove();
      } else if (isUnknown) {
        overallState = DecommissionStates.UNKNOWN;
      } else if (decommissioned == nns) {
        overallState = DecommissionStates.DECOMMISSIONED;
      } else if ((decommissioned + decomInProg) == nns) {
        overallState = DecommissionStates.DECOMMISSION_INPROGRESS;
      } else if ((decommissioned + decomInProg < nns) 
        && (decommissioned + decomInProg > 0)){
        overallState = DecommissionStates.PARTIALLY_DECOMMISSIONED;
      } else {
        LOG.warn("Cluster console encounters a not handled situtation.");
      }
        
      // insert overall state
      nnStatus.put(OVERALL_STATUS, overallState.toString());
    }
  }

  /**
   * update unknown status in datanode status map for every unreported namenode
   */
  private void updateUnknownStatus(Map<String, Map<String, String>> statusMap,
      List<String> unreportedNn) {
    if (unreportedNn == null || unreportedNn.isEmpty()) {
      // no unreported namenodes
      return;
    }
    
    for (Map.Entry<String, Map<String,String>> entry : statusMap.entrySet()) {
      String dn = entry.getKey();
      Map<String, String> nnStatus = entry.getValue();
      for (String nn : unreportedNn) {
        nnStatus.put(nn, DecommissionStates.UNKNOWN.toString());
      }
      statusMap.put(dn, nnStatus);
    }
  }

  /**
   * Get datanode http port from configration
   */
  private int getDatanodeHttpPort(Configuration conf) {
    String address = conf.get("dfs.datanode.http.address", "");
    if (address.equals("")) {
      return -1;
    }
    return Integer.parseInt(address.split(":")[1]);
  }
  
  static class NameNodeMXBeanObject implements NameNodeMXBean {
    private static final ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = null;
    String httpAddress = null;
    
    NameNodeMXBeanObject(InetSocketAddress namenode, Configuration conf)
      throws IOException, URISyntaxException {
      httpAddress = DFSUtil.getInfoServer(namenode, conf, isAvatar);
      InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(httpAddress);
      String nameNodeMXBeanContent = DFSUtil.getHTMLContent(
          new URI("http", null, infoSocAddr.getAddress().getHostAddress(), 
              infoSocAddr.getPort(), "/namenodeMXBean", null, null));
      TypeReference<Map<String, Object>> type = 
          new TypeReference<Map<String, Object>>() { };
      values = mapper.readValue(nameNodeMXBeanContent, type);
    }

    public String getVersion() {
      return null;
    }
    
    public long getUsed() {
      return -1L; 
    }
    
    public long getFree() {
      return Long.parseLong((String)values.get(FREE));
    }
    
    public long getTotal() {
      return Long.parseLong((String)values.get(TOTAL));
    }
    
    public String getSafemode() {
      return null;
    }
    
    public boolean isUpgradeFinalized() {
      return true; 
    }
    
    public long getNonDfsUsedSpace() {
      return Long.parseLong((String)values.get(NON_DFS_USEDSPACE));
    }
    
    public float getPercentUsed(){
      return -1.0f;
    }
    
    public float getPercentRemaining() {
      return -1.0f;
    }
    
    public long getNamespaceUsed() {
      return Long.parseLong((String)values.get(NAMESPACE_USED));
    }
    
    public float getPercentNamespaceUsed() {
      return -1.0f;
    }
      
    public long getTotalBlocks() {
      return Long.parseLong((String)values.get(TOTAL_BLOCKS));
    }
    
    public long getTotalFilesAndDirectories() {
      return Long.parseLong((String) values.get(TOTAL_FILES_AND_DIRECTORIES));
    }
    
    public long getNumberOfMissingBlocks() {
      return Long.parseLong((String)values.get(NUMBER_MISSING_BLOCKS));
    }
    
    public int getThreads() {
      return -1;
    }

    public String getLiveNodes() {
      return (String)values.get(LIVE_NODES);
    }
    
    public String getDeadNodes() {
      return (String)values.get(DEAD_NODES);
    }
    
    public String getDecomNodes() {
      return (String)values.get(DECOM_NODES);
    }
    
    public int getNamespaceId() {
      return -1;
    }
    
    public String getNameserviceId() {
      return null;
    }

    public String getSafeModeText() {
      return (String)values.get(SAFE_MODE_TEXT);
    }
    
    public Map<NameNodeKey, String> getNNSpecificKeys() {
      TypeReference<Map<String, String>> type = 
          new TypeReference<Map<String, String>>() { };
      Map<NameNodeKey, String> result = new HashMap<NameNodeKey, String>();
      try {
        Map<String, String> tmp = NamenodeMXBeanHelper.mapper.readValue(
            (String)values.get(NNSPECIFIC_KEYS), type);
        for (String key: tmp.keySet()) {
          result.put(new NameNodeKey(key), tmp.get(key));
        }
        return result;
      } catch (Exception e) {
        LOG.error(e);
        return null;
      } 
    }
    
    public boolean getIsPrimary() {
      return Boolean.parseBoolean((String)values.get(IS_PRIMARY));
    }
  }

  /**
   * Class for connecting to Namenode over http or local fsnamesystem 
   */
  static class NamenodeMXBeanHelper {
    public static final ObjectMapper mapper = new ObjectMapper();
    private final InetSocketAddress rpcAddress;
    private final String address;
    private final Configuration conf;
    private final NameNodeMXBean mxbeanProxy;
    
    NamenodeMXBeanHelper(InetSocketAddress addr, Configuration conf) 
        throws IOException, MalformedObjectNameException,
        URISyntaxException{
      this(addr, conf, null);
    }
    
    NamenodeMXBeanHelper(InetSocketAddress addr, Configuration conf, 
        NameNode localnn) throws IOException, URISyntaxException {
      this.rpcAddress = addr;
      this.address = addr.toString();
      this.conf = conf;
      if (localnn == null) {
        mxbeanProxy = new NameNodeMXBeanObject(addr, conf);
      } else {
        LOG.info("Call local namenode " + this.address + " directly");
        mxbeanProxy = localnn.getNamesystem();
      }
    }
    
    /** Get the map corresponding to the JSON string */
    private static Map<String, Map<String, Object>> getNodeMap(String json)
        throws IOException {
      TypeReference<Map<String, Map<String, Object>>> type = 
        new TypeReference<Map<String, Map<String, Object>>>() { };
      return mapper.readValue(json, type);
    }
    
    /**
     * Process JSON string returned from connection to get the number of
     * live datanodes.
     * 
     * @param json JSON output that contains live node status.
     * @param nn namenode status to return information in
     */
    private static void getLiveNodeCount(String json, NamenodeStatus nn)
        throws IOException {
      // Map of datanode host to (map of attribute name to value)
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      
      nn.liveDatanodeCount = nodeMap.size();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        // Inner map of attribute name to value
        Map<String, Object> innerMap = entry.getValue();
        if (innerMap != null) {
          if (((String) innerMap.get("adminState"))
              .equals(AdminStates.DECOMMISSIONED.toString())) {
            nn.liveDecomCount++;
          }
          if (((Boolean) innerMap.get("excluded"))
              .booleanValue() == true) {
            nn.liveExcludeCount++;
          }
        }
      }
    }
  
    /**
     * Count the number of dead datanode based on the JSON string returned 
     * 
     * @param nn namenode
     * @param json JSON string returned from http or local fsnamesystem
     */
    private static void getDeadNodeCount(String json, NamenodeStatus nn)
        throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      
      nn.deadDatanodeCount = nodeMap.size();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        Map<String, Object> innerMap = entry.getValue();
        if (innerMap != null && !innerMap.isEmpty()) {
          if (((Boolean) innerMap.get("decommissioned"))
              .booleanValue() == true) {
            nn.deadDecomCount++;
          }
          if (((Boolean) innerMap.get("excluded"))
              .booleanValue() == true) {
            nn.deadExcludeCount++;
          }
        }
      }
    }
    
    public NamenodeStatus getNamenodeStatus()
        throws IOException, MalformedObjectNameException {
      NamenodeStatus nn = new NamenodeStatus();
      nn.address = this.address;
      nn.filesAndDirectories = mxbeanProxy.getTotalFilesAndDirectories();
      nn.capacity = mxbeanProxy.getTotal();
      nn.free = mxbeanProxy.getFree();
      nn.nsUsed = mxbeanProxy.getNamespaceUsed();
      nn.nonDfsUsed = mxbeanProxy.getNonDfsUsedSpace();
      nn.blocksCount = mxbeanProxy.getTotalBlocks();
      nn.missingBlocksCount = mxbeanProxy.getNumberOfMissingBlocks();
      nn.httpAddress = DFSUtil.getInfoServer(rpcAddress, conf, isAvatar);
      nn.safeModeText = mxbeanProxy.getSafeModeText();
      getLiveNodeCount(mxbeanProxy.getLiveNodes(), nn);
      getDeadNodeCount(mxbeanProxy.getDeadNodes(), nn);
      nn.namenodeSpecificInfo = mxbeanProxy.getNNSpecificKeys();
      if (nn.namenodeSpecificInfo == null) {
        throw new IOException("Namenode SpecificInfo is null");
      }
      nn.isPrimary = mxbeanProxy.getIsPrimary();
      return nn;
    }
    
    /**
     * Connect to namenode to get decommission node information.
     * @param statusMap data node status map
     */
    private void getDecomNodeInfoForReport(
        Map<String, Map<String, String>> statusMap) throws IOException,
        MalformedObjectNameException {
      getLiveNodeStatus(statusMap, address, mxbeanProxy.getLiveNodes());
      getDeadNodeStatus(statusMap, address, mxbeanProxy.getDeadNodes());
      getDecommissionNodeStatus(statusMap, address, mxbeanProxy.getDecomNodes());
    }
  
    /**
     * Process the JSON string returned from http or local fsnamesystem to get
     * live datanode status. Store the information into datanode status map and
     * Decommissionnode.
     * 
     * @param statusMap Map of datanode status. Key is datanode, value
     *          is an inner map whose key is namenode, value is datanode status.
     *          reported by each namenode.
     * @param namenodeAddr address of the namenode
     * @param decomnode update Decommissionnode with alive node status
     * @param json JSON string contains datanode status
     * @throws IOException
     */
    private static void getLiveNodeStatus(
        Map<String, Map<String, String>> statusMap, String namenodeAddr,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap != null && !nodeMap.isEmpty()) {
        List<String> liveDecommed = new ArrayList<String>();
        for (Map.Entry<String, Map<String, Object>> entry: nodeMap.entrySet()) {
          Map<String, Object> innerMap = entry.getValue();
          String dn = entry.getKey();
          if (innerMap != null) {
            if (innerMap.get("adminState").equals(
                AdminStates.DECOMMISSIONED.toString())) {
              liveDecommed.add(dn);
            }
            // the inner map key is namenode, value is datanode status.
            Map<String, String> nnStatus = statusMap.get(dn);
            if (nnStatus == null) {
              nnStatus = new HashMap<String, String>();
            }
            nnStatus.put(namenodeAddr, (String) innerMap.get("adminState"));
            // map whose key is datanode, value is the inner map.
            statusMap.put(dn, nnStatus);
          }
        }
      }
    }
  
    /**
     * Process the JSON string returned from http or local fsnamesystem to get
     * the dead datanode information. Store the information into datanode status
     * map and Decommissionnode.
     * 
     * @param statusMap map with key being datanode, value being an
     *          inner map (key:namenode, value:decommisionning state).
     * @param address  
     * @param json
     * @throws IOException
     */
    private static void getDeadNodeStatus(
        Map<String, Map<String, String>> statusMap, String address,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      List<String> deadDn = new ArrayList<String>();
      List<String> deadDecommed = new ArrayList<String>();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        deadDn.add(entry.getKey());
        Map<String, Object> deadNodeDetailMap = entry.getValue();
        String dn = entry.getKey();
        if (deadNodeDetailMap != null && !deadNodeDetailMap.isEmpty()) {
          // NN - status
          Map<String, String> nnStatus = statusMap.get(dn);
          if (nnStatus == null) {
            nnStatus = new HashMap<String, String>();
          }
          if (((Boolean) deadNodeDetailMap.get("decommissioned"))
              .booleanValue() == true) {
            deadDecommed.add(dn);
            nnStatus.put(address, AdminStates.DECOMMISSIONED.toString());
          } else {
            nnStatus.put(address, DEAD);
          }
          // dn-nn-status
          statusMap.put(dn, nnStatus);
        }
      }
    }
  
    /**
     * We process the JSON string returned from http or local fsnamesystem
     * to get the decommisioning datanode information.
     * 
     * @param dataNodeStatusMap map with key being datanode, value being an
     *          inner map (key:namenode, value:decommisionning state).
     * @param address 
     * @param json JSON string returned 
     */
    private static void getDecommissionNodeStatus(
        Map<String, Map<String, String>> dataNodeStatusMap, String address,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      List<String> decomming = new ArrayList<String>();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        String dn = entry.getKey();
        decomming.add(dn);
        // nn-status
        Map<String, String> nnStatus = new HashMap<String, String>();
        if (dataNodeStatusMap.containsKey(dn)) {
          nnStatus = dataNodeStatusMap.get(dn);
        }
        nnStatus.put(address, AdminStates.DECOMMISSION_INPROGRESS.toString());
        // dn-nn-status
        dataNodeStatusMap.put(dn, nnStatus);
      }
    }

    public boolean isAvatar() {
      return isAvatar;
    }
  }

  /**
   * This class contains cluster statistics.
   */
  static class ClusterStatus {
    /** Exception indicates failure to get cluster status */
    Exception error = null;
    
    /** Cluster status information */
    String clusterid = "";
    Long totalFilesAndDirectories = 0L;
    Long total_sum = 0L;
    Long clusterDfsUsed = 0L;
    Long nonDfsUsed_sum = 0L;
    Long free_sum = 0L;
    Long totalMissingBlocks = 0L;
    Long totalBlocks = 0L;
    List<InetSocketAddress> nnAddrs = null;
    /** List of namenodes in the cluster */
    final List<NamenodeStatus> nnList = new ArrayList<NamenodeStatus>();
    int countPrimary = 0;
    
    /** Map of namenode host and exception encountered when getting status */
    final Map<String, Exception> nnExceptions = new HashMap<String, Exception>();
    
    String[] getStatsNames() {
      return new String[] {
        "Namenode role",
        "Files And Directories",
        "Configured Capacity",
        "DFS Used",
        "Non DFS Used",
        "DFS Remaining",
        "DFS Used%",
        "DFS Remaining%",
        "Missing Blocks",
        "Blocks"
      };
    }
    
    Object[] getStats() {
      int nns = countPrimary < 1 ? 1 : countPrimary;
      Long total_capacity = this.total_sum / nns;
      Long remaining_capacity = this.free_sum /nns;
      Float dfs_used_percent = total_capacity < 1e-10 ? 0.0f : 
        (Float)(this.clusterDfsUsed*100.0f) / total_capacity;
      Float dfs_remaining_percent = total_capacity < 1e-10 ? 0.0f:
        (Float)(remaining_capacity*100.0f) / total_capacity;
      return new Object[]{
        "",
        this.totalFilesAndDirectories,
        total_capacity,
        this.clusterDfsUsed,
        this.nonDfsUsed_sum / nns,
        remaining_capacity,
        dfs_used_percent,
        dfs_remaining_percent, 
        this.totalMissingBlocks,
        this.totalBlocks};
    }
    
    Map<NameNodeKey, String> getNamenodeSpecificKeys(){
      return new HashMap<NameNodeKey, String>();
    }
    
    String getNamenodeSpecificKeysName(){
      return "Namenode specific keys:";
    }
    
    public void setError(Exception e) {
      error = e;
    }
    
    public void addNamenodeStatus(NamenodeStatus nn) {
      nnList.add(nn);
      if(!nn.isPrimary)
        return;
      countPrimary++;
      // Add namenode status to cluster status (only if primary)
      totalFilesAndDirectories += nn.filesAndDirectories;
      total_sum += nn.capacity;
      free_sum += nn.free;
      clusterDfsUsed += nn.nsUsed;
      nonDfsUsed_sum += nn.nonDfsUsed;
      totalMissingBlocks += nn.missingBlocksCount;
      totalBlocks += nn.blocksCount;
    }

    public void addException(String address, Exception e) {
      nnExceptions.put(address, e);
    }
    
    public boolean isAvatar() {
      return isAvatar;
    }
  }
  
  /**
   * This class stores namenode statistics to be used to generate cluster
   * web console report.
   */
  static class NamenodeStatus {
    String address = "";
    Long filesAndDirectories = 0L;
    Long capacity = 0L;
    Long nsUsed = 0L;
    Long nonDfsUsed = 0L;
    Long free = 0L;
    Long missingBlocksCount = 0L;
    Long blocksCount = 0L;
    int liveDatanodeCount = 0;
    int liveExcludeCount = 0;
    int liveDecomCount = 0;
    int deadDatanodeCount = 0;
    int deadExcludeCount = 0;
    int deadDecomCount = 0;
    String httpAddress = null;
    String safeModeText = "";
    
    boolean isPrimary = false;
    
    Map<NameNodeKey, String> namenodeSpecificInfo = new HashMap<NameNodeKey, String>();
    
    Object[] getStats() {
      Float dfs_used_percent = capacity == 0L ? 0.0f : 
        (Float)(this.nsUsed*100.0f) / capacity;
      Float dfs_remaining_percent = capacity == 0L ? 0.0f:
        (Float)(this.free*100.0f) / capacity;
      return new Object[]{
        this.isPrimary ? "Primary" : "Standby",
        this.filesAndDirectories,
        this.capacity,
        this.nsUsed,
        this.nonDfsUsed,
        this.free,
        dfs_used_percent,
        dfs_remaining_percent,
        this.missingBlocksCount,
        this.blocksCount,
        this.namenodeSpecificInfo};
    }
    
    Map<NameNodeKey, String> getNamenodeSpecificKeys(){
      return this.namenodeSpecificInfo;
    }
  }

  /**
   * cluster-wide decommission state of a datanode
   */
  public enum DecommissionStates {
    /*
     * If datanode state is decommissioning at one or more namenodes and 
     * decommissioned at the rest of the namenodes.
     */
    DECOMMISSION_INPROGRESS("Decommission In Progress"),
    
    /* If datanode state at all the namenodes is decommissioned */
    DECOMMISSIONED("Decommissioned"),
    
    /*
     * If datanode state is not decommissioning at one or more namenodes and 
     * decommissioned/decommissioning at the rest of the namenodes.
     */
    PARTIALLY_DECOMMISSIONED("Partially Decommissioning"),
    
    /*
     * If datanode state is not known at a namenode, due to problems in getting
     * the datanode state from the namenode.
     */
    UNKNOWN("Unknown");

    final String value;
    
    DecommissionStates(final String v) {
      this.value = v;
    }

    public String toString() {
      return value;
    }
  }

  /**
   * This class consolidates the decommissioning datanodes information in the
   * cluster and generates decommissioning reports in XML.
   */
  static class DecommissionStatus {
    /** Error when set indicates failure to get decomission status*/
    final Exception error;
    
    /** Map of dn host <-> (Map of NN host <-> decommissioning state) */
    final List<InetSocketAddress> nnAddrs;
    final Map<String, Map<String, String>> statusMap;
    final int httpPort;
    int decommissioned = 0;   // total number of decommissioned nodes
    int decommissioning = 0;  // total number of decommissioning datanodes
    int partial = 0;          // total number of partially decommissioned nodes
    
    /** Map of namenode and exception encountered when getting decom status */
    Map<String, Exception> exceptions = new HashMap<String, Exception>();

    private DecommissionStatus(Map<String, Map<String, String>> statusMap, 
        List<InetSocketAddress> nnAddrs,
        int httpPort, Map<String, Exception> exceptions) {
      this(statusMap, nnAddrs, httpPort, exceptions, null);
    }

    public DecommissionStatus(Exception e) {
      this(null, null, -1, null, e);
    }
    
    private DecommissionStatus(Map<String, Map<String, String>> statusMap,
        List<InetSocketAddress> nnAddrs,
         int httpPort, Map<String, Exception> exceptions,
        Exception error) {
      this.statusMap = statusMap;
      this.nnAddrs = nnAddrs;
      this.httpPort = httpPort;
      this.exceptions = exceptions;
      this.error = error;
    }

    /**
     * Count the total number of decommissioned/decommission_inprogress/
     * partially decommissioned datanodes.
     */
    public void countDecommissionDatanodes() {
      for (String dn : statusMap.keySet()) {
        Map<String, String> nnStatus = statusMap.get(dn);
        String status = nnStatus.get(OVERALL_STATUS);
        if (status.equals(DecommissionStates.DECOMMISSIONED.toString())) {
          decommissioned++;
        } else if (status.equals(DecommissionStates.DECOMMISSION_INPROGRESS
            .toString())) {
          decommissioning++;
        } else if (status.equals(DecommissionStates.PARTIALLY_DECOMMISSIONED
            .toString())) {
          partial++;
        }
      }
    }
  }

} 

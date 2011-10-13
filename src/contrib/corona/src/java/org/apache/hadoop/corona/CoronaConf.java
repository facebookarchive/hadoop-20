package org.apache.hadoop.corona;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class CoronaConf extends Configuration {

  public static final Log LOG = LogFactory.getLog(CoronaConf.class);

  public static final String CM_ADDRESS =           "cm.server.address";
  public static final String CM_HTTP_ADDRESS =           "cm.server.http.address";
  public static final String NODE_EXPIRY_INTERVAL =     "cm.node.expiryinterval";
  public static final String SESSION_EXPIRY_INTERVAL =     "cm.session.expiryinterval";
  public static final String NOTIFIER_POLL_INTERVAL =   "cm.notifier.pollinterval";
  public static final String NOTIFIER_RETRY_INTERVAL_FACTOR =  "cm.notifier.retry.interval.factor";
  public static final String NOTIFIER_RETRY_INTERVAL_START =  "cm.notifier.retry.interval.start";
  public static final String NOTIFIER_RETRY_MAX =  "cm.notifier.retry.max";
  public static final String CPU_TO_RESOURCE_PARTITIONING = "cm.cpu.to.resource.partitioning";
  public static final String CM_SOTIMEOUT = "cm.server.sotimeout";
  public static final String NODE_RESERVED_MEMORY_MB = "cm.node.reserved.memory.mb";
  public static final String NODE_RESERVED_DISK_GB = "cm.node.reserved.disk.gb";

  // these are left in the mapred.fairscheduler namespace to make sure they are
  // compatible with the current fairscheduler. client can be expected to send jobs
  // to corona and/or classic hadoop with same configuration
  public static final String IMPLICIT_POOL_PROPERTY = "mapred.fairscheduler.poolnameproperty";
  public static final String EXPLICIT_POOL_PROPERTY = "mapred.fairscheduler.pool";

  public static final String POOL_CONFIG_FILE = "corona.xml";

  private Map<Integer, Map<String, Integer>> cachedCpuToResourcePartitioning = null;

  public CoronaConf(Configuration conf) {
    super(conf);
  }

  public int getCMSoTimeout() {
    return getInt(CM_SOTIMEOUT, 30*1000);
  }

  public String getClusterManagerAddress() {
    return get(CM_ADDRESS, "localhost:8888");
  }

  public String getClusterManagerHttpAddress() {
    return get(CM_HTTP_ADDRESS, "localhost:0");
  }

  public static String getClusterManagerAddress(Configuration conf) {
    return conf.get(CM_ADDRESS, "localhost:8888");
  }

  public int getNodeExpiryInterval() {
    return getInt(NODE_EXPIRY_INTERVAL, 10 * 60 * 1000);
  }

  public int getSessionExpiryInterval() {
    int val = getInt(SESSION_EXPIRY_INTERVAL, 0);

    if (val != 0)
      return val;

    // if the session expiry interval is not specified then we compute
    // one based on the exponential backoff intervals of the session 
    // notification retries

    val = getNotifierRetryIntervalStart();
    int factor = getNotifierRetryIntervalFactor();
    for(int i=1; i<getNotifierRetryMax(); i++) {
      val += val*factor;
    }
    return val;
  }


  public int getNotifierPollInterval() {
    return getInt(NOTIFIER_POLL_INTERVAL, 1000);
  }

  public int getNotifierRetryIntervalFactor() {
    return getInt(NOTIFIER_RETRY_INTERVAL_FACTOR, 4);
  }

  public int getNotifierRetryIntervalStart() {
    return getInt(NOTIFIER_RETRY_INTERVAL_START, 5000);
  }

  public int getNotifierRetryMax() {
    return getInt(NOTIFIER_RETRY_MAX, 5);
  }

  public Map<Integer, Map<String, Integer>> getCpuToResourcePartitioning() {

    if (cachedCpuToResourcePartitioning != null)
      return cachedCpuToResourcePartitioning;

    String jsonStr = get(CPU_TO_RESOURCE_PARTITIONING, "");
    Map<Integer, Map<String, Integer>> ret = new HashMap<Integer, Map<String, Integer>> ();

    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readValue(jsonStr, JsonNode.class);

      Iterator<String> iter = rootNode.getFieldNames();
      while (iter.hasNext()) {
        String field = iter.next();
        Integer numCpu = Integer.parseInt(field);

        if ((numCpu < 0) || (numCpu > 64)) {
          throw new RuntimeException("Number of CPUs: " + numCpu + " is not in range 0-64");
        }

        JsonNode val = rootNode.get(field);
        if (!val.isObject()) {
          throw new RuntimeException("Resource Partitioning: " + val.toString() + " is not a object");
        }

        HashMap<String, Integer> resourcePartition = null;

        Iterator<String> valIter = val.getFieldNames();
        while (valIter.hasNext()) {
          String resourceType = valIter.next();
          JsonNode resourceVal = val.get(resourceType);
          int resourceSlots = 0;

          if (!resourceVal.isInt() || ((resourceSlots = resourceVal.getIntValue()) < 0) ||
              resourceSlots > 64)  {
            throw new RuntimeException("Resource Partition value: " + resourceVal.toString() +
                                       " is not a valid number");
          }
          if (resourcePartition == null) {
            resourcePartition = new HashMap<String, Integer> ();
          }

          resourcePartition.put(resourceType, new Integer(resourceSlots));
        }

        if (resourcePartition != null) {
          ret.put(numCpu, resourcePartition);
        }
      }

      return ret;

    } catch (Exception e) {
      LOG.error(jsonStr + " is not a valid value for option: " + CPU_TO_RESOURCE_PARTITIONING);
      throw new RuntimeException (e);
    }
  }

  public String getPoolName() {
    String poolNameProperty = get(IMPLICIT_POOL_PROPERTY, "user.name");
    return get(EXPLICIT_POOL_PROPERTY, get(poolNameProperty, "")).trim();
  }

  public int getNodeReservedMemoryMB() {
    return getInt(NODE_RESERVED_MEMORY_MB, 0);
  }

  public int getNodeReservedDiskGB() {
    return getInt(NODE_RESERVED_DISK_GB, 0);
  }
}

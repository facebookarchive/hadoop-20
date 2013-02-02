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

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Utility class for corona configuration.
 */
public class CoronaConf extends Configuration {
  /** Logger. */
  public static final Log LOG = LogFactory.getLog(CoronaConf.class);

  /** The includes file. */
  public static final String HOSTS_FILE = "cm.hosts";
  /** The excludes file. */
  public static final String EXCLUDE_HOSTS_FILE = "cm.hosts.exclude";
  /**
   * The name of the file which will contain the CM's state when it goes for
   * an upgrade.
   */
  public static final String CM_STATE_FILE = "cm.state";
  /** The RPC address of the Cluster Manager. */
  public static final String CM_ADDRESS = "cm.server.address";
  /**
   * This boolean property is used to fix whether compression would be used
   * while saving the CM state or not. While debugging, it is preferable
   * that this should be false.
   */
  public static final String CM_COMPRESS_STATE = "cm.compress.state";
  /** The HTTP UI address for the Cluster Manager. */
  public static final String CM_HTTP_ADDRESS = "cm.server.http.address";
  /** The RPC address of the Proxy Job Tracker. */
  public static final String PROXY_JOB_TRACKER_ADDRESS =
    "corona.proxy.job.tracker.rpcaddr";
  /** The Thrift address of the Proxy Job Tracker. */
  public static final String PROXY_JOB_TRACKER_THRIFT_ADDRESS =
    "corona.proxy.job.tracker.thriftaddr";
  /** The interval after which a cluster node is timed out. */
  public static final String NODE_EXPIRY_INTERVAL = "cm.node.expiryinterval";
  /** Allow unconfigured pools? */
  public static final String CONFIGURED_POOLS_ONLY =
      "cm.configured.pools.only";
  /**
   * The number of sessions that flag node for failed connections after which
   * the node is considered bad.
   */
  public static final String NODE_MAX_FAILED_CONNECTIONS =
      "cm.node.max.failed.connections";
  /**
   * The number of failed connections to a node after which a session flags the
   * node as bad.
   */
  public static final String NODE_MAX_FAILED_CONNECTIONS_SESSION =
      "cm.node.max.failed.connections.session";
  /**
   * The number of sessions that flag node for failures after which the node is
   * considered bad.
   */
  public static final String NODE_MAX_FAILURES =
      "cm.node.max.failures";
  /**
   * The number of failures on a node after which a session flags the node as
   * bad.
   */
  public static final String NODE_MAX_FAILURES_SESSION =
      "cm.node.max.failures.session";
  /** The interval after which a session is timed out. */
  public static final String SESSION_EXPIRY_INTERVAL =
      "cm.session.expiryinterval";
  public static final String CM_NOTIFIER_THREAD_COUNT =
    "cm.notifier.numnotifiers";
  /** How often a notifier thread will poll its queue of tasks. */
  public static final String NOTIFIER_POLL_INTERVAL =
      "cm.notifier.pollinterval";
  /** The retry interval factor for a notifier. */
  public static final String NOTIFIER_RETRY_INTERVAL_FACTOR =
      "cm.notifier.retry.interval.factor";
  /** The retry interval start for a notifier. */
  public static final String NOTIFIER_RETRY_INTERVAL_START =
      "cm.notifier.retry.interval.start";
  /** The max retries for a notifier. */
  public static final String NOTIFIER_RETRY_MAX =
      "cm.notifier.retry.max";
  /** JSON configuration specifying the CPU->Resource allocation. */
  public static final String CPU_TO_RESOURCE_PARTITIONING =
      "cm.cpu.to.resource.partitioning";
  /** Timeout. */
  public static final String CM_SOTIMEOUT = "cm.server.sotimeout";
  /** Minimum free memory on a node before scheduling on it. */
  public static final String NODE_RESERVED_MEMORY_MB =
      "cm.node.reserved.memory.mb";
  /** Minimum free disk on a node before scheduling on it. */
  public static final String NODE_RESERVED_DISK_GB = "cm.node.reserved.disk.gb";
  /** Log directory for sessions. */
  public static final String SESSIONS_LOG_ROOT = "corona.sessions.log.dir";
  /** Maximum number of retired sessions to keep in memory. */
  public static final String MAX_RETIRED_SESSIONS =
    "cm.sessions.num.retired";

  // these are left in the mapred.fairscheduler namespace to make sure they are
  // compatible with the current fairscheduler. client can be expected to send jobs
  // to corona and/or classic hadoop with same configuration
  public static final String IMPLICIT_POOL_PROPERTY = "mapred.fairscheduler.poolnameproperty";
  /**
   * In the format of <pool group>.<pool> (i.e. ads.nonsla)
   * Specifies a default pool group PoolGroupManager.DEFAULT_POOL_GROUP if
   * the pool group is not specified.
   * i.e. ads_nonsla -> defaultpoolgroup.ads_nonsla
   */
  public static final String EXPLICIT_POOL_PROPERTY = "mapred.fairscheduler.pool";

  /** Where the general config file is stored. */
  public static final String CONFIG_FILE_PROPERTY = "cm.config.file";

  /** Default general config file location */
  public static final String DEFAULT_CONFIG_FILE = "corona.xml";

  /** Where the pools config file is stored. */
  public static final String POOLS_CONFIG_FILE_PROPERTY =
      "cm.pools.config.file";

  /**
   * Default pools config file location (same as general config file
   * by default).
   */
  public static final String DEFAULT_POOLS_CONFIG_FILE = "corona.xml";

  /**
   * Property for specifying the number of ms to wait between pools config
   * generation (if specified).
   */
  public static final String POOLS_RELOAD_PERIOD_MS_PROPERTY =
      "cm.pools.reload.period.ms";

  /**
   * Property for specifying the number of ms to wait between pools config
   * generation (if specified).
   */
  public static final String CONFIG_RELOAD_PERIOD_MS_PROPERTY =
      "cm.config.reload.period.ms";

  /** Class to generate the pools config */
  public static final String POOLS_CONFIG_DOCUMENT_GENERATOR_PROPERTY =
      "cm.pools.config.document.generator";

  /** number of task trackers restarted in one batch */
  public static final String CORONA_NODE_RESTART_BATCH =
      "corona.node.restart.batch";

  /** interval for restarting task trackers batches */
  public static final String CORONA_NODE_RESTART_INTERVAL =
      "corona.node.restart.interval";

  /** The max time CM will wait for JT heartbeat to be in sync */
  public static final String CM_HEARTBEAT_DELAY_MAX =
      "cm.heartbeat.delay.max";

  private Map<Integer, Map<ResourceType, Integer>>
    cachedCpuToResourcePartitioning = null;

  public CoronaConf(Configuration conf) {
    super(conf);
  }

  public int getCMSoTimeout() {
    return getInt(CM_SOTIMEOUT, 60*1000);
  }

  public String getClusterManagerAddress() {
    return get(CM_ADDRESS, "localhost:8888");
  }

  public String getClusterManagerHttpAddress() {
    return get(CM_HTTP_ADDRESS, "localhost:0");
  }

  public String getProxyJobTrackerAddress() {
    return get(PROXY_JOB_TRACKER_ADDRESS , "localhost:50035");
  }

  public String getProxyJobTrackerThriftAddress() {
    return get(PROXY_JOB_TRACKER_THRIFT_ADDRESS, "localhost:50036");
  }

  public static String getClusterManagerAddress(Configuration conf) {
    return conf.get(CM_ADDRESS, "localhost:8888");
  }

  public int getNodeExpiryInterval() {
    return getInt(NODE_EXPIRY_INTERVAL, 10 * 60 * 1000);
  }

  public String getSessionsLogDir() {
    return get(SESSIONS_LOG_ROOT, "/tmp/history");
  }

  public int getNumRetiredSessions() {
    return getInt(MAX_RETIRED_SESSIONS, 1000);
  }

  public int getMaxSessionsPerDir() {
    return getInt("corona.history.max.per.dir", 1000);
  }

  public long getLogDirRotationInterval() {
    return getLong("corona.history.roll.period", 60L * 60 * 1000);
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

  /**
   * Get and cache the cpu to resource partitioning for this object.
   *
   * @return Mapping of cpu to resources (cached)
   */
  public Map<Integer, Map<ResourceType, Integer>> getCpuToResourcePartitioning() {
    if (cachedCpuToResourcePartitioning == null) {
      cachedCpuToResourcePartitioning =
          getUncachedCpuToResourcePartitioning(this);
    }
    return cachedCpuToResourcePartitioning;
  }

  /**
   * Determine the cpu to resource partitioning for a configuration
   *
   * @param conf Configuration with the cpu to resource partitioning
   * @return Mapping of cpu to resources
   */
  public static Map<Integer, Map<ResourceType, Integer>>
  getUncachedCpuToResourcePartitioning(Configuration conf) {
    String jsonStr = conf.get(CPU_TO_RESOURCE_PARTITIONING, "");
    Map<Integer, Map<ResourceType, Integer>> ret =
        new HashMap<Integer, Map<ResourceType, Integer>>();

    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readValue(jsonStr, JsonNode.class);

      Iterator<String> iter = rootNode.getFieldNames();
      while (iter.hasNext()) {
        String field = iter.next();
        Integer numCpu = Integer.parseInt(field);

        if ((numCpu < 0) || (numCpu > 64)) {
          throw new RuntimeException(
            "Number of CPUs: " + numCpu + " is not in range 0-64");
        }

        JsonNode val = rootNode.get(field);
        if (!val.isObject()) {
          throw new RuntimeException(
            "Resource Partitioning: " + val.toString() + " is not a object");
        }

        Map<ResourceType, Integer> resourcePartition = null;

        Iterator<String> valIter = val.getFieldNames();
        while (valIter.hasNext()) {
          String resourceTypeString = valIter.next();
          JsonNode resourceVal = val.get(resourceTypeString);
          int resourceSlots = 0;

          if (!resourceVal.isInt() ||
              ((resourceSlots = resourceVal.getIntValue()) < 0) ||
              resourceSlots > 64)  {
            throw new RuntimeException(
              "Resource Partition value: " + resourceVal.toString() +
              " is not a valid number");
          }
          if (resourcePartition == null) {
            resourcePartition =
                new EnumMap<ResourceType, Integer>(ResourceType.class);
          }

          try {
            ResourceType resourceType = ResourceType.valueOf(resourceTypeString);
            resourcePartition.put(resourceType, new Integer(resourceSlots));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot correctly parse resource type " +
                resourceTypeString + ", must be one of " +
                Arrays.toString(ResourceType.values()));
          }
        }

        if (resourcePartition != null) {
          ret.put(numCpu, resourcePartition);
        }
      }

      return ret;

    } catch (JsonParseException e) {
      LOG.error(jsonStr + " is not a valid value for option: " +
                CPU_TO_RESOURCE_PARTITIONING);
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      LOG.error(jsonStr + " is not a valid value for option: " +
                CPU_TO_RESOURCE_PARTITIONING);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error(jsonStr + " is not a valid value for option: " +
                CPU_TO_RESOURCE_PARTITIONING);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the pool info.  In order to support previous behavior, a single pool
   * name is accepted.
   * @return Pool info, using a default pool group if the
   *         explicit pool can not be found
   */
  public PoolInfo getPoolInfo() {
    String poolNameProperty = get(IMPLICIT_POOL_PROPERTY, "user.name");
    String explicitPool =
        get(EXPLICIT_POOL_PROPERTY, get(poolNameProperty, "")).trim();
    String[] poolInfoSplitString = explicitPool.split("[.]");
    if (poolInfoSplitString != null && poolInfoSplitString.length == 2) {
      return new PoolInfo(poolInfoSplitString[0], poolInfoSplitString[1]);
    } else if (!explicitPool.isEmpty()) {
      return new PoolInfo(PoolGroupManager.DEFAULT_POOL_GROUP, explicitPool);
    } else {
      return PoolGroupManager.DEFAULT_POOL_INFO;
    }
  }

  public int getNodeReservedMemoryMB() {
    return getInt(NODE_RESERVED_MEMORY_MB, 0);
  }

  public int getNodeReservedDiskGB() {
    return getInt(NODE_RESERVED_DISK_GB, 0);
  }

  /**
   * @return The number of sessions that report too many failed connections in
   *         order to blacklist a node.
   */
  public int getMaxFailedConnections() {
    return getInt(NODE_MAX_FAILED_CONNECTIONS, 20);
  }

  /**
   * @return The number of failed connections to a node encountered by a session
   *         in order for it to count towards blacklisting the node.
   */
  public int getMaxFailedConnectionsPerSession() {
    return getInt(NODE_MAX_FAILED_CONNECTIONS_SESSION, 1);
  }

  /**
   * @return The number of sessions that report too many failures in order to
   *         blacklist a node.
   */
  public int getMaxFailures() {
    return getInt(NODE_MAX_FAILURES, 40);
  }

  /**
   * @return The number of failures encountered by a session in order for it to
   *         count towards blacklisting the node.
   */
  public int getMaxFailuresPerSession() {
    return getInt(NODE_MAX_FAILURES_SESSION, 5);
  }

  public String getHostsFile() {
    return get(HOSTS_FILE, "");
  }

  public String getExcludesFile() {
    return get(EXCLUDE_HOSTS_FILE, "");
  }

  /**
   * Get the address of the file used to save the state of the ClusterManager
   * when it goes down for an upgrade
   *
   * @return A String, containing the address of the file used to save the
   *          ClusterManager state.
   */
  public String getCMStateFile() {
    return get(CM_STATE_FILE, "cm.state");
  }

  /**
   * Return the flag which indicates if we will be using compression while
   * saving the ClusterManager state.
   *
   * @return A boolean, which is true if we are going to use compression while
   *          saving the CM state.
   */
  public boolean getCMCompressStateFlag() {
    return getBoolean(CM_COMPRESS_STATE, false);
  }

  public int getCMNotifierThreadCount() {
    return getInt(CM_NOTIFIER_THREAD_COUNT, 17);
  }

  /**
   * Get the general config file location
   *
   * @return General config file location (default if not set)
   */
  public String getConfigFile() {
    return get(CONFIG_FILE_PROPERTY, DEFAULT_CONFIG_FILE);
  }

  /**
   * Get the pools config file location
   *
   * @return Pools config file location (default if not set)
   */
  public String getPoolsConfigFile() {
    return get(POOLS_CONFIG_FILE_PROPERTY, DEFAULT_POOLS_CONFIG_FILE);
  }

  /**
   * Only allow configured pools?
   *
   * @return True if only configured pools is allowed, false otherwise
   */
  public boolean onlyAllowConfiguredPools() {
    return getBoolean(CONFIGURED_POOLS_ONLY, false);
  }

  /**
   * Get the milliseconds to wait between trying to generate pools config
   *
   * @return Milliseconds to wait between trying to generate pools config
   */
  public long getPoolsReloadPeriodMs() {
    // Default of 5 minutes
    return getLong(POOLS_RELOAD_PERIOD_MS_PROPERTY, 5 * 60000);
  }

  /**
   * Get the milliseconds to wait between reloading config files
   *
   * @return Milliseconds to wait between reloading config files
   */
  public long getConfigReloadPeriodMs() {
    // Default of 1 minute
    return getLong(CONFIG_RELOAD_PERIOD_MS_PROPERTY, 60000);
  }

  /**
   * Get the pools config document generator class
   *
   * @return Null if not specified, otherwise the generator class.
   */
  public Class<?> getPoolsConfigDocumentGeneratorClass() {
    return getClass(POOLS_CONFIG_DOCUMENT_GENERATOR_PROPERTY, null);
  }

  public int  getCoronaNodeRestartBatch() {
    return getInt(CORONA_NODE_RESTART_BATCH, 1000);
  }

  public long getCoronaNodeRestartInterval() {
    return getLong(CORONA_NODE_RESTART_INTERVAL, 1800000L);
  }

  public long getCMHeartbeatDelayMax() {
    return getLong(CM_HEARTBEAT_DELAY_MAX, 600000);
  }
}

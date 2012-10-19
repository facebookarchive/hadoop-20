package org.apache.hadoop.corona;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test the pool manager and various names.
 */
public class TestPoolManager extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/corona/test/data")).getAbsolutePath();
  private static final CoronaConf conf = new CoronaConf(new Configuration());
  private static final String CONFIG_FILE_PATH = new File(TEST_DIR,
      (new CoronaConf(new Configuration())).
      getConfigFile()).getAbsolutePath();
  final static List<ResourceType> TYPES =
      Collections.unmodifiableList(
          Arrays.asList(ResourceType.MAP, ResourceType.REDUCE));

  @Override
  public void setUp() {
    // Use the same config file for general config and pools config
    conf.set(CoronaConf.CONFIG_FILE_PROPERTY, CONFIG_FILE_PATH);
    conf.set(CoronaConf.POOLS_CONFIG_FILE_PROPERTY, CONFIG_FILE_PATH);
  }

  /**
   * Test various usernames without any configured pools.
   */
  public void testUsername() {
    CoronaConf conf = new CoronaConf(new Configuration());
    ConfigManager configManager = new ConfigManager();
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, false);
    SessionInfo sessionInfo = new SessionInfo();

    Session session = new Session(10000, "user", sessionInfo, configManager);
    PoolInfo poolInfo =
        PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("user"));

    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, true);
    poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals(PoolGroupManager.DEFAULT_POOL));
  }

  /**
   * When using configured pools, the configured poolnames should work.
   * @throws IOException
   */
  public void testConfiguredPools() throws IOException {
    new File(TEST_DIR).mkdir();
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);

    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <defaultSchedulingMode>FAIR</defaultSchedulingMode>\n");
    out.write("  <group name=\"groupA\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <schedulingMode>FIFO</schedulingMode>\n");
    out.write("    </pool>");
    out.write("    <pool name=\"poolB\">\n");
    out.write("      <schedulingMode>FAIR</schedulingMode>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    CoronaConf conf = new CoronaConf(new Configuration());
    ConfigManager configManager = new ConfigManager(TYPES, conf);
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, false);
    SessionInfo sessionInfo = new SessionInfo();

    Session session = new Session(10000, "user", sessionInfo, configManager);
    PoolInfo poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("user"));

    session = new Session(10000, "poolA", sessionInfo, configManager);
    poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("poolA"));

    session = new Session(10000, "poolC", sessionInfo, configManager);
    poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("poolC"));

    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, true);
    session = new Session(10000, "user", sessionInfo, configManager);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("user"));

    session = new Session(10000, "poolA", sessionInfo, configManager);
    poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals("poolA"));

    session = new Session(10000, "poolC", sessionInfo, configManager);
    poolInfo = PoolGroupManager.getPoolInfo(session);
    assert(poolInfo.getPoolGroupName().equals(
        PoolGroupManager.DEFAULT_POOL_GROUP));
    assert(poolInfo.getPoolName().equals(PoolGroupManager.DEFAULT_POOL));
  }
}

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
  final static String CONFIG_FILE_PATH = new File(TEST_DIR,
      CoronaConf.POOL_CONFIG_FILE).getAbsolutePath();
  final static List<ResourceType> TYPES =
      Collections.unmodifiableList(
          Arrays.asList(ResourceType.MAP, ResourceType.REDUCE));

  /**
   * Test various usernames without any configured pools.
   */
  public void testUsername() {
    CoronaConf conf = new CoronaConf(new Configuration());
    ConfigManager configManager = new ConfigManager();
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, false);
    PoolManager poolManager =
        new PoolManager(ResourceType.MAP, configManager, conf);
    Session session = new Session("user", null);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("user"));
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, true);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("default"));
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
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <schedulingMode>FIFO</schedulingMode>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolB\">\n");
    out.write("    <schedulingMode>FAIR</schedulingMode>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    CoronaConf conf = new CoronaConf(new Configuration());
    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, false);
    PoolManager poolManager =
        new PoolManager(ResourceType.MAP, configManager, conf);
    Session session = new Session("user", null);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("user"));
    session = new Session("poolA", null);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("poolA"));
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, true);
    session = new Session("user", null);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("default"));
    session = new Session("poolA", null);
    assert(poolManager.getPoolName(
        session, configManager, conf).equals("poolA"));
  }
}

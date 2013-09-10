package org.apache.hadoop.corona;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.json.JSONException;

import junit.framework.TestCase;

public class TestConfigManager extends TestCase {

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/corona/test/data")).getAbsolutePath();
  private final CoronaConf conf = new CoronaConf(new Configuration());
  private static final String CONFIG_FILE_PATH = new File(TEST_DIR,
      (new CoronaConf(new Configuration())).
      getConfigFile()).getAbsolutePath();
  final static List<ResourceType> TYPES =
      Collections.unmodifiableList(
          Arrays.asList(ResourceType.MAP, ResourceType.REDUCE));

  @Override
  protected void setUp() throws Exception {
    // Use the same config file for general config and pools config
    conf.set(CoronaConf.CONFIG_FILE_PROPERTY, CONFIG_FILE_PATH);
    conf.set(CoronaConf.POOLS_CONFIG_FILE_PROPERTY, CONFIG_FILE_PATH);
    new File(TEST_DIR).mkdirs();
  }

  @Override
  protected void tearDown() throws Exception {
    new File(TEST_DIR).delete();
  }

  public void testEmptyFile() throws IOException {
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration />\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    PoolInfo poolInfo = new PoolInfo("defaultGroup", "myPool");
    assertEquals(ScheduleComparator.FIFO, configManager.getPoolComparator(poolInfo));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfo, ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfo, ResourceType.REDUCE));
    assertEquals(0, configManager.getPoolMinimum(poolInfo, ResourceType.MAP));
    assertEquals(0, configManager.getPoolMinimum(poolInfo, ResourceType.REDUCE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.RACK));
    assertEquals(0, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.RACK));
    assertEquals(1.0, configManager.getWeight(poolInfo));
  }

  public void testPoolComparator() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <defaultSchedulingMode>FAIR</defaultSchedulingMode>\n");
    out.write("  <group name=\"defaultGroup\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <schedulingMode>FIFO</schedulingMode>\n");
    out.write("    </pool>");
    out.write("    <pool name=\"poolB\">\n");
    out.write("      <schedulingMode>FAIR</schedulingMode>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(ScheduleComparator.FIFO,
        configManager.getPoolComparator(new PoolInfo("defaultGroup",
                                                     "poolA")));
    assertEquals(ScheduleComparator.FAIR,
        configManager.getPoolComparator(new PoolInfo("defaultGroup",
                                                     "poolB")));
    assertEquals(ScheduleComparator.FAIR,
        configManager.getPoolComparator(new PoolInfo("defaultGroup",
                                                     "poolC")));
  }

  public void testPoolGroupComparator() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <group name=\"firstGroup\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(ScheduleComparator.PRIORITY,
        configManager.getPoolGroupComparator("firstGroup"));
  }

  public void testMinTasks() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <group name=\"groupA\">\n");
    out.write("    <minMAP>10</minMAP>\n");
    out.write("    <minREDUCE>20</minREDUCE>\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <minMAP>1</minMAP>\n");
    out.write("      <minREDUCE>2</minREDUCE>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("  <group name=\"groupB\">\n");
    out.write("    <pool name=\"poolB\">\n");
    out.write("      <minMAP>3</minMAP>\n");
    out.write("    </pool>");
    out.write("    <pool name=\"poolC\">\n");
    out.write("      <minREDUCE>4</minREDUCE>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    PoolInfo poolInfoA = new PoolInfo("groupA", "poolA");
    PoolInfo poolInfoB = new PoolInfo("groupB", "poolB");
    PoolInfo poolInfoC = new PoolInfo("groupB", "poolC");
    PoolInfo poolInfoD = new PoolInfo("groupB", "poolD");

    assertEquals(10, configManager.getPoolGroupMinimum(
        poolInfoA.getPoolGroupName(), ResourceType.MAP));
    assertEquals(20, configManager.getPoolGroupMinimum(
        poolInfoA.getPoolGroupName(), ResourceType.REDUCE));

    assertEquals(1, configManager.getPoolMinimum(poolInfoA, ResourceType.MAP));
    assertEquals(2, configManager.getPoolMinimum(poolInfoA, ResourceType.REDUCE));
    assertEquals(3, configManager.getPoolMinimum(poolInfoB, ResourceType.MAP));
    assertEquals(0, configManager.getPoolMinimum(poolInfoB, ResourceType.REDUCE));
    assertEquals(0, configManager.getPoolMinimum(poolInfoC, ResourceType.MAP));
    assertEquals(4, configManager.getPoolMinimum(poolInfoC, ResourceType.REDUCE));
    assertEquals(0, configManager.getPoolMinimum(poolInfoD, ResourceType.MAP));
    assertEquals(0, configManager.getPoolMinimum(poolInfoD, ResourceType.REDUCE));
  }

  public void testMaxTasks() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <group name=\"groupA\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <maxMAP>1</maxMAP>\n");
    out.write("      <maxREDUCE>2</maxREDUCE>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("  <group name=\"groupB\">\n");
    out.write("    <pool name=\"poolB\">\n");
    out.write("      <maxMAP>3</maxMAP>\n");
    out.write("    </pool>");
    out.write("    <pool name=\"poolC\">\n");
    out.write("      <maxREDUCE>4</maxREDUCE>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    PoolInfo poolInfoA = new PoolInfo("groupA", "poolA");
    PoolInfo poolInfoB = new PoolInfo("groupB", "poolB");
    PoolInfo poolInfoC = new PoolInfo("groupB", "poolC");
    PoolInfo poolInfoD = new PoolInfo("groupD", "poolD");

    assertEquals(1, configManager.getPoolMaximum(poolInfoA, ResourceType.MAP));
    assertEquals(2, configManager.getPoolMaximum(poolInfoA, ResourceType.REDUCE));
    assertEquals(3, configManager.getPoolMaximum(poolInfoB, ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfoB, ResourceType.REDUCE));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfoC, ResourceType.MAP));
    assertEquals(4, configManager.getPoolMaximum(poolInfoC, ResourceType.REDUCE));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfoD, ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getPoolMaximum(poolInfoD, ResourceType.REDUCE));
  }

  public void testWeight() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <group name=\"defaultGroup\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <weight>2.0</weight>\n");
    out.write("      <maxREDUCE>2</maxREDUCE>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    PoolInfo poolInfoA = new PoolInfo("defaultGroup", "poolA");
    PoolInfo poolInfoB = new PoolInfo("defaultGroup", "poolB");

    assertEquals(2.0, configManager.getWeight(poolInfoA));
    assertEquals(1.0, configManager.getWeight(poolInfoB));
  }

  public void testPriority() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <group name=\"firstGroup\">\n");
    out.write("    <pool name=\"poolA\">\n");
    out.write("      <priority>2</priority>\n");
    out.write("    </pool>");
    out.write("    <pool name=\"poolB\">\n");
    out.write("      <priority>1</priority>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("  <group name=\"secondGroup\">\n");
    out.write("    <pool name=\"poolC\">\n");
    out.write("      <priority>3</priority>\n");
    out.write("    </pool>");
    out.write("  </group>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(2, configManager.getPriority(new PoolInfo("firstGroup",
        "poolA")));
    assertEquals(1, configManager.getPriority(new PoolInfo("firstGroup",
        "poolB")));
    assertEquals(3, configManager.getPriority(new PoolInfo("secondGroup",
        "poolC")));
  }


  public void testLocalityWait() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitMAP>1000</nodeLocalityWaitMAP>\n");
    out.write("  <rackLocalityWaitMAP>3000</rackLocalityWaitMAP>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <weight>2.0</weight>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(1000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));
    assertEquals(3000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.RACK));
    assertEquals(0L, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.NODE));
    assertEquals(0L, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.RACK));
  }

  public void testPreemptParameters() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <shareStarvingRatio>0.1</shareStarvingRatio>\n");
    out.write("  <starvingTimeForShare>3000</starvingTimeForShare>\n");
    out.write("  <starvingTimeForMinimum>4000</starvingTimeForMinimum>\n");
    out.write("  <preemptedTaskMaxRunningTime>5000</preemptedTaskMaxRunningTime>\n");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(0.1, configManager.getShareStarvingRatio());
    assertEquals(3000L, configManager.getStarvingTimeForShare());
    assertEquals(4000L, configManager.getStarvingTimeForMinimum());
    assertEquals(5000L, configManager.getPreemptedTaskMaxRunningTime());
  }

  public void testRedirect() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <redirect source=\"source.pool_source\" " +
        "destination=\"destination.pool_destination\" />\n");
    // Shouldn't cause an issue
    out.write("  <redirect source=\"\" " +
        "destination=\"should.notwork\" />\n");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    PoolInfo sourcePoolInfo = new PoolInfo("source", "pool_source");
    PoolInfo destinationPoolInfo = new PoolInfo("destination",
        "pool_destination");
    assertEquals(destinationPoolInfo,
        configManager.getRedirect(destinationPoolInfo));
    assertEquals(destinationPoolInfo,
        configManager.getRedirect(sourcePoolInfo));
    assertEquals(null, configManager.getRedirect(null));
  }

  public void testReload() throws IOException, SAXException, 
                                  ParserConfigurationException, JSONException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitMAP>1000</nodeLocalityWaitMAP>\n");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, conf);
    assertEquals(1000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));

    out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitMAP>3000</nodeLocalityWaitMAP>\n");
    out.write("</configuration>\n");
    out.close();

    // Set the modification time so it gets reloaded
    new File(CONFIG_FILE_PATH).setLastModified(0);
    configManager.reloadAllConfig(false);
    assertEquals(3000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));
  }

  static class TestPoolsConfigDocumentGenerator implements
      PoolsConfigDocumentGenerator {
    /** Changes over time */
    private int minMap = 10;
    /** Fail? */
    private boolean fail = false;

    @Override
    public void initialize(CoronaConf conf) {
      fail = conf.getBoolean("test.fail", false);
      // Nothing to do here.
    }

    @Override
    public Document generatePoolsDocument() {
      // Fake a failure?
      if (fail == true) {
        return null;
      }
      minMap += 10;
      DocumentBuilderFactory documentBuilderFactory =
          DocumentBuilderFactory.newInstance();
      Document document;
      try {
        document =
            documentBuilderFactory.newDocumentBuilder().newDocument();
      } catch (ParserConfigurationException e) {
        throw new IllegalStateException(
            "generatePoolConfig: Failed to create a new document");
      }

      Element root =
          document.createElement(ConfigManager.CONFIGURATION_TAG_NAME);
      document.appendChild(root);
      Element group =
          document.createElement(ConfigManager.GROUP_TAG_NAME);
      group.setAttribute(ConfigManager.NAME_ATTRIBUTE, "testGroup");
      root.appendChild(group);
      Element maps =
          document.createElement(
              ConfigManager.MIN_TAG_NAME_PREFIX + ResourceType.MAP);
      maps.setTextContent(Integer.toString(minMap));
      group.appendChild(maps);

      return document;
    }
  }

  public void testPoolsConfigGenerator() throws InterruptedException {
    conf.setClass(CoronaConf.POOLS_CONFIG_DOCUMENT_GENERATOR_PROPERTY,
        TestPoolsConfigDocumentGenerator.class,
        PoolsConfigDocumentGenerator.class);
    conf.setLong(CoronaConf.POOLS_RELOAD_PERIOD_MS_PROPERTY, 100);
    conf.setLong(CoronaConf.CONFIG_RELOAD_PERIOD_MS_PROPERTY, 100);
    ConfigManager configManager = new ConfigManager(TYPES, conf);
    int minMap =
        configManager.getPoolGroupMinimum("testGroup", ResourceType.MAP);
    assert((minMap % 10) == 0);
    // Long enough for at least 5 changes
    Thread.sleep(500);
    int changedMinMap =
        configManager.getPoolGroupMinimum("testGroup", ResourceType.MAP);
    assert((changedMinMap % 10) == 0);
    assert(changedMinMap > minMap);
  }

  public void testFailPoolsConfigGenerator() throws InterruptedException {
    conf.setClass(CoronaConf.POOLS_CONFIG_DOCUMENT_GENERATOR_PROPERTY,
        TestPoolsConfigDocumentGenerator.class,
        PoolsConfigDocumentGenerator.class);
    conf.setLong(CoronaConf.POOLS_RELOAD_PERIOD_MS_PROPERTY, 100);
    conf.setLong(CoronaConf.CONFIG_RELOAD_PERIOD_MS_PROPERTY, 100);
    conf.setBoolean("test.fail", true);
    try {
      ConfigManager configManager = new ConfigManager(TYPES, conf);
      // Should have thrown an exception
      assertEquals(true, false);
    } catch (IllegalStateException e) {
      // Passed!
      System.out.println("Got expected exception " + e.getMessage());
    }
  }
}

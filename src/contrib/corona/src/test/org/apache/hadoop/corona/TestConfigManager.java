package org.apache.hadoop.corona;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import junit.framework.TestCase;

public class TestConfigManager extends TestCase {

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/corona/test/data")).getAbsolutePath();
  final static String CONFIG_FILE_PATH = new File(TEST_DIR,
      CoronaConf.POOL_CONFIG_FILE).getAbsolutePath();
  final static List<ResourceType> TYPES =
      Collections.unmodifiableList(
          Arrays.asList(ResourceType.MAP, ResourceType.REDUCE));

  @Override
  protected void setUp() throws Exception {
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

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    String poolName = "myPool";
    assertEquals(ScheduleComparator.FIFO, configManager.getComparator(poolName));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum(poolName, ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum(poolName, ResourceType.REDUCE));
    assertEquals(0, configManager.getMinimum(poolName, ResourceType.MAP));
    assertEquals(0, configManager.getMinimum(poolName, ResourceType.REDUCE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.RACK));
    assertEquals(0, configManager.getLocalityWait(ResourceType.REDUCE, LocalityLevel.RACK));
    assertEquals(1.0, configManager.getWeight(poolName));
  }

  public void testComparator() throws IOException {
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

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(ScheduleComparator.FIFO, configManager.getComparator("poolA"));
    assertEquals(ScheduleComparator.FAIR, configManager.getComparator("poolB"));
    assertEquals(ScheduleComparator.FAIR, configManager.getComparator("poolC"));
  }

  public void testMinTasks() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <minMAP>1</minMAP>\n");
    out.write("    <minREDUCE>2</minREDUCE>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolB\">\n");
    out.write("    <minMAP>3</minMAP>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolC\">\n");
    out.write("    <minREDUCE>4</minREDUCE>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1, configManager.getMinimum("poolA", ResourceType.MAP));
    assertEquals(2, configManager.getMinimum("poolA", ResourceType.REDUCE));
    assertEquals(3, configManager.getMinimum("poolB", ResourceType.MAP));
    assertEquals(0, configManager.getMinimum("poolB", ResourceType.REDUCE));
    assertEquals(0, configManager.getMinimum("poolC", ResourceType.MAP));
    assertEquals(4, configManager.getMinimum("poolC", ResourceType.REDUCE));
    assertEquals(0, configManager.getMinimum("poolD", ResourceType.MAP));
    assertEquals(0, configManager.getMinimum("poolD", ResourceType.REDUCE));
  }

  public void testMaxTasks() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <maxMAP>1</maxMAP>\n");
    out.write("    <maxREDUCE>2</maxREDUCE>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolB\">\n");
    out.write("    <maxMAP>3</maxMAP>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolC\">\n");
    out.write("    <maxREDUCE>4</maxREDUCE>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1, configManager.getMaximum("poolA", ResourceType.MAP));
    assertEquals(2, configManager.getMaximum("poolA", ResourceType.REDUCE));
    assertEquals(3, configManager.getMaximum("poolB", ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolB", ResourceType.REDUCE));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolC", ResourceType.MAP));
    assertEquals(4, configManager.getMaximum("poolC", ResourceType.REDUCE));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolD", ResourceType.MAP));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolD", ResourceType.REDUCE));
  }

  public void testWeight() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <weight>2.0</weight>\n");
    out.write("    <maxREDUCE>2</maxREDUCE>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(2.0, configManager.getWeight("poolA"));
    assertEquals(1.0, configManager.getWeight("poolB"));
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

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
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

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(0.1, configManager.getShareStarvingRatio());
    assertEquals(3000L, configManager.getStarvingTimeForShare());
    assertEquals(4000L, configManager.getStarvingTimeForMinimum());
    assertEquals(5000L, configManager.getPreemptedTaskMaxRunningTime());
  }

  public void testReload() throws IOException, SAXException, ParserConfigurationException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitMAP>1000</nodeLocalityWaitMAP>\n");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));

    out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitMAP>3000</nodeLocalityWaitMAP>\n");
    out.write("</configuration>\n");
    out.close();

    // Set the modification time so it gets reloaded
    new File(CONFIG_FILE_PATH).setLastModified(0);
    configManager.reloadConfig();
    assertEquals(3000L, configManager.getLocalityWait(ResourceType.MAP, LocalityLevel.NODE));
  }
}

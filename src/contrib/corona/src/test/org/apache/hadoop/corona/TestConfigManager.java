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
  final static List<String> TYPES =
      Collections.unmodifiableList((Arrays.asList("M", "R")));
  final static String M = TYPES.get(0);
  final static String R = TYPES.get(1);

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
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum(poolName, M));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum(poolName, R));
    assertEquals(0, configManager.getMinimum(poolName, M));
    assertEquals(0, configManager.getMinimum(poolName, R));
    assertEquals(0, configManager.getLocalityWait(M, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(R, LocalityLevel.NODE));
    assertEquals(0, configManager.getLocalityWait(M, LocalityLevel.RACK));
    assertEquals(0, configManager.getLocalityWait(R, LocalityLevel.RACK));
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
    out.write("    <minM>1</minM>\n");
    out.write("    <minR>2</minR>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolB\">\n");
    out.write("    <minM>3</minM>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolC\">\n");
    out.write("    <minR>4</minR>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1, configManager.getMinimum("poolA", M));
    assertEquals(2, configManager.getMinimum("poolA", R));
    assertEquals(3, configManager.getMinimum("poolB", M));
    assertEquals(0, configManager.getMinimum("poolB", R));
    assertEquals(0, configManager.getMinimum("poolC", M));
    assertEquals(4, configManager.getMinimum("poolC", R));
    assertEquals(0, configManager.getMinimum("poolD", M));
    assertEquals(0, configManager.getMinimum("poolD", R));
  }

  public void testMaxTasks() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <maxM>1</maxM>\n");
    out.write("    <maxR>2</maxR>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolB\">\n");
    out.write("    <maxM>3</maxM>\n");
    out.write("  </pool>");
    out.write("  <pool name=\"poolC\">\n");
    out.write("    <maxR>4</maxR>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1, configManager.getMaximum("poolA", M));
    assertEquals(2, configManager.getMaximum("poolA", R));
    assertEquals(3, configManager.getMaximum("poolB", M));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolB", R));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolC", M));
    assertEquals(4, configManager.getMaximum("poolC", R));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolD", M));
    assertEquals(Integer.MAX_VALUE, configManager.getMaximum("poolD", R));
  }

  public void testWeight() throws IOException {
    FileWriter out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <weight>2.0</weight>\n");
    out.write("    <maxR>2</maxR>\n");
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
    out.write("  <nodeLocalityWaitM>1000</nodeLocalityWaitM>\n");
    out.write("  <rackLocalityWaitM>3000</rackLocalityWaitM>\n");
    out.write("  <pool name=\"poolA\">\n");
    out.write("    <weight>2.0</weight>\n");
    out.write("  </pool>");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1000L, configManager.getLocalityWait(M, LocalityLevel.NODE));
    assertEquals(3000L, configManager.getLocalityWait(M, LocalityLevel.RACK));
    assertEquals(0L, configManager.getLocalityWait(R, LocalityLevel.NODE));
    assertEquals(0L, configManager.getLocalityWait(R, LocalityLevel.RACK));
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
    out.write("  <nodeLocalityWaitM>1000</nodeLocalityWaitM>\n");
    out.write("</configuration>\n");
    out.close();

    ConfigManager configManager = new ConfigManager(TYPES, CONFIG_FILE_PATH);
    assertEquals(1000L, configManager.getLocalityWait(M, LocalityLevel.NODE));
    
    out = new FileWriter(CONFIG_FILE_PATH);
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
    out.write("  <nodeLocalityWaitM>3000</nodeLocalityWaitM>\n");
    out.write("</configuration>\n");
    out.close();
    
    // Set the modification time so it gets reloaded
    new File(CONFIG_FILE_PATH).setLastModified(0);
    configManager.reloadConfig();
    assertEquals(3000L, configManager.getLocalityWait(M, LocalityLevel.NODE));
  }
}

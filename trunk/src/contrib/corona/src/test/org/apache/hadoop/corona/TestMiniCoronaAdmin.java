package org.apache.hadoop.corona;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

public class TestMiniCoronaAdmin extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/corona/test/data")).getAbsolutePath();
  private static final Log LOG = LogFactory.getLog(TestMiniCoronaAdmin.class);
  private MiniCoronaCluster corona = null;

  public void testExclude() throws Exception {
    LOG.info("Starting testExlude");
    JobConf conf = new JobConf();
    // Start with an empty excludes file.
    File excludesFile = new File(TEST_DIR, "excludes");
    conf.set(CoronaConf.EXCLUDE_HOSTS_FILE, excludesFile.getAbsolutePath());
    excludesFile.delete();
    excludesFile.createNewFile();

    corona = new MiniCoronaCluster.Builder().conf(conf).numTaskTrackers(1)
        .build();

    // 0 excluded nodes at first.
    NodeManager nm = corona.getClusterManager().nodeManager;
    assertEquals(0, nm.getExcludedNodeCount());
    
    // Wait for 1 node to be alive.
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 30 * 1000 &&
           nm.getAliveNodeCount() < 1) {
      Thread.sleep(100);
    }
    List<String> aliveNodes = nm.getAliveNodes();
    assertEquals(1, aliveNodes.size());

    // Exclude the only host.
    PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
        excludesFile)));
    writer.println(aliveNodes.get(0));
    writer.close();

    conf.set(CoronaConf.CM_ADDRESS,
        "localhost:" + corona.getClusterManagerPort());
    CoronaAdmin coronaAdmin = new CoronaAdmin();
    coronaAdmin.setConf(conf);
    String[] args = { "-refreshNodes" };
    int ret = ToolRunner.run(coronaAdmin, args);
    assertEquals("Refresh nodes failed", 0, ret);

    // Should have 1 excluded host now.
    assertEquals(1, nm.getExcludedNodes().size());
  }

  @Override
  protected void setUp() {
    new File(TEST_DIR).mkdirs();
  }

  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }
}

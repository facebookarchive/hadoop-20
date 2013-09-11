package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.TestGenericJournalConf.DummyJournalManager;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Startup and checkpoint tests
 * 
 */
public class TestAllowFormat {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private static final Log LOG =
    LogFactory.getLog(TestStartup.class.getName());
  private Configuration config;
  private File hdfsDir=null;

  @Before
  public void setUp() throws Exception {
    config = new Configuration();
    String baseDir = System.getProperty("test.build.data", "/tmp");

    hdfsDir = new File(baseDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    LOG.info("--hdfsdir is " + hdfsDir.getAbsolutePath());
    config.set("dfs.name.dir", new File(hdfsDir, "name").getPath());
    config.set("dfs.data.dir", new File(hdfsDir, "data").getPath());

    config.set("fs.checkpoint.dir",new File(hdfsDir, "secondary").getPath());

    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
  }

  /**
   * clean up
   */
  @After
  public void tearDown() throws Exception {
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '"
                            + hdfsDir + "'");
    }	
  }

   /**
   * start MiniDFScluster, try formatting with different settings
   * @throws IOException
   */
  @Test
  public void testAllowFormat() throws IOException {
    LOG.info("--starting mini cluster");
    // manage dirs parameter set to false 
    MiniDFSCluster cluster = null;
    NameNode nn;
    // 1. Create a new cluster and format DFS
    try {
      config.setBoolean("dfs.namenode.support.allowformat", true);
      cluster = new MiniDFSCluster(0, config, 1, true, false, false,  null, 
				   null, null, null);
      cluster.waitActive();
      assertNotNull(cluster);

      nn = cluster.getNameNode();
      assertNotNull(nn);
      LOG.info("--mini cluster created OK");
    } catch (IOException e) {
      fail(StringUtils.stringifyException(e));
      System.err.println("Could not create/format cluster");
      throw e;
    }
    // 2. Try formatting DFS with allowformat false.
    // NOTE: the cluster must be shut down for format to work.
    LOG.info("--verifying format will fail with allowformat false");
    config.setBoolean("dfs.namenode.support.allowformat", false);
    try {
      cluster.shutdown();
      nn.format(config);
      fail("Format succeeded, when it should have failed");
    } catch (IOException e) { // expected to fail
      LOG.info("Expected failure: " + StringUtils.stringifyException(e));
      LOG.info("--done verifying format will fail with allowformat false");
    }
    // 3. Try formatting DFS with allowformat true
    LOG.info("--verifying format will succeed with allowformat true");
    config.setBoolean("dfs.namenode.support.allowformat", true);
    try {
      nn.format(config);
      LOG.info("--done verifying format will succeed with allowformat true");
    } catch (IOException e) {
      fail(StringUtils.stringifyException(e));
      System.err.println("Format with allowformat true failed");
      throw e;
    }
    if (cluster!=null) {
        cluster.shutdown();
        LOG.info("--stopping mini cluster");
    }
  }
  
  /**
   * Test to ensure that format is called for non-file journals.
   */
  @Test
  public void testFormatShouldBeIgnoredForNonFileBasedDirs() throws Exception {
    Configuration conf = new Configuration();
    File nameDir = new File(hdfsDir, "name");
    if (nameDir.exists()) {
      FileUtil.fullyDelete(nameDir);
    }
        
    conf.setBoolean("dfs.namenode.support.allowformat", true);
    conf.set("dfs.name.edits.journal-plugin" + ".dummy",
        DummyJournalManager.class.getName());
    conf.set("dfs.name.edits.dir",
        "dummy://test");
    conf.set("dfs.name.dir", nameDir.getPath());

    NameNode.format(conf, false, true);
    assertTrue(DummyJournalManager.formatCalled);
    assertTrue(DummyJournalManager.shouldPromptCalled);
  }
}

package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import junit.framework.TestCase;

public class TestNameNodeUpgrade extends TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Start the NN in regular mode and the upgradeTime should be zero.
   * @throws IOException
   * @throws InterruptedException
   */
  public void testNnNoUpgrade() throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
	    Thread.sleep(1000 * 60);
	    FSNamesystem ns = cluster.getNameNode().getNamesystem();
	    assertTrue(ns.getUpgradeTime() == 0);
    } finally {
    	cluster.shutdown();
    }
  }

  /**
   * Start the NN in upgrade mode and verify the upgradeTime
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private MiniDFSCluster startNnInUpgrade() 
    throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 1, true, true, 
        StartupOption.UPGRADE, null);
    Thread.sleep(1000 * 60);
    FSNamesystem ns = cluster.getNameNode().getNamesystem();
    assertTrue(ns.getUpgradeTime() >= 1);  

    return cluster;
  }
  
  /**
   * Restart the NN and verify the upgradeTime in upgrade status.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void testNnUpgrade1() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
	    cluster = startNnInUpgrade();
	    cluster.restartNameNodes();
	    Thread.sleep(1000 * 60);
	    FSNamesystem ns = cluster.getNameNode().getNamesystem();
	    assertTrue(ns.getUpgradeTime() >= 1);
    } finally {
    	if (null != cluster) {
    		cluster.shutdown();
    	}
    }
  }
  
  /**
   * Finalize the NN and verify the upgradeTime.
   * @throws Exception
   */
  public void testNnUpgrade2() throws Exception {
    MiniDFSCluster cluster = null; 
    try {
	    cluster = startNnInUpgrade();
	    cluster.finalizeCluster(cluster.getNameNode().getConf());
	
	    FSNamesystem ns = cluster.getNameNode().getNamesystem();
	    assertTrue(ns.getUpgradeTime() == 0);
    } finally {
    	if (null != cluster) {
    		cluster.shutdown();
    	}
    }
  }
}

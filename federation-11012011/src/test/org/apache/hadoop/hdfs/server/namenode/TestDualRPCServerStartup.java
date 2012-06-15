package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestSafeMode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

import junit.framework.TestCase;

public class TestDualRPCServerStartup extends TestCase {
  static Log LOG = LogFactory.getLog(TestDualRPCServerStartup.class);

  public void testClientNodeStartup() {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      // disable safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      NameNode.setDNProtocolAddress(conf, "localhost:0");
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();

      fs = cluster.getFileSystem();
      Path file1 = new Path("/tmp/testManualSafeMode/file1");
      Path file2 = new Path("/tmp/testManualSafeMode/file2");

      LOG.info("Created file1 and file2.");

      // create two files with one block each.
      DFSTestUtil.createFile(fs, file1, 1000, (short) 1, 0);
      DFSTestUtil.createFile(fs, file2, 2000, (short) 1, 0);
      cluster.shutdown();

      LOG.info("Creating DFSAdmin");
      // now bring up just the NameNode.
      cluster = new MiniDFSCluster(conf, 0, false, null);
      // waitActive doesn't work since it uses DFSClient
      LOG.info("Creating DFSAdmin");
      // DFSAdmin modifies the conf so we need a copy of it
      Configuration adminConf = new Configuration(conf);
      
      DFSAdmin admin = new DFSAdmin(adminConf);
      try {
        admin.run(new String[] { "-refreshNodes" });
      } catch (Exception ex) {
        LOG.error(ex);
        fail();
      }

      NameNode namenode = cluster.getNameNode();

      assertTrue("No datanode is started. Should be in SafeMode", namenode
          .isInSafeMode());
      try {
        DFSClient client = new DFSClient(conf);
        fail("The client server should not be initialized yet");
      } catch (IOException ex) {
      }

      // now bring up the datanode and wait for it to be active.
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();

      LOG.info("Datanode is started.");
      try {
        DFSClient client = new DFSClient(conf);
      } catch (IOException ex) {
        fail("The client server should be up by now");
      }


    } catch (IOException ex) {
      LOG.info(ex);
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.StaticMapping;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestStartupDefaultRack {

  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @Test
  public void testStartup() throws Exception {
    conf = new Configuration();
    conf.setClass("dfs.block.replicator.classname",
        BlockPlacementPolicyConfigurable.class, BlockPlacementPolicy.class);
    File baseDir = MiniDFSCluster.getBaseDirectory(conf);
    baseDir.mkdirs();
    File hostsFile = new File(baseDir, "hosts");
    FileOutputStream out = new FileOutputStream(hostsFile);
    out.write("h1\n".getBytes());
    out.write("h2\n".getBytes());
    out.write("h3\n".getBytes());
    out.close();
    conf.set("dfs.hosts", hostsFile.getAbsolutePath());
    StaticMapping.addNodeToRack("h1", "/r1");
    StaticMapping.addNodeToRack("h2", "/r2");
    StaticMapping.addNodeToRack("h3", NetworkTopology.DEFAULT_RACK);
    cluster = new MiniDFSCluster(conf, 3, new String[] { "/r1", "/r2",
        NetworkTopology.DEFAULT_RACK }, new String[] { "h1", "h2", "h3" },
        true, false);
    DFSTestUtil util = new DFSTestUtil("/testStartup", 10, 10, 1024);
    util.createFiles(cluster.getFileSystem(), "/");
    util.checkFiles(cluster.getFileSystem(), "/");
    assertEquals(2,
        cluster.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length);
    cluster.shutdown();
  }
}

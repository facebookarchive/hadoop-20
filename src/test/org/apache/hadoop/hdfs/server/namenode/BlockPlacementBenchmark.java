package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;

import org.junit.After;
import org.junit.Test;

public class BlockPlacementBenchmark {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final long totalRuns = 10000000;
  private static final int BLOCK_SIZE = 1024;

  public void setUp(boolean configurable) throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    if (configurable) {
      conf.setClass("dfs.block.replicator.classname",
          BlockPlacementPolicyConfigurable.class, BlockPlacementPolicy.class);
      cluster = new MiniDFSCluster(conf, 10, null, null, true, true);
    } else {
      cluster = new MiniDFSCluster(conf, 10, true, null);
    }
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  private void runBenchmark(String testname) {
    BlockPlacementPolicy policy = cluster.getNameNode().namesystem.replicator;
    Random r = new Random();
    ArrayList <DatanodeDescriptor> dns = cluster.getNameNode().namesystem
      .getDatanodeListForReport(DatanodeReportType.ALL);
    long start = System.currentTimeMillis();
    for (long i = 0; i < totalRuns; i++) {
      policy.chooseTarget("", 3, dns.get(r.nextInt(dns.size())), BLOCK_SIZE);
    }
    System.out.println("TOTAL TIME FOR " + totalRuns + " runs : of " + testname
        + " : " + (System.currentTimeMillis() - start));
  }

  @Test
  public void runBenchmarkDefault() throws Exception {
    setUp(false);
    runBenchmark("BlockPlacementPolicyDefault");
  }

  @Test
  public void runBenchmarkConfigurable() throws Exception {
    setUp(false);
    runBenchmark("BlockPlacementPolicyConfigurable");
  }

}

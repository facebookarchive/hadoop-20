/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.Balancer.BalancerBlock;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancer {
  private static final Configuration CONF = new Configuration();
  final private static long CAPACITY = 500L;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";
  final private static String RACK2 = "/rack2";
  final static private String fileName = "/tmp.txt";
  final static private Path filePath = new Path(fileName);
  private MiniDFSCluster cluster;

  ClientProtocol client;

  static final int DEFAULT_BLOCK_SIZE = 10;
  private Random r = new Random();

  static {
    CONF.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    CONF.setInt("io.bytes.per.checksum", DEFAULT_BLOCK_SIZE);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    CONF.setLong("dfs.balancer.movedWinWidth", 2000L);
    CONF.setInt(BalancerConfigKeys.DFS_BALANCER_MIN_REPLICAS_KEY, 1);
    Balancer.setBlockMoveWaitTime(1000L) ;
  }

  /* create a file with a length of <code>fileLen</code> */
  private void createFile(long fileLen, short replicationFactor)
  throws IOException {
    FileSystem fs = null;
    try {
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, fileLen, 
          replicationFactor, r.nextLong());
      DFSTestUtil.waitReplication(fs, filePath, replicationFactor);
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }


  /* fill up a cluster with <code>numNodes</code> datanodes 
   * whose used space to be <code>size</code>
   */
  private Block[] generateBlocks(Configuration conf, long size, short numNodes,
      short replicationFactor) throws IOException {
    try {
      cluster = new MiniDFSCluster(conf, numNodes, true, null);
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      long fileLen = size/replicationFactor;
      createFile(fileLen, replicationFactor);

      List<LocatedBlock> locatedBlocks = client.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();

      int numOfBlocks = locatedBlocks.size();
      Block[] blocks = new Block[numOfBlocks];
      for(int i=0; i<numOfBlocks; i++) {
        Block b = locatedBlocks.get(i).getBlock();
        blocks[i] = new Block(b.getBlockId(), b.getNumBytes(), b.getGenerationStamp());
      }

      return blocks;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }
  }

  /* Distribute all blocks according to the given distribution */
  Block[][] distributeBlocks(Block[] blocks, short replicationFactor, 
      final long[] distribution ) {
    // make a copy
    long[] usedSpace = new long[distribution.length];
    System.arraycopy(distribution, 0, usedSpace, 0, distribution.length);

    List<List<Block>> blockReports = 
      new ArrayList<List<Block>>(usedSpace.length);
    Block[][] results = new Block[usedSpace.length][];
    for(int i=0; i<usedSpace.length; i++) {
      blockReports.add(new ArrayList<Block>());
    }
    for(int i=0; i<blocks.length; i++) {
      for(int j=0; j<replicationFactor; j++) {
        boolean notChosen = true;
        while(notChosen) {
          int chosenIndex = r.nextInt(usedSpace.length);
          if( usedSpace[chosenIndex]>0 ) {
            notChosen = false;
            blockReports.get(chosenIndex).add(blocks[i]);
            usedSpace[chosenIndex] -= blocks[i].getNumBytes();
          }
        }
      }
    }
    for(int i=0; i<usedSpace.length; i++) {
      List<Block> nodeBlockList = blockReports.get(i);
      results[i] = nodeBlockList.toArray(new Block[nodeBlockList.size()]);
    }
    return results;
  }

  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Afterwards a balancer is running to balance the cluster.
   */
  private void testUnevenDistribution(Configuration conf, long distribution[], long capacities[],
      String[] racks, short replicationFactor) throws Exception {
    int numDatanodes = distribution.length;
    if (capacities.length != numDatanodes || racks.length != numDatanodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    // calculate total space that need to be filled
    long totalUsedSpace=0L;
    for(int i=0; i<distribution.length; i++) {
      totalUsedSpace += distribution[i];
    }

    // fill the cluster
    Block[] blocks = generateBlocks(conf, totalUsedSpace, (short) numDatanodes,
        replicationFactor);

    // redistribute blocks
    Block[][] blocksDN = distributeBlocks(blocks, replicationFactor,
        distribution);

    // restart the cluster: do NOT format the cluster
    conf.set("dfs.safemode.threshold.pct", "0.0f");
    try {
      cluster = new MiniDFSCluster(0, conf, numDatanodes,
          false, true, null, racks, capacities);
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);
  
      cluster.injectBlocks(blocksDN);
  
      long totalCapacity = 0L;
      for(long capacity:capacities) {
        totalCapacity += capacity;
      }
      assertEquals(Balancer.SUCCESS, runBalancer(conf, totalUsedSpace, totalCapacity));
      assertBalanced(totalUsedSpace, totalCapacity);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }
    
  }

  /* wait for one heartbeat */
  private void waitForHeartBeat( long expectedUsedSpace, long expectedTotalSpace )
  throws IOException {
    long[] status = client.getStats();
    while(status[0] != expectedTotalSpace || status[1] != expectedUsedSpace ) {
      DFSTestUtil.waitNMilliSecond(100);
      status = client.getStats();
    }
  }

  /* This test start a one-node cluster, fill the node to be 30% full;
   * It then adds an empty node and start balancing.
   * @param newCapacity new node's capacity
   * @param new 
   */
  private void test(Configuration conf, long[] capacities, String[] racks, long newCapacity,
                    String newRack, int expectedStatus) throws Exception {
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    try {
      cluster = new MiniDFSCluster(0, conf, capacities.length, true, true, null,
          racks, capacities);
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      long totalCapacity=0L;
      for(long capacity:capacities) {
        totalCapacity += capacity;
      }
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      createFile(totalUsedSpace/numOfDatanodes, (short)numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null,
          new String[]{newRack}, new long[]{newCapacity});

      totalCapacity += newCapacity;

      // SUCCESS here means that balancer was successfully initialized for each of the
      // namenodes but cluster is not necessarily balanced
      assertEquals(Balancer.SUCCESS, runBalancer(conf, totalUsedSpace, totalCapacity));
      if (expectedStatus == Balancer.SUCCESS) {
        assertBalanced(totalUsedSpace, totalCapacity);
      } else {
        // If could not balance then should not touch
        long[] expected = new long[capacities.length + 1];
        for (int i = 0; i < capacities.length; i++) {
          expected[i] = totalUsedSpace / numOfDatanodes;
        }
        expected[capacities.length] = 0L;
        assertNotBalanced(totalUsedSpace, totalCapacity, expected);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }
  }

  /* Start balancer and check expected result */
  private int runBalancer(Configuration conf, long totalUsedSpace, long totalCapacity)
  throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);

    // start rebalancing
    final List<InetSocketAddress> namenodes =new ArrayList<InetSocketAddress>();
    namenodes.add(NameNode.getClientProtocolAddress(conf));
    return Balancer.run(namenodes, conf);
  }

  /** When function exits then cluster is balanced (no other guarantees, might loop forever) */
  private void assertBalanced(long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);
    boolean balanced;
    do {
      DatanodeInfo[] datanodeReport = 
        client.getDatanodeReport(DatanodeReportType.ALL);
      assertEquals(datanodeReport.length, cluster.getDataNodes().size());
      balanced = true;
      double avgUtilization = ((double)totalUsedSpace)/totalCapacity*100;
      for(DatanodeInfo datanode:datanodeReport) {
        double util = ((double) datanode.getDfsUsed()) / datanode.getCapacity()
            * 100;
        if (Math.abs(avgUtilization - util) > 10 || util > 99) {
          balanced = false;
          DFSTestUtil.waitNMilliSecond(100);
          break;
        }
      }
    } while(!balanced);
  }

  private void assertNotBalanced(long totalUsedSpace, long totalCapacity,
        long[] expectedUtilizations) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);
    DatanodeInfo[] datanodeReport = client.getDatanodeReport(DatanodeReportType.ALL);
    long[] utilizations = new long[expectedUtilizations.length];
    int i = 0;
    for (DatanodeInfo datanode : datanodeReport) {
      totalUsedSpace -= datanode.getDfsUsed();
      totalCapacity -= datanode.getCapacity();
      utilizations[i++] = datanode.getDfsUsed();
    }
    assertEquals(0, totalUsedSpace);
    assertEquals(0, totalCapacity);
    assertEquals(expectedUtilizations.length, utilizations.length);
    Arrays.sort(expectedUtilizations);
    Arrays.sort(utilizations);
    assertTrue(Arrays.equals(expectedUtilizations, utilizations));
  }

  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster*/
  @Test(timeout=60000)
  public void testBalancer0() throws Exception {
    /** one-node cluster test*/
    // add an empty node with half of the CAPACITY & the same rack
    test(new Configuration(CONF), new long[]{CAPACITY}, new String[]{RACK0}, CAPACITY / 2, RACK0,
        Balancer.SUCCESS);

    /** two-node cluster test */
    test(new Configuration(CONF), new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, Balancer.SUCCESS);
  }
  
  /** Test unevenly distributed cluster */
  @Test(timeout=60000)
  public void testBalancer1() throws Exception {
    testUnevenDistribution(new Configuration(CONF),
                                  new long[]{50 * CAPACITY / 100, 10 * CAPACITY / 100},
                                  new long[]{CAPACITY, CAPACITY},
                                  new String[]{RACK0, RACK1}, (short) 1);
  }

  @Test(timeout=60000)
  public void testBalancer2() throws Exception {
    testBalancerDefaultConstructor(new Configuration(CONF), new long[]{CAPACITY, CAPACITY},
                                          new String[]{RACK0, RACK1}, CAPACITY, RACK2);
  }

  @Test(timeout=60000)
  public void testBalancer3() throws Exception {
    testUnevenDistribution(new Configuration(CONF), new long[] { 995 * CAPACITY / 1000,
        90 * CAPACITY / 100, 995 * CAPACITY / 1000, 90 * CAPACITY / 100 },
        new long[] { CAPACITY, CAPACITY, CAPACITY, CAPACITY }, new String[] {
            RACK0, RACK0, RACK1, RACK1 }, (short) 1);
  }

  @Test(timeout=60000)
  public void testBalancer4() throws Exception {
    Configuration conf = new Configuration(CONF);
    conf.setInt(BalancerConfigKeys.DFS_BALANCER_MIN_REPLICAS_KEY, 2);
    test(conf, new long[]{CAPACITY}, new String[]{RACK0}, CAPACITY / 2, RACK0,
                Balancer.NO_MOVE_BLOCK);
  }

  @Test(timeout=60000)
  public void testBalancer6() throws Exception {
    Configuration conf = new Configuration(CONF);
    conf.setInt(BalancerConfigKeys.DFS_BALANCER_FETCH_COUNT_KEY, 0);
    test(conf, new long[]{CAPACITY}, new String[]{RACK0}, CAPACITY / 2, RACK0,
        Balancer.NO_MOVE_BLOCK);
  }

  @Test(timeout=60000)
  public void testBalancer8() throws Exception {
    testUnevenDistribution(new Configuration(CONF),
        new long[] { 20 * CAPACITY / 100, 20 * CAPACITY / 100, 20 * CAPACITY / 100,
            20 * CAPACITY / 100, 1 * CAPACITY / 100 },
        new long[] { CAPACITY, CAPACITY, CAPACITY, CAPACITY, CAPACITY },
        new String[] { RACK0, RACK0, RACK0, RACK0, RACK0 },
        (short) 1);
  }

  @Test(timeout=60000)
  public void testBalancer9() throws Exception {
    testUnevenDistribution(new Configuration(CONF),
        new long[] { 80 * CAPACITY / 100, 80 * CAPACITY / 100, 80 * CAPACITY / 100,
            80 * CAPACITY / 100, 100 * CAPACITY / 100 },
        new long[] { CAPACITY, CAPACITY, CAPACITY, CAPACITY, CAPACITY },
        new String[] { RACK0, RACK0, RACK0, RACK0, RACK0 },
        (short) 1);
  }

  @Test(timeout=60000)
  public void testBalancerBlockComparator() throws Exception {
    Collection<BalancerBlock> srcBlockList =
        new TreeSet<BalancerBlock>(new BalancerBlock.BalancerBlockComparator());
    int[] replicas = new int[] {1, 2, 3, 1, 3, 1};
    int id = 0;
    for (int locationsNum : replicas) {
      BalancerBlock block = new BalancerBlock(new Block(id, 5, id));
      id++;
      for (int i = 0; i < locationsNum; i++) {
        block.addLocation(new Balancer.Target(new DatanodeInfo(), 30));
      }
      srcBlockList.add(block);
    }
    Arrays.sort(replicas);
    int i = 0;
    for (BalancerBlock block : srcBlockList) {
      assertEquals(replicas[replicas.length - i - 1], block.getLocations().size());
      i++;
    }
    assertEquals(i, replicas.length);
  }

  private void testBalancerDefaultConstructor(Configuration conf, long[] capacities,
      String[] racks, long newCapacity, String newRack)
      throws Exception {
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    try {
      cluster = new MiniDFSCluster(0, conf, capacities.length, true, true, null, 
          racks, capacities);
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      long totalCapacity = 0L;
      for (long capacity : capacities) {
        totalCapacity += capacity;
      }
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity * 3 / 10;
      createFile(totalUsedSpace / numOfDatanodes, (short) numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[] { newRack },
          new long[] { newCapacity });
      totalCapacity += newCapacity;
      // run balancer and validate results
      assertEquals(Balancer.SUCCESS, runBalancer(conf, totalUsedSpace, totalCapacity));
      assertBalanced(totalUsedSpace, totalCapacity);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }
  }
}

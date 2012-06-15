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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyConfigurable.RackRingInfo;
import org.mortbay.log.Log;

import junit.framework.TestCase;

public class TestConfigurableBlockPlacement extends TestCase {

  private static Random r = new Random();

  static String[] locations = new String[] {
    "/d1/r1",
    "/d1/r1",
    "/d1/r2",
    "/d1/r2",
    "/d1/r3",
    "/d1/r3",
    "/d1/r3",
    "/d1/r4",
    "/d1/r4",
    "/d1/r5",
    "/d1/r5",
    "/d1/r5",
    "/d1/r5",
  };

  private static String[] testracks = {
    "/d1/r1",
    "/d1/r2",
    "/d1/r3",
    "/d1/r4",
    "/d1/r5"
  };

  public static final int numHosts = locations.length;

  public static String[] testHosts = new String[numHosts];
  static{
    for (int i=0; i<numHosts; i++) {
      testHosts[i] = "h"+i;
    }
  }

  private class TestHostsReader extends HostsFileReader {

    public TestHostsReader() throws IOException {
      super("", "");
    }

    Set<String> hosts = new HashSet<String>(Arrays.asList(testHosts));

    public synchronized Set<String> getHosts() {
      return hosts;
    }
  }

  static private class TestMapping implements DNSToSwitchMapping {
    protected boolean assignDefaultRack = false;

    public static String staticresolve(String name) {
      int host = Integer.parseInt(name.substring(1));
      return locations[host];
    }

    @Override
    public List<String> resolve(List<String> names) {
      ArrayList<String> result = new ArrayList<String>(names.size());
      String nameForDefaultRack = names.get(r.nextInt(names.size()));
      for (String name: names) {
        if (name.equals(nameForDefaultRack) && assignDefaultRack) {
          result.add(NetworkTopology.DEFAULT_RACK);
        } else {
          result.add(staticresolve(name));
        }
      }
      return result;
    }

  }

  private static final DatanodeDescriptor[] dataNodes;

  static {
    dataNodes = new DatanodeDescriptor[testHosts.length];
    for (int i = 0; i < testHosts.length; i++) {
      long capacity = 200000000000L;
      dataNodes[i] = new DatanodeDescriptor(
                            new DatanodeID(testHosts[i]+":5020"),
                            TestMapping.staticresolve(testHosts[i]),
                            testHosts[i], capacity, 0, capacity, 0, 0 );
    }
  }

  Configuration conf = new Configuration();

  public static class TestClusterStats implements FSClusterStats {
    public int getTotalLoad() {
      return 0;
    }
  }

  public static class VerifiablePolicy
                    extends BlockPlacementPolicyConfigurable {

    private static final long SEED = 33;

    private class StringComparator implements Comparator<String> {
      public int compare(String o1, String o2) {
        int hc1 = o1.hashCode();
        int hc2 = o2.hashCode();

        if (hc1 < hc2) return -1;
        if (hc1 > hc2) return 1;
        return 0;
      }
    }

    /**
     * The two classes below are used to hash rack and host names when forming
     * the rings. They can be overwritten to implement different strategies
     */

    public VerifiablePolicy() {
      super(SEED);
      rackComparator = new StringComparator();
      hostComparator = new StringComparator();
    }

    public int testRandomIntInWindow(int begin, int end, int n,
        Set<Integer> sortedExcludeList) {
      return randomIntInWindow(begin, end, n, sortedExcludeList);
    }

    public void checkRacks() throws Exception {

      assertTrue("Rack structures differ in size",
                    racks.size() == racksMap.size());

      assertEquals("Rack structures have unexpected size", testracks.length,
                                                           racks.size());

      for (String rack: testracks) {
        assertTrue("Rack not found",racksMap.containsKey(rack));
        RackRingInfo rackInfo = racksMap.get(rack);
        assertEquals("Index mismatch", rack, racks.get(rackInfo.index));
        assertEquals("Host structures differ in size for rack " + rack,
                        rackInfo.rackNodes.size(),
                        rackInfo.rackNodesMap.size());

        int numHosts = 0;
        for (String host: testHosts) {
          if (TestMapping.staticresolve(host).equals(rack)) {
            numHosts++;
            assertTrue("Host not found in rack " + rack,
                              rackInfo.rackNodesMap.containsKey(host));
            assertEquals("Index mismatch", host,
                rackInfo.rackNodes.get(rackInfo.rackNodesMap.get(host)));
          }
        }
        assertEquals("Host structures have unexpected size for rack " + rack,
                          numHosts, rackInfo.rackNodes.size());
      }

    }

  }

  /**
   * @throws Exception
   */
  public void testRandomIntInWindow() throws Exception {

    VerifiablePolicy policy = new VerifiablePolicy();
    boolean[] testArray = new boolean[97];

    for (int begin=0; begin < testArray.length; begin++) {
      for (int length=1; length < testArray.length*2; length++) {
        for (int i=0; i<testArray.length; i++) {
          testArray[i] = false;
        }
        HashSet<Integer> excludeSet = new HashSet<Integer>();

        for (int i=0; i<length; i++) {
          int idx = policy.randomIntInWindow(begin, length, testArray.length,
              excludeSet);
          if (idx < 0) {
            continue;
          }
          testArray[idx] = true;
          excludeSet.add(new Integer(idx));
        }

        for (int i = 0; i < length; i++) {
          int index = (begin+i)%testArray.length;
          assertTrue("Failed for index "+index,testArray[index]);
        }

        for (int i=0; i < (testArray.length - length); i++) {
          assertFalse(testArray[(begin+length+i)%testArray.length]);
        }
      }
    }
  }


  public void testPolicyInitAndFirstChoice() throws Exception {
    VerifiablePolicy policy = new VerifiablePolicy();
    Configuration conf = new Configuration();
    TestClusterStats stats = new TestClusterStats();
    NetworkTopology clusterMap = new NetworkTopology();
    TestHostsReader hostsReader = new TestHostsReader();
    TestMapping dnsToSwitchMapping = new TestMapping();


    for (DatanodeDescriptor d: dataNodes) {
      clusterMap.add(d);
    }

    conf.setInt("dfs.replication.rackwindow", 1);
    conf.setInt("dfs.replication.machineWindow", 2);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);

    policy.checkRacks();

    ArrayList<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    int bls = 1024;
    int maxR = 100;

    excludedNodes.put(dataNodes[0], dataNodes[0]);  // h0

    policy.chooseFirstInRemoteRack(dataNodes[0],excludedNodes,bls,maxR,results);

    assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[0]));
    assertTrue("Wrong First Choice!",
        results.get(0).equals(dataNodes[2]) ||
        results.get(0).equals(dataNodes[3]));

    policy.chooseFirstInRemoteRack(dataNodes[0],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
    assertTrue("Wrong Second Choice!",
        results.get(1).equals(dataNodes[2]) ||
        results.get(1).equals(dataNodes[3]));

    policy.chooseFirstInRemoteRack(dataNodes[0],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Third Choice!",
        results.get(2).equals(dataNodes[2]) ||
        results.get(2).equals(dataNodes[3]));


    results.clear();
    excludedNodes.clear();

    excludedNodes.put(dataNodes[2], dataNodes[2]); // h2

    policy.chooseFirstInRemoteRack(dataNodes[2],excludedNodes,bls,maxR,results);

    assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[2]));
    assertTrue("Wrong First Choice!",
        results.get(0).equals(dataNodes[4]) ||
        results.get(0).equals(dataNodes[5]));

    policy.chooseFirstInRemoteRack(dataNodes[2],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
    assertTrue("Wrong Second Choice!",
        results.get(1).equals(dataNodes[4]) ||
        results.get(1).equals(dataNodes[5]));

    policy.chooseFirstInRemoteRack(dataNodes[2],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Third Choice!",
        results.get(2).equals(dataNodes[4]) ||
        results.get(2).equals(dataNodes[5]));


    results.clear();
    excludedNodes.clear();

    excludedNodes.put(dataNodes[3], dataNodes[3]); //h3

    policy.chooseFirstInRemoteRack(dataNodes[3],excludedNodes,bls,maxR,results);

    assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[3]));
    assertTrue("Wrong First Choice!",
        results.get(0).equals(dataNodes[5]) ||
        results.get(0).equals(dataNodes[6]));

    policy.chooseFirstInRemoteRack(dataNodes[3],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
    assertTrue("Wrong Second Choice!",
        results.get(1).equals(dataNodes[5]) ||
        results.get(1).equals(dataNodes[6]));

    policy.chooseFirstInRemoteRack(dataNodes[3],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Third Choice!",
        results.get(2).equals(dataNodes[5]) ||
        results.get(2).equals(dataNodes[6]));

    for (int i = 4; i < 7; i++) { //h4..6
      results.clear();
      excludedNodes.clear();

      excludedNodes.put(dataNodes[i], dataNodes[i]);

      policy.chooseFirstInRemoteRack(dataNodes[i],excludedNodes,bls,maxR,results);

      assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[i]));
      assertTrue("Wrong First Choice!",
          results.get(0).equals(dataNodes[7]) ||
          results.get(0).equals(dataNodes[8]));

      policy.chooseFirstInRemoteRack(dataNodes[i],excludedNodes,bls,maxR,results);

      assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
      assertTrue("Wrong Second Choice!",
          results.get(1).equals(dataNodes[7]) ||
          results.get(1).equals(dataNodes[8]));

      policy.chooseFirstInRemoteRack(dataNodes[i],excludedNodes,bls,maxR,results);

      assertFalse("Wrong Third Choice!",
          results.get(2).equals(dataNodes[i]) ||
          results.get(2).equals(dataNodes[7]) ||
          results.get(2).equals(dataNodes[8]));
    }

    results.clear();
    excludedNodes.clear();

    excludedNodes.put(dataNodes[7], dataNodes[7]); // h7

    policy.chooseFirstInRemoteRack(dataNodes[7],excludedNodes,bls,maxR,results);

    assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[2]));
    assertTrue("Wrong First Choice!",
        results.get(0).equals(dataNodes[9]) ||
        results.get(0).equals(dataNodes[10]));

    policy.chooseFirstInRemoteRack(dataNodes[7],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
    assertTrue("Wrong Second Choice!",
        results.get(1).equals(dataNodes[9]) ||
        results.get(1).equals(dataNodes[10]));

    policy.chooseFirstInRemoteRack(dataNodes[2],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Third Choice!",
        results.get(2).equals(dataNodes[7]) ||
        results.get(2).equals(dataNodes[9]) ||
        results.get(2).equals(dataNodes[10]));

    results.clear();
    excludedNodes.clear();

    excludedNodes.put(dataNodes[8], dataNodes[8]); // h8

    policy.chooseFirstInRemoteRack(dataNodes[8],excludedNodes,bls,maxR,results);

    assertFalse("Wrong First Choice!", results.get(0).equals(dataNodes[2]));
    assertTrue("Wrong First Choice!",
        results.get(0).equals(dataNodes[11]) ||
        results.get(0).equals(dataNodes[12]));

    policy.chooseFirstInRemoteRack(dataNodes[8],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Second Choice!", results.get(1).equals(results.get(0)));
    assertTrue("Wrong Second Choice!",
        results.get(1).equals(dataNodes[11]) ||
        results.get(1).equals(dataNodes[12]));

    policy.chooseFirstInRemoteRack(dataNodes[8],excludedNodes,bls,maxR,results);

    assertFalse("Wrong Third Choice!",
        results.get(2).equals(dataNodes[8]) ||
        results.get(2).equals(dataNodes[11]) ||
        results.get(2).equals(dataNodes[12]));

    conf.setInt("dfs.replication.rackwindow", 1);
    conf.setInt("dfs.replication.machineWindow", 1);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);

    for (int i=0; i<2; i++) {  // h9..h12
      for (int j=0; j<2; j++) {
        int node1 = 9 + i*2 + j;
        int node2 = i;

        results.clear();
        excludedNodes.clear();

        excludedNodes.put(dataNodes[node1], dataNodes[node1]);

        policy.chooseFirstInRemoteRack(dataNodes[node1],excludedNodes,
            bls,maxR,results);
        assertTrue("Wrong First Choice!",
            results.get(0).equals(dataNodes[node2]));
      }
    }


    for (int rackWindow = 2; rackWindow < 4; rackWindow++) {
      conf.setInt("dfs.replication.rackwindow", rackWindow);
      conf.setInt("dfs.replication.machineWindow", 1);

      policy.initialize(conf, stats, clusterMap,
                        hostsReader, dnsToSwitchMapping, null);

      for (int i=0; i< testracks.length; i++) {
        for(int j=0; j<dataNodes.length; j++) {
          if(dataNodes[j].getNetworkLocation().equals(testracks[i])) {
            Log.info("Testing racks with window " + rackWindow +
                " for host "+ dataNodes[j]);

            results.clear();
            excludedNodes.clear();

            for (int k=0; k<rackWindow; k++) {
              policy.chooseFirstInRemoteRack(dataNodes[j],excludedNodes,
                  bls,maxR,results);
            }

            boolean[] racksChosen = new boolean[rackWindow];
            for (int k=0; k<rackWindow; k++) {
              racksChosen[k] = false;
            }

            for (int k=0; k<rackWindow; k++) {
              for(int l=0; l<rackWindow; l++) {
                if(testracks[(i+l+1)%testracks.length].
                    equals(results.get(k).getNetworkLocation())){
                  racksChosen[l] = true;
                }
              }
            }

            for(int k=0; k<rackWindow; k++) {
              assertTrue("Not all racks chosen!",racksChosen[k]);
            }
          }
        }
      }
    }

  }

  private static class RefreshNodesThread extends Thread {
    VerifiablePolicy policy;
    RefreshNodesThread(VerifiablePolicy policy) {
      this.policy = policy;
    }
    public void run() {
      for(int i = 0; i < 100; i++) {
        policy.hostsUpdated();
      }
    }
  }

  private VerifiablePolicy initTest() throws Exception {
    VerifiablePolicy policy = new VerifiablePolicy();
    Configuration conf = new Configuration();
    TestClusterStats stats = new TestClusterStats();
    NetworkTopology clusterMap = new NetworkTopology();
    TestHostsReader hostsReader = new TestHostsReader();
    TestMapping dnsToSwitchMapping = new TestMapping();

    for (DatanodeDescriptor d: dataNodes) {
      clusterMap.add(d);
    }

    conf.setInt("dfs.replication.rackwindow", 2);
    conf.setInt("dfs.replication.machineWindow", 2);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);
    return policy;
  }

  public void testRefreshNodesWithDefaultRack() throws Exception {
    VerifiablePolicy policy = initTest();
    TestMapping mapping = (TestMapping) policy.dnsToSwitchMapping;
    mapping.assignDefaultRack = true;
    try {
      policy.hostsUpdated();
      fail("Did not throw : " + DefaultRackException.class);
    } catch (DefaultRackException e) {}
    mapping.assignDefaultRack = false;
    policy.hostsUpdated();
  }

  public void testChooseTargetWithDefaultRack() throws Exception {
    VerifiablePolicy policy = initTest();
    TestMapping mapping = (TestMapping) policy.dnsToSwitchMapping;
    mapping.assignDefaultRack = true;
    try {
      policy.hostsUpdated();
      fail("Did not throw : " + DefaultRackException.class);
    } catch (DefaultRackException e) {}

    // Verify there is no default rack.
    RackRingInfo info = policy.racksMap.get(NetworkTopology.DEFAULT_RACK);
    assertNull(info);

    HashMap<Node, Node> emptyMap = new HashMap<Node, Node>();
    List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
    for (int i = 0; i < dataNodes.length; i++) {
      policy.chooseTarget(3, dataNodes[i], emptyMap, 512, 4, results, true);
    }
  }

  public void testRefreshNodesWhileChoosingTarget() throws Exception {

    VerifiablePolicy policy = initTest();

    HashMap <Node, Node> emptyMap = new HashMap<Node, Node>();
    List <DatanodeDescriptor> results = new ArrayList <DatanodeDescriptor>();
    DatanodeDescriptor writer = dataNodes[0];

    // Replication Factor 3
    Thread refreshNodes = new RefreshNodesThread(policy);
    refreshNodes.setPriority(Thread.MIN_PRIORITY);
    refreshNodes.start();
    for (int i = 0; i < 1000; i++) {
      DatanodeDescriptor fwriter = policy.chooseTarget(3, writer, emptyMap,
          512, 4, results, true);
    }
    refreshNodes.join();
    assertEquals(testracks.length, policy.racks.size());
    for (int i = 0; i < testracks.length; i++) {
      assertEquals(testracks[i], policy.racks.get(i));
    }
  }

  public void testChooseTarget() throws Exception {
    VerifiablePolicy policy = new VerifiablePolicy();
    Configuration conf = new Configuration();
    TestClusterStats stats = new TestClusterStats();
    NetworkTopology clusterMap = new NetworkTopology();
    TestHostsReader hostsReader = new TestHostsReader();
    TestMapping dnsToSwitchMapping = new TestMapping();

    for (DatanodeDescriptor d: dataNodes) {
      clusterMap.add(d);
    }

    conf.setInt("dfs.replication.rackwindow", 2);
    conf.setInt("dfs.replication.machineWindow", 2);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);

    HashMap <Node, Node> emptyMap = new HashMap<Node, Node>();
    List <DatanodeDescriptor> results = new ArrayList <DatanodeDescriptor>();
    DatanodeDescriptor writer = dataNodes[0];

    // Replication Factor 2
    DatanodeDescriptor fwriter = policy.chooseTarget(2, writer, emptyMap,
        512, 4, results, true);

    assertEquals(writer.getNetworkLocation(), fwriter.getNetworkLocation());
    assertEquals(writer.getNetworkLocation(),
        results.get(0).getNetworkLocation());
    assertEquals(results.get(0).getNetworkLocation(),
        results.get(1).getNetworkLocation());
    assertFalse(results.get(0).getHost().equals(
        results.get(1).getHost()));

    results.clear();
    emptyMap.clear();
    writer = dataNodes[0];

    // Replication Factor 3
    fwriter = policy.chooseTarget(3, writer, emptyMap,
        512, 4, results, true);

    assertEquals(writer.getNetworkLocation(), fwriter.getNetworkLocation());
    assertEquals(writer.getNetworkLocation(),
        results.get(0).getNetworkLocation());
    assertEquals(results.get(1).getNetworkLocation(),
        results.get(2).getNetworkLocation());
    assertFalse(results.get(0).getNetworkLocation().equals(
        results.get(1).getNetworkLocation()));
  }

  public void testFindBest() throws Exception {
    VerifiablePolicy policy = new VerifiablePolicy();
    Configuration conf = new Configuration();
    TestClusterStats stats = new TestClusterStats();
    NetworkTopology clusterMap = new NetworkTopology();
    TestHostsReader hostsReader = new TestHostsReader();
    TestMapping dnsToSwitchMapping = new TestMapping();


    for (DatanodeDescriptor d: dataNodes) {
      clusterMap.add(d);
    }

    conf.setInt("dfs.replication.rackwindow", 2);
    conf.setInt("dfs.replication.machineWindow", 2);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);

    DatanodeDescriptor[] r;

    r = policy.findBest(Arrays.asList(
        dataNodes[2],
        dataNodes[9],
        dataNodes[10],
        dataNodes[11],
        dataNodes[12],
        dataNodes[8],
        dataNodes[7]));
    assertEquals(dataNodes[2],r[0]);
    assertEquals(dataNodes[8],r[1]);
    assertEquals(dataNodes[7],r[2]);

    conf.setInt("dfs.replication.rackwindow", 1);
    conf.setInt("dfs.replication.machineWindow", 2);

    policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);

    r = policy.findBest(Arrays.asList(
        dataNodes[2],
        dataNodes[9],
        dataNodes[11]));
    assertEquals(dataNodes[2],r[0]);
    assertNull(r[1]);
    assertNull(r[2]);

    r = policy.findBest(Arrays.asList(
        dataNodes[2],
        dataNodes[6],
        dataNodes[9],
        dataNodes[12]));
    assertNull(r[0]);
    assertEquals(dataNodes[9],r[1]);
    assertEquals(dataNodes[12],r[2]);

    r = policy.findBest(Arrays.asList(
        dataNodes[2],
        dataNodes[4],
        dataNodes[9],
        dataNodes[12]));
    assertEquals(dataNodes[2],r[0]);
    assertEquals(dataNodes[4],r[1]);
    assertNull(r[2]);

  }

  public void testRemainingReplicas() throws Exception {

    VerifiablePolicy policy = new VerifiablePolicy();
    Configuration conf = new Configuration();
    TestClusterStats stats = new TestClusterStats();
    NetworkTopology clusterMap = new NetworkTopology();
    TestHostsReader hostsReader = new TestHostsReader();
    TestMapping dnsToSwitchMapping = new TestMapping();


    for (DatanodeDescriptor d: dataNodes) {
      clusterMap.add(d);
    }

    ArrayList<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    int bls = 1024;
    int maxR = 100;

    DatanodeDescriptor[] r;
    int replicas;

    for(int rackWindow = 2; rackWindow < 4; rackWindow++) {

      conf.setInt("dfs.replication.rackwindow", rackWindow);
      conf.setInt("dfs.replication.machineWindow", 2);

      policy.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);


      for (int i=0; i<dataNodes.length; i++) {

        // STRESS TEST 1: FOR EVERY 1 REPLICA
        results.clear();
        excludedNodes.clear();

        results.add(dataNodes[i]);
        excludedNodes.put(dataNodes[i], dataNodes[i]);

        Log.info("testing (" + dataNodes[i] + ")");

        policy.chooseRemainingReplicas(2, excludedNodes, bls, maxR, results);
        assertEquals(3,results.size());
        r = policy.findBest(results);
        assertNotNull(r[0]);
        assertNotNull(r[1]);
        assertNotNull(r[2]);

        for (int j=i+1; j<dataNodes.length; j++) {


          // STRESS TEST 2: FOR EVERY 2 REPLICAS
          results.clear();
          excludedNodes.clear();

          results.add(dataNodes[i]);
          excludedNodes.put(dataNodes[i], dataNodes[i]);
          results.add(dataNodes[j]);
          excludedNodes.put(dataNodes[j], dataNodes[j]);

          r = policy.findBest(results);
          replicas = 0;
          for (int c=0; c<3; c++)
            if (r[c] == null)
              replicas++;

          Log.info("testing (" + dataNodes[i] + "," + dataNodes[j] + ")");

          policy.chooseRemainingReplicas(replicas, excludedNodes,
              bls, maxR, results);

          assertEquals(2+replicas,results.size());
          r = policy.findBest(results);
          assertNotNull(r[0]);
          assertNotNull(r[1]);
          assertNotNull(r[2]);

          for(int k=j+1; k<dataNodes.length; k++) {

            // STRESS TEST 3: FOR EVERY 3 REPLICA
            results.clear();
            excludedNodes.clear();

            results.add(dataNodes[i]);
            excludedNodes.put(dataNodes[i], dataNodes[i]);
            results.add(dataNodes[j]);
            excludedNodes.put(dataNodes[j], dataNodes[j]);
            results.add(dataNodes[k]);
            excludedNodes.put(dataNodes[k], dataNodes[k]);

            r = policy.findBest(results);
            replicas = 0;
            for (int c=0; c<3; c++)
              if (r[c] == null)
                replicas++;

            Log.info("testing (" + dataNodes[i] + "," + dataNodes[j] +
                "," + dataNodes[k] + ")");

            policy.chooseRemainingReplicas(replicas, excludedNodes,
                bls, maxR, results);

            assertEquals(3+replicas,results.size());
            r = policy.findBest(results);
            assertNotNull(r[0]);
            assertNotNull(r[1]);
            assertNotNull(r[2]);


            for(int l=k+1; l<dataNodes.length; l++) {

              // STRESS TEST 4: FOR EVERY 4 REPLICA
              results.clear();
              excludedNodes.clear();

              results.add(dataNodes[i]);
              excludedNodes.put(dataNodes[i], dataNodes[i]);
              results.add(dataNodes[j]);
              excludedNodes.put(dataNodes[j], dataNodes[j]);
              results.add(dataNodes[k]);
              excludedNodes.put(dataNodes[k], dataNodes[k]);
              results.add(dataNodes[l]);
              excludedNodes.put(dataNodes[l], dataNodes[l]);

              r = policy.findBest(results);
              replicas = 0;
              for (int c=0; c<3; c++)
                if (r[c] == null)
                  replicas++;

              if (replicas > 0)
                Log.info("testing (" + dataNodes[i] + "," + dataNodes[j] +
                  "," + dataNodes[k] + "," + dataNodes[l] + ")");

              policy.chooseRemainingReplicas(replicas, excludedNodes,
                  bls, maxR, results);

              assertEquals(4+replicas,results.size());
              r = policy.findBest(results);
              assertNotNull(r[0]);
              assertNotNull(r[1]);
              assertNotNull(r[2]);
            }
          }
        }
      }
    }
  }

}

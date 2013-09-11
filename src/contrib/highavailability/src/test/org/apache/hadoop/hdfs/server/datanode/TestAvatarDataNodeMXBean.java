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
package org.apache.hadoop.hdfs.server.datanode;

import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

/**
 * Class for testing {@link DataNodeMXBean} implementation
 */
public class TestAvatarDataNodeMXBean {
  
  private void checkMXBean(DataNodeMXBean datanode) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "hadoop:service=DataNode,name=DataNodeInfo");
    // get attribute "ClusterId"
    String serviceIds = (String) mbs.getAttribute(mxbeanName, "ServiceIds");
    Assert.assertEquals(datanode.getServiceIds(), serviceIds);
    // get attribute "Version"
    String version = (String)mbs.getAttribute(mxbeanName, "Version");
    Assert.assertEquals(datanode.getVersion(), version);
    // get attribute "RpcPort"
    String rpcPort = (String)mbs.getAttribute(mxbeanName, "RpcPort");
    Assert.assertEquals(datanode.getRpcPort(), rpcPort);
    // get attribute "HttpPort"
    String httpPort = (String)mbs.getAttribute(mxbeanName, "HttpPort");
    Assert.assertEquals(datanode.getHttpPort(), httpPort);
    // get attribute "NamenodeAddresses"
    String namenodeAddresses = (String)mbs.getAttribute(mxbeanName,
        "NamenodeAddresses");
    Assert.assertEquals(datanode.getNamenodeAddresses(), namenodeAddresses);
    // get attribute "getVolumeInfo"
    String volumeInfo = (String)mbs.getAttribute(mxbeanName, "VolumeInfo");
    Assert.assertEquals(replaceDigits(datanode.getVolumeInfo()),
        replaceDigits(volumeInfo));
  }
  
  @Test
  public void testAvatarDataNode() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    Configuration conf = new Configuration();
    // populate repl queues on standby (in safe mode)
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0f);
    conf.setLong("fs.avatar.standbyfs.initinterval", 1000);
    conf.setLong("fs.avatar.standbyfs.checkinterval", 1000);
    MiniAvatarCluster cluster = null;
    try {
      cluster = new MiniAvatarCluster(conf, 1, true, null, null, 2, true); 
      List<AvatarDataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      checkMXBean(datanodes.get(0));
    } finally {
      if (cluster != null) { 
        cluster.shutDown();
      }
      MiniAvatarCluster.shutDownZooKeeper();
    }
  }
  
  @Test
  public void testDataNode() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(0, conf, 1, true, null, 2);
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      checkMXBean(datanodes.get(0));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  private static String replaceDigits(final String s) {
    return s.replaceAll("[0-9]+", "_DIGITS_");
  }
  
}

package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

public class BlockPlacementPolicyFakeData extends BlockPlacementPolicyDefault {

  public static BlockPlacementPolicyFakeData lastInstance = null;
  public DatanodeDescriptor overridingDatanode = null;
  
  
  public BlockPlacementPolicyFakeData() {
    super();
    
    FSNamesystem.LOG.info("new BlockPlacementPolicyFakeData: " + this);
    lastInstance = this;
  }
  
  public BlockPlacementPolicyFakeData(Configuration conf,  FSClusterStats stats,
                           NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap, null, null, null);
    
    FSNamesystem.LOG.info("new BlockPlacementPolicyFakeData: " + this);
    lastInstance = this;
  }
  
  DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    List<Node> exlcNodes,
                                    long blocksize) {
    
    if (overridingDatanode != null) {
      FSNamesystem.LOG.info("Block Placement: using override target node " + overridingDatanode.getName());
      return new DatanodeDescriptor[] { overridingDatanode };
    }
    
    return super.chooseTarget(numOfReplicas, writer, chosenNodes, exlcNodes, blocksize);
  }
  
  
  public static void setNameNodeReplicator(NameNode namenode, BlockPlacementPolicy policy) {
    namenode.namesystem.replicator = policy;
  }
}

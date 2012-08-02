package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.ReflectionUtils;

public class TopologyCache extends Configured {

  public static final Log LOG =
    LogFactory.getLog(TopologyCache.class);

  protected ConcurrentMap<String, Node> hostnameToNodeMap = new ConcurrentHashMap<String, Node>();
  protected NetworkTopology clusterMap = new NetworkTopology();
  protected DNSToSwitchMapping dnsToSwitchMapping;
  private Set<Node> nodesAtMaxLevel =
    Collections.newSetFromMap(new ConcurrentHashMap<Node, Boolean>());

  public TopologyCache(Configuration conf) {
    super(conf);

    dnsToSwitchMapping = ReflectionUtils.newInstance
      (conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
                     DNSToSwitchMapping.class), conf);
    LOG.info("DnsToSwitchMapping class = " + dnsToSwitchMapping.getClass().getName());
  }


  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    Node n = hostnameToNodeMap.get(name);

    // it's ok if multiple threads try to resolve the same host at the same time
    // the assumption is that resolve() will return a canonical node object and
    // the put operation is therefore idempotent
    if (n == null) {
      n = resolveAndGetNode(name);
      hostnameToNodeMap.put(name, n);
      // Make an entry for the node at the max level in the cache
      nodesAtMaxLevel.add(
        getParentNode(n, NetworkTopology.DEFAULT_HOST_LEVEL - 1));
    }

    return n;
  }

  private Node resolveAndGetNode(String name) {
    List <String> rNameList = dnsToSwitchMapping.resolve(Arrays.asList(new String [] {name}));
    String networkLoc = NodeBase.normalize(rNameList.get(0));
    Node node = null;

    // we depend on clusterMap to get a canonical node object
    // we synchronize this section to guarantee that two concurrent
    // insertions into the clusterMap don't happen (resulting in
    // multiple copies of the same node being created and returned)
    synchronized (clusterMap) {
      while ((node = clusterMap.getNode(networkLoc+"/"+name)) == null) {
        clusterMap.add(new NodeBase(name, networkLoc));
      }
    }

    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }
}

package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapred.ResourceTracker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.CoronaJobTracker;

public class TstUtils {

  public static int nodesPerRack = 10;
  public static short numCpuPerNode = (short)8;

  public static ComputeSpecs std_spec;
  static {
    std_spec = new ComputeSpecs(numCpuPerNode);
    std_spec.setNetworkMBps((short)100);
    std_spec.setMemoryMB(1024);
    std_spec.setDiskGB(1024);
  }
  public static ComputeSpecs free_spec = new ComputeSpecs();
  public static String std_cpu_to_resource_partitioning = "{\"1\":{\"M\":1, \"R\":1}}";

  public static int getNodePort(int i) {
    return (40000 + i);
  }

  public static String getNodeHost(int i) {
    int rack = i / nodesPerRack;
    int node = i % nodesPerRack;

    return "192.168." + rack + "." + node;
  }

  public static InetAddress getNodeAddress(int i) {
    return new InetAddress(getNodeHost(i), getNodePort(i));
  }

  public static List<ResourceRequest> createRequests(int numRequests, int numNodes) {
    int numMappers = numRequests * 3 /4;
    int numReducers = numRequests - numMappers;
    return createRequests(numNodes, numMappers, numReducers);
  }

  public static List<ResourceRequest> createRequests(int numNodes, int numMappers, int numReducers) {
    ArrayList<ResourceRequest> ret = new ArrayList<ResourceRequest> (numMappers + numReducers);
    for (int i=0; i<numMappers; i++) {
      ResourceRequest req = new ResourceRequest(
        i, ResourceTracker.RESOURCE_TYPE_MAP);
      req.setHosts(Arrays.asList(TstUtils.getNodeHost(i % numNodes),
                                TstUtils.getNodeHost((i+1) % numNodes),
                                TstUtils.getNodeHost((i+2) % numNodes)));
      req.setSpecs(Utilities.UnitComputeSpec);
      ret.add(req);
    }

    for (int i=0; i<numReducers; i++) {
      ResourceRequest req = new ResourceRequest(
        numMappers + i, ResourceTracker.RESOURCE_TYPE_REDUCE);
      req.setSpecs(Utilities.UnitComputeSpec);
      ret.add(req);
    }
    return ret;
  }

  public static void reliableSleep(long ms) {
    long start, now;
    start = now = System.currentTimeMillis();
    do {
      try {
        Thread.sleep (ms - (now - start));
      } catch (InterruptedException e) {
        System.out.println("Test caught interrupted exception");
      }
      now =  System.currentTimeMillis();
    } while ((now - start) < ms);
  }
}

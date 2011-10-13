package org.apache.hadoop.corona;

import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * a collection of utility classes and functions
 */
public class Utilities {

  public static final ComputeSpecs UnitComputeSpec = new ComputeSpecs((short)1);
  private static HashMap<String, ResourceRequest> unitResourceRequestMap = 
    new HashMap<String, ResourceRequest> ();

  public static ResourceRequest getUnitResourceRequest(String type) {
    ResourceRequest req = unitResourceRequestMap.get(type);
    if (req == null) {
      req = new ResourceRequest(1, type);
      req.setSpecs(UnitComputeSpec);

      // instead of using concurrent classes or locking - we clone and replace
      // the map with a new entry. this makes sense because this structure is
      // entirely read-only and will be populated quickly at bootstrap time

      HashMap<String, ResourceRequest> newMap = new HashMap<String, ResourceRequest> ();
      for(Map.Entry<String, ResourceRequest> entry: unitResourceRequestMap.entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }
      newMap.put(type, req);
      unitResourceRequestMap = newMap;
    }
    return (req);
  }

  public static void incrComputeSpecs(ComputeSpecs target, ComputeSpecs incr) {
    target.numCpus += incr.numCpus;
    target.memoryMB += incr.memoryMB;
    target.diskGB += incr.diskGB;
  }

  public static void decrComputeSpecs(ComputeSpecs target, ComputeSpecs decr) {
    target.numCpus -= decr.numCpus;
    target.memoryMB -= decr.memoryMB;
    target.diskGB -= decr.diskGB;
  }

  public static Object removeReference(List l, Object o) {
    Iterator iter = l.iterator();
    while (iter.hasNext()) {
      Object no = iter.next();
      if (no == o) {
        iter.remove();
        return o;
      }
    }
    return null;
  }

  public static void waitThreadTermination(Thread thread) {
    while (thread != null && thread.isAlive()) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
      }
    }
  }

  public static final Pattern inetAddressPattern = Pattern.compile("(.+):(\\d+)");
  public static InetAddress appInfoToAddress(String info) {
    Matcher m = inetAddressPattern.matcher(info);
    if (m.find()) {
      int port = Integer.parseInt(m.group(2));
      return new InetAddress(m.group(1), port);
    }
    return null;
  }
}

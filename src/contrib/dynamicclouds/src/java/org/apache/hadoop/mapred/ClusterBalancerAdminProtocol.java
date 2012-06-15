package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface ClusterBalancerAdminProtocol extends VersionedProtocol {

  /**
   * Version 1: Initial version.
   */
  public static final long versionID = 1L;

  public String getCurrentStatus(String clusterName) throws IOException;

  public int rebalance() throws IOException;

  public int moveMachines(String from, String to, int numMachines)
          throws IOException;
}

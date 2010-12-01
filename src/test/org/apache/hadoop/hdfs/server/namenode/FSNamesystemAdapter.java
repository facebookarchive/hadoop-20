package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

public class FSNamesystemAdapter {
  public static void forceNamenodeLeaseRecovery(
    String src, FSNamesystem namesystem
  ) throws IOException {
    LeaseManager.Lease lease = namesystem.leaseManager.getLeaseByPath(src);

    if (lease != null) {
      namesystem.internalReleaseLease(lease, src);
    }
  }

  public static void assignLease(
    String src, String newHolder, FSNamesystem namesystem
  ) {
    LeaseManager.Lease lease = namesystem.leaseManager.getLeaseByPath(src);

    if (lease != null) {
      namesystem.leaseManager.reassignLease(lease, src, newHolder);
    }
  }

  public static void corruptFileForTesting(
    String src, FSNamesystem namesystem
  ) throws IOException {
    namesystem.corruptFileForTesting(src);
  }
}

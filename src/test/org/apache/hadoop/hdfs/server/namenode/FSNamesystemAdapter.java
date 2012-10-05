package org.apache.hadoop.hdfs.server.namenode;

import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class FSNamesystemAdapter {
  public static void forceNamenodeLeaseRecovery(
    String src, FSNamesystem namesystem
  ) throws IOException {
    LeaseManager.Lease lease = namesystem.leaseManager.getLeaseByPath(src);

    if (lease != null) {
      namesystem.internalReleaseLeaseOne(lease, src);
    }
  }

  public static void assignLease(
    String src, String newHolder, FSNamesystem namesystem
  ) {
    LeaseManager.Lease lease = namesystem.leaseManager.getLeaseByPath(src);

    if (lease != null) {
      // not atomic, but probably ok for testing purposes
      namesystem.leaseManager.removeLease(lease, newHolder);
      namesystem.leaseManager.addLease(newHolder, src,
                                       System.currentTimeMillis());
    }
  }
  
  public static void expireLease(String src, FSNamesystem namesystem) {
    // use Mockito to insert a return value of false
    LeaseManager.Lease lease = spy(namesystem.leaseManager.getLeaseByPath(src));
    Answer<Boolean> contextBasedValue = new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock)
        throws Throwable {
        
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        // this will expire only when called during NamenodeFsck.fsck() :)
        for (StackTraceElement element : stack) {
          if (element.getMethodName().contains("checkForCorruptOpenFiles")) {
            return true;
          }
        }
        
        return (Boolean) invocationOnMock.callRealMethod();
      }
    };
    
    doAnswer(contextBasedValue)
      .when(lease)
      .expiredHardLimit();

    // this will remove the lease and add our mocked one
    namesystem.leaseManager.replaceLease(lease);
  }
  
  public static void corruptFileForTesting(
    String src, FSNamesystem namesystem
  ) throws IOException {
    namesystem.corruptFileForTesting(src);
    expireLease(src, namesystem);
  }
}

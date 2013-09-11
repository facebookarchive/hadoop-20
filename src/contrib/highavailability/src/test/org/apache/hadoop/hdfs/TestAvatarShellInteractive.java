package org.apache.hadoop.hdfs;

import java.io.PushbackInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.FailoverTestUtil.FailoverTestUtilHandler;
import org.apache.hadoop.hdfs.TestAvatarShell.ShortTxidWaitAvatarShell;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestAvatarShellInteractive extends AvatarSetupUtil {

  public void setUp(String name) throws Exception {
    Configuration conf = new Configuration();
    super.setUp(false, conf, name);
  }

  public void testFailoverInteractive(String[] input) throws Exception {
    testFailoverInteractive(input, false);
  }

  public void testFailoverInteractive(String[] input,
      boolean simulateEditLogCrash) throws Exception {
    int blocksBefore = blocksInFile();
    if (simulateEditLogCrash) {
      FailoverTestUtilHandler handler = new FailoverTestUtilHandler();
      InjectionHandler.set(handler);
    }
    // Txid will now mismatch.
    cluster.getPrimaryAvatar(0).avatar.namesystem.getEditLog()
        .setLastWrittenTxId(0);
    PushbackInputStream stream = new PushbackInputStream(System.in,
        100);
    System.setIn(stream);
    // PushbackInputStream processes in reverse order.
    for (int i = input.length - 1; i >= 0; i--) {
      stream.unread(input[i].getBytes());
    }
    ShortTxidWaitAvatarShell shell = new ShortTxidWaitAvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-failover" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverInteractiveYes() throws Exception {
    setUp("testFailoverInteractiveYes");
    testFailoverInteractive(new String[] { "Y\n" });
  }

  @Test(expected = AssertionError.class)
  public void testFailoverInteractiveNo() throws Exception {
    setUp("testFailoverInteractiveNo");
    testFailoverInteractive(new String[] { "N\n" });
  }

  public void testWrongInput(String input[]) throws Exception {
    PushbackInputStream stream = new PushbackInputStream(System.in,
        100);
    System.setIn(stream);
    // PushbackInputStream processes in reverse order.
    for (int i = input.length - 1; i >= 0; i--) {
      stream.unread(input[i].getBytes());
    }
    AvatarShell.handleRemoteException(
        new RemoteException("org.apache.hadoop.hdfs.server.namenode.StandbyStateException",
          "Test"));
  }

  @Test
  public void testFailoverInteractiveYesAfterWrongInput() throws Exception {
    testWrongInput(new String[] { "a\n", "b\n", "c\n", "Y\n" });
  }

  @Test(expected = RemoteException.class)
  public void testFailoverInteractiveNoAfterWrongInput() throws Exception {
    testWrongInput(new String[] { "a\n", "b\n", "c\n", "N\n" });
  }

  @Test(expected =AssertionError.class)
  public void testFailoverInteractiveYesWithEditLogCrash() throws Exception {
    setUp("testFailoverInteractiveYesWithEditLogCrash");
    testFailoverInteractive(new String[] { "Y\n" }, true);
  }
}

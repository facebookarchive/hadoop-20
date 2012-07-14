package org.apache.hadoop.hdfs;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;

public class TestAvatarShell extends AvatarSetupUtil {

  public void setUp(boolean federation) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("dfs.datanode.fullblockreport.delay", 200);
    super.setUp(federation, conf);
  }

  @Test
  public void testFailoverWithAvatarShell() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    // Wait for shutdown thread to finish.
    Thread.sleep(10000);
    assertEquals(0, shell.run(new String[] { "-one", "-setAvatar", "primary" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellNew() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-failover" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellStandby() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();
    cluster.failOver();
    cluster.restartStandby();

    AvatarShell shell = new AvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-failover" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellFederation()
      throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();
    AvatarShell shell = new AvatarShell(conf);
    String nsId = cluster.getNameNode(0).nameserviceId;
    assertEquals(
        0,
        shell.run(new String[] { "-failover", "-service", nsId }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellStandbyFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();
    cluster.failOver(0);
    cluster.restartStandby(0);

    AvatarShell shell = new AvatarShell(conf);
    String nsId = cluster.getNameNode(0).nameserviceId;
    assertEquals(
        0,
        shell.run(new String[] { "-failover", "-service", nsId }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testAvatarShellLeaveSafeMode() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    primaryAvatar.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    assertTrue(primaryAvatar.isInSafeMode());
    assertEquals(0, shell.run(new String[] { "-zero", "-leaveSafeMode" }));
    assertFalse(primaryAvatar.isInSafeMode());
    assertFalse(cluster.getPrimaryAvatar(0).avatar.isInSafeMode());
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  @Test
  public void testAvatarShellLeaveSafeMode1() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();
    cluster.failOver();
    cluster.restartStandby();

    AvatarShell shell = new AvatarShell(conf);
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    primaryAvatar.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    assertTrue(primaryAvatar.isInSafeMode());
    assertEquals(0, shell.run(new String[] { "-one", "-leaveSafeMode" }));
    assertFalse(primaryAvatar.isInSafeMode());
    assertFalse(cluster.getPrimaryAvatar(0).avatar.isInSafeMode());
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithWaitTxid() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    assertEquals(0, shell.run(new String[] { "-waittxid" }));
    assertEquals(0, shell.run(new String[] { "-one", "-setAvatar", "primary" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithWaitTxidWithService() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    String nsId = cluster.getNameNode(0).nameserviceId;
    assertEquals(0, shell.run(new String[] { "-waittxid", "-service", nsId }));
    assertEquals(0, shell.run(new String[] { "-one", "-setAvatar", "primary" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  private static class MyAvatarShell extends AvatarShell {

    public MyAvatarShell(Configuration conf) {
      super(conf);
    }

    @Override
    protected long getMaxWaitTimeForWaitTxid() {
      return 1000 * 15; // 15 seconds.
    }
  }

  @Test
  public void testFailoverWithWaitTxidFail() throws Exception {
    setUp(false);
    AvatarShell shell = new MyAvatarShell(conf);
    String nsId = cluster.getNameNode(0).nameserviceId;
    // This should fail.
    assertEquals(-1, shell.run(new String[] { "-waittxid", "-service", nsId }));
  }

}

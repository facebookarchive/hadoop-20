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
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestAvatarForceFailover extends AvatarSetupUtil {

  private class TestAvatarForceFailoverHandler extends InjectionHandler {
    public boolean simulateFailure = false;
    @Override
    public boolean _falseCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_SHUTDOWN) {
        return simulateFailure;
      }
      return false;
    }
  }

  @Before
  public void setup() {
    InjectionHandler.clear();
  }

  final static Log LOG = LogFactory.getLog(TestAvatarForceFailover.class);

  @Test
  public void testForceFailoverBasic() throws Exception {
    failover("testForceFailoverBasic");
  }

  private void failover(String name) throws Exception {
    setUp(false, name);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary");
    cluster.killPrimary();
    LOG.info("failing over");
    cluster.failOver(true);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testForceFailoverWithPrimaryFail() throws Exception {
    TestAvatarForceFailoverHandler h = new TestAvatarForceFailoverHandler();
    h.simulateFailure = true;
    InjectionHandler.set(h);
    failover("testForceFailoverWithPrimaryFail");
  }

  private void failoverShell(String name) throws Exception {
    setUp(false, name);
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    // Wait for shutdown thread to finish.
    Thread.sleep(3000);
    assertEquals(0,
        shell.run(new String[] { "-one", "-setAvatar", "primary", "force" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testForceFailoverShell() throws Exception {
    failoverShell("testForceFailoverShell");
  }

  @Test
  public void testForceFailoverShellWithPrimaryFail() throws Exception {
    TestAvatarForceFailoverHandler h = new TestAvatarForceFailoverHandler();
    h.simulateFailure = true;
    InjectionHandler.set(h);
    failoverShell("testForceFailoverShellWithPrimaryFail");
  }
}

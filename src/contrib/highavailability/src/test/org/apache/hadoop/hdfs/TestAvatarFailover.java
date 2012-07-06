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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;

public class TestAvatarFailover extends AvatarSetupUtil {
  final static Log LOG = LogFactory.getLog(TestAvatarFailover.class);

  /**
   * Test if we can get block locations after killing the primary avatar
   * and failing over to the standby avatar.
   */
  @Test
  public void testFailOver() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary");
    cluster.killPrimary();
    LOG.info("failing over");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testFailOverWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary");
    cluster.killPrimary(0);
    LOG.info("failing over");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  /**
   * Test if we can get block locations after killing the standby avatar.
   */
  @Test
  public void testKillStandby() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testKillStandbyWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  /**
   * Test if we can kill and resurrect the standby avatar and then do
   * a failover.
   */
  @Test
  public void testResurrectStandbyFailOver() throws Exception {
    setUp(false);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby();
    LOG.info("restarting standby");
    cluster.restartStandby();

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary");
    cluster.killPrimary();
    LOG.info("failing over");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testResurrectStandbyFailOverWithFederation() throws Exception {
    setUp(true);
    int blocksBefore = blocksInFile();

    LOG.info("killing standby");
    cluster.killStandby(0);
    LOG.info("restarting standby");
    cluster.restartStandby(0);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary");
    cluster.killPrimary(0);
    LOG.info("failing over");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }


  /**
   * Test if we can get block locations after killing primary avatar,
   * failing over to standby avatar (making it the new primary),
   * restarting a new standby avatar, killing the new primary avatar and
   * failing over to the restarted standby.
   */
  @Test
  public void testDoubleFailOver() throws Exception {
    setUp(false, true);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary 1");
    cluster.killPrimary();
    LOG.info("failing over 1");
    cluster.failOver();
    LOG.info("restarting standby");
    cluster.restartStandby();

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary 2");
    cluster.killPrimary();
    LOG.info("failing over 2");
    cluster.failOver();

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }
  
  @Test
  public void testDoubleFailOverWithFederation() throws Exception {
    setUp(true, true);
    int blocksBefore = blocksInFile();

    LOG.info("killing primary 1");
    cluster.killPrimary(0);
    LOG.info("failing over 1");
    cluster.failOver(0);
    LOG.info("restarting standby");
    cluster.restartStandby(0);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
      // do nothing
    }

    LOG.info("killing primary 2");
    cluster.killPrimary(0);
    LOG.info("failing over 2");
    cluster.failOver(0);

    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testDatanodeStartupDuringFailover() throws Exception {
    setUp(false);
    cluster.killPrimary();
    cluster.restartDataNodes(false);
    long start = System.currentTimeMillis();
    int live = 0;
    int total = 3;
    while (System.currentTimeMillis() - start < 30000 && live != total) {
      live = cluster.getStandbyAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.LIVE).length;
      total = cluster.getStandbyAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.ALL).length;
    }
    assertEquals(total, live);
  }

  private static boolean passDeadDnFailover = true;
  private static boolean failedOver = false;

  private class FailoverThread extends Thread {
    public void run() {
      try {
        cluster.failOver();
        failedOver = true;
      } catch (Exception e) {
        passDeadDnFailover = false;
      }
    }
  }

  @Test
  public void testDatanodeStartupFailover() throws Throwable {
    setUp(false, true);
    cluster.shutDownDataNodes();
    Thread fThread = new FailoverThread();
    fThread.setDaemon(true);
    fThread.start();
    cluster.restartDataNodes(false);
    fThread.join(30000);
    try {
      assertTrue(passDeadDnFailover);
      assertTrue(failedOver);
    } catch (Throwable e) {
      fThread.interrupt();
      throw e;
    }
  }
}

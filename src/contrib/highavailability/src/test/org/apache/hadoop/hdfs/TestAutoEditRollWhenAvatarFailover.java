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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

public class TestAutoEditRollWhenAvatarFailover extends AvatarSetupUtil {
  final static Log LOG = LogFactory.getLog(TestAutoEditRollWhenAvatarFailover.class);

  /**
   * Test if we can get block locations after killing primary avatar,
   * failing over to standby avatar (making it the new primary),
   * restarting a new standby avatar, killing the new primary avatar and
   * failing over to the restarted standby.
   * 
   * Write logs for a while to make sure automatic rolling are triggered.
   */
  @Test
  public void testDoubleFailOverWithAutomaticRoll() throws Exception {
    setUp(false, "testDoubleFailOverWithAutomaticRoll");
    
    // To make sure it's never the case that both primary and standby
    // issue rolling, we use a injection handler. 
    final AtomicBoolean startKeepThread = new AtomicBoolean(true);
    final AtomicInteger countAutoRolled = new AtomicInteger(0);
    final AtomicBoolean needFail = new AtomicBoolean(false);
    final AtomicLong currentThreadId = new AtomicLong(-1);
    final Object waitFor10Rolls = new Object();
    InjectionHandler.set(new InjectionHandler() {
      @Override
      protected void _processEvent(InjectionEventI event, Object... args) {
        if (event == InjectionEvent.FSEDIT_AFTER_AUTOMATIC_ROLL) {
          countAutoRolled.incrementAndGet();
          if (countAutoRolled.get() >= 10) {
            synchronized (waitFor10Rolls) {
              waitFor10Rolls.notifyAll();
            }
          }
          
          if (!startKeepThread.get()) {
            currentThreadId.set(-1);
          } else if (currentThreadId.get() == -1) {
            currentThreadId.set(Thread.currentThread().getId());            
          } else if (currentThreadId.get() != Thread.currentThread().getId()) {
            LOG.warn("[Thread " + Thread.currentThread().getId()
                + "] expected: " + currentThreadId);
            needFail.set(true);
          }
          
          LOG.info("[Thread " + Thread.currentThread().getId()
              + "] finish automatic log rolling, count "
              + countAutoRolled.get());
          
          // Increase the rolling time a little bit once after 7 auto rolls 
          if (countAutoRolled.get() % 7 == 3) {
            DFSTestUtil.waitNMilliSecond(75);
          }
        }
      }
    });
    
    FileSystem fs = cluster.getFileSystem();

    // Add some transactions during a period of time before failing over.
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      fs.setTimes(new Path("/"), 0, 0);
      DFSTestUtil.waitNMilliSecond(100);
      if (i % 10 == 0) {
        LOG.info("================== executed " + i + " queries");        
      }
      if (countAutoRolled.get() >= 10) {
        LOG.info("Automatic rolled 10 times.");
        long duration = System.currentTimeMillis() - startTime;
        TestCase.assertTrue("Automatic rolled 10 times in just " + duration
            + " msecs, which is too short", duration > 4500);
        break;
      }
    }
    TestCase.assertTrue("Only " + countAutoRolled
        + " automatic rolls triggered, which is lower than expected.",
        countAutoRolled.get() >= 10);
    
    // Tune the rolling timeout temporarily to avoid race conditions
    // only triggered in tests
    cluster.getPrimaryAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(5000);
    cluster.getStandbyAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(5000);
    
    LOG.info("================== killing primary 1");
    
    cluster.killPrimary();

    
    // Fail over and make sure after fail over, automatic edits roll still
    // will happen.
    countAutoRolled.set(0);
    startKeepThread.set(false);
    currentThreadId.set(-1);
    LOG.info("================== failing over 1");
    cluster.failOver();
    cluster.getPrimaryAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(1000);
    LOG.info("================== restarting standby");
    cluster.restartStandby();
    cluster.getStandbyAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(1000);
    LOG.info("================== Finish restarting standby");


    // Wait for automatic rolling happens if there is no new transaction.
    startKeepThread.set(true);
    
    startTime = System.currentTimeMillis();
    long waitDeadLine = startTime + 20000;
    synchronized (waitFor10Rolls) {
      while (System.currentTimeMillis() < waitDeadLine
          && countAutoRolled.get() < 10) {
        waitFor10Rolls.wait(waitDeadLine - System.currentTimeMillis());
      }
    }
    TestCase.assertTrue("Only " + countAutoRolled
        + " automatic rolls triggered, which is lower than expected.",
        countAutoRolled.get() >= 10);    
    long duration = System.currentTimeMillis() - startTime;
    TestCase.assertTrue("Automatic rolled 10 times in just " + duration
        + " msecs", duration > 9000);
    
    // failover back 
    countAutoRolled.set(0);
    startKeepThread.set(false);
    currentThreadId.set(-1);
    
    cluster.getPrimaryAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(6000);
    cluster.getStandbyAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(6000);
    
    LOG.info("================== killing primary 2");
    cluster.killPrimary();
    LOG.info("================== failing over 2");
    cluster.failOver();
    
    cluster.getPrimaryAvatar(0).avatar.namesystem.getFSImage().getEditLog()
        .setTimeoutRollEdits(1000);

    // Make sure after failover back, automatic rolling can still happen.
    startKeepThread.set(true);
    
    for (int i = 0; i < 100; i++) {
      fs.setTimes(new Path("/"), 0, 0);
      DFSTestUtil.waitNMilliSecond(200);
      if (i % 10 == 0) {
        LOG.info("================== executed " + i + " queries");        
      }
      if (countAutoRolled.get() > 10) {
        LOG.info("Automatic rolled 10 times.");
        duration = System.currentTimeMillis() - startTime;
        TestCase.assertTrue("Automatic rolled 10 times in just " + duration
            + " msecs, which is too short", duration > 9000);
        break;
      }
    }
    TestCase.assertTrue("Only " + countAutoRolled
        + " automatic rolls triggered, which is lower than expected.",
        countAutoRolled.get() >= 10);

    InjectionHandler.clear();
    
    if (needFail.get()) {
      TestCase
          .fail("Automatic rolling doesn't happen in the same thread when should.");
    }
  }
}

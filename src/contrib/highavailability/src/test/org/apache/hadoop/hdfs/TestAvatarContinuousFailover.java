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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarContinuousFailover extends FailoverLoadTestUtil {
  private static int FAILOVERS = 3;
  private static int THREADS = 3;

  @Test(timeout=300000)
  public void testContinuousFailover() throws Exception {
    List<LoadThread> threads = new ArrayList<LoadThread>();
    InjectionHandler.set(new TestHandler());
    for (int i = 0; i < THREADS; i++) {
      LoadThread T = new LoadThread();
      T.setDaemon(true);
      T.start();
      threads.add(T);
    }

    for (int i = 0; i < FAILOVERS; i++) {
      LOG.info("------------ FAILOVER ------------ " + i);
      cluster.failOver();
      cluster.restartStandby();
      Thread.sleep(15000);
    }

    for (LoadThread thread : threads) {
      thread.cancel();
    }

    for (LoadThread thread : threads) {
      thread.join();
    }

    LOG.info("GETADDR : " + get_addr + " GETSTAT : " + get_stats);
    assertTrue(FAILOVERS >= get_addr);
    assertTrue(FAILOVERS >= get_stats);
    assertTrue(pass);
  }
}

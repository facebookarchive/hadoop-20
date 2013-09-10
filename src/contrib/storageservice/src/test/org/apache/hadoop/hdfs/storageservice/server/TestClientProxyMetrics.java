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
package org.apache.hadoop.hdfs.storageservice.server;

import org.apache.hadoop.hdfs.storageservice.server.ClientProxyMetrics.EventDurationCalculator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestClientProxyMetrics {

  @Test
  public void testEventDurationCalculator0() throws InterruptedException {
    EventDurationCalculator calc0 = new EventDurationCalculator();
    long start = System.nanoTime();
    calc0.eventStarted();
    Thread.sleep(300);
    calc0.eventEnded();
    Thread.sleep(600);
    calc0.eventStarted();
    Thread.sleep(100);
    calc0.eventEnded();
    Thread.sleep(400);
    calc0.eventStarted();
    Thread.sleep(200);
    calc0.eventEnded();
    long end = System.nanoTime();
    assertEquals(1600D, (end - start) / 1e6, 10D);
    assertEquals(200D, calc0.getMean(), 10D);
  }

  @Test
  public void testEventDurationCalculator1() throws InterruptedException {
    EventDurationCalculator calc0 = new EventDurationCalculator();
    long start = System.nanoTime();
    calc0.eventStarted();
    Thread.sleep(300);
    calc0.eventStarted();
    Thread.sleep(600);
    calc0.eventStarted();
    Thread.sleep(100);
    calc0.eventEnded();
    Thread.sleep(400);
    calc0.eventEnded();
    Thread.sleep(200);
    calc0.eventEnded();
    long end = System.nanoTime();
    assertEquals(1600D, (end - start) / 1e6, 10D);
    assertEquals(933D, calc0.getMean(), 10D);
  }
}

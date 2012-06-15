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

package org.apache.hadoop.util;

import javax.management.StandardMBean;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

public class TestPulseChecker {

  private int registerCount = 0;
  private int unregisterCount = 0;

  class MockBeanTracker extends BeanTracker {
    public MockBeanTracker(String serviceName) {
      super(serviceName);
    }
    @Override
    public void register(Object beanObject) {
      registerCount++;
    }
    @Override
    public void unregister() {
      unregisterCount++;
    }
    public StandardMBean createBean(Object beanObject) {
      return null;
    }
  }

  class CheckableDaemon implements PulseCheckable {
    public Boolean alive = true;
    public Boolean isAlive() {
      return alive;
    }
  }

  private BeanTracker beanTracker;
  private CheckableDaemon daemon;
  private PulseChecker pulseChecker;

  @Before
  public void setUp() {
    daemon = new CheckableDaemon();
    beanTracker = new MockBeanTracker("service1");
    pulseChecker = new PulseChecker(daemon, beanTracker);
  }

  @After
  public void tearDown() {
    registerCount = 0;
    unregisterCount = 0;
    beanTracker = null;
    daemon = null;
    pulseChecker = null;
  }

  @Test
  public void testInstantiation() {
    assertEquals("One bean has been registered when instantatied",
      registerCount, 1
    );
  }

  @Test
  public void testAlive() {
    assertEquals("PulseChecker has the same value as the daemon",
      pulseChecker.isAlive(), daemon.isAlive()
    );

    daemon.alive = !daemon.alive;

    assertEquals("PulseChecker still has the same value as the daemon",
      pulseChecker.isAlive(), daemon.isAlive()
    );
  }

  @Test
  public void testShutdown() {
    pulseChecker.shutdown();

    assertEquals("One bean has been unregistered when shutdown",
      unregisterCount, 1
    );
  }
  
}

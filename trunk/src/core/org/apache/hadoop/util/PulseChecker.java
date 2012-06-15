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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.hadoop.util.BeanTracker;

/** 
 * Daemons can sometimes get stuck in a bad state. For instance, if there is an
 * error and the daemon tries to shut itself down, it's possible for threads to
 * keep running and the process will never completely go away. PulseChecker
 * allows these daemons to report their status so that an external process can
 * kill it if it slips into this zombie state.
 */
public class PulseChecker implements PulseMBean {

  public static final Log LOG = LogFactory.getLog(PulseChecker.class);

  private PulseCheckable daemon;
  private BeanTracker beanTracker;

  private static class PulseBeanTracker extends BeanTracker {
    public PulseBeanTracker(String serviceName) {
      super(serviceName);
      this.name = "pulse";
    }
    public StandardMBean createBean(Object beanObject) {
      try {
        return new StandardMBean((PulseMBean) beanObject, PulseMBean.class);
      } catch(NotCompliantMBeanException e) {
        LOG.error("Could not create PulseMBean for " + serviceName, e);
      }
      return null;
    }
  }

  public static PulseChecker create(PulseCheckable daemon, String serviceName) {
    return new PulseChecker(daemon, new PulseBeanTracker(serviceName));
  }

  /**
    Should only be used in tests.

    @param daemon
    @param beanTracker
  */
  protected PulseChecker(PulseCheckable daemon, BeanTracker beanTracker) {
    this.daemon = daemon;
    this.beanTracker = beanTracker;

    this.initialize();
  }

  public Boolean isAlive() {
    return daemon.isAlive();
  }

  public void shutdown() {
    beanTracker.unregister();
  }

  protected void initialize() {
    beanTracker.register(this);
  }

}

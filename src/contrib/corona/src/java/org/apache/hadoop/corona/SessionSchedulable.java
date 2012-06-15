/*
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

package org.apache.hadoop.corona;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Maintains parameters for a session of a given type
 */
public class SessionSchedulable extends Schedulable {
  /** Logger */
  private static final Log LOG = LogFactory.getLog(SessionSchedulable.class);
  /** The underlying session this schedulable is handling */
  private final Session session;

  /** Time when the latest locality wait period has started */
  private long localityWaitStartTime;
  /** The current required level of locality */
  private LocalityLevel localityRequired;
  private LocalityLevel lastLocality;
  /** Is the schedulable waiting for the locality to be satisfied */
  private boolean localityWaitStarted;

  /**
   * Construct as Schedulable object of a specific type for a session
   * @param session the session to schedule
   * @param type the type of the resource this schedulable is responsible for
   */
  public SessionSchedulable(Session session, ResourceType type) {
    super(session.getName(), type);
    this.session = session;
    this.localityRequired = LocalityLevel.NODE;
    this.localityWaitStartTime = Long.MAX_VALUE;
    this.localityWaitStarted = false;
  }

  @Override
  public void snapshot() {
    synchronized (session) {
      if (session.isDeleted()) {
        requested = 0;
        pending = 0;
        granted = 0;
      } else {
        requested = session.getRequestCountForType(getType());
        pending = session.getPendingRequestForType(getType()).size();
        granted = session.getGrantCountForType(getType());
        lastLocality = LocalityLevel.NODE;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshot for " + session.getHandle() +
          ":" + this.getType() + "{requested = " + requested +
          ", pending = " + pending +
          ", granted = " + granted + "}");
    }
  }

  @Override
  public long getStartTime() {
    return session.getStartTime();
  }

  @Override
  public int getPriority() {
    return session.getPriority();
  }

  @Override
  public long getDeadline() {
    return session.getDeadline();
  }

  public Session getSession() {
    return session;
  }

  public LocalityLevel getLastLocality() {
    return lastLocality;
  }

  /**
   * Check if the locality level satisfies the locality requirements of
   * the session
   * @param currentLevel locality level to check
   * @return true of the locality level provided is at least as good
   * as the required locality level, false otherwise
   */
  public boolean isLocalityGoodEnough(LocalityLevel currentLevel) {
    return !localityRequired.isBetterThan(currentLevel);
  }

  /**
   * Start the locality wait and record the start time if the session
   * is not in locality wait mode yet
   * @param now the time of the start of the locality wait
   */
  public void startLocalityWait(long now) {
    if (localityWaitStarted) {
      return;
    }
    localityWaitStarted = true;
    localityWaitStartTime = now;
  }

  /**
   * Adjust the locality requirement based on the current
   * locality and the locality wait times.
   * If the current required locality is node and enough time
   * has passed - update it to rack.
   * If the current is rack and enough time has passed - update to any
   *
   * @param now current time
   * @param nodeWait node locality wait
   * @param rackWait rack locality wait
   */
  public void adjustLocalityRequirement(
      long now, long nodeWait, long rackWait) {
    if (!localityWaitStarted) {
      return;
    }
    if (localityRequired == LocalityLevel.ANY) {
      return;
    }
    if (localityRequired == LocalityLevel.NODE) {
      if (now - localityWaitStartTime > nodeWait) {
        setLocalityLevel(LocalityLevel.RACK);
      }
    }
    if (localityRequired == LocalityLevel.RACK) {
      if (now - localityWaitStartTime > rackWait) {
        setLocalityLevel(LocalityLevel.ANY);
      }
    }
  }

  /**
   * Update the required locality level for the session
   * @param level the new required locality level
   */
  public void setLocalityLevel(LocalityLevel level) {
    localityRequired = level;
    lastLocality = level;
    localityWaitStarted = false;
    localityWaitStartTime = Long.MAX_VALUE;
  }
}

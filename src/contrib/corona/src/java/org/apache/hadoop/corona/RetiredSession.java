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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A bunch of immutable information about a retired session
 */
public class RetiredSession {
  /** The id of the retired session */
  private final String sessionId;
  /** The full session info of the retired session */
  private final SessionInfo info;
  /** The start time of the retired session */
  private final long startTime;
  /** The deleted time of the retired session */
  private final long deletedTime;
  /** The status of the retired session */
  private final SessionStatus status;
  /** Priority of the retired session */
  private final SessionPriority priority;
  /** Pool info of the retired session */
  private final PoolInfo poolInfo;

  /**
   * The context for a given resource. Contains detailed information
   * about a single resource type usage in a session
   */
  public static class Context {
    /** The max number of concurrent requests */
    private final int maxConcurrentRequestCount;
    /** The number of fulfilled requests for the session */
    private final int fulfilledRequestCount;
    /** The number of revoked requests for the session */
    private final int revokedRequestCount;
    
    private List<Long> resourceUsages = null;

    /**
     * A default private constructor
     */
    private Context() {
      this.maxConcurrentRequestCount = 0;
      this.fulfilledRequestCount = 0;
      this.revokedRequestCount = 0;
    }

    /**
     * Construct a context for a resource given all the information
     * @param maxConcurrentRequestCount the max number of requests that were
     * present at the same time
     * @param fulfilledRequestCount the number of requests that were fulfilled
     * @param revokedRequestCount the number of requests that were revoked
     */
    private Context(int maxConcurrentRequestCount,
                   int fulfilledRequestCount,
                   int revokedRequestCount,
                   List<Long> resourceUsages) {

      this.maxConcurrentRequestCount = maxConcurrentRequestCount;
      this.fulfilledRequestCount = fulfilledRequestCount;
      this.revokedRequestCount = revokedRequestCount;
      if (resourceUsages != null) {
        this.resourceUsages = new ArrayList<Long>(resourceUsages);
      }
    }
  }

  /** The table of contexts for different resources */
  private final Map<ResourceType, Context> typeToContext;

  /**
   * Construct a retired session object for a given Session
   * @param session the session
   */
  public RetiredSession(Session session) {
    sessionId = session.getSessionId();
    info = session.getInfo();
    startTime = session.getStartTime();
    deletedTime = session.getDeletedTime();
    status = session.getStatus();
    priority = SessionPriority.findByValue(session.getPriority());
    poolInfo = session.getPoolInfo();

    Set<ResourceType> types = session.getTypes();

    typeToContext = new EnumMap<ResourceType, Context>(ResourceType.class);
    for (ResourceType type : types) {
      typeToContext.put(type,
          new Context(session.getMaxConcurrentRequestCountForType(type),
                           session.getFulfilledRequestCountForType(type),
                           session.getRevokedRequestCountForType(type),
                           session.getResourceUsageForType(type)));
    }
  }

  /**
   * Get the context for a given resource type
   * @param type the type of the resource
   * @return the context object for this resource
   */
  protected Context getContext(ResourceType type) {
    Context c = typeToContext.get(type);
    if (c == null) {
      c = new Context();
      typeToContext.put(type, c);
    }
    return c;
  }

  public String getName() {
    return info.name;
  }

  public String getUserId() {
    return info.userId;
  }

  public String getUrl() {
    return info.url;
  }

  public SessionPriority getPriority() {
    return priority;
  }

  public PoolInfo getPoolInfo() {
    return poolInfo;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getDeletedTime() {
    return deletedTime;
  }

  /**
   * Get the maximum number of requests of a given type that were requested
   * at the same time
   * @param type the type of the resource
   * @return the maximum number of requests that were outstanding
   * at the same time
   */
  public int getMaxConcurrentRequestCountForType(ResourceType type) {
    return getContext(type).maxConcurrentRequestCount;
  }

  /**
   * Get the count of requests of a given type that were fulfilled
   * @param type the type of a resource
   * @return the count of the requests that were fulfilled
   */
  public int getFulfilledRequestCountForType(ResourceType type) {
    return getContext(type).fulfilledRequestCount;
  }

  /**
   * Get the count of requests of a given type that were revoked from
   * the session
   * @param type the type of the resource
   * @return the count of the requests that were revoked
   */
  public int getRevokedRequestCountForType(ResourceType type) {
    return getContext(type).revokedRequestCount;
  }
  
  public String getSessionId() {
    return sessionId;
  }
  
  public SessionStatus getStatus() {
    return status;
  }
  
  public List<Long> getResourceUsageForType(ResourceType type) {
    return this.getContext(type).resourceUsages;
  }
}

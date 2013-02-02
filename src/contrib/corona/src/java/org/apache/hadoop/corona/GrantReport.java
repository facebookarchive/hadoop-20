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

/**
 * Data transfer object for grant reports that is passed to
 * jobresources.jsp (Cluster Manager).
 */
public class GrantReport {
  /** Grant identifier */
  private final int grantId;
  /** Address of the grant */
  private final String address;
  /** Type of the resource */
  private final ResourceType type;
  /** Time the grant was issued. */
  private final long grantedTime;

  /**
   * Constructor.
   *
   * @param grantId id of grant
   * @param address Address of the grant
   * @param type Type of the resource
   */
  public GrantReport(int grantId, String address, ResourceType type,
                     long grantedTime) {
    this.grantId = grantId;
    this.address = address;
    this.type = type;
    this.grantedTime = grantedTime;
  }

  public int getGrantId() {
    return grantId;
  }

  public String getAddress() {
    return address;
  }

  public ResourceType getType() {
    return type;
  }

  public long getGrantedTime() {
    return grantedTime;
  }
}

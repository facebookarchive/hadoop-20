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
package org.apache.hadoop.mapred;

/**
 * Data transfer object to get the mapper and reducer grants for
 * coronajobdetails.jsp (Corona Proxy Job Tracker).
 */
public class ResourceUsage {
  /** Total mapper grants */
  private final int totalMapperGrants;
  /** Total reducer grants */
  private final int totalReducerGrants;

  public ResourceUsage(int totalMapperGrants,
                       int totalReducerGrants) {
    this.totalMapperGrants = totalMapperGrants;
    this.totalReducerGrants = totalReducerGrants;
  }

  public int getTotalMapperGrants() {
    return totalMapperGrants;
  }

  public int getTotalReducerGrants() {
    return totalReducerGrants;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("total mapper grants = " + totalMapperGrants);
    sb.append(" total reducer grants = " + totalReducerGrants);
    return sb.toString();
  }
}

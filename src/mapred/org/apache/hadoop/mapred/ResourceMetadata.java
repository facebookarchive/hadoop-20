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

package org.apache.hadoop.mapred;

/**
 * Metadata about a resource.  Used with {@link PoolMetadata}.
 */
public class ResourceMetadata {
  /** Pool name */
  private final String poolName;
  /**
   * Resources used that are guaranteed and desired (taking into account max).
   */
  private final int guaranteedUsedAndDesired;
  /** Maximum resources that are allowed for this pool */
  private final int maxAllowed;
  /** Number of resources currently used for this pool */
  private final int currentlyUsed;
  /** Desired number of resources after considering the max allowed */
  private final int desiredAfterConstraints;
  /** Mutable expected number of resources that should be used */
  private int expectedUsed = 0;

  /**
   * Only constructor to this object
   *
   * @param poolName Name of the pool
   * @param minGuaranteed Minimum guaranteed resources
   * @param maxAllowed Maximum allowed resources
   * @param currentlyUsed Resources currently used
   * @param desired Desired number of resources
   */
  public ResourceMetadata(
      final String poolName,
      final int minGuaranteed,
      final int maxAllowed,
      final int currentlyUsed,
      final int desired) {
    this.poolName = poolName;
    this.maxAllowed = maxAllowed;
    this.currentlyUsed = currentlyUsed;
    this.desiredAfterConstraints = Math.min(desired, maxAllowed);
    this.guaranteedUsedAndDesired =
        Math.min(minGuaranteed, desiredAfterConstraints);
  }

  public String getPoolName() {
    return poolName;
  }

  public int getGuaranteedUsedAndDesired() {
    return guaranteedUsedAndDesired;
  }

  public int getMaxAllowed() {
    return maxAllowed;
  }

  public int getCurrentlyUsed() {
    return currentlyUsed;
  }

  public int getDesiredAfterConstraints() {
    return desiredAfterConstraints;
  }

  public int getExpectedUsed() {
    return expectedUsed;
  }

  /**
   * Increment the expected used number of resources.  This is the only
   * modifier allowed for expected used.
   */
  public void incrExpectedUsed() {
    ++expectedUsed;
  }

  @Override
  public int hashCode() {
    return maxAllowed + currentlyUsed * 17 + desiredAfterConstraints * 31 +
        guaranteedUsedAndDesired * 37;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != obj.getClass()) {
      return false;
    }
    ResourceMetadata resourceMetadata = (ResourceMetadata) obj;
    return poolName.equals(resourceMetadata.getPoolName()) &&
        maxAllowed == resourceMetadata.getMaxAllowed() &&
        currentlyUsed == resourceMetadata.getCurrentlyUsed() &&
        desiredAfterConstraints ==
          resourceMetadata.getDesiredAfterConstraints() &&
        guaranteedUsedAndDesired ==
          resourceMetadata.getGuaranteedUsedAndDesired() &&
        expectedUsed == resourceMetadata.getExpectedUsed();
  }
}


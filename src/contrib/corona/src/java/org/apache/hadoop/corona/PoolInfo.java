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

import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Immutable object that contains the pool group name and pool name.
 */
public class PoolInfo implements Comparable<PoolInfo> {
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(PoolInfo.class);
  /** Invalid regex for pool group and pool names */
  private static final String INVALID_REGEX = ".*[^0-9a-z\\-\\_].*";
  /** Compiled invalid regex used for checking pool group and pool names */
  private static final Pattern INVALID_REGEX_PATTERN =
      Pattern.compile(INVALID_REGEX);
  /** Immutable pool group name */
  private final String poolGroupName;
  /** Immutable pool name */
  private final String poolName;

  /**
   * Convert this object to PoolInfoStrings for Thrift
   * @param poolInfo Pool info
   * @return {@link PoolInfo} converted to a Thrift form
   */
  public static PoolInfoStrings createPoolInfoStrings(PoolInfo poolInfo) {
    if (poolInfo == null) {
      return null;
    }

    return new PoolInfoStrings(poolInfo.getPoolGroupName(),
                               poolInfo.getPoolName());
  }

  /**
   * Create PoolInfo object from a properly formatted string
   * <poolgroup>.<pool> or return null
   *
   * @param poolInfoString String to parse
   * @return Valid PoolInfo object or null if unable to parse
   */
  public static PoolInfo createPoolInfo(String poolInfoString) {
    if (poolInfoString == null || poolInfoString.isEmpty()) {
      LOG.warn("createPoolInfo: Null or empty input " + poolInfoString);
      return null;
    }

    String[] poolInfoSplitString = poolInfoString.split("[.]");
    if (poolInfoSplitString.length != 2) {
      LOG.warn("createPoolInfo: Couldn't parse " + poolInfoString);
      return null;
    }

    return new PoolInfo(poolInfoSplitString[0], poolInfoSplitString[1]);
  }

  /**
   * Convert this object from PoolInfoStrings for Thrift
   * @param poolInfoStrings Thrift representation of a {@link PoolInfo}
   * @return Converted {@link PoolInfo}
   */
  public static PoolInfo createPoolInfo(PoolInfoStrings poolInfoStrings) {
    if (poolInfoStrings == null) {
      return null;
    }

    return new PoolInfo(poolInfoStrings.getPoolGroupName(),
                        poolInfoStrings.getPoolName());
  }

  /**
   * Create a single string from a {@link PoolInfo} object
   * @param poolInfo Pool info
   * @return Pool group joined to pool name with a '.'.  If missing a pool
   *  (only pool group, then no dot)
   */
  public static String createStringFromPoolInfo(PoolInfo poolInfo) {
    if (poolInfo == null) {
      return null;
    }

    return poolInfo.getPoolGroupName() +
        ((poolInfo.getPoolName() == null) ? "" :
          "." + poolInfo.getPoolName());
  }

  /**
   * Returns whether or not the given pool name is legal.
   *
   * Legal pool names are of nonzero length and are formed only of alphanumeric
   * characters, underscores (_), and hyphens (-).
   * @param poolInfo the name of the pool to check
   * @return true if the name is a valid pool name, false otherwise
   */
  public static boolean isLegalPoolInfo(PoolInfo poolInfo) {
    if (poolInfo == null || poolInfo.getPoolGroupName() == null ||
        poolInfo.getPoolName() == null) {
      return false;
    }
    if (INVALID_REGEX_PATTERN.matcher(poolInfo.getPoolGroupName()).matches() ||
        poolInfo.getPoolGroupName().isEmpty()) {
      return false;
    }
    if (INVALID_REGEX_PATTERN.matcher(poolInfo.getPoolName()).matches() ||
        poolInfo.getPoolName().isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Constructor.
   * @param poolGroupName Name of the pool group
   * @param poolName Name of the pool
   */
  public PoolInfo(String poolGroupName, String poolName) {
    this.poolGroupName = poolGroupName;
    this.poolName = poolName;
  }

  public String getPoolGroupName() {
    return poolGroupName;
  }

  public String getPoolName() {
    return poolName;
  }

  @Override
  public String toString() {
    return "(poolGroup=" + poolGroupName + ",pool=" + poolName + ")";
  }

  @Override
  public int hashCode() {
    int poolGroupHash =
        (poolGroupName == null) ? 13 : poolGroupName.hashCode();
    return poolGroupHash * ((poolName == null) ? 17 : poolName.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final PoolInfo other = (PoolInfo) obj;

    if ((poolGroupName != null) &&
        (!poolGroupName.equals(other.poolGroupName))) {
      return false;
    }
    if ((other.getPoolGroupName() != null) &&
        (!other.getPoolGroupName().equals(poolGroupName))) {
      return false;
    }

    if ((poolName != null) &&
        (!poolName.equals(other.poolName))) {
      return false;
    }
    if ((other.getPoolName() != null) &&
        (!other.getPoolName().equals(poolName))) {
      return false;
    }

    return true;
  }

  @Override
  public int compareTo(PoolInfo other) {
    if (getPoolGroupName() == null && other.getPoolGroupName() == null) {
      // Go through
    } else if (getPoolGroupName() == null && other.getPoolGroupName() != null) {
      return -1;
    } else if (getPoolGroupName() != null && other.getPoolGroupName() == null) {
      return 1;
    } else {
      int ret = getPoolGroupName().compareTo(other.getPoolGroupName());
      if (ret != 0) {
        return ret;
      }
    }

    if (getPoolName() == null && other.getPoolName() == null) {
      return 0;
    } else if (getPoolName() == null && other.getPoolName() != null) {
      return -1;
    } else if (getPoolName() != null && other.getPoolName() == null) {
      return 1;
    } else {
      return getPoolName().compareTo(other.getPoolName());
    }
  }
}

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

import java.util.Arrays;

/**
 * A Class gauges the resource utilization information on the TaskTracker
 * See {@link LocalJobUtilization}, {@link TaskTrackerUtilization}
 */
abstract public class UtilizationGauger {
  protected TaskTrackerUtilization ttUtilization;
  protected LocalJobUtilization[] localJobUtilization;
  public UtilizationGauger() {
    ttUtilization = new TaskTrackerUtilization();
  }
  /**
   * obtain some static system information
   */
  abstract public void initialGauge();
  /**
   * obtain runtime usage information
   */
  abstract public void gauge();
  /**
   * @return an array contains running job information on the TaskTracker
   */
  public LocalJobUtilization[] getLocalJobUtilization() {
    return localJobUtilization;
  }
  /**
   * @return the resource utilization information of the TaskTracker
   */
  public TaskTrackerUtilization getTaskTrackerUtilization() {
    return ttUtilization;
  }

  @Override
  public String toString() {
    String output = ttUtilization.toString();
    if (localJobUtilization == null) {
      return output;
    }
    String[] result = new String[localJobUtilization.length];
    int i = 0;
    for (LocalJobUtilization jobUtil : localJobUtilization) {
      result[i++] = jobUtil.toString();
    }
    Arrays.sort(result);
    for (String s : result) {
      output += s;
    }
    return output;
  }
}

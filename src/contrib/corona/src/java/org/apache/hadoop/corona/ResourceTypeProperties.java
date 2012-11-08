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
import java.util.List;

/**
 * Utility class to specify the properties of resource types.
 */
public class ResourceTypeProperties {
  /**
   * Disallow construction.
   */
  private ResourceTypeProperties() {
  }

  /**
   * Tells if preemption is allowed for a resource type.
   * @param type The type of the resource.
   * @return Is preemption allowed for the the specified type.
   */
  public static boolean canBePreempted(ResourceType type) {
    // Preemption is not allowed for JOBTRACKER grants.
    switch (type) {
    case MAP:
      return true;
    case REDUCE:
      return true;
    case JOBTRACKER:
      return false;
    default:
      throw new RuntimeException("Undefined Preemption behavior for " + type);
    }
  }

  /**
   * Returns the required locality levels, in order of preference,
   * for the resource type.
   * @param type The type of the resource.
   * @return The list of locality levels.
   */
  public static List<LocalityLevel> neededLocalityLevels(ResourceType type) {
    List<LocalityLevel> l = new ArrayList<LocalityLevel>();
    switch (type) {
    case MAP:
      l.add(LocalityLevel.NODE);
      l.add(LocalityLevel.RACK);
      l.add(LocalityLevel.ANY);
      break;
    case REDUCE:
      l.add(LocalityLevel.ANY);
      break;
    case JOBTRACKER:
      l.add(LocalityLevel.ANY);
      break;
    default:
      throw new RuntimeException("Undefined locality behavior for " + type);
    }
    return l;
  }
}

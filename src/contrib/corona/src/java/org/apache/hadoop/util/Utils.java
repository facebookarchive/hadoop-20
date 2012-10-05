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
package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A class of utility functions used by corona
 */
public class Utils {
  /**
   * Hiding the default constructor
   */
  private Utils() {
  }

  /**
   * Return a difference of the two sets as a list of objects
   *
   * @param <T> the type of the sets. Has to be the same for both sets
   * @param s1 first set
   * @param s2 second set
   * @return the set difference of s1 and s2 as a list
   */
  public static <T> List<T> setDifference(Set<T> s1, Set<T> s2) {
    List<T> diff = new ArrayList<T>();
    for (T one : s1) {
      if (!s2.contains(one)) {
        diff.add(one);
      }
    }
    return diff;
  }

}

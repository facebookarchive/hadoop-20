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

package org.apache.hadoop.mapreduce;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An alternative to using String.intern().
 */
public class CounterNames {
  public static final Log LOG = LogFactory.getLog(CounterNames.class);

  private static Map<String, String> uniqueStrings =
    new ConcurrentHashMap<String, String>();
  
  public static void clearIfNecessary(int clearThreshold) {
    if (uniqueStrings.size() > clearThreshold) {
      LOG.warn("Unique Strings map reached size of " + clearThreshold +
          ", clearing");
      uniqueStrings.clear();
    } else {
      LOG.info("Unique Strings map size is " + clearThreshold +
          ", not clearing");
    }
  }

  public static String intern(String s) {
    if (s == null) {
      return null;
    }
    String unique = uniqueStrings.get(s);
    if (unique == null) {
      unique = s;
      uniqueStrings.put(unique, unique);
    }
    return unique;
  }
}

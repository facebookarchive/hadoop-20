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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class SafeModeUtil {

  public static SafeModeInfo getInstance(FSNamesystem namesystem) {
    return getInstance(new Configuration(), namesystem);
  }

  public static SafeModeInfo getInstance(Configuration conf,
      FSNamesystem namesystem) {
    Class<?> clazz = conf.getClass("dfs.safemode.impl",
        NameNodeSafeModeInfo.class);

    return (SafeModeInfo) ReflectionUtils.newInstance(clazz, new Class[] {
        Configuration.class, FSNamesystem.class }, new Object[] { conf,
        namesystem });
  }
}

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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.StringUtils;

/**
 * Default implementation of PathNameChecker
 *
 * Checks that the the path does not contain ":" and "//"
 * Path should not have relative elements ".", ".."
 */
public class DefaultPathNameChecker implements PathNameChecker {

  @Override
  public boolean isValidPath(String path) {
    String[] components = StringUtils.split(path, Path.SEPARATOR_CHAR);
    return isValidPath(path, components);
  }
  
  @Override
  public boolean isValidPath(String path, String[] names) {

    if (!defaultCheck(path, names)) {
      return false;
    }

    for(String element : names) {
      if (element.equals("..") ||
          element.equals(".")  ||
          element.indexOf("\n") >= 0 ||
          element.indexOf("\r") >= 0 ||
          element.indexOf("\t") >= 0 ||
          (element.indexOf(":") >= 0)) {
        return false;
      }
    }
    return true;
  }
  
  public static boolean defaultCheck(String path, String[] names) {
    if (!path.startsWith(Path.SEPARATOR)
        || names.length > FSConstants.MAX_PATH_DEPTH
        || path.contains("//")) {
      return false;
    }
    return true;
  }
}

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

import java.util.StringTokenizer;

/**
 * Default implementation of PathNameChecker
 *
 * Checks that the the path does not contain ":" and "//"
 * Path should not have relative elements ".", ".."
 */
public class DefaultPathNameChecker implements PathNameChecker {

  @Override
  public boolean isValidPath(String path) {

    // Path must be absolute.
    if (!path.startsWith(Path.SEPARATOR)) {
      return false;
    }

    if (path.contains("//")) {
      return false;
    }
    StringTokenizer tokens = new StringTokenizer(path, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") ||
          element.equals(".")  ||
          (element.indexOf(":") >= 0)) {
        return false;
      }
    }
    return true;
  }

}

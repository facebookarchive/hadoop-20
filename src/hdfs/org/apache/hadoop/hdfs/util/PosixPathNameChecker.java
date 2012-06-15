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
import org.apache.hadoop.util.StringUtils;

public class PosixPathNameChecker implements PathNameChecker {

  /**
   * Test whether the character c belongs to the accepted list of posix
   * filename characters A-Za-z0-9._-
   * @param c
   * @return
   */
  public boolean isValidPosixFileChar(char c) {
    if ((((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) ||
           ((c >= '0') && (c <= '9'))
           || (c == '.') || (c == '_') || (c == '-'))) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Test whether filename is a valid posix filename
   * A posix filename must contain characters A-Za-z0-9._- and - must not be
   * the first character
   *
   */
  public boolean isValidPosixFileName(String name) {
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (i == 0) {
        if (c == '-') {
          return false;
        }
      }
      if (!isValidPosixFileChar(c)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test whether path is a valid posix path
   * A posix filename must contain characters A-Za-z0-9._- and - must not be
   * the first character
   * A valid path will have posix filenames separated by single '/'
   *
   * @param path
   * @return
   */
  @Override
  public boolean isValidPath(String path) {
    String[] components = StringUtils.split(path, Path.SEPARATOR_CHAR);
    return isValidPath(path, components);
  }

  @Override
  public boolean isValidPath(String path, String[] names) {
    if (!DefaultPathNameChecker.defaultCheck(path, names)) {
      return false;
    }
    
    for(String element : names) {
      if (!isValidPosixFileName(element)) {
        return false;
      }
    }
    return true;
  }
}
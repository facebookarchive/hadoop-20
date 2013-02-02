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

import org.apache.hadoop.io.UTF8;

public class PosixUserNameChecker implements UserNameChecker {

  @Override
  public boolean isValidUserName(String username) {
    if (username == null || username.length() == 0)
      return false;
    int len = username.length();
    char[] carray = UTF8.getCharArray(len);
    username.getChars(0, len, carray, 0);
    char fc = carray[0];
    if (!((fc >= 'a' && fc <= 'z') || fc == '_')) {
      return false;
    }
    for (int i = 1; i < len; i++) {
      char c = carray[i];
      if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
          || c == '_' || (c == '$' && i == len - 1))) {
        return false;
      }
    }
    return true;
  }
}

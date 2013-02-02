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

import org.apache.commons.lang.RandomStringUtils;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestPosixUserNameChecker {

  private static PosixUserNameChecker checker = new PosixUserNameChecker();

  @Test
  public void test() {
    check("abc", true);
    check("a_bc", true);
    check("a1234bc", true);
    check("a1234bc45", true);
    check("_a1234bc45$", true);
    check("_a123-4bc45$", true);
    check("abc_", true);
    check("ab-c_$", true);
    check("_abc", true);
    check("ab-c$", true);
    check("-abc", false);
    check("-abc_", false);
    check("a$bc", false);
    check("9abc", false);
    check("-abc", false);
    check("$abc", false);
    check("", false);
    check(null, false);
    check(RandomStringUtils.randomAlphabetic(100).toLowerCase(), true);
    check(RandomStringUtils.randomNumeric(100), false);
    check("a" + RandomStringUtils.randomNumeric(100), true);
    check("_" + RandomStringUtils.randomNumeric(100), true);
    check("a" + RandomStringUtils.randomAlphanumeric(100).toLowerCase(), true);
    check("_" + RandomStringUtils.randomAlphanumeric(100).toLowerCase(), true);
    check(RandomStringUtils.random(1000), false); //unicode
  }

  private void check(String username, boolean expected) {
    assertEquals(expected, checker.isValidUserName(username));
  }
}

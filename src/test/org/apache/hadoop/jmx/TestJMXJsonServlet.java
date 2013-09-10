/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.jmx;

public class TestJMXJsonServlet extends JMXJsonServletTestCase {

  public void testBasicJVMQueries() throws Exception {
    assertRequestMatches("?qry=java.lang:type=Runtime",
        "\"beanName\"\\s*:\\s*\"java.lang:type=Runtime\"", "\"className\"");

    assertRequestMatches("?qry=java.lang:type=Memory",
        "\"beanName\"\\s*:\\s*\"java.lang:type=Memory\"", "\"className\"");
  }

  public void testMultipleBeanQuery() throws Exception {
    assertRequestMatches("?qry=java.lang:type=Runtime" + "&qry=java.lang:type=Memory",
        "\"beanName\"\\s*:\\s*\"java.lang:type=Runtime\"",
        "\"beanName\"\\s*:\\s*\"java.lang:type=Memory\"");
  }

  public void testWithoutQuery() throws Exception {
    assertRequestMatches("", "\"beanName\"\\s*:\\s*\"java.lang:type=Memory\"");
  }

  public void testBadQuery() throws Exception {
    assertRequestMatches("?qry=invalid%20query", "\"exception\" :");
  }

  public void testUnregisteredBean() throws Exception {
    assertRequestMatches("?qry=this:looks=legitimate", "\\{\n\\}");
  }

  public void testJSONPOutput() throws Exception {
    assertRequestMatches("?qry=java.lang:type=Memory&callback=mycallback", "^mycallback\\(\\{",
        "\\}\\);$");
  }

}

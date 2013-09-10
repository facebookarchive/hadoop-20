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

package org.apache.hadoop.jmx;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.hadoop.http.HttpServer;

abstract class JMXJsonServletTestCase extends TestCase {
  private static HttpServer server;
  private static URL baseUrl;

  protected void setUp() throws Exception {
    server = new HttpServer("jmx", "0.0.0.0", 0, true);
    server.start();
    baseUrl = new URL("http://localhost:" + server.getPort() + "/");
  }

  protected void tearDown() throws Exception {
    server.stop();
  }

  private URL testUrl(String in) throws MalformedURLException {
    return new URL(baseUrl, in);
  }

  protected String getURLContents(String url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = null;
    HttpURLConnection conn = (HttpURLConnection)testUrl(url).openConnection();
    if (conn.getResponseCode() >= 400) {
      in = conn.getErrorStream();
    } else {
      in = conn.getInputStream();
    }
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }

  protected void assertRequestMatches(String query, String... matches) throws IOException {
    String contents = getURLContents("/jmx" + query);
    for (String match : matches) {
      Pattern p = Pattern.compile(match);
      Matcher m = p.matcher(contents);
      assertTrue("'" + p + "' does not match " + contents, m.find());
    }
  }

}

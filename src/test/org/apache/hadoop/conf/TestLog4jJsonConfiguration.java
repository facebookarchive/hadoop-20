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
package org.apache.hadoop.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class TestLog4jJsonConfiguration {

  static Logger LOG = Logger.getLogger(TestLog4jJsonConfiguration.class);

  private JSONObject createJSONObject() throws JSONException {
    // Construct json object for log4j
    JSONObject log4j_default = new JSONObject();
    log4j_default.put("hadoop.root.logger", "INFO,console_test,RFA");
    log4j_default.put("hadoop.log.dir", ".");
    log4j_default.put("hadoop.log.file", "hadoop.log");
    log4j_default.put("log4j.rootLogger", "${hadoop.root.logger}");
    log4j_default.put("log4j.threshhold", "ALL");

    // console appender
    log4j_default.put("log4j.appender.console_test", "org.apache.log4j.ConsoleAppender");
    log4j_default.put("log4j.appender.console_test.target", "System.err");
    log4j_default.put("log4j.appender.console_test.layout", "org.apache.log4j.PatternLayout");
    log4j_default.put("log4j.appender.console_test.layout.ConversionPattern",
        "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n");

    // RFA appender
    log4j_default.put("log4j.appender.RFA", "org.apache.log4j.RollingFileAppender");
    log4j_default.put("log4j.appender.RFA.File", "/dev/null");
    log4j_default.put("log4j.appender.RFA.MaxFileSize", "1MB");
    log4j_default.put("log4j.appender.RFA.MaxBackupIndex", "30");
    log4j_default.put("log4j.appender.RFA.layout", "org.apache.log4j.PatternLayout");
    log4j_default.put("log4j.appender.RFA.layout.ConversionPattern",
        "%d{ISO8601} %-5p %c{2} - %m%n");

    // Construct the final json object consisting for core-site and hdfs-site
    JSONObject log4j = new JSONObject();
    log4j.put("default", log4j_default);
    JSONObject properties_files = new JSONObject();
    properties_files.put("log4j", log4j);

    JSONObject json = new JSONObject();
    json.put("properties_files", properties_files);
    return json;
  }

  @Test
  public void testLog4jJsonConfiguration() throws Exception {
    JSONObject json = createJSONObject();
    new Log4jJSONConfigurator().doConfigure(json, LogManager.getLoggerRepository());

    Logger log = LOG.getRootLogger();

    assertEquals("failure - level does not match", log.getEffectiveLevel(), Level.INFO);

    Appender console_test = log.getAppender("console_test");
    assertTrue("failure - no console_test appender configured", console_test != null);
    assertEquals("failure - console_test logger has the wrong class", ConsoleAppender.class,
        console_test.getClass());
    assertEquals("failure - console_test logger target is wrong", "System.err",
        ((ConsoleAppender) console_test).getTarget());
    assertEquals("failure - console_test logger layout is wrong", PatternLayout.class, console_test
        .getLayout().getClass());
    assertTrue("failure - console_test logger layout.ConversionPattern is wrong",
        "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n".equals(((PatternLayout) console_test.getLayout())
            .getConversionPattern()));

    Appender rfa = log.getAppender("RFA");
    assertTrue("failure - no RFA appender configured", rfa != null);
    assertEquals("failure - RFA logger has the wrong class", RollingFileAppender.class,
        rfa.getClass());
    // assertEquals("failure - RFA logger file is wrong", "./hadoop.log",
    // ((RollingFileAppender) rfa).getFile());
    assertEquals("failure - RFA logger MaxFileSize is wrong", 1048576l,
        ((RollingFileAppender) rfa).getMaximumFileSize());
    assertEquals("failure - RFA logger MaxBackupIndex is wrong", 30,
        ((RollingFileAppender) rfa).getMaxBackupIndex());
    assertEquals("failure - RFA logger layout is wrong", PatternLayout.class, rfa.getLayout()
        .getClass());
    assertTrue("failure - RFA logger layout.ConversionPattern is wrong",
        "%d{ISO8601} %-5p %c{2} - %m%n".equals(((PatternLayout) rfa.getLayout())
            .getConversionPattern()));
  }
}

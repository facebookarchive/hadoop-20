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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.PropertiesFromJSON;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.Configurator;
import org.apache.log4j.spi.LoggerRepository;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * log4j Logger Configurator class for configuring log4j using JSON objects.
 * 
 * log4j has a default configuration policy
 * (http://logging.apache.org/log4j/1.2/manual.html)
 * To use that default configuration policy with this class set the following on
 * the command line:
 * 
 * -Dlog4j.configuratorClass=org.apache.hadoop.conf.Log4jJSONConfigurator
 * -Dlog4j.configuration=json_config_file
 * 
 * optionally add:
 * -Dlogj4.json.default.object.path=some.dot.delimited.object.path (see
 * footnote)
 * -Dlog4j.debug (to see log4j debug messages on stdout)
 * 
 * This expects that -Dlog4j.configuration has valid JSON in it
 * 
 * Here's an example JSONObject:
 * 
 * {
 * 'properties_files': {
 * 'log4j': {
 * 'default': {
 * 'hadoop.root.logger': 'INFO,console',
 * 'hadoop.log.dir': '.',
 * 'hadoop.log.file': 'hadoop.log',
 * ...
 * }
 * }
 * }
 * }
 * 
 * Using this example, set
 * -Dlog4j.json.default.object.path=properties_files.log4j.default will use
 * the equivalent object: json['properties_files']['logj4']['default'] to
 * configure log4j. See the docs in org.apache.hadoop.util.PropertiesFromJSON
 * for more details
 */
public class Log4jJSONConfigurator implements Configurator {
  public static final String LOG4J_JSON_DEFAULT_OBJECT_PATH = System.getProperty(
      "log4j.json.default.object.path", "properties_files.log4j.default");
  public static final String LOG4J_JSON_CUSTOM_OBJECT_PATH = System.getProperty(
      "log4j.json.custom.object.path", null);

  @Override
  public void doConfigure(URL url, LoggerRepository repository) {
    new PropertyConfigurator().doConfigure(getProperties(url), repository);
  }

  void doConfigure(JSONObject json, LoggerRepository repository) {
    new PropertyConfigurator().doConfigure(getProperties(json), repository);
  }

  private static Properties getProperties(JSONObject json) {
    try {
      return PropertiesFromJSON.getProperties(json, LOG4J_JSON_DEFAULT_OBJECT_PATH,
          LOG4J_JSON_CUSTOM_OBJECT_PATH);
    } catch (JSONException e) {
      LogLog.error("JSONException: " + e.getMessage());
    }
    return null;
  }

  private static Properties getProperties(URL configURL) {
    try {
      URLConnection conn = configURL.openConnection();
      InputStream in = conn.getInputStream();
      String jsonString = IOUtils.toString(in, "UTF-8");
      JSONObject json = new JSONObject(jsonString);
      return getProperties(json);
    } catch (IOException e) {
      LogLog.error("IOException: " + e.getMessage());
    } catch (JSONException e) {
      LogLog.error("JSONException: " + e.getMessage());
    }
    return null;
  }
}

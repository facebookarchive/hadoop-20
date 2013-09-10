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

package org.apache.hadoop.util;

import java.util.Iterator;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * static-ish class that builds a Properties object from JSON (or a JSON file)
 * 
 * Takes a JSONObject or an InputStream and a root object path. The root object
 * path
 * provides a tree-like traversal from the JSONObject pass (or read in) before
 * building
 * the properties object
 * 
 * Example JSON object:
 * {
 * 'log4j_properties': {
 * 'default': {
 * 'hadoop.root.logger': 'INFO,console',
 * 'hadoop.log.dir': '.',
 * 'hadoop.log.file': 'hadoop.log',
 * ...
 * }
 * },
 * }
 * 
 * The root object path is '.' delimited roughly translates as follows:
 * 'log4j_properties.default' => json['logj4_properties']['default']
 * 'root.key1.key2' => json['root']['key1']['key2']
 * 
 * If the traversal path needs to resolve a JSONArray instead of a JSONObject
 * use indicies instead of object names, e.g.:
 * 'root.0.1.2' => json['root'][0][1][2]
 * 
 * If a method that takes customObjectPath is called then it will first try to
 * return
 * the customObjectPath and if that doesn't exist fall back to using the
 * rootObjectPath.
 * This facilitates configs like:
 * {
 * 'log4j_properties': {
 * 'default': {...},
 * 'TESTDFS': {...},
 * }
 * 
 * So 'TESTDFS' will be used if it exists, otherwise it can fall back to 'default'
 */
public class PropertiesFromJSON {

  private PropertiesFromJSON() {
  }

  /**
   * Uses an object path to traverse the json object and return a Properties
   * object
   * from the JSONObject (map<string, string>) object at the end of the path. If
   * customObjectPath
   * is null or isn't a valid path for the json object then rootObjectPath is
   * used as the path.
   * 
   * @param json
   * @param rootObjectPath
   * @param customObjectPath
   * @return
   * @throws JSONException
   * @throws InvalidJSONPathException
   */
  public static Properties getProperties(JSONObject json, String rootObjectPath,
      String customObjectPath) throws JSONException, InvalidJSONPathException {
    if (customObjectPath != null && !customObjectPath.equals("")) {
      try {
        return getPropertiesFromTraversal(json, customObjectPath);
      } catch (InvalidJSONPathException e) {
        // fall through to using rootObjectPath
      }
    }
    return getPropertiesFromTraversal(json, rootObjectPath);
  }

  /**
   * Uses an object path to traverse the json array and return a Properties
   * object
   * from the JSONObject (map<string, string>) object at the end of the path. If
   * customObjectPath
   * is null or isn't a valid path for the json object then rootObjectPath is
   * used as the path.
   * 
   * @param json
   * @param rootObjectPath
   * @param customObjectPath
   * @return
   * @throws JSONException
   * @throws InvalidJSONPathException
   */
  public static Properties getProperties(JSONArray json, String rootObjectPath,
      String customObjectPath) throws JSONException, InvalidJSONPathException {
    if (customObjectPath != null && !customObjectPath.equals("")) {
      try {
        return getPropertiesFromTraversal(json, customObjectPath);
      } catch (InvalidJSONPathException e) {
        // fall through to using rootObjectPath
        //
      }
    }
    return getPropertiesFromTraversal(json, rootObjectPath);
  }

  private static Properties getPropertiesFromTraversal(JSONObject json, String objectPath)
      throws JSONException, InvalidJSONPathException {
    if (objectPath != null && !objectPath.equals("")) {
      if (objectPath.startsWith("$.")) {
        return getPropertiesFromTraversal(json, objectPath.substring(2));
      }

      int nextDotIndex = objectPath.indexOf(".");
      String nextObjectPath = null;
      String remainderObjectPath;
      if (nextDotIndex < 0) {
        nextObjectPath = objectPath;
        remainderObjectPath = null;
      } else {
        nextObjectPath = objectPath.substring(0, nextDotIndex);
        remainderObjectPath = objectPath.substring(nextDotIndex + 1);
      }
      if (!json.has(nextObjectPath)) {
        throw new InvalidJSONPathException("'" + nextObjectPath + "' cannot be found in "
            + json.toString());
      }

      JSONObject nextJSONObject = json.optJSONObject(nextObjectPath);
      if (nextJSONObject != null) {
        return getPropertiesFromTraversal(nextJSONObject, remainderObjectPath);
      }

      JSONArray nextJSONArray = json.optJSONArray(nextObjectPath);
      if (nextJSONArray != null) {
        return getPropertiesFromTraversal(nextJSONArray, remainderObjectPath);
      }

      throw new InvalidJSONPathException("'" + nextObjectPath
          + "' is neither a JSONObject nor a JSONArray: " + json.toString());
    }

    Properties properties = new Properties();
    String key = null;
    for (Iterator<?> keys = json.keys(); keys.hasNext(); key = (String) keys.next()) {
      try {
        properties.setProperty(key, json.getString(key));
      } catch (JSONException e) {
      }
    }
    return properties;
  }

  private static Properties getPropertiesFromTraversal(JSONArray json, String objectPath)
      throws JSONException, InvalidJSONPathException {
    if (objectPath == null || objectPath.equals("")) {
      throw new InvalidJSONPathException("no index provided to reference JSONArray "
          + json.toString());
    }

    int nextDotIndex = objectPath.indexOf(".");
    int nextObjectIndex;
    String nextObjectPath = null;
    String remainderObjectPath;
    try {
      if (nextDotIndex < 0) {
        nextObjectPath = objectPath;
        remainderObjectPath = null;
      } else {
        nextObjectPath = objectPath.substring(0, nextDotIndex - 1);
        remainderObjectPath = objectPath.substring(nextDotIndex + 1);
      }
      nextObjectIndex = Integer.valueOf(nextObjectPath);
    } catch (NumberFormatException nfe) {
      throw new InvalidJSONPathException("'" + nextObjectPath
          + "' is not a valid integer index for " + json.toString());
    }
    if (nextObjectIndex < 0 || json.length() < nextObjectIndex) {
      throw new InvalidJSONPathException("'" + nextObjectIndex
          + "' is larger than the size of JSONArray " + json.toString());
    }

    JSONObject nextJSONObject = json.optJSONObject(nextObjectIndex);
    if (nextJSONObject != null) {
      return getPropertiesFromTraversal(nextJSONObject, remainderObjectPath);
    }

    JSONArray nextJSONArray = json.optJSONArray(nextObjectIndex);
    if (nextJSONArray != null) {
      return getPropertiesFromTraversal(nextJSONArray, remainderObjectPath);
    }

    throw new InvalidJSONPathException("'" + nextObjectPath
        + "' is neither a JSONObject nor a JSONArray: " + json.toString());
  }
}

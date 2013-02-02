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
package org.apache.hadoop.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class LogSample{
  public Map<String, Long> intColumns =
    new HashMap<String, Long>();
  public Map<String, String> normalColumns =
    new HashMap<String, String>();
  public Map<String, String> denormColumns =
    new HashMap<String, String>();
  public Map<String, List<String> > normvecColumns =
    new HashMap<String, List<String>>();

  public void addIntValue(String key, Long value){
    intColumns.put(key, value);
  }

  public void incrIntValue(String key, Long value){
    if(intColumns.containsKey(key)){
      intColumns.put(key, intColumns.get(key) + value);
    }else{
      intColumns.put(key, value);
    }
  }

  public void addNormalValue(String key, String value){
    normalColumns.put(key, value);
  }

  public void addDenormValue(String key, String value){
    denormColumns.put(key, value);
  }

  public void addNormVectorValue(String key, List<String> array){
    normvecColumns.put(key, array);
  }

  public String toJSON() {
    return toJSON(true);
  }

  public String toString() {
    return toJSON(false);
  }

  private String toJSON(boolean addTime) {
    JSONObject jsonObject = new JSONObject();
    try {
      // auto-fill time if necessary
      JSONObject intColumnsJSON = new JSONObject(intColumns);
      if (addTime &&!intColumnsJSON.has("time")) {
        intColumnsJSON.put("time", System.currentTimeMillis() / 1000);
      }
      jsonObject.put("int", intColumnsJSON);
      if(!normalColumns.isEmpty()) {
        jsonObject.put("normal", normalColumns);
      }
      if(!denormColumns.isEmpty()) {
        jsonObject.put("denorm", denormColumns);
      }
      if(!normvecColumns.isEmpty()) {
        jsonObject.put("normvector", normvecColumns);
      }
      return jsonObject.toString();
    } catch (JSONException e) {
      return e.toString();
    }
  }
}

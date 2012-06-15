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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A class with the information of a raid codec.
 * A raid codec has the information of
 * 1. Which ErasureCode used
 * 2. Stripe and parity length
 * 3. Parity directory location
 * 4. Codec priority
 */
public class Codec {

  public static final Log LOG = LogFactory.getLog(Codec.class);

  /**
   * Used by ErasureCode.init() to get Code specific extra parameters.
   */
  public final JSONObject json;

  /**
   * id of the codec. Used by policy in raid.xml
   */
  public final String id;

  /**
   * Number of blocks in one stripe
   */
  public final int stripeLength;

  /**
   * Number of parity blocks of the codec for one stripe
   */
  public final int parityLength;

  /**
   * The full class name of the ErasureCode used
   */
  public final String erasureCodeClass;

  /**
   * Human readable description of the codec
   */
  public final String description;

  /**
   * Where to store the parity files
   */
  public final String parityDirectory;

  /**
   * Where to store the temp parity files
   */
  public final String tmpParityDirectory;

  /**
   * Where to store the temp har files
   */
  public final String tmpHarDirectory;

  /**
   * Priority of the codec.
   *
   * Purge parity files:
   *   When parity files of two Codecs exists, the parity files of the lower
   *   priority codec will be purged.
   *
   * Generating parity files:
   *   When a source files are under two policies, the policy with a higher
   *   codec priority will be triggered.
   */
  public final int priority;

  private static List<Codec> codecs;
  private static Map<String, Codec> idToCodec;

  /**
   * Get single instantce of the list of codecs ordered by priority.
   */
  public static List<Codec> getCodecs() {
    return Codec.codecs;
  }

  /**
   * Get the instance of the codec by id
   */
  public static Codec getCodec(String id) {
    return idToCodec.get(id);
  }

  static {
    try {
      Configuration.addDefaultResource("hdfs-default.xml");
      Configuration.addDefaultResource("hdfs-site.xml");
      initializeCodecs(new Configuration());
    } catch (JSONException e) {
      LOG.fatal("Fail initialize Raid codecs", e);
      System.exit(-1);
    }
  }

  protected static void initializeCodecs(Configuration conf) throws JSONException {
    String source = conf.get("raid.codecs.json");
    if (source == null) {
      codecs = Collections.emptyList();
      idToCodec = Collections.emptyMap();
      return;
    }
    JSONArray jsonArray = new JSONArray(source);
    codecs = new ArrayList<Codec>();
    idToCodec = new HashMap<String, Codec>();
    for (int i = 0; i < jsonArray.length(); ++i) {
      Codec codec = new Codec(jsonArray.getJSONObject(i));
      idToCodec.put(codec.id, codec);
      codecs.add(codec);
    }
    Collections.sort(codecs, new Comparator<Codec>() {
      @Override
      public int compare(Codec c1, Codec c2) {
        // Higher priority on top
        return c2.priority - c1.priority;
      }
    });
    codecs = Collections.unmodifiableList(codecs);
    idToCodec = Collections.unmodifiableMap(idToCodec);
  }

  public Codec(JSONObject json) throws JSONException {
    this.json = json;
    this.id = json.getString("id");
    this.parityLength = json.getInt("parity_length");
    this.stripeLength = json.getInt("stripe_length");
    this.erasureCodeClass = json.getString("erasure_code");
    this.parityDirectory = json.getString("parity_dir");
    this.priority = json.getInt("priority");
    this.description = getJSONString(json, "description", "");

    this.tmpParityDirectory = getJSONString(
        json, "tmp_parity_dir", "/tmp" + this.parityDirectory);

    this.tmpHarDirectory = getJSONString(
        json, "tmp_har_dir", "/tmp" + this.parityDirectory + "_har");
  }

  static private String getJSONString(
      JSONObject json, String key, String defaultResult) {
    String result = defaultResult;
    try {
      result = json.getString(key);
    } catch (JSONException e) {
    }
    return result;
  }

  public ErasureCode createErasureCode(Configuration conf) throws IOException {
    // Create the scheduler
    Class<? extends ErasureCode> erasureCode
      = conf.getClass(erasureCodeClass, ReedSolomonCode.class, ErasureCode.class);
    ErasureCode code = (ErasureCode) ReflectionUtils.newInstance(erasureCode, conf);
    code.init(this);
    return code;
  }

}

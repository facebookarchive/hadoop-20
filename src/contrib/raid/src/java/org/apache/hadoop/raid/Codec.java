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
import java.io.Serializable;
import java.lang.IllegalArgumentException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class Codec implements Serializable {

  public static final Log LOG = LogFactory.getLog(Codec.class);
  
  public static final String ERASURE_CODE_KEY_PREFIX = "hdfs.raid.erasure.code.";

  /**
   * Used by ErasureCode.init() to get Code specific extra parameters.
   */
  public final String jsonStr;

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
  public String parityDirectoryPS;
  

  /**
   * Where to store the temp parity files
   */
  public final String tmpParityDirectory;
  public String tmpParityDirectoryPS;

  /**
   * Where to store the temp har files
   */
  public final String tmpHarDirectory;
  public String tmpHarDirectoryPS;
  
  /**
   * Simulate the block fix or not
   */
  public boolean simulateBlockFix;

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

  /**
   * Is file-level raiding or directory-level raiding
   */
  public boolean isDirRaid;

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
      Configuration.addDefaultResource("raid-default.xml");
      Configuration.addDefaultResource("hdfs-site.xml");
      Configuration.addDefaultResource("raid-site.xml");
      initializeCodecs(new Configuration());
    } catch (Exception e) {
      LOG.fatal("Fail initialize Raid codecs", e);
      System.exit(-1);
    }
  }

  public static void initializeCodecs(Configuration conf) throws IOException {
    try {
      String source = conf.get("raid.codecs.json");
      if (source == null) {
        codecs = Collections.emptyList();
        idToCodec = Collections.emptyMap();
        if (LOG.isDebugEnabled()) {
          LOG.debug("None Codec is specified");
        }
        return;
      }
      JSONArray jsonArray = new JSONArray(source);
      List<Codec> localCodecs = new ArrayList<Codec>();
      Map<String, Codec> localIdToCodec = new HashMap<String, Codec>();
      for (int i = 0; i < jsonArray.length(); ++i) {
        Codec codec = new Codec(jsonArray.getJSONObject(i));
        localIdToCodec.put(codec.id, codec);
        localCodecs.add(codec);
      }
      Collections.sort(localCodecs, new Comparator<Codec>() {
        @Override
        public int compare(Codec c1, Codec c2) {
          // Higher priority on top
          return c2.priority - c1.priority;
        }
      });
      codecs = Collections.unmodifiableList(localCodecs);
      idToCodec = Collections.unmodifiableMap(localIdToCodec);
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  private Codec(JSONObject json) throws JSONException {
    this.jsonStr = json.toString();
    this.id = json.getString("id");
    this.parityLength = json.getInt("parity_length");
    this.stripeLength = json.getInt("stripe_length");
    this.erasureCodeClass = json.getString("erasure_code");
    this.parityDirectory = json.getString("parity_dir");
    this.priority = json.getInt("priority");
    this.description = getJSONString(json, "description", "");
    this.isDirRaid = Boolean.parseBoolean(getJSONString(json, "dir_raid", "false"));
    this.tmpParityDirectory = getJSONString(
        json, "tmp_parity_dir", "/tmp" + this.parityDirectory);
    this.tmpHarDirectory = getJSONString(
        json, "tmp_har_dir", "/tmp" + this.parityDirectory + "_har");
    this.simulateBlockFix = json.getBoolean("simulate_block_fix"); 
    checkDirectory(parityDirectory);
    checkDirectory(tmpParityDirectory);
    checkDirectory(tmpHarDirectory);
    setPathSeparatorDirectories();
  }

  /**
   * Make sure the direcotry string has the format "/a/b/c"
   */
  private void checkDirectory(String d) {
    if (!d.startsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Bad directory:" + d);
    }
    if (d.endsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Bad directory:" + d);
    }
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

  public ErasureCode createErasureCode(Configuration conf) {
    // Create the scheduler
    Class<?> erasureCode = null;
    try {
      erasureCode = conf.getClass(ERASURE_CODE_KEY_PREFIX + this.id,
            conf.getClassByName(this.erasureCodeClass));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ErasureCode code = (ErasureCode) ReflectionUtils.newInstance(erasureCode,
        conf);
    code.init(this.stripeLength, this.parityLength);
    return code;
  }

  @Override
  public String toString() {
    if (jsonStr == null) {
      return "Test codec " + id;
    } else {
      return jsonStr;
    }
  }
  
  public String getParityPrefix() {
    String prefix = this.parityDirectory;
    if (!prefix.endsWith(Path.SEPARATOR)) {
      prefix += Path.SEPARATOR;
    }
    return prefix;
  }

  /**
   * Used by unit test only
   */
  static void addCodec(Codec codec) {
    List<Codec> newCodecs = new ArrayList<Codec>();
    newCodecs.addAll(codecs);
    newCodecs.add(codec);
    codecs = Collections.unmodifiableList(newCodecs);

    Map<String, Codec> newIdToCodec = new HashMap<String, Codec>();
    newIdToCodec.putAll(idToCodec);
    newIdToCodec.put(codec.id, codec);
    idToCodec = Collections.unmodifiableMap(newIdToCodec);
  }

  /**
   * Used by unit test only
   */
  static void clearCodecs() {
    codecs = Collections.emptyList();
    idToCodec = Collections.emptyMap();
  }

  /**
   * Used by unit test only
   */
  Codec(String id,
                int parityLength,
                int stripeLength,
                String erasureCodeClass,
                String parityDirectory,
                int priority,
                String description,
                String tmpParityDirectory,
                String tmpHarDirectory,
                boolean isDirRaid,
                boolean simulateBlockFix) {
    this.jsonStr = null;
    this.id = id;
    this.parityLength = parityLength;
    this.stripeLength = stripeLength;
    this.erasureCodeClass = erasureCodeClass;
    this.parityDirectory = parityDirectory;
    this.priority = priority;
    this.description = description;
    this.tmpParityDirectory = tmpParityDirectory;
    this.tmpHarDirectory = tmpHarDirectory;
    this.isDirRaid = isDirRaid;
    this.simulateBlockFix = simulateBlockFix;
    setPathSeparatorDirectories();
  }
  
  private void setPathSeparatorDirectories() {
    parityDirectoryPS = parityDirectory + Path.SEPARATOR;
    tmpParityDirectoryPS = tmpParityDirectory + Path.SEPARATOR;
    tmpHarDirectoryPS = tmpHarDirectory + Path.SEPARATOR;
  }
}

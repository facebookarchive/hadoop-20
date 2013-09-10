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

package org.apache.hadoop.raid.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.raid.Codec;

/**
 * Maintains information about one policy
 */
public class PolicyInfo implements Writable {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.raid.protocol.PolicyInfo");
  protected static final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Path srcPath;            // the specified src path
  private Path fileListPath;       // this path to the file containing list of files.
  private String policyName;       // name of policy
  private String codecId;          // the codec used
  private boolean shouldRaid;      // Should we raid files or not.
  private String description;      // A verbose description of this policy
  private Configuration conf;      // Hadoop configuration

  private Properties properties;   // Policy-dependent properties

  /**
   * Create the empty object
   */
  public PolicyInfo() {
    this.conf = null;
    this.policyName = "";
    this.description = "";
    this.srcPath = null;
    this.properties = new Properties();
    this.shouldRaid = true;
  }

  /**
   * Create the metadata that describes a policy
   */
  public PolicyInfo(String policyName, Configuration conf) {
    this.conf = conf;
    this.policyName = policyName;
    this.description = "";
    this.srcPath = null;
    this.properties = new Properties();
    this.shouldRaid = true;
  }

  /**
   * Copy fields from another PolicyInfo
   */
  public void copyFrom(PolicyInfo other) {
    if (other.conf != null) {
      this.conf = other.conf;
    }
    if (other.policyName != null && other.policyName.length() > 0) {
      this.policyName = other.policyName;
    }
    if (other.description != null && other.description.length() > 0) {
      this.description = other.description;
    }
    if (other.codecId != null) {
      this.codecId = other.codecId;
    }
    if (other.srcPath != null) {
      this.srcPath = other.srcPath;
    }
    if (other.fileListPath != null) {
      this.fileListPath = other.fileListPath;
    }
    for (Object key : other.properties.keySet()) {
      String skey = (String) key;
      this.properties.setProperty(skey, other.properties.getProperty(skey));
    }
    LOG.info(this.policyName + ".codecId " + codecId);
    LOG.info(this.policyName + ".fileListPath " + fileListPath);
    LOG.info(this.policyName + ".srcpath " + srcPath);
  }

  /**
   * Sets the input path on which this policy has to be applied
   */
  public void setSrcPath(String in) throws IOException {
    srcPath = new Path(in);
    srcPath = srcPath.makeQualified(srcPath.getFileSystem(conf));
  }

  /**
   * Sets the location of the file that contains the list of files to be raided.
   */
  public void setFileListPath(Path p) {
    this.fileListPath = p;
  }

  /**
   * Set the codec used by this policy
   */
  public void setCodecId(String id) {
    this.codecId = id;
  }

  public void setShouldRaid(boolean val) {
    this.shouldRaid = val;
  }

  /**
   * Set the description of this policy.
   */
  public void setDescription(String des) {
    this.description = des;
  }

  /**
   * Sets an internal property.
   * @param name property name.
   * @param value property value.
   */
  public void setProperty(String name, String value) {
    properties.setProperty(name, value);
  }
  
  /**
   * Returns the value of an internal property.
   * @param name property name.
   */
  public String getProperty(String name) {
    return properties.getProperty(name);
  }
  
  /**
   * Get the name of this policy.
   */
  public String getName() {
    return this.policyName;
  }

  /**
   * Get the destination path of this policy.
   */
  public String getCodecId() {
    return this.codecId;
  }

  public boolean getShouldRaid() {
    return this.shouldRaid;
  }

  /**
   * Get the srcPath
   */
  public Path getSrcPath() {
    return srcPath;
  }

  /**
   * Get the expanded (unglobbed) forms of the srcPaths
   */
  public List<Path> getSrcPathExpanded() throws IOException {
    FileSystem fs = srcPath.getFileSystem(conf);

    // globbing on srcPath
    FileStatus[] gpaths = fs.globStatus(srcPath);
    if (gpaths == null) {
      return Collections.emptyList();
    }
    List<Path> results = new ArrayList<Path>(gpaths.length);
    for (FileStatus f : gpaths) {
      results.add(f.getPath().makeQualified(fs));
    }
    return removeConflictPath(results);
  }

  /**
   * Get the file list path.
   */
  public Path getFileListPath() {
    return fileListPath;
  }

  private List<Path> removeConflictPath(List<Path> expandedPaths)
      throws IOException {
    String destPrefix = Codec.getCodec(codecId).parityDirectory;
    destPrefix = normalizePath(destPrefix);
    List<Path> filtered = new ArrayList<Path>(expandedPaths.size());
    for (Path path : expandedPaths) {
      String pathStr = path.toString();
      if (pathStr.startsWith(destPrefix) || destPrefix.startsWith(pathStr)) {
        LOG.warn("Bad policy file. Path:" + pathStr +
            " conflicts with Prefix:" + destPrefix);
        continue;
      }
      filtered.add(path);
    }
    return filtered;
  }

  private String normalizePath(String path) {
    if (!path.endsWith(Path.SEPARATOR)) {
      path += Path.SEPARATOR;
    }
    return path;
  }
  /**
   * Convert this policy into a printable form
   */
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append("Policy Name:\t" + policyName + " --------------------\n");
    buff.append("Source Path:\t" + srcPath + "\n");
    buff.append("Codec:\t" + codecId + "\n");
    for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
      String name = (String) e.nextElement(); 
      buff.append( name + ":\t" + properties.getProperty(name) + "\n");
    }
    if (description.length() > 0) {
      int len = Math.min(description.length(), 80);
      String sub = description.substring(0, len).trim();
      sub = sub.replaceAll("\n", " ");
      buff.append("Description:\t" + sub + "...\n");
    }
    return buff.toString();
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (PolicyInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new PolicyInfo(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    if (srcPath == null) {
      Text.writeString(out, "");
    } else {
      Text.writeString(out, srcPath.toString());
    }
    if (fileListPath == null) {
      Text.writeString(out, "");
    } else {
      Text.writeString(out, fileListPath.toString());
    }
    Text.writeString(out, policyName);
    Text.writeString(out, codecId);
    Text.writeString(out, description);
    out.writeInt(properties.size());
    for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
      String name = (String) e.nextElement(); 
      Text.writeString(out, name);
      Text.writeString(out, properties.getProperty(name));
    }
  }

  public void readFields(DataInput in) throws IOException {
    String text = Text.readString(in);
    if (text.length() == 0) {
      this.srcPath = null;
    } else {
      this.srcPath = new Path(text);
    }
    text = Text.readString(in);
    if (text.length() == 0) {
      this.fileListPath = null;
    } else {
      this.fileListPath = new Path(text);
    }
    this.policyName = Text.readString(in);
    this.codecId = Text.readString(in);
    this.description = Text.readString(in);
    for (int n = in.readInt(); n>0; n--) {
      String name = Text.readString(in);
      String value = Text.readString(in);
      properties.setProperty(name,value);
    }
  }
}

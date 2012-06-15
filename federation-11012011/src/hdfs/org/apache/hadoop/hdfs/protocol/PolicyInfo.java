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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Properties;
import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
import java.text.SimpleDateFormat;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

/**
 * Maintains information about one policy
 */
public class PolicyInfo implements Writable {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.hdfs.protocol.PolicyInfo");
  protected static final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static public class PathInfo {
    public Path rpath;              // the specified path
    public Properties myproperties;   // intended repl factor for file
    PathInfo(Path rpath, Properties prop) {
      this.rpath = rpath;
      this.myproperties = prop;
    }
    public String getProperty(String name) {
      return myproperties.getProperty(name);
    }
    public Path getPath() {
      return rpath;
    }
  }

  private Path srcPath;        // the specified src path
  private String description;      // A verbose description of this policy
  private Configuration conf;      // Hadoop configuration

  private Properties properties;   // Policy-dependent properties

  private List<PathInfo> destPath; // all the destination paths 

  /**
   * Create the empty object
   */
  public PolicyInfo() {
    this.conf = null;
    this.description = "";
    this.srcPath = null;
    this.destPath = null;
    this.properties = new Properties();
  }

  /**
   * Create the metadata that describes a policy
   */
  public PolicyInfo(String  srcPath, Configuration conf) {
    this.conf = conf;
    this.description = "";
    this.srcPath = new Path(srcPath);
    this.destPath = null;
    this.properties = new Properties();
  }

  /**
   * Copy fields from another PolicyInfo
   */
  public void copyFrom(PolicyInfo other) throws IOException {
    if (other.conf != null) {
      this.conf = other.conf;
    }
    if (other.description != null && other.description.length() > 0) {
      this.description = other.description;
    }
    if (other.srcPath != null) {
      this.srcPath = other.srcPath;
    }
    for (Object key : other.properties.keySet()) {
      String skey = (String) key;
      this.properties.setProperty(skey, other.properties.getProperty(skey));
    }
    
    if (other.destPath != null) {
      for (PathInfo p:destPath) {
        this.addDestPath(p.rpath.toString(), p.myproperties);
      }
    }
  }

  /**
   * Sets the input path on which this policy has to be applied
   */
  public void setSrcPath(String in) throws IOException {
    srcPath= new Path(in);
    if (!srcPath.isAbsolute() || !srcPath.toUri().isAbsolute()) {
      throw new IOException("Path " + in +  " is not absolute.");
    }
  }

  /**
   * Sets the destination path on which this policy has to be applied
   */
  public void addDestPath(String in, Properties repl) throws IOException {
    Path dPath = new Path(in);
    if (!dPath.isAbsolute() || !dPath.toUri().isAbsolute()) {
      throw new IOException("Path " + in +  " is not absolute.");
    }
    PathInfo pinfo = new PathInfo(dPath, repl);
    if (this.destPath == null) {
      this.destPath = new ArrayList<PathInfo>();
    }
    this.destPath.add(pinfo);
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
   * Get the srcPath
   */
  public Path getSrcPath() {
    return srcPath;
  }

  /**
   * Get the destPath
   */
  public List<PathInfo> getDestPaths() throws IOException {
    return destPath;
  }

  /**
   * Get the Configuration
   */
  public Configuration getConf() throws IOException {
    return this.conf;
  }

  /**
   * Convert this policy into a printable form
   */
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append("Source Path:\t" + srcPath + "\n");
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
    if (destPath != null) {
      for (PathInfo p:destPath) {
        buff.append("Destination Path:\t" + p.rpath + "\n");
        for (Enumeration<?> e = p.myproperties.propertyNames(); e.hasMoreElements();) {
          String name = (String) e.nextElement(); 
          buff.append( name + ":\t\t" + p.myproperties.getProperty(name) + "\n");
        }
      }
    }
    return buff.toString();
  }

  /**
   * Sort Policies based on their srcPath. reverse lexicographical order.
   */
  public static class CompareByPath implements Comparator<PolicyInfo> {
    public CompareByPath() throws IOException {
    }
    public int compare(PolicyInfo l1, PolicyInfo l2) {
      return 0 - l1.getSrcPath().compareTo(l2.getSrcPath());
    }
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
    Text.writeString(out, srcPath.toString());
    Text.writeString(out, description);
    out.writeInt(properties.size());
    for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
      String name = (String) e.nextElement(); 
      Text.writeString(out, name);
      Text.writeString(out, properties.getProperty(name));
    }
    out.writeInt(destPath.size());
    for (PathInfo p:destPath) {
      Text.writeString(out, p.rpath.toString());
      out.writeInt(p.myproperties.size());
      for (Enumeration<?> e = p.myproperties.propertyNames(); e.hasMoreElements();) {
        String name = (String) e.nextElement(); 
        Text.writeString(out, name);
        Text.writeString(out, p.myproperties.getProperty(name));
      }
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.srcPath = new Path(Text.readString(in));
    this.description = Text.readString(in);
    for (int n = in.readInt(); n>0; n--) {
      String name = Text.readString(in);
      String value = Text.readString(in);
      properties.setProperty(name,value);
    }
    for (int n = in.readInt(); n>0; n--) {
      String destPath = Text.readString(in);
      Properties p = new Properties();
      for (int m = in.readInt(); m>0; m--) {
        String name = Text.readString(in);
        String value = Text.readString(in);
        p.setProperty(name,value);
      }
      this.addDestPath(destPath, p);
    }
  }
}

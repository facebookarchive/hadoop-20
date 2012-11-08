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

import java.io.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
public class HostsFileReader {
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;
  
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);

  public HostsFileReader(String inFile, String exFile) throws IOException {
    this(inFile, exFile, false);
  }

  public HostsFileReader(String inFile, String exFile, boolean enforceValidation)
      throws IOException {
    if (enforceValidation) {
      validateHostFiles(inFile, exFile);
    }
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  private void readFileToSet(String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    if (!file.exists()) {
      LOG.warn("Reading hosts file: " + filename + " File does not exist");
      return;
    }
    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].trim().startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].equals("")) {
              set.add(nodes[i]);  // might need to add canonical name
            }
          }
        }
      } 
      LOG.info("Reading hosts file: " + filename + ", read " + set.size() + " nodes");
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public Set<String> getNewIncludes() throws IOException {
    if (!includesFile.equals("")) {
      Set<String> newIncludes = new HashSet<String>();
      readFileToSet(includesFile, newIncludes);
      return newIncludes;
    }
    return null;
  }

  public Set<String> getNewExcludes() throws IOException {
    if (!excludesFile.equals("")) {
      Set<String> newExcludes = new HashSet<String>();
      readFileToSet(excludesFile, newExcludes);
      return newExcludes;
    }
    return null;
  }

  public synchronized void switchFiles(Set<String> includes,
      Set<String> excludes) {
    if (includes != null) {
      this.includes = includes;
    }
    if (excludes != null) {
      this.excludes = excludes;
    }
  }

  public synchronized void refresh() throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    if (!includesFile.equals("")) {
      Set<String> newIncludes = new HashSet<String>();
      readFileToSet(includesFile, newIncludes);
      // switch the new hosts that are to be included
      includes = newIncludes;
      LOG.info("Refreshing hosts - read: " + includes.size() + " includes hosts");
    }
    if (!excludesFile.equals("")) {
      Set<String> newExcludes = new HashSet<String>();
      readFileToSet(excludesFile, newExcludes);
      // switch the excluded hosts
      excludes = newExcludes;
      LOG.info("Refreshing hosts - read: " + excludes.size() + " excludes hosts");
    }
  }

  public synchronized Set<String> getHosts() {
    return includes;
  }

  public synchronized Set<String> getHostNames() {
    Set<String> hostNames = new HashSet<String>();
    for (String host: includes) {
      // get the node's host name
      int colon = host.indexOf(":");
      String hostName = (colon == -1) ? host : host.substring(0, colon);
      hostNames.add(hostName);
    }
    return hostNames;
  }

  public synchronized Set<String> getExcludedHosts() {
    return excludes;
  }

  public synchronized void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }
  
  public synchronized void updateFileNames(String includesFile,
      String excludesFile) throws IOException {
    updateFileNames(includesFile, excludesFile, false);
  }

  public synchronized void updateFileNames(String includesFile,
      String excludesFile, boolean validate) throws IOException {
    if (validate) {
      validateHostFiles(includesFile, excludesFile);
    }
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
  
  public static void validateHostFiles(String inFile, String exFile)
      throws IOException {
    File include = new File(inFile);
    File exclude = new File(exFile);

    String msg = "";
    if (!inFile.equals("") && !include.exists())
      msg += "Host includes file " + include + " does not exist!\n";
    if (!exFile.equals("") && !exclude.exists())
      msg += "Host excludes file " + exclude + " does not exist!\n";

    if (!msg.isEmpty()) {
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * Checks if a host is part of the cluster as per configuration.
   * For this the host must be "included" and not "excluded".
   * An empty includes files means the host is "included".
   * @param host The host name.
   * @return A boolean indicating if the host is allowed.
   */
  public synchronized boolean isAllowedHost(String host) {
    boolean isIncluded = includes.isEmpty() || includes.contains(host);
    boolean isExcluded = excludes.contains(host);
    return isIncluded && !isExcluded;
  }
}

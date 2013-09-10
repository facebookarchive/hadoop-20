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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.util.FlushableLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class ValidateNamespaceDirPolicy {
  
  private static final Log LOG = LogFactory
      .getLog(ValidateNamespaceDirPolicy.class.getName());
  
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);
  
  /**
   * Used for storing properties of each of the storage locations.
   */
  static class NNStorageLocation {
    public NNStorageLocation(URI location, String mountPoint,
        StorageLocationType type) {
      this.location = location;
      this.mountPoint = mountPoint;
      this.type = type;
    }

    URI location;
    String mountPoint;
    StorageLocationType type;
    
    public String toString() {
      return "location: " + location + ",mount point: " + mountPoint
          + ", type: " + type;
    }
  }


  public static Map<URI, NNStorageLocation> validate(Configuration conf)
      throws IOException {
    Map<URI, NNStorageLocation> locationMap = new HashMap<URI, NNStorageLocation>();
    int policy = conf.getInt("dfs.name.dir.policy", 0);

    String nameDirConfig = "dfs.name.dir";
    Collection<URI> dirNamesURIs = NNStorageConfiguration.getNamespaceDirs(conf);
    validatePolicy(conf, policy, dirNamesURIs, nameDirConfig, locationMap);

    String nameEditsDirConfig = "dfs.name.edits.dir";
    Collection<URI> editsDirNamesURIs = NNStorageConfiguration.getNamespaceEditsDirs(conf);
    validatePolicy(conf, policy, editsDirNamesURIs, nameEditsDirConfig, locationMap);
    
    return locationMap;
  }

  static void validatePolicy(Configuration conf,
                        int policy,
                        Collection<URI> configuredLocations,
                        String configName,
                        Map<URI, NNStorageLocation> result)
    throws IOException {
    /* DFS name node directory policy:
      0 - No enforcement
      1 - Enforce that there should be at least two copies and they must be on
          different devices
      2 - Enforce that there should be at least two copies on different devices
          and at least one must be on an NFS/remote device
    */
    
    // convert uri's for directory names
    
    List<NNStorageLocation> locations = new ArrayList<NNStorageLocation>();
    String shared = conf.get(configName + ".shared");
    URI sharedLocation = shared == null ? null : Util.stringAsURI(shared);
    
    for (URI name : configuredLocations) {
      FLOG.info("Conf validation - checking location: " + name);
      NNStorageLocation desc = checkLocation(name, conf, sharedLocation);
      locations.add(desc);
      result.put(desc.location, desc);
      FLOG.info("Conf validation - checked location: " + desc);
    }
    
    switch (policy) {
      case 0:
        // No check needed.
        break;

      case 1:
      case 2:
        boolean foundRemote = false;
        HashSet<String> mountPoints = new HashSet<String>();

        // Check that there should be at least two copies
        if (locations.size() < 2) {
          throw new IOException("Configuration parameter " + configName
                      + " violated DFS name node directory policy:"
                      + " There should be at least two copies.");
        }

        for (NNStorageLocation location : locations) {
          foundRemote |= (location.type == StorageLocationType.REMOTE 
              || location.type == StorageLocationType.SHARED);
          mountPoints.add(location.mountPoint);
        }

        // Check that there should be at least two directories on different
        // mount points
        if (mountPoints.size() < 2) {
          throw new IOException("Configuration parameter " + configName
                      + " violated DFS name node directory policy:"
                      + " There must be at least two copies on different"
                      + " devices");
        }

        // If policy is 2, check that at least one directory is on NFS device
        if (policy == 2 && !foundRemote) {
          throw new IOException("Configuration parameter " + configName
                      + " violated DFS name node directory policy:"
                      + " There must be at least one copy on an NFS/remote device");
        }

        break;

      default:
        throw new IOException(
          "Unexpected configuration parameters: dfs.name.dir.policy = "
            + policy
            + ", must be between 0 and 2.");
    }
  }
  
  private static NNStorageLocation checkLocation(URI location,
      Configuration conf, URI sharedLocation) throws IOException {
    
    // check shared locations (for file and non-file locations
    boolean isShared = false;
    if (sharedLocation != null) {
      // check if the location is shared     
      isShared = location.equals(sharedLocation);
    }
   
    // handle non-file locations
    if ((location.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) != 0)) {
      // non-file locations are all remote - we might want to add more checks in the future
      // mount point is set to the uri scheme
      return new NNStorageLocation(location,
          location.getScheme().toLowerCase(),
          isShared ? StorageLocationType.SHARED : StorageLocationType.REMOTE);
    } else {   
      // enforce existence
      checkDirectory(location.getPath());
      
      try {
        return getInfoUnix(location, isShared);
      } catch (Exception e) {
        FLOG.info("Failed to fetch information with unix based df", e);
      }
      try {
        return getInfoMacOS(location, isShared);
      } catch (Exception e) {
        FLOG.info("Failed to fetch information with macos based df", e);
      }
      throw new IOException("Failed to run df");
    }
  }
  
  private static NNStorageLocation getInfoUnix(URI location, boolean isShared)
      throws Exception {
    String command = "df -P -T " + location.getPath();
    String[] fields = execCommand(command);

    if (fields.length < 2)
      throw new IOException("Unexpected output from command ' " + command);
    
    // check if the location is remote
    boolean isRemote = fields[1].equals("nfs");
    StorageLocationType type = isShared ? StorageLocationType.SHARED :
      (isRemote ? StorageLocationType.REMOTE : StorageLocationType.LOCAL);

    // "Type" is the second column, and "Mounted on" is the last column
    return new NNStorageLocation(location, fields[fields.length - 1], type);
  }
  
  private static NNStorageLocation getInfoMacOS(URI location, boolean isShared)
      throws Exception {
    String command = "df -P " + location.getPath();
    String[] fields = execCommand(command);

    if (fields.length < 2)
      throw new IOException("Unexpected output from command" + command);
    
    String mountPoint = fields[fields.length - 1];
    
    // check if the location is remote
    command = "df -P -T nfs" + location.getPath();
    fields = execCommand(command);
    // if there is output then the path is mounted on nfs
    boolean isRemote = (fields.length == 0) ? false
        : true;
    
    StorageLocationType type = isShared ? StorageLocationType.SHARED :
      (isRemote ? StorageLocationType.REMOTE : StorageLocationType.LOCAL);

    // "Type" is the second column, and "Mounted on" is the last column
    return new NNStorageLocation(location, mountPoint, type);
  }
  
  private static String[] execCommand(String command) throws IOException {
    Process p = Runtime.getRuntime().exec(command);
    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
    // Skip the first line, which is the header info
    br.readLine();
    String output = br.readLine();
    FLOG.info("Running: " + command + ", output: " + output);
    if (output == null || output.isEmpty()) {
      return new String[0];
    }
    return output.split("\\s+");
  }
  
  private static void checkDirectory(String name) throws IOException {
    // check existence
    File dir = new File(name);
    if (!dir.exists() || !dir.isDirectory()) {
      throw new IOException("Validation failed for " + name
          + ". Storage directory does not exist, or is not a directory");
    }
  }
}

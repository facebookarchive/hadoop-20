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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.fs.DF;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class ValidateNamespaceDirPolicy {
  
  private static final Log LOG = LogFactory
      .getLog(ValidateNamespaceDirPolicy.class.getName());

  public static void validate(Configuration conf) throws IOException {
    int policy = conf.getInt("dfs.name.dir.policy", 0);

    String nameDirConfig = "dfs.name.dir";
    Collection<URI> dirNamesURIs = NNStorageConfiguration.getNamespaceDirs(conf);
    validatePolicy(conf, policy, dirNamesURIs, nameDirConfig);

    String nameEditsDirConfig = "dfs.name.edits.dir";
    Collection<URI> editsDirNamesURIs = NNStorageConfiguration.getNamespaceEditsDirs(conf);
    validatePolicy(conf, policy, editsDirNamesURIs, nameEditsDirConfig);
  }

  private static void validatePolicy(Configuration conf,
                        int policy,
                        Collection<URI> locations,
                        String configName)
    throws IOException {
    /* DFS name node directory policy:
      0 - No enforcement
      1 - Enforce that there should be at least two copies and they must be on
          different devices
      2 - Enforce that there should be at least two copies on different devices
          and at least one must be on an NFS device
    */
    
    // convert uri's for directory names
    Collection<String> dirNames = new ArrayList<String>();
    for (URI u : locations) {
      LOG.info("NNStorage validation : checking path: " + u);
      if ((u.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0)) {
        LOG.info("NNStorage validation : path: " + u
            + " will be processed as a file");
        dirNames.add(u.getPath());
      }
    }
    
    switch (policy) {
      case 0:
        // No check needed.
        break;

      case 1:
      case 2:
        boolean foundNFS = false;
        String commandPrefix = "df -P -T ";
        HashSet<String> mountPoints = new HashSet<String>();

        // Check that there should be at least two copies
        if (dirNames.size() < 2) {
          throw new IOException("Configuration parameter " + configName
                      + " violated DFS name node directory policy:"
                      + " There should be at least two copies.");
        }

        for (String name : dirNames) {
          String command = commandPrefix + name;
          Process p = Runtime.getRuntime().exec(command);
          BufferedReader stdInput = new BufferedReader(new
                 InputStreamReader(p.getInputStream()));

          // Skip the first line, which is the header info
          String outputStr = stdInput.readLine();
          outputStr = stdInput.readLine();

          // Parse the command output to get the "Type" and "Mounted on"
          String[] fields = outputStr.split("\\s+");

          if (fields.length < 2)
            throw new IOException("Unexpected output from command ' " + command
                        + "': " + outputStr);

          // "Type" is the second column, and "Mounted on" is the last column
          if (fields[1].equals("nfs"))
            foundNFS = true;
          mountPoints.add(fields[fields.length - 1]);
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
        if (policy == 2 && !foundNFS) {
          throw new IOException("Configuration parameter " + configName
                      + " violated DFS name node directory policy:"
                      + " There must be at least one copy on an NFS device");
        }

        break;

      default:
        throw new IOException(
          "Unexpected configuration parameters: dfs.name.dir.policy = "
            + policy
            + ", must be between 0 and 2.");
    }
  }
}

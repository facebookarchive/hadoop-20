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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import java.io.FileWriter;
import java.io.BufferedWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Check for files that have a replication factor of less than 3
 * And which do not have a parity file created for them
 */
public class MissingParityFiles {

  private Configuration conf;
  private boolean directoryTraversalShuffle;
  private int directoryTraversalThreads;
  private short replicationLimit = 3;
  private boolean restoreReplication;

  public MissingParityFiles(Configuration conf) {
    this(conf, false);
  }

  public MissingParityFiles(Configuration conf, boolean restoreReplication) {
    this.conf = conf;
    this.directoryTraversalShuffle =
        conf.getBoolean(RaidNode.RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
    this.directoryTraversalThreads =
        conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
    this.replicationLimit = (short) conf.getInt("raid.missingparity.replicationlimit", 3);
    this.restoreReplication = restoreReplication;
  }
  
  public void findMissingParityFiles(Path root, PrintStream out) throws IOException {
    FileSystem fs = root.getFileSystem(conf);
    List<Path> allPaths = Arrays.asList(root);
    DirectoryTraversal.Filter filter = new MissingParityFilter(conf, replicationLimit);
    boolean allowUseStandby = false;
    DirectoryTraversal traversal =
      new DirectoryTraversal("Missing Parity Retriever ", allPaths, fs, filter,
        directoryTraversalThreads, directoryTraversalShuffle, allowUseStandby);
    FileStatus newFile;
    while ((newFile = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
      Path filePath = newFile.getPath();
      out.println(filePath.toUri().getPath());
      if (restoreReplication) {
        System.err.println("Setting replication=" + replicationLimit + " for " + filePath);
        fs.setReplication(filePath, replicationLimit);
      }
    }
  }

  static class MissingParityFilter implements DirectoryTraversal.Filter {
    Configuration conf;
    int limit;

    MissingParityFilter(Configuration conf, int limit) throws IOException {
      this.conf = conf;
      this.limit = limit;
    }

    @Override
    public boolean check(FileStatus f) throws IOException {
      if (f.isDir()) return false;

      Path filePath = f.getPath();
      if (isParityFile(filePath)) {
        return false;
      }
      if (f.getReplication() < limit) {
        boolean found = false;
        for (Codec c : Codec.getCodecs()) {
          ParityFilePair parityPair =
            ParityFilePair.getParityFile(c, f, conf);
          if (parityPair != null) {
            found = true;
            break;
          }
        }
        if (!found) {
          return true;
        }
      }
      return false;
    }

    public boolean isParityFile(Path filePath) {
      String pathStr = filePath.toUri().getPath();
      for (Codec c : Codec.getCodecs()) {
        if (pathStr.startsWith(c.getParityPrefix())) {
          return true;
        }
      }
      return false;
    }
  }

}

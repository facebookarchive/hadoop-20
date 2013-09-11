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

package org.apache.hadoop.raid.tools;

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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.DirectoryTraversal;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Verify the source files have the expected replication
 */
public class ParityVerifier {
  private Configuration conf;
  private boolean directoryTraversalShuffle;
  private int directoryTraversalThreads;
  private short replicationLimit = 3;
  private boolean restoreReplication;
  public final Codec code;

  public ParityVerifier(Configuration conf, boolean restoreReplication, int
      replicationLimit, Codec code) {
    this.code = code;
    this.conf = conf;
    this.directoryTraversalShuffle =
        conf.getBoolean(RaidNode.RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
    this.directoryTraversalThreads =
        conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
    this.replicationLimit = (short)replicationLimit; 
    this.restoreReplication = restoreReplication;
  }
  
  public void verifyParities(Path root, PrintStream out) throws IOException {
    FileSystem fs = root.getFileSystem(conf);
    List<Path> allPaths = Arrays.asList(root);
    DirectoryTraversal.Filter filter = new VerifyParityFilter(conf, replicationLimit, 
        code, restoreReplication);
    boolean allowUseStandby = false;
    DirectoryTraversal traversal =
      new DirectoryTraversal("Parity Verifier Retriever ", allPaths, fs, filter,
        directoryTraversalThreads, directoryTraversalShuffle, allowUseStandby);
    FileStatus newFile;
    while ((newFile = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
      Path filePath = newFile.getPath();
      out.println(filePath.toUri().getPath());
    }
  }

  static class VerifyParityFilter implements DirectoryTraversal.Filter {
    Configuration conf;
    int limit;
    Codec code;
    boolean restoreReplication;

    VerifyParityFilter(Configuration conf, int limit, Codec code, 
        boolean restoreReplication) throws IOException {
      this.conf = conf;
      this.limit = limit;
      this.code = code;
      this.restoreReplication = restoreReplication;
    }
    
    public boolean checkSrc(FileStatus srcStat, short limit, boolean
        restoreReplication, FileSystem fs) throws IOException {
      if (srcStat.getReplication() < limit) {
        if (restoreReplication) {
          System.err.println("Setting replication=" + limit + " for "
        + srcStat.getPath());
          fs.setReplication(srcStat.getPath(), limit);
        } else {
          System.err.println("misreplication: " + srcStat.getPath() +
              " size: " + srcStat.getLen());
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean check(FileStatus parityStat) throws IOException {
      if (parityStat.isDir()) return false;
      Path parityPath = parityStat.getPath();
      FileSystem fs = parityPath.getFileSystem(conf);;
      String parityPathStr = parityPath.toUri().getPath();
      String src = parityPathStr.replaceFirst(code.getParityPrefix(),
          Path.SEPARATOR);
      Path srcPath = new Path(src);
      FileStatus srcStat;
      try {
        srcStat = fs.getFileStatus(srcPath);
      } catch (FileNotFoundException ioe) {
        return false;
      }
      if (!code.isDirRaid) {
        return checkSrc(srcStat, (short)limit, restoreReplication, fs);
      } else {
        List<FileStatus> stats = RaidNode.listDirectoryRaidFileStatus(conf,
            fs, srcPath);
        if (stats == null || stats.size() == 0) {
          return false;
        }
        boolean result = false;
        for (FileStatus stat : stats) {
          if (checkSrc(stat, (short)limit, restoreReplication, fs)) {
            result = true;
          }
        }
        return result;
      }
    }
  }

}

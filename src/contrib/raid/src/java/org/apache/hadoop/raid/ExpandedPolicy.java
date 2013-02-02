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
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.protocol.PolicyInfo;

class ExpandedPolicy {
  final String srcPrefix;
  final long modTimePeriod;
  final Codec codec;
  final int targetReplication;
  final PolicyInfo parentPolicy;

  ExpandedPolicy(String srcPrefix, long modTimePeriod,
      Codec codec, int targetReplication, PolicyInfo parentPolicy) {
    this.srcPrefix = srcPrefix;
    this.modTimePeriod = modTimePeriod;
    this.codec = codec;
    this.targetReplication = targetReplication;
    this.parentPolicy = parentPolicy;
  }
  
  /*
   * Return state of other policy 
   */
  RaidState match(FileStatus f, long mtime, long now, Configuration conf,
      List<FileStatus> lfs)
          throws IOException {
    String pathStr = normalizePath(f.getPath());
    if (pathStr.startsWith(srcPrefix)) {
      return getBasicState(f, mtime, now, false, conf, lfs);
    }
    return RaidState.NOT_RAIDED_NO_POLICY;
  }
  
  RaidState getBasicState(FileStatus f, long mtime, long now, 
      boolean skipParityCheck, Configuration conf, List<FileStatus> lfs)
          throws IOException {
    if (f.isDir() != codec.isDirRaid) {
      return RaidState.NOT_RAIDED_NO_POLICY;
    }
    if (now - mtime < modTimePeriod) {
      return RaidState.NOT_RAIDED_TOO_NEW;
    }
    long repl = f.isDir()?
        DirectoryStripeReader.getReplication(lfs):
        f.getReplication();
    if (repl == targetReplication) {
      if (skipParityCheck || 
          ParityFilePair.parityExists(f, codec, conf)) {
        return RaidState.RAIDED;
      }
    } 
    return RaidState.NOT_RAIDED_BUT_SHOULD;
  }
  
  static List<ExpandedPolicy> expandPolicy(PolicyInfo info)
      throws IOException {
    List<ExpandedPolicy> result = new ArrayList<ExpandedPolicy>();
    for (Path srcPath : info.getSrcPathExpanded()) {
      String srcPrefix = normalizePath(srcPath);
      long modTimePeriod = Long.parseLong(info.getProperty("modTimePeriod"));
      int targetReplication =
        Integer.parseInt(info.getProperty("targetReplication"));
      Codec codec = Codec.getCodec(info.getCodecId());
      ExpandedPolicy ePolicy = new ExpandedPolicy(
          srcPrefix, modTimePeriod, codec, targetReplication, info);
      result.add(ePolicy);
    }
    return result;
  }
  
  static class ExpandedPolicyComparator
  implements Comparator<ExpandedPolicy> {
    @Override
    public int compare(ExpandedPolicy p1, ExpandedPolicy p2) {
      if (p1.srcPrefix.length() > p2.srcPrefix.length()) {
        // Prefers longer prefix
        return -1;
      }
      if (p1.srcPrefix.length() < p2.srcPrefix.length()) {
        return 1;
      }
      if (p1.codec.priority > p2.codec.priority) {
        // Prefers higher priority
        return -1;
      }
      if (p1.codec.priority < p2.codec.priority) {
        return 1;
      }
      // Prefers lower target replication factor
      return p1.targetReplication < p2.targetReplication ? -1 : 1;
    }
  }
  
  private static String normalizePath(Path p) {
    String result = p.toUri().getPath();
    if (!result.endsWith(Path.SEPARATOR)) {
      result += Path.SEPARATOR;
    }
    return result;
  }
}

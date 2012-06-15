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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.protocol.PolicyInfo;

class ExpandedPolicy {
  final String srcPrefix;
  final long modTimePeriod;
  final ErasureCodeType code;
  final int targetReplication;
  final PolicyInfo parentPolicy;

  ExpandedPolicy(String srcPrefix, long modTimePeriod,
      ErasureCodeType code, int targetReplication, PolicyInfo parentPolicy) {
    this.srcPrefix = srcPrefix;
    this.modTimePeriod = modTimePeriod;
    this.code = code;
    this.targetReplication = targetReplication;
    this.parentPolicy = parentPolicy;
  }
  boolean match(FileStatus f, long mtime, long now) {
    String pathStr = normalizePath(f.getPath());
    if (pathStr.startsWith(srcPrefix)) {
      if (now - mtime > modTimePeriod) {
        return true;
      }
    }
    return false;
  }
  static List<ExpandedPolicy> expandPolicy(PolicyInfo info)
      throws IOException {
    List<ExpandedPolicy> result = new ArrayList<ExpandedPolicy>();
    for (Path srcPath : info.getSrcPathExpanded()) {
      String srcPrefix = normalizePath(srcPath);
      long modTimePeriod = Long.parseLong(info.getProperty("modTimePeriod"));
      int targetReplication =
        Integer.parseInt(info.getProperty("targetReplication"));
      ErasureCodeType code = info.getErasureCode();
      ExpandedPolicy ePolicy = new ExpandedPolicy(
          srcPrefix, modTimePeriod, code, targetReplication, info);
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
      if (p1.code == ErasureCodeType.RS &&
          p2.code == ErasureCodeType.XOR) {
        // Prefers RS code
        return -1;
      }
      if (p1.code == ErasureCodeType.XOR &&
          p2.code == ErasureCodeType.RS) {
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

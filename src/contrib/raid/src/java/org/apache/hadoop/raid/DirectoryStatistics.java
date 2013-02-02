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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.raid.Statistics.Counters;
import org.apache.hadoop.raid.protocol.PolicyInfo;

public class DirectoryStatistics extends Statistics {

  public DirectoryStatistics(Codec codec, Configuration conf) {
    super(codec, conf);
  }
  
  /**
   * Collect the statistics of a source directory. Return true if the file 
   * should be raided but not.
   * @throws IOException 
   */
  public boolean addSourceFile(FileSystem fs, PolicyInfo info, FileStatus src,
      RaidState.Checker checker, long now, int targetReplication) 
          throws IOException {
    
    List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(
                    fs.getConf(), fs, src.getPath()); 
    if (lfs == null) {
      return false;
    }
    RaidState state = checker.check(info, src, now, false, lfs);
    Counters counters = stateToSourceCounters.get(state);
    counters.inc(lfs);
    if (state == RaidState.RAIDED) {
      long paritySize = computeParitySize(lfs, targetReplication);
      estimatedParitySize += paritySize;
      estimatedDoneParitySize += paritySize;
      estimatedDoneSourceSize += DirectoryStripeReader.getDirPhysicalSize(lfs);
      return false;
    }
    if (state == RaidState.NOT_RAIDED_BUT_SHOULD) {
      estimatedDoneParitySize += computeParitySize(lfs, targetReplication);
      estimatedDoneSourceSize += DirectoryStripeReader.getDirPhysicalSize(lfs);
      return true;
    }
    return false;
  } 
  
  private long computeParitySize(List<FileStatus> lfs, int targetReplication) {
    long numBlocks = DirectoryStripeReader.getBlockNum(lfs);
    long parityBlockSize = DirectoryStripeReader.getParityBlockSize(
        new Configuration(), lfs);
    long parityBlocks = 
        RaidNode.numStripes(numBlocks, stripeLength) * parityLength;
    return parityBlocks * targetReplication * parityBlockSize;
  }
}

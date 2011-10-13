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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Capacity statistics for a type of erasure code.
 */
public class Statistics {

  final private ErasureCodeType codeType;
  final private int parityLength;
  final private int stripeLength;
  private long estimatedParitySize = 0;
  private long estimatedDoneParitySize = 0;
  private long estimatedDoneSourceSize = 0;

  private Map<RaidState, Counters> stateToSourceCounters;
  private Counters parityCounters;
  private Map<Integer, Counters> numBlocksToRaidedCounters;

  public Statistics(ErasureCodeType codeType, Configuration conf) {
    this.codeType = codeType;
    this.stripeLength = RaidNode.getStripeLength(conf);
    this.parityLength = this.codeType == ErasureCodeType.XOR ?
        1: RaidNode.rsParityLength(conf);
    this.parityCounters = new Counters();
    Map<RaidState, Counters> m = new HashMap<RaidState, Counters>();
    for (RaidState state : RaidState.values()) {
      m.put(state, new Counters());
    }
    this.stateToSourceCounters = new EnumMap<RaidState, Counters>(m);
    this.numBlocksToRaidedCounters = new HashMap<Integer, Counters>();
  }

  public static class Counters {
    private long numFiles = 0L;
    private long numBlocks = 0L;
    private long numBytes = 0L;
    private long numLogical = 0L;

    private void inc(FileStatus status) {
      numFiles += 1;
      numBlocks += computeNumBlocks(status);
      numLogical += status.getLen();
      numBytes += status.getLen() * status.getReplication();
    }

    public long getNumFiles() {
      return numFiles;
    }
    public long getNumBlocks() {
      return numBlocks;
    }
    public long getNumBytes() {
      return numBytes;
    }
    public long getNumLogical() {
      return numLogical; // logical bytes
    }
    @Override
    public String toString() {
      return "files:" + numFiles + " blocks:" + numBlocks +
          " bytes:" + numBytes + " logical:" + numLogical;
    }
    public String htmlRow() {
      return td(StringUtils.humanReadableInt(numFiles)) +
             td(StringUtils.humanReadableInt(numBlocks)) +
             td(StringUtils.byteDesc(numBytes)) +
             td(StringUtils.byteDesc(numLogical));
    }
    public static String htmlRowHeader() {
      return td("Files") + td("Blocks") +
        td("Bytes") + td("Logical");
    }
  }

  /**
   * Collect the statistics of a source file. Return true if the file should be
   * raided but not.
   */
  public boolean addSourceFile(PolicyInfo info, FileStatus src,
      RaidState.Checker checker, long now, int targetReplication)
      throws IOException {
    RaidState state = checker.check(info, src, now, true);
    Counters counters = stateToSourceCounters.get(state);
    counters.inc(src);
    if (state == RaidState.RAIDED) {
      incRaided(src);
      long paritySize = computeParitySize(src, targetReplication);
      estimatedParitySize += paritySize;
      estimatedDoneParitySize += paritySize;
      estimatedDoneSourceSize += src.getLen() * targetReplication;
      return false;
    }
    if (state == RaidState.NOT_RAIDED_BUT_SHOULD) {
      estimatedDoneParitySize += computeParitySize(src, targetReplication);
      estimatedDoneSourceSize += src.getLen() * targetReplication;
      return true;
    }
    return false;
  }

  public void addParityFile(FileStatus parityFile) {
    parityCounters.inc(parityFile);
  }
  
  private void incRaided(FileStatus raidedFile) {
    int numBlocks = computeNumBlocks(raidedFile);
    Counters counters = numBlocksToRaidedCounters.get(numBlocks);
    if (counters == null) {
      counters = new Counters();
      numBlocksToRaidedCounters.put(numBlocks, counters);
    }
    counters.inc(raidedFile);
  }

  private long computeParitySize(FileStatus src, int targetReplication) {
    long numBlocks = computeNumBlocks(src);
    long parityBlocks =
      (long)Math.ceil(((double)numBlocks) / stripeLength) * parityLength;
    return parityBlocks * targetReplication * src.getBlockSize();
  }

  private static int computeNumBlocks(FileStatus status) {
    return (int)Math.ceil(((double)(status.getLen())) / status.getBlockSize());
  }

  public Counters getSourceCounters(RaidState state) {
    return stateToSourceCounters.get(state);
  }

  public Counters getRaidedCounters(int numBlocks) {
    return numBlocksToRaidedCounters.get(numBlocks);
  }

  public Counters getParityCounters() {
    return parityCounters;
  }

  public ErasureCodeType getCodeType() {
    return codeType;
  }

  public long getEstimatedParitySize() {
    return estimatedParitySize;
  }

  public double getEffectiveReplication() {
    Counters raidedCounters = stateToSourceCounters.get(RaidState.RAIDED);
    double physical = raidedCounters.getNumBytes() +
        parityCounters.getNumBytes();
    double logical = raidedCounters.getNumLogical();
    if (logical == 0) {
      // divided by 0
      return -1;
    }
    return physical / logical;
  }

  /**
   * Get the saving of this code in bytes
   * @return The saving in bytes
   */
  public long getSaving() {
    try {
      DFSClient dfs = new DFSClient(new Configuration());
      Counters raidedCounters = stateToSourceCounters.get(RaidState.RAIDED);
      long physical = raidedCounters.getNumBytes() +
          parityCounters.getNumBytes();
      long logical = raidedCounters.getNumLogical();
      return logical * dfs.getDefaultReplication() - physical;
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Get the estimated saving of this code in bytes when RAIDing is done
   * @return The saving in bytes
   */
  public long getDoneSaving() {
    try {
      DFSClient dfs = new DFSClient(new Configuration());
      Counters raidedCounters = stateToSourceCounters.get(RaidState.RAIDED);
      Counters shouldRaidCounters =
          stateToSourceCounters.get(RaidState.NOT_RAIDED_BUT_SHOULD);
      long physical = estimatedDoneSourceSize + estimatedDoneParitySize;
      long logical = raidedCounters.getNumLogical() +
          shouldRaidCounters.getNumLogical();
      return logical * dfs.getDefaultReplication() - physical;
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(codeType + " Statistics\n");
    for (RaidState state : RaidState.values()) {
      sb.append(state + ": " +
          stateToSourceCounters.get(state).toString() + "\n");
    }
    sb.append("PARITY: " + parityCounters.toString() + "\n");
    return sb.toString();
  }

  public String htmlTable() {

    RaidState[] statesToShow = new RaidState[]{
        RaidState.RAIDED, RaidState.NOT_RAIDED_TOO_NEW,
        RaidState.NOT_RAIDED_TOO_SMALL, RaidState.NOT_RAIDED_BUT_SHOULD};

    StringBuilder sb = new StringBuilder();
    sb.append(tr(td("STATE") + Counters.htmlRowHeader()));
    for (RaidState state : statesToShow) {
      Counters counters = stateToSourceCounters.get(state);
      sb.append(tr(td(state.toString()) + counters.htmlRow()));
    }
    sb.append(tr(td("PARITY") + parityCounters.htmlRow()));
    return table(sb.toString());
  }

  private static String tr(String s) {
    return JspUtils.tr(s);
  }
  private static String td(String s) {
    return JspUtils.td(s);
  }
  private static String table(String s) {
    return JspUtils.table(s);
  }

}

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
import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Capacity statistics for a type of erasure code.
 */
public class Statistics implements Serializable {
  final protected String codecId;
  final protected Codec codec;
  final protected int parityLength;
  final protected int stripeLength;
  protected long estimatedParitySize = 0L;
  protected long estimatedDoneParitySize = 0L;
  protected long estimatedDoneSourceSize = 0L;

  protected Map<RaidState, Counters> stateToSourceCounters;
  protected Counters parityCounters;
  protected Map<Integer, Counters> numBlocksToRaidedCounters;

  public Statistics(Codec codec, Configuration conf) {
    this.codec = codec;
    this.codecId = codec.id;
    this.stripeLength = codec.stripeLength;
    this.parityLength = codec.parityLength;
    this.parityCounters = new Counters();
    Map<RaidState, Counters> m = new HashMap<RaidState, Counters>();
    for (RaidState state : RaidState.values()) {
      m.put(state, new Counters());
    }
    this.stateToSourceCounters = new EnumMap<RaidState, Counters>(m);
    this.numBlocksToRaidedCounters = new HashMap<Integer, Counters>();
  }

  public static class Counters implements Serializable {
    private long numDirs = 0L;
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
    
    /**
     * Increment counters for directory
     * @param lfs List of FileStatus for files under the direcotry
     */
    protected void inc(List<FileStatus> lfs) {
      numDirs += 1;
      numFiles += lfs.size();
      numBlocks += DirectoryStripeReader.getBlockNum(lfs);
      numLogical += DirectoryStripeReader.getDirLogicalSize(lfs);
      numBytes += DirectoryStripeReader.getDirPhysicalSize(lfs);
    }
    
    public long getNumDirs() {
      return numDirs;
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
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }
      Counters counters = (Counters) obj;
      return (numDirs == counters.numDirs &&
              numFiles == counters.numFiles &&
              numBlocks == counters.numBlocks &&
              numBytes == counters.numBytes &&
              numLogical == counters.numLogical);
    }
    @Override
    public int hashCode() {
      int hash = 7;
      hash = 37 * hash + (int) (numDirs ^ (numDirs >>> 32));
      hash = 37 * hash + (int) (numFiles ^ (numFiles >>> 32));
      hash = 37 * hash + (int) (numBlocks ^ (numBlocks >>> 32));
      hash = 37 * hash + (int) (numBytes ^ (numBytes >>> 32));
      hash = 37 * hash + (int) (numLogical ^ (numLogical >>> 32));
      return hash;
    }
    @Override
    public String toString() {
      return "dirs: " + numDirs + " files:" + numFiles + " blocks:"
          + numBlocks + " bytes:" + numBytes + " logical:" + numLogical;
    }
    public String htmlRow(Codec codec) {
      String dirColumn = !codec.isDirRaid? "": 
        td(StringUtils.humanReadableInt(numDirs));
      return dirColumn + td(StringUtils.humanReadableInt(numFiles)) +
             td(StringUtils.humanReadableInt(numBlocks)) +
             td(StringUtils.byteDesc(numBytes)) +
             td(StringUtils.byteDesc(numLogical));
    }
    public static String htmlRowHeader(Codec codec) {
      String dirHeader = !codec.isDirRaid? "":
        td("Dirs");
      return dirHeader + td("Files") + td("Blocks") +
        td("Bytes") + td("Logical");
    }
  }

  /**
   * Collect the statistics of a source file. Return true if the file should be
   * raided but not.
   */
  public boolean addSourceFile(FileSystem fs, PolicyInfo info, FileStatus src,
      RaidState.Checker checker, long now, int targetReplication)
      throws IOException {
    RaidState state = checker.check(info, src, now, false);
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
  public long getSaving(Configuration conf) {
    try {
      DFSClient dfs = ((DistributedFileSystem)FileSystem.get(conf)).getClient();
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
  public long getDoneSaving(Configuration conf) {
    try {
      DFSClient dfs = ((DistributedFileSystem)FileSystem.get(conf)).getClient();
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
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    Statistics other = (Statistics) obj;
    return (codecId.equals(other.codecId) &&
            parityLength == other.parityLength &&
            stripeLength == other.stripeLength &&
            estimatedParitySize == other.estimatedParitySize &&
            estimatedDoneParitySize == other.estimatedDoneParitySize &&
            estimatedDoneSourceSize == other.estimatedDoneSourceSize &&
            stateToSourceCounters.equals(other.stateToSourceCounters) &&
            parityCounters.equals(other.parityCounters) &&
            numBlocksToRaidedCounters.equals(other.numBlocksToRaidedCounters));
  }
  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + codecId.hashCode();
    hash = 37 * hash + (int) (parityLength ^ (parityLength >>> 32));
    hash = 37 * hash + (int) (stripeLength ^ (stripeLength >>> 32));
    hash = 37 * hash + (int) (estimatedParitySize ^
                              (estimatedParitySize >>> 32));
    hash = 37 * hash + (int) (estimatedDoneParitySize ^
                              (estimatedDoneParitySize >>> 32));
    hash = 37 * hash + (int) (estimatedDoneSourceSize ^
                              (estimatedDoneSourceSize >>> 32));
    hash = 37 * hash + (stateToSourceCounters != null ?
                        stateToSourceCounters.hashCode() : 0);
    hash = 37 * hash + (parityCounters != null ?
                        parityCounters.hashCode() : 0);
    hash = 37 * hash + (numBlocksToRaidedCounters != null ?
                        numBlocksToRaidedCounters.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(codecId + " Statistics\n");
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
    sb.append(tr(td("STATE") + Counters.htmlRowHeader(codec)));
    for (RaidState state : statesToShow) {
      Counters counters = stateToSourceCounters.get(state);
      sb.append(tr(td(state.toString()) + counters.htmlRow(codec)));
    }
    sb.append(tr(td("PARITY") + parityCounters.htmlRow(codec)));
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

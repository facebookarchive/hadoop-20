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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.raid.Statistics.Counters;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Verifies {@link Statistics} collects raid statistics
 */
public class TestStatisticsCollector extends TestCase {
  final static Log LOG = LogFactory.getLog(TestStatisticsCollector.class);
  final static Random rand = new Random();
  final Configuration conf = new Configuration();
  final FakeConfigManager fakeConfigManager = new FakeConfigManager();
  final FakeRaidNode fakeRaidNode = new FakeRaidNode();


  public void testExcludes() throws IOException {
    conf.set("raid.exclude.patterns", "/exclude/,/df_mf/");
    RaidState.Checker checker = new RaidState.Checker(
      new ArrayList<PolicyInfo>(), conf);
    assertEquals(true, checker.shouldExclude("/a/b/c/df_mf/foo/bar"));
    assertEquals(false, checker.shouldExclude("/a/b/c/xdf_mf/foo/bar"));
  }

  public void testTimeFromName() {
    assertEquals(
      new Date(2011 - 1900, 0, 12).getTime(),
      RaidState.Checker.mtimeFromName("/a/b/c/ds=2011-01-12/d/e/f"));
    assertEquals(
      new Date(2011 - 1900, 0, 12).getTime(),
      RaidState.Checker.mtimeFromName("/a/b/c/ds=2011-01-12-02/d/e/f"));
    assertEquals(
      -1,
      RaidState.Checker.mtimeFromName("/a/b/c/ds=2011/d/e/f"));
    assertEquals(
      -1,
      RaidState.Checker.mtimeFromName("/a/b/c/d/e/f"));
  }

  public void testCollect() throws Exception {
    MiniDFSCluster dfs = null;
    try {
      dfs = new MiniDFSCluster(conf, 3, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      verifySourceCollect(ErasureCodeType.RS, fs);
      verifySourceCollect(ErasureCodeType.XOR, fs);
      verifyParityCollect(ErasureCodeType.RS, fs);
      verifyParityCollect(ErasureCodeType.XOR, fs);
      verifyLongPrefixOverride(fs);
      verifyRsCodeOverride(fs);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  public void verifySourceCollect(ErasureCodeType code, FileSystem fs)
      throws Exception {
    PolicyInfo info = new PolicyInfo("Test-Raided-" + code, conf);
    info.setSrcPath("/a/b");
    info.setProperty("modTimePeriod", "0");
    info.setProperty("targetReplication", "1");
    info.setErasureCode(code.toString());

    PolicyInfo infoTooNew = new PolicyInfo("Test-Too-New-" + code, conf);
    infoTooNew.setSrcPath("/a/new");
    infoTooNew.setProperty("modTimePeriod", "" + Long.MAX_VALUE);
    infoTooNew.setProperty("targetReplication", "1");
    infoTooNew.setErasureCode(code.toString());
    
    createFile(fs, new Path("/a/b/TOO_SMALL"), 1, 1, 1024L);
    createFile(fs, new Path("/a/b/d/TOO_SMALL"), 2, 2, 1024L);
    createFile(fs, new Path("/a/b/f/g/RAIDED"), 1, 3, 1024L);
    createFile(fs, new Path("/a/b/f/g/h/RAIDED"), 1, 4, 1024L);
    createFile(fs, new Path("/a/b/f/g/NOT_RAIDED"), 3, 5, 1024L);

    createFile(fs, new Path("/a/new/i/TOO_NEW"), 3, 4, 1024L);
    createFile(fs, new Path("/a/new/j/TOO_NEW"), 3, 5, 1024L);

    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoTooNew);
    collector.collect(allPolicies);
    Statistics st = collector.getRaidStatistics(code);
    LOG.info("Statistics collected " + st);
    LOG.info("Statistics html:\n " + st.htmlTable());
    Counters raided = st.getSourceCounters(RaidState.RAIDED);
    Counters tooSmall = st.getSourceCounters(RaidState.NOT_RAIDED_TOO_SMALL);
    Counters tooNew = st.getSourceCounters(RaidState.NOT_RAIDED_TOO_NEW);
    Counters notRaided = st.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    assertCounters(raided, 2, 7, 7 * 1024L, 7 * 1024L);
    assertCounters(tooSmall, 2, 3, 5 * 1024L, 3 * 1024L);
    assertCounters(tooNew, 2, 9, 27 * 1024L, 9 * 1024L);
    assertCounters(notRaided, 1, 5, 15 * 1024L, 5 * 1024L);
    fs.delete(new Path("/a"), true);
  }

  public void verifyParityCollect(ErasureCodeType code, FileSystem fs)
      throws Exception {
    LOG.info("Start testing parity collect for " + code);
    Path parityPath = RaidNode.getDestinationPath(code, conf);
    fs.mkdirs(parityPath);
    createFile(fs, new Path(parityPath+ "/a"), 1, 1, 1024L);
    createFile(fs, new Path(parityPath + "/b/c"), 2, 2, 1024L);
    createFile(fs, new Path(parityPath + "/d/e/f"), 3, 3, 1024L);
    List<PolicyInfo> empty = Collections.emptyList();
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    collector.collect(empty);
    Statistics st = collector.getRaidStatistics(code);
    assertCounters(st.getParityCounters(), 3, 6, 14 * 1024L, 6 * 1024L);
    LOG.info("Statistics collected " + st);
    LOG.info("Statistics html:\n " + st.htmlTable());
    fs.delete(parityPath, true);
  }

  public void verifyLongPrefixOverride(FileSystem fs) throws Exception {
    PolicyInfo info = new PolicyInfo("Test", conf);
    info.setSrcPath("/a/b");
    info.setProperty("modTimePeriod", "0");
    info.setProperty("targetReplication", "1");
    info.setErasureCode("RS");

    PolicyInfo infoLongPrefix = new PolicyInfo("Long-Prefix", conf);
    infoLongPrefix.setSrcPath("/a/b/c");
    infoLongPrefix.setProperty("modTimePeriod", "0");
    infoLongPrefix.setProperty("targetReplication", "2");
    infoLongPrefix.setErasureCode("XOR");

    createFile(fs, new Path("/a/b/k"), 3, 4, 1024L);
    createFile(fs, new Path("/a/b/c/d"), 3, 5, 1024L);
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoLongPrefix);
    collector.collect(allPolicies);
    Statistics xorSt = collector.getRaidStatistics(ErasureCodeType.XOR);
    Statistics rsSt = collector.getRaidStatistics(ErasureCodeType.RS);
    Counters xorShouldRaid = xorSt.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    Counters rsShouldRaid = rsSt.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    Counters rsOther = rsSt.getSourceCounters(RaidState.NOT_RAIDED_OTHER_POLICY);
    assertCounters(xorShouldRaid, 1, 5, 15 * 1024L, 5 * 1024L);
    assertCounters(rsShouldRaid, 1, 4, 12 * 1024L, 4 * 1024L);
    assertCounters(rsOther, 1, 5, 15 * 1024L, 5 * 1024L);
    fs.delete(new Path("/a"), true);
  }
 
  public void verifyRsCodeOverride(FileSystem fs) throws Exception {
    PolicyInfo info = new PolicyInfo("Test", conf);
    info.setSrcPath("/a/b/*");
    info.setProperty("modTimePeriod", "0");
    info.setProperty("targetReplication", "1");
    info.setErasureCode("XOR");

    PolicyInfo infoLongPrefix = new PolicyInfo("Long-Prefix", conf);
    infoLongPrefix.setSrcPath("/a/b/c");
    infoLongPrefix.setProperty("modTimePeriod", "0");
    infoLongPrefix.setProperty("targetReplication", "2");
    infoLongPrefix.setErasureCode("RS");

    createFile(fs, new Path("/a/b/k"), 3, 4, 1024L);
    createFile(fs, new Path("/a/b/c/d"), 3, 5, 1024L);
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoLongPrefix);
    collector.collect(allPolicies);
    Statistics xorSt = collector.getRaidStatistics(ErasureCodeType.XOR);
    Statistics rsSt = collector.getRaidStatistics(ErasureCodeType.RS);
    Counters xorShouldRaid = xorSt.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    Counters xorOther = xorSt.getSourceCounters(RaidState.NOT_RAIDED_OTHER_POLICY);
    Counters rsShouldRaid = rsSt.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    assertCounters(xorShouldRaid, 1, 4, 12 * 1024L, 4 * 1024L);
    assertCounters(xorOther, 1, 5, 15 * 1024L, 5 * 1024L);
    assertCounters(rsShouldRaid, 1, 5, 15 * 1024L, 5 * 1024L);
    fs.delete(new Path("/a"), true);
  }


  private void assertCounters(Counters counters, long numFiles,
      long numBlocks, long numBytes, long numLogicalBytes) {
    assertEquals(numFiles, counters.getNumFiles());
    assertEquals(numBlocks, counters.getNumBlocks());
    assertEquals(numBytes, counters.getNumBytes());
    assertEquals(numLogicalBytes, counters.getNumLogical());
  }

  private static long createFile(
      FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
      throws IOException {
    CRC32 crc = new CRC32();
    int bufSize = fileSys.getConf().getInt("io.file.buffer.size", 4096);
    FSDataOutputStream stm = fileSys.create(
        name, true, bufSize, (short)repl, blocksize);
    // fill random data into file
    byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }

  private class FakeRaidNode extends RaidNode {
    @Override
    int getRunningJobsForPolicy(String policyName) {
      return 0;
    }

    @Override
    void raidFiles(PolicyInfo info, List<FileStatus> paths) throws IOException {
      LOG.info("Raiding " + paths.size() +
          " files for policy:" + info.getName());
    }

    @Override
    public String raidJobsHtmlTable(boolean running) {
      return null;
    }
  }

  private class FakeConfigManager extends ConfigManager {
    @Override
    public int getMaxFilesPerJob() {
      return 3;
    }

    @Override
    public int getMaxJobsPerPolicy() {
      return 10;
    }
  }
}

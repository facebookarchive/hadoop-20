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
import java.util.Map;
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
import org.apache.hadoop.raid.Utils.Builder;

/**
 * Verifies {@link Statistics} collects raid statistics
 */
public class TestStatisticsCollector extends TestCase {
  public TestStatisticsCollector(String name) {
    super(name);
  }

  final static Log LOG = LogFactory.getLog(TestStatisticsCollector.class);
  final static Random rand = new Random();
  final Configuration conf = new Configuration();
  final FakeConfigManager fakeConfigManager = new FakeConfigManager();
  final FakeRaidNode fakeRaidNode = new FakeRaidNode();
  
  // Test scenario with mixed file-level raid and dir-level raid 
  public void testCollectMixedRaid() throws Exception {
    MiniDFSCluster dfs = null;
    try {
      conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 0L);
      dfs = new MiniDFSCluster(conf, 3, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      TestDirectoryRaidDfs.setupStripeStore(conf, fs);
      // load the test directory codecs.
      Utils.loadTestCodecs(conf, new Builder[] {Utils.getDirXORBuilder(), 
          Utils.getDirRSBuilder(), Utils.getRSBuilder(),
          Utils.getXORBuilder()});
      List<PolicyInfo> allPolicies = new ArrayList<PolicyInfo>();
      for (Codec codec: Codec.getCodecs()) {
        PolicyInfo info = new PolicyInfo(codec.id, conf);
        info.setSrcPath("/a");
        info.setProperty("modTimePeriod", "0");
        info.setProperty("targetReplication", "1");
        info.setCodecId(codec.id);
        allPolicies.add(info);
      }
      createFile(fs, new Path("/a/dir-xor/RAIDED1"), 1, 3, 512L);
      createFile(fs, new Path("/a/dir-xor/RAIDED2"), 1, 3, 512L);
      doRaid(fs, new Path("/a/dir-xor"), Codec.getCodec("dir-xor"), 1, 1);
      
      createFile(fs, new Path("/a/dir-rs/RAIDED1"), 1, 3, 512L);
      createFile(fs, new Path("/a/dir-rs/RAIDED2"), 1, 3, 512L);
      doRaid(fs, new Path("/a/dir-rs"), Codec.getCodec("dir-rs"), 1, 1);
      
      createFile(fs, new Path("/a/xor/RAIDED1"), 1, 3, 512L);
      createFile(fs, new Path("/a/xor/RAIDED2"), 1, 3, 512L);
      doRaid(fs, new Path("/a/xor/RAIDED1"), Codec.getCodec("xor"), 1, 1);
      doRaid(fs, new Path("/a/xor/RAIDED2"), Codec.getCodec("xor"), 1, 1);
      
      createFile(fs, new Path("/a/rs/RAIDED1"), 1, 3, 512L);
      createFile(fs, new Path("/a/rs/RAIDED2"), 1, 3, 512L);
      doRaid(fs, new Path("/a/rs/RAIDED1"), Codec.getCodec("rs"), 1, 1);
      doRaid(fs, new Path("/a/rs/RAIDED2"), Codec.getCodec("rs"), 1, 1);
      
      StatisticsCollector collector =
          new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
      collector.collect(allPolicies);
      for (Codec codec : Codec.getCodecs()) {
        Statistics st = collector.getRaidStatistics(codec.id);
        Counters raided = st.getSourceCounters(RaidState.RAIDED);
        if (codec.isDirRaid) {
          assertCounters(raided, 1, 2, 6, 6 * 512L, 6 * 512L);
        } else {
          assertCounters(raided, 2, 6, 6 * 512L, 6 * 512L);
        }
      }
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  // Test the case that raided files are counted for low-priority policies
  public void testLowPriorityRaidedPolicy() throws Exception {
    MiniDFSCluster dfs = null;
    try {
      conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 0L);
      dfs = new MiniDFSCluster(conf, 3, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      TestDirectoryRaidDfs.setupStripeStore(conf, fs);
      Utils.loadTestCodecs(conf, new Builder[] {Utils.getDirXORBuilder(), 
          Utils.getDirRSBuilder(), Utils.getXORBuilder(),
          Utils.getRSBuilder()});
      verifyLowPrioritySourceCollect("xor", "rs", fs);
      verifyLowPrioritySourceCollect("dir-xor", "dir-rs", fs);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  public void verifyLowPrioritySourceCollect(String lpId, String hpId, 
      FileSystem fs) throws IOException {
    PolicyInfo lpInfo = new PolicyInfo("Test-Low-Pri-" + lpId, conf);
    lpInfo.setSrcPath("/a");
    lpInfo.setProperty("modTimePeriod", "0");
    lpInfo.setProperty("targetReplication", "1");
    lpInfo.setCodecId(lpId);

    PolicyInfo hpInfo = new PolicyInfo("Test-High-Pri-" + hpId, conf);
    hpInfo.setSrcPath("/a");
    hpInfo.setProperty("modTimePeriod", "0");
    hpInfo.setProperty("targetReplication", "2");
    hpInfo.setCodecId(hpId);
    
    Codec lpCodec = Codec.getCodec(lpId);
    createFile(fs, new Path("/a/b/f/g/RAIDED"), 1, 3, 1024L);
    doRaid(fs, new Path("/a/b/f/g/RAIDED"), lpCodec, 1, 1);
    createFile(fs, new Path("/a/b/f/h/RAIDED"), 1, 4, 1024L);
    doRaid(fs, new Path("/a/b/f/h/RAIDED"), lpCodec, 1, 1);
    createFile(fs, new Path("/a/b/f/i/NOT_RAIDED"), 3, 5, 1024L);
    
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(lpInfo, hpInfo);
    collector.collect(allPolicies);
    Statistics lpSt = collector.getRaidStatistics(lpId);
    LOG.info("Statistics collected " + lpSt);
    LOG.info("Statistics html:\n " + lpSt.htmlTable());
    Counters raided = lpSt.getSourceCounters(RaidState.RAIDED);
    Counters tooSmall = lpSt.getSourceCounters(RaidState.NOT_RAIDED_TOO_SMALL);
    Counters tooNew = lpSt.getSourceCounters(RaidState.NOT_RAIDED_TOO_NEW);
    Counters otherPolicy = lpSt.getSourceCounters(RaidState.NOT_RAIDED_OTHER_POLICY);
    Counters notRaided = lpSt.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    assertCounters(raided, lpCodec.isDirRaid? 2 : 0, 2, 7, 7 * 1024L, 7 * 1024L);
    assertCounters(tooSmall, 0, 0, 0, 0L, 0L);
    assertCounters(tooNew, 0, 0, 0, 0L, 0L);
    // the NOT_RAIDED files could be raided by high priority policy 
    assertCounters(otherPolicy, lpCodec.isDirRaid? 1 : 0, 1, 5, 15 * 1024L,
        5 * 1024L);
    assertCounters(notRaided, 0, 0, 0, 0L, 0L);
    fs.delete(new Path("/a"), true);
  }
  
  public void testExcludes() throws IOException {
    conf.set("raid.exclude.patterns", "/exclude/,/df_mf/");
    RaidState.Checker checker = new RaidState.Checker(
      new ArrayList<PolicyInfo>(), conf);
    assertEquals(true, checker.shouldExclude("/a/b/c/df_mf/foo/bar"));
    assertEquals(false, checker.shouldExclude("/a/b/c/xdf_mf/foo/bar"));
  }

  public void testExcludeTrash() throws IOException {
    RaidState.Checker checker = new RaidState.Checker(
      new ArrayList<PolicyInfo>(), conf);
    assertEquals(true, checker.shouldExclude("/a/b/c/.Trash/foo/bar"));
    assertEquals(false, checker.shouldExclude("/a/b/c/foo/bar"));
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
      TestDirectoryRaidDfs.setupStripeStore(conf, fs);
      Utils.loadTestCodecs(conf);
      verifySourceCollect("rs", fs, false);
      verifySourceCollect("xor", fs, false);
      verifyParityCollect("rs", fs);
      verifyParityCollect("xor", fs);
      verifyLongPrefixOverride(fs);
      verifyRsCodeOverride(fs);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  public void testCollectDirRaid() throws Exception {
    
    MiniDFSCluster dfs = null;
    try {
      conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 0L);
      dfs = new MiniDFSCluster(conf, 3, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      TestDirectoryRaidDfs.setupStripeStore(conf, fs);
      // load the test directory codecs.
      Utils.loadTestCodecs(conf, new Builder[] {Utils.getDirXORBuilder(), 
          Utils.getDirRSBuilder()});
      verifyDirRaidSourceCollect("dir-rs", fs, false);
      verifyDirRaidSourceCollect("dir-xor", fs, false);
      verifyParityCollect("dir-rs", fs);
      verifyParityCollect("dir-xor", fs);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  public void testSnapshot() throws Exception {
    conf.set(StatisticsCollector.STATS_SNAPSHOT_FILE_KEY,
             "/tmp/raidStatsSnapshot");
    MiniDFSCluster dfs = null;
    try {
      Utils.loadTestCodecs(conf);
      dfs = new MiniDFSCluster(conf, 3, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      TestDirectoryRaidDfs.setupStripeStore(conf, fs);
      verifySourceCollect("rs", fs, true);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  public void verifyDirRaidSourceCollect(String codecId, FileSystem fs,
                                         boolean writeAndRestoreSnapshot) 
                                             throws IOException {
    PolicyInfo info = new PolicyInfo("Test-Dir-Raid-" + codecId, conf);
    info.setSrcPath("/a");
    info.setProperty("modTimePeriod", "0");
    info.setProperty("targetReplication", "1");
    info.setCodecId(codecId);
    
    PolicyInfo infoTooNew = new PolicyInfo("Test-Too-New-" + codecId, conf);
    infoTooNew.setSrcPath("/new");
    infoTooNew.setProperty("modTimePeriod", "" + Long.MAX_VALUE);
    infoTooNew.setProperty("targetReplication", "1");
    infoTooNew.setCodecId(codecId);
    
    Codec curCodec = Codec.getCodec(codecId);
    
    createFile(fs, new Path("/a/b/NOT_RAIDED1"), 1, 1, 1024L);
    createFile(fs, new Path("/a/b/NOT_RAIDED2"), 2, 2, 1024L);
    createFile(fs, new Path("/a/b/NOT_RAIDED3"), 1, 3, 1024L);
    
    createFile(fs, new Path("/a/c/RAIDED1"), 1, 1, 1024L);
    createFile(fs, new Path("/a/c/RAIDED2"), 1, 2, 1024L);
    createFile(fs, new Path("/a/c/RAIDED3"), 1, 3, 1024L);
    createFile(fs, new Path("/a/c/RAIDED4"), 1, 4, 1024L);
    createFile(fs, new Path("/a/c/RAIDED5"), 1, 5, 1024L);
    doRaid(fs, new Path("/a/c"), curCodec, 1, 1);
    
    createFile(fs, new Path("/a/d/TOO_SMALL1"), 1, 1, 1024L);
    createFile(fs, new Path("/a/d/TOO_SMALL2"), 2, 1, 1024L);

    createFile(fs, new Path("/new/TOO_NEW1"), 3, 4, 1024L);
    createFile(fs, new Path("/new/TOO_NEW2"), 3, 5, 1024L);
    
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoTooNew);
    collector.collect(allPolicies);
    Statistics st = collector.getRaidStatistics(codecId);
    
    LOG.info("Dir Statistics collected " + st);
    LOG.info("Dir Statistics html:\n " + st.htmlTable());
    Counters raided = st.getSourceCounters(RaidState.RAIDED);
    Counters tooSmall = st.getSourceCounters(RaidState.NOT_RAIDED_TOO_SMALL);
    Counters tooNew = st.getSourceCounters(RaidState.NOT_RAIDED_TOO_NEW);
    Counters notRaided = st.getSourceCounters(RaidState.NOT_RAIDED_BUT_SHOULD);
    
    assertCounters(raided, 1, 5, 15, 15 * 1024L, 15 * 1024L);
    assertCounters(tooSmall, 1, 2, 2, 3 * 1024L, 2 * 1024L);
    assertCounters(tooNew, 1, 2, 9, 27 * 1024L, 9 * 1024L);
    assertCounters(notRaided, 1, 3, 6, 8 * 1024L, 6 * 1024L);
  }
  
  public void verifySourceCollect(String codecId, FileSystem fs,
                                  boolean writeAndRestoreSnapshot)
      throws Exception {
    PolicyInfo info = new PolicyInfo("Test-Raided-" + codecId, conf);
    info.setSrcPath("/a/b");
    info.setProperty("modTimePeriod", "0");
    info.setProperty("targetReplication", "1");
    info.setCodecId(codecId);

    PolicyInfo infoTooNew = new PolicyInfo("Test-Too-New-" + codecId, conf);
    infoTooNew.setSrcPath("/a/new");
    infoTooNew.setProperty("modTimePeriod", "" + Long.MAX_VALUE);
    infoTooNew.setProperty("targetReplication", "1");
    infoTooNew.setCodecId(codecId);
    
    Codec curCodec = Codec.getCodec(codecId);
    createFile(fs, new Path("/a/b/TOO_SMALL"), 1, 1, 1024L);
    createFile(fs, new Path("/a/b/d/TOO_SMALL"), 2, 2, 1024L);
    createFile(fs, new Path("/a/b/f/g/RAIDED"), 1, 3, 1024L);
    doRaid(fs, new Path("/a/b/f/g/RAIDED"), curCodec, 1, 1);
    createFile(fs, new Path("/a/b/f/g/h/RAIDED"), 1, 4, 1024L);
    doRaid(fs, new Path("/a/b/f/g/h/RAIDED"), curCodec, 1, 1);
    createFile(fs, new Path("/a/b/f/g/NOT_RAIDED"), 3, 5, 1024L);

    createFile(fs, new Path("/a/new/i/TOO_NEW"), 3, 4, 1024L);
    createFile(fs, new Path("/a/new/j/TOO_NEW"), 3, 5, 1024L);

    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoTooNew);
    collector.collect(allPolicies);
    Statistics st = collector.getRaidStatistics(codecId);
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

    if (writeAndRestoreSnapshot) {
      Map<String, Statistics> stats = collector.getRaidStatisticsMap();
      assertTrue(collector.writeStatsSnapshot());
      collector.clearRaidStatisticsMap();
      assertEquals(null, collector.getRaidStatisticsMap());

      assertTrue(collector.readStatsSnapshot());
      Map<String, Statistics> diskStats =
        collector.getRaidStatisticsMap();
      assertEquals(stats, diskStats);
    }
  }

  public void verifyParityCollect(String codecId, FileSystem fs)
      throws Exception {
    LOG.info("Start testing parity collect for " + codecId);
    Codec codec = Codec.getCodec(codecId);
    Path parityPath = new Path(codec.parityDirectory);
    if (fs.exists(parityPath)) {
      fs.delete(parityPath, true);
    }
    fs.mkdirs(parityPath);
    createFile(fs, new Path(parityPath+ "/a"), 1, 1, 1024L);
    createFile(fs, new Path(parityPath + "/b/c"), 2, 2, 1024L);
    createFile(fs, new Path(parityPath + "/d/e/f"), 3, 3, 1024L);
    List<PolicyInfo> empty = Collections.emptyList();
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    collector.collect(empty);
    Statistics st = collector.getRaidStatistics(codecId);
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
    info.setCodecId("rs");

    PolicyInfo infoLongPrefix = new PolicyInfo("Long-Prefix", conf);
    infoLongPrefix.setSrcPath("/a/b/c");
    infoLongPrefix.setProperty("modTimePeriod", "0");
    infoLongPrefix.setProperty("targetReplication", "2");
    infoLongPrefix.setCodecId("xor");

    createFile(fs, new Path("/a/b/k"), 3, 4, 1024L);
    createFile(fs, new Path("/a/b/c/d"), 3, 5, 1024L);
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoLongPrefix);
    collector.collect(allPolicies);
    Statistics xorSt = collector.getRaidStatistics("xor");
    Statistics rsSt = collector.getRaidStatistics("rs");
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
    info.setCodecId("xor");

    PolicyInfo infoLongPrefix = new PolicyInfo("Long-Prefix", conf);
    infoLongPrefix.setSrcPath("/a/b/c");
    infoLongPrefix.setProperty("modTimePeriod", "0");
    infoLongPrefix.setProperty("targetReplication", "2");
    infoLongPrefix.setCodecId("rs");

    createFile(fs, new Path("/a/b/k"), 3, 4, 1024L);
    createFile(fs, new Path("/a/b/c/d"), 3, 5, 1024L);
    StatisticsCollector collector =
        new StatisticsCollector(fakeRaidNode, fakeConfigManager, conf);
    List<PolicyInfo> allPolicies = Arrays.asList(info, infoLongPrefix);
    collector.collect(allPolicies);
    Statistics xorSt = collector.getRaidStatistics("xor");
    Statistics rsSt = collector.getRaidStatistics("rs");
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
  
  private void assertCounters(Counters counters, long numDirs, long numFiles,
      long numBlocks, long numBytes, long numLogicalBytes) {
    assertEquals(numDirs, counters.getNumDirs());
    assertCounters(counters, numFiles, numBlocks, numBytes, numLogicalBytes);
  }
  
  private static void doRaid(FileSystem fileSys, Path name, Codec codec, 
      int targetRepl, int metaRepl) throws IOException {
    FileStatus fileStat = fileSys.getFileStatus(name);
    if (codec.isDirRaid && !fileStat.isDir()) {
      // raid its parent
      fileStat = fileSys.getFileStatus(name.getParent());
    }
    assertTrue(RaidNode.doRaid(fileSys.getConf(),
        fileStat,
        new Path(codec.parityDirectory), codec,
        new RaidNode.Statistics(),
        RaidUtils.NULL_PROGRESSABLE,
        false, targetRepl, metaRepl));
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
    public String raidJobsHtmlTable(JobMonitor.STATUS running) {
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

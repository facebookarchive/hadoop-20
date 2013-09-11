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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.raid.DistRaid.DistRaidMapper;
import org.apache.hadoop.raid.DistRaid.EncodingCandidate;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

public class TestRetryTaskEncoding extends TestMultiTasksEncoding {
  final static Log LOG =
      LogFactory.getLog("org.apache.hadoop.raid.TestRetryTaskEncodi");
  
  class TestEncodingHandler extends InjectionHandler {
    Map<InjectionEventI, AtomicInteger> events =
        new HashMap<InjectionEventI, AtomicInteger>(); 
    Random rand = new Random();
    HashMap<InjectionEventI, Double> specialFailProbs;
    double defaultFailProb;
    public Path lockFile;
    public Path parityFile;
    public Path finalTmpPath;
    public Long numStripes;
    
    public TestEncodingHandler(double defaultProb,
        HashMap<InjectionEventI, Double> specialProbs) {
      defaultFailProb = defaultProb;
      specialFailProbs = specialProbs;
      for (InjectionEventI event: new InjectionEventI[]{
          InjectionEvent.RAID_ENCODING_FAILURE_PUT_CHECKSUM,
          InjectionEvent.RAID_ENCODING_FAILURE_RENAME_FILE,
          InjectionEvent.RAID_ENCODING_FAILURE_CONCAT_FILE,
          InjectionEvent.RAID_ENCODING_FAILURE_GET_SRC_STRIPES,
          InjectionEvent.RAID_ENCODING_FAILURE_PUT_STRIPE,
      }) {
        events.put(event, new AtomicInteger(0));
      }
    }

    @Override
    public void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (!events.containsKey(event)) {
        return;
      }
      int count = events.get(event).incrementAndGet();
      if (event == InjectionEvent.RAID_ENCODING_FAILURE_PUT_STRIPE) {
        parityFile = (Path)args[0];
        finalTmpPath = (Path)args[1];
        assertTrue("parity file should exist", fileSys.exists(parityFile));
        assertFalse("finalTmpPath should not exist", fileSys.exists(finalTmpPath));
      }
      Double failProb = null;
      // Only use the specialFailProb for the first time
      if (specialFailProbs != null && count == 1) {
        failProb = specialFailProbs.get(event);
      }
      if (failProb == null) {
        failProb = defaultFailProb;
      }
      if (rand.nextDouble() < failProb) {
        throw new IOException(event.toString());
      }
    }
  }

  public class EncodingThread extends Thread {
    public PolicyInfo policyInfo = null;
    public boolean succeed = false;
    public int retryNum = 0;
    public EncodingCandidate encodingCandidate = null;
    public IOException exp = null;
    public int threadNum = 0;
    
    public EncodingThread(PolicyInfo pi, EncodingCandidate newEC,
        int newRetryNum, int newThreadNum) {
      policyInfo = pi;
      encodingCandidate = newEC;
      retryNum = newRetryNum;
      threadNum = newThreadNum;
    }
    
    public void run() {
      try {
        succeed = DistRaidMapper.doRaid(retryNum, encodingCandidate.toString(),
            conf, policyInfo, new RaidNode.Statistics(), Reporter.NULL);
        LOG.info("Finished Thread " + threadNum + " " + succeed);
      } catch (IOException ioe) {
        exp = ioe;
      }
    }
  }
  
  // Run one round of encoding job
  // Each task could have multiple instances to simulate race conditions
  public boolean runEncodingTasks(Codec codec, FileStatus stat,
      PolicyInfo info, int retryNum) throws Exception {
    String jobId = RaidNode.getJobID(conf);
    LOG.info("Set local raid job id: " + jobId);
    List<EncodingCandidate> lec =
        RaidNode.splitPaths(conf, codec, stat); 
    EncodingThread[] threads = new EncodingThread[lec.size()];
    boolean succeed = false;
    for (int i = 0; i < lec.size(); i++) {
      threads[i] = new EncodingThread(info, lec.get(i), retryNum, i);
    }
    for (EncodingThread et: threads) {
      et.start();
    }
    for (EncodingThread et: threads) {
      et.join();
      succeed |= et.succeed;
      if (et.exp != null) {
        LOG.warn("Exception from Thread " + et.threadNum + 
            et.encodingCandidate + " :" + et.exp.getMessage()); 
      }
    }
    return succeed;
  }
  
  public void testRetryTask() throws Exception {
    LOG.info("Test testRetryTask started.");
    createClusters(false);
    RaidNode.createChecksumStore(conf, true);
    Path raidDir = new Path("/raidtest/1");
    HashMap<Codec, Long[]> fileCRCs = new HashMap<Codec, Long[]>();
    HashMap<Codec, Path> filePaths = new HashMap<Codec, Path>();
    HashMap<InjectionEventI, Double> specialFailProbs = 
        new HashMap<InjectionEventI, Double>();
    PolicyInfo info = new PolicyInfo();
    info.setProperty("targetReplication", Integer.toString(targetReplication));
    info.setProperty("metaReplication", Integer.toString(metaReplication));
    try {
      createTestFiles(raidDir, filePaths, fileCRCs, null);
      LOG.info("Test testRetryTask created test files");
      for (Codec codec: Codec.getCodecs()) {
        Path filePath = filePaths.get(codec);
        FileStatus stat = fileSys.getFileStatus(filePath);
        info.setCodecId(codec.id);
        
        LOG.info("Codec: " + codec + ", Path: " + filePath + 
                 " Sync every task to the finalize stage, " + 
                 "all partial parity files are generated");
        specialFailProbs.clear();
        specialFailProbs.put(
            InjectionEvent.RAID_ENCODING_FAILURE_CONCAT_FILE, 1.0);
        specialFailProbs.put(
            InjectionEvent.RAID_ENCODING_FAILURE_GET_SRC_STRIPES, 1.0);
        specialFailProbs.put(
            InjectionEvent.RAID_ENCODING_FAILURE_PUT_STRIPE, 1.0);
        TestEncodingHandler h = new TestEncodingHandler(0.0, 
            specialFailProbs);
        InjectionHandler.set(h);
        assertEquals("Should succeed", true, 
            runEncodingTasks(codec, stat, info, 1000));
        assertEquals("Only did two concats, one failed, one succeeded ", 2, 
            h.events.get(
                InjectionEvent.RAID_ENCODING_FAILURE_CONCAT_FILE).get());
        if (codec.isDirRaid) {
          assertEquals("Only did two getSrcStripes, one failed, two succeeded", 
              2, h.events.get(
                  InjectionEvent.RAID_ENCODING_FAILURE_GET_SRC_STRIPES).get());
          assertEquals("Only did two putStripes, one failed, one succeeded", 2,
              h.events.get(InjectionEvent.RAID_ENCODING_FAILURE_PUT_STRIPE).get());
        }
        if (!codec.isDirRaid) {
          TestRaidDfs.waitForFileRaided(LOG, fileSys, filePath, 
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(filePath.getParent())),
              targetReplication);
        } else {
          TestRaidDfs.waitForDirRaided(LOG, fileSys, filePath,
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(raidDir)), 
                  targetReplication);
        } 
        TestRaidDfs.waitForReplicasReduction(fileSys, filePath,
            targetReplication);
      }
      verifyCorrectness(raidDir, fileCRCs, null);
      LOG.info("Test testRetryTask successful.");
    } catch (Exception e) {
      LOG.info("testRetryTask Exception ", e);
      throw e;
    } finally {
      stopClusters();
      
    }
    LOG.info("Test testRetryTask completed.");
  }
  
  public void testLargeFailureRateEncoding() throws Exception {
    LOG.info("Test testLargeFailureRateEncoding started.");
    createClusters(false);
    RaidNode.createChecksumStore(conf, true);
    Path raidDir = new Path("/raidtest/1");
    HashMap<Codec, Long[]> fileCRCs = new HashMap<Codec, Long[]>();
    HashMap<Codec, Path> filePaths = new HashMap<Codec, Path>();
    PolicyInfo info = new PolicyInfo();
    info.setProperty("targetReplication", Integer.toString(targetReplication));
    info.setProperty("metaReplication", Integer.toString(metaReplication));
    try {
      createTestFiles(raidDir, filePaths, fileCRCs, null);
      LOG.info("Test testLargeFailureRateEncoding created test files");
      // create the InjectionHandler
      for (Codec codec: Codec.getCodecs()) {
        Path filePath = filePaths.get(codec);
        FileStatus stat = fileSys.getFileStatus(filePath);
        info.setCodecId(codec.id);
        boolean succeed = false;
        TestEncodingHandler h = new TestEncodingHandler(0.5, 
            null);
        InjectionHandler.set(h);
        succeed = runEncodingTasks(codec, stat, info, 100);
        assertTrue("We should succeed", succeed);
        if (!codec.isDirRaid) {
          TestRaidDfs.waitForFileRaided(LOG, fileSys, filePath, 
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(filePath.getParent())),
              targetReplication);
        } else {
          TestRaidDfs.waitForDirRaided(LOG, fileSys, filePath,
              new Path(codec.parityDirectory,
                  RaidNode.makeRelative(raidDir)), 
                  targetReplication);
        } 
        TestRaidDfs.waitForReplicasReduction(fileSys, filePath,
            targetReplication);
      }
      verifyCorrectness(raidDir, fileCRCs, null);
      LOG.info("Test testLargeFailureRateEncoding successful.");
    } catch (Exception e) {
      LOG.info("testLargeFailureRateEncoding Exception ", e);
      throw e;
    } finally {
      stopClusters();
      
    }
    LOG.info("Test testLargeFailureRateEncoding completed.");
  }
  
  @Override 
  public void testFileListPolicy() throws Exception {
  }
}

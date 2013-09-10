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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FinalizeCheckpointException;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Test;

public class TestAvatarCheckpointingQJM extends TestAvatarCheckpointing {
  @Test
  public void testMergeWithCheckPoint() throws Exception {
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, null, false);
    InjectionHandler.set(h);
    setUp("testMergeWithCheckPoint", true);
    long blockSize = TestFileCreation.blockSize;
    long fileLen = TestFileCreation.blockSize * numDataBlocks;
    // Create files
    Path root = new Path("/user/");
    Path rsFile = new Path(root, "rsFile");
    long rsCRC = DFSTestUtil.createFile(fs, rsFile, fileLen, (short)1, 1, null,
        blockSize);
    
    Path xorFile = new Path(root, "xorFile");
    long xorCRC = DFSTestUtil.createFile(fs, xorFile, fileLen, (short)2, 2, null,
        blockSize);
    
    // Create parity files
    Path parityRoot = new Path("/raid/user/");
    Path parityRSFile = new Path(parityRoot, "rsFile");
    DFSTestUtil.createFile(fs, parityRSFile, blockSize * numRSParityBlocks, 
        (short)1, 3, null, blockSize);
   
    Path parityXORFile = new Path(parityRoot, "xorFile");
    DFSTestUtil.createFile(fs, parityXORFile, blockSize, (short)2, 4, null, 
        blockSize);
    
    int checksums[] = new int[numDataBlocks];
    for (int i = 0 ;i < numDataBlocks; i++) {
      checksums[i] = i+1;
    }
   
    FileStatus rsStat = fs.getFileStatus(rsFile);
    FileStatus xorStat = fs.getFileStatus(xorFile);
    LocatedBlocks rsLbs = dfs.getLocatedBlocks(rsFile, 0, fileLen);
    LocatedBlocks xorLbs = dfs.getLocatedBlocks(xorFile, 0, fileLen);
    fs.setTimes(parityRSFile, rsStat.getModificationTime(), 0);
    fs.setTimes(parityXORFile, xorStat.getModificationTime(), 0);
    
    LOG.info("Merge source files and parity files");
    ((DistributedFileSystem)fs).merge(parityRSFile, rsFile, "rs", checksums);
    ((DistributedFileSystem)fs).merge(parityXORFile, xorFile, "xor", checksums);
    
    assertTrue("Parity file doesn't exist", !dfs.exists(parityRSFile));
    assertTrue("Parity file doesn't exist", !dfs.exists(parityXORFile));
    LOG.info("Verify merge");
    FileStatus xorStatBeforeRestart = 
        TestMergeFile.verifyMergeFiles(dfs, xorStat, xorLbs, xorFile, fileLen, xorCRC);
    FileStatus rsStatBeforeRestart =
        TestMergeFile.verifyMergeFiles(dfs, rsStat, rsLbs, rsFile, fileLen, rsCRC);
    LOG.info("NN checkpointing");
    h.doCheckpoint();
    
    // Restart the namenode
    LOG.info("NN restarting");
    cluster.restartAvatarNodes();
    // Verify merge again
    LOG.info("Verify merge after the NN restarts");
    assertTrue("Parity file doesn't exist", !dfs.exists(parityRSFile));
    assertTrue("Parity file doesn't exist", !dfs.exists(parityXORFile));
    FileStatus xorStatAfterRestart = 
        TestMergeFile.verifyMergeFiles(dfs, xorStat, xorLbs, xorFile, fileLen, xorCRC);
    FileStatus rsStatAfterRestart =
        TestMergeFile.verifyMergeFiles(dfs, rsStat, rsLbs, rsFile, fileLen, rsCRC);
    assertEquals("Restarting doesn't change modificaiton time",
        xorStatBeforeRestart.getModificationTime(),
        xorStatAfterRestart.getModificationTime());
    assertEquals("Restarting doesn't chagne modification time",
        rsStatBeforeRestart.getModificationTime(),
        rsStatAfterRestart.getModificationTime());
  }

	@Test
  public void testFailSuccFailQuiesce() throws Exception {
  	doTestFailSuccFailQuiesce(true);
	}
	
	@Test
  public void testFailCheckpointOnceAndRestartStandby() throws Exception {
  	doTestFailCheckpointOnceAndRestartStandby(true);
  }
	
	@Test
  public void testFailCheckpointMultiAndCrash() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointMultiAndCrash");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null,
        null, true);
    InjectionHandler.set(h);
    setUp("testFailCheckpointMultiAndCrash", true);
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    try {
      h.failNextCheckpoint = true;
      h.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) { 
      // checkpoint fails during finalization (see the checkpointing handler)
      assertTrue(e instanceof FinalizeCheckpointException);
      assertTrue(AvatarSetupUtil.isIngestAlive(standby));
      LOG.info("Expected: Checkpoint failed", e);
    }
    
    // current txid should be 20 + SLS + ENS + SLS + initial ckpt
    assertEquals(25, getCurrentTxId(primary));
    
    try {
      h.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) { 
      // checkpoint fails during finalization (see the checkpointing handler)
      assertTrue(e instanceof FinalizeCheckpointException);
      assertTrue(AvatarSetupUtil.isIngestAlive(standby));
      LOG.info("Expected: Checkpoint failed", e);
    }
    
    // roll adds 2 transactions
    assertEquals(27, getCurrentTxId(primary));
    
    try {
      h.doCheckpoint();
      fail("Should get IOException here");
    } catch (Exception e) {
      // checkpoint fails during finalization (see the checkpointing handler)
      assertTrue(e instanceof FinalizeCheckpointException);
      assertTrue(AvatarSetupUtil.isIngestAlive(standby));
      LOG.info("Expected: Checkpoint failed", e);
    }
    
    // roll adds 2 transactions
    assertEquals(29, getCurrentTxId(primary));
  }
	
	@Test
  public void testHardLinkWithCheckPoint() throws Exception {
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, null, false);
    InjectionHandler.set(h);
    setUp("testHardLinkWithCheckPoint", true);
    
    // Create a new file
    Path root = new Path("/user/");
    Path file10 = new Path(root, "file1");
    FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file10, 1);
    byte[] content = TestFileCreation.writeFile(stm1);
    stm1.close();

    LOG.info("Create the hardlinks");
    Path file11 =  new Path(root, "file-11");
    Path file12 =  new Path(root, "file-12");
    fs.hardLink(file10, file11);
    fs.hardLink(file11, file12);

    LOG.info("Verify the hardlinks");
    TestFileHardLink.verifyLinkedFileIdenticial(fs, cluster.getPrimaryAvatar(0).avatar,
        fs.getFileStatus(file10), fs.getFileStatus(file11), content);
    TestFileHardLink.verifyLinkedFileIdenticial(fs, cluster.getPrimaryAvatar(0).avatar,
        fs.getFileStatus(file10), fs.getFileStatus(file12), content);

    LOG.info("NN checkpointing");
    h.doCheckpoint();
    
    // Restart the namenode
    LOG.info("NN restarting");
    cluster.restartAvatarNodes();
    
    // Verify the hardlinks again
    LOG.info("Verify the hardlinks again after the NN restarts");
    TestFileHardLink.verifyLinkedFileIdenticial(fs, cluster.getPrimaryAvatar(0).avatar, 
        fs.getFileStatus(file10), fs.getFileStatus(file11), content);
    TestFileHardLink.verifyLinkedFileIdenticial(fs, cluster.getPrimaryAvatar(0).avatar, 
        fs.getFileStatus(file10), fs.getFileStatus(file12), content);
  }
	
	@Test
  public void testFailCheckpointOnCorruptImage() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointOnCorruptImage");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(
        null, null, false);
    InjectionHandler.set(h);
    
    // first checkpoint will succeed (most recent ckptxid = 1)
    setUp(3600, "testFailCheckpointOnCorruptImage", true, true);   
    // image will be corrupted for second - manual checkpoint
    h.corruptImage = true;
    
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
   
    // do second checkpoint
    try {
      h.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) {  
      // checkpoint fails during finalizations (see the checkpointing handler)
      assertTrue(e instanceof FinalizeCheckpointException);
      assertTrue(AvatarSetupUtil.isIngestAlive(standby));
    }
    assertEquals(1, primary.getCheckpointSignature().getMostRecentCheckpointTxId());
  }
	
	@Test
  public void testCheckpointReprocessEdits() throws Exception {
    LOG.info("TEST: ----> testCheckpointReprocessEdits");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null,
        null, false);   
    setUp("testCheckpointReprocessEdits", true);
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;

    h.reprocessIngest = true;
    // set the handler later no to interfere with the previous checkpoint
    InjectionHandler.set(h);
    // checkpoint should be ok
    h.doCheckpoint();
    assertEquals(23, primary.getCheckpointSignature()
        .getMostRecentCheckpointTxId());
  }
}

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

package org.apache.hadoop.hdfs.profiling;

import java.io.File;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSClientReadProfilingData;
import org.apache.hadoop.fs.FSDataNodeReadProfilingData;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestProfilingData {
  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  Path root = null;
  final static String TEST_DIR = new File(
      System.getProperty("test.build.data",
          "build/contrib/raid/test/data")).getAbsolutePath();
  final static int NUM_DATANODES = 1;
  final Random random = new Random(System.currentTimeMillis());
  private static int BLOCK_SIZE = 1024 * 1024;
  private static int MAX_BLOCKS = 20;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  static final Log LOG = LogFactory.getLog(TestProfilingData.class);

  public void mySetup(boolean inlineChecksum) throws Exception {
    conf = new Configuration();
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() 
          + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists

    conf.setInt("fs.trash.interval", 1440);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
  }

  public void myShutDown() throws Exception {
    if (fileSys != null) {
      fileSys.close();
    }
    if (dfs != null) {
      dfs.shutdown();
    }
  }

  @Test
  public void testProfilingPread() throws Exception {
    profilePread(false);
  } 
  
  @Test
  public void testProfilingPreadWithInlineChecksum() throws Exception {
    profilePread(true);
  }
  
  private void profilePread(boolean inlineChecksum) throws Exception {
    try {
      mySetup(inlineChecksum);
      Path srcPath = new Path("/test/profiling/testpread");
      int fileLen = 8 * BLOCK_SIZE;
      DFSTestUtil.createFile(fileSys, srcPath, fileLen, (short) 1, System.currentTimeMillis());

      DFSClient.startReadProfiling();
      FSDataInputStream in = fileSys.open(srcPath);

      byte[] buffer = new byte[BLOCK_SIZE];
      in.read(0, buffer, 0, BLOCK_SIZE/2);
      in.read(BLOCK_SIZE, buffer, 0, BLOCK_SIZE/2);
      in.read(BLOCK_SIZE*2 + BLOCK_SIZE/2, buffer, 0, BLOCK_SIZE/2);
      in.close();

      DFSReadProfilingData pData = DFSClient.getDFSReadProfilingData();
      DFSClient.stopReadProfiling();
      
      LOG.info("Result: " + pData.toString());

      List<FSClientReadProfilingData> cliDataList = pData.getClientProfilingDataList();
      
      int i = 0;
      LOG.info("Pread profiling result with inline checksum " + (inlineChecksum ? "enabled." : "disabled."));
      for (FSClientReadProfilingData cliData : cliDataList) {
        long readTime = cliData.getReadTime();
        LOG.info(i + ".readTime: " + readTime);
        LOG.info(i + ".preadBlockSeekContextTime: " + (double)cliData.getPreadBlockSeekContextTime() / readTime);
        LOG.info(i + ".preadChooseDataNodeTime: " + (double)cliData.getPreadChooseDataNodeTime() / readTime);
        LOG.info(i + ".preadActualGetFromOneDNTime: " + (double)cliData.getPreadActualGetFromOneDNTime() / readTime);
        LOG.info(i + ".preadGetBlockReaderTime: " + (double)cliData.getPreadGetBlockReaderTime() / readTime);
        LOG.info(i + ".preadAllTime: " + (double)cliData.getPreadAllTime() / readTime);
        LOG.info(i + ".blockreader_readChunkTime: " + (double)cliData.getReadChunkTime() / readTime);
        LOG.info(i + ".blockreader_verifyCheckSumTime: " + (double)cliData.getVerifyChunkCheckSumTime() / readTime);
        
        List<FSDataNodeReadProfilingData> dnDataList = cliData.getDataNodeReadProfilingDataList();
        
        for (int j = 0; j < dnDataList.size(); j++) {
          FSDataNodeReadProfilingData dnData = dnDataList.get(j);
          LOG.info(i + "-" + j + ".dn_readChunkDataTime: " + (double)dnData.getReadChunkDataTime() / readTime);
          LOG.info(i + "-" + j + ".dn_readChunkCheckSumTime: " + (double)dnData.getReadChunkCheckSumTime() / readTime);
          LOG.info(i + "-" + j + ".dn_copyChunkDataTime: " + (double)dnData.getCopyChunkDataTime() / readTime);
          LOG.info(i + "-" + j + ".dn_copyChunkCheckSumTime: " + (double)dnData.getCopyChunkChecksumTime() / readTime);
          LOG.info(i + "-" + j + ".dn_verifyCheckSumTime: " + (double)dnData.getVerifyCheckSumTime() / readTime);
          LOG.info(i + "-" + j + ".dn_updateChunkCheckSumTime: " + (double)dnData.getUpdateChunkCheckSumTime() / readTime);
          LOG.info(i + "-" + j + ".dn_transferChunkToClientTime: " + (double)dnData.getTransferChunkToClientTime() / readTime);
          LOG.info(i + "-" + j + ".dn_sendChunkToClientTime: " + (double)dnData.getSendChunkToClientTime() / readTime);
          LOG.info(i + "-" + j + ".dn_sendBlockTime: " + (double)dnData.getSendBlockTime() / readTime);
          
        }
        i++;
      }
    } finally {
      myShutDown();
    }
  }
}

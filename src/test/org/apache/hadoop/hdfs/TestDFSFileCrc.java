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

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataBlockScannerSet;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DatanodeBlockInfo;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.util.DataChecksum;
import org.apache.log4j.Level;
import org.junit.Assert;

public class TestDFSFileCrc extends junit.framework.TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataBlockScannerSet.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private static final Random RAN = new Random();

  public void testFileCrc() throws IOException, InterruptedException {
    testFileCrcInternal(true, false, false, false, 1);
    testFileCrcInternal(true, true, true, true, 1);
    testFileCrcInternal(false, true, true, true, 1);
    testFileCrcInternal(true, true, false, true, 2);
    testFileCrcInternal(false, true, false, true, 2);
    testFileCrcInternal(true, false, false, true, 5);
    testFileCrcInternal(false, false, false, true, 5);
  }
  
  private void testFileCrcInternal(boolean inlineChecksum,
      boolean triggerBlockRecovery, boolean waitForScannerRebuild,
      boolean updateWhileWrite, int attempts) throws IOException,
      InterruptedException {
    if (waitForScannerRebuild) {
      DataBlockScannerSet.TIME_SLEEP_BETWEEN_SCAN = 500;
    }
    
    MiniDFSCluster cluster = null; 
    try {
      ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);
  
      final long seed = RAN.nextLong();
      System.out.println("seed=" + seed);
      RAN.setSeed(seed);
  
      final Configuration conf = new Configuration();

      conf.set(FSConstants.SLAVE_HOST_NAME, "127.0.0.1");
      conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
      conf.setBoolean("dfs.update.blockcrc.when.write", updateWhileWrite);
      if (!waitForScannerRebuild) {
        conf.setInt("dfs.datanode.scan.period.hours", -1);
      } else {
        conf.setInt("dfs.datanode.scan.period.hours", 1);        
      }
   
      cluster = new MiniDFSCluster(conf, 4, true, null);;
      final FileSystem hdfs = cluster.getFileSystem();

      final String dir = "/filechecksum";
      final int block_size = 1024;
      final int buffer_size = conf.getInt("io.file.buffer.size", 4096);
      conf.setInt("io.bytes.per.checksum", 512);

      // Check non-existent file
      final Path nonExistentPath = new Path("/non_existent");
      assertFalse(hdfs.exists(nonExistentPath));
      try {
        hdfs.getFileChecksum(nonExistentPath);
        Assert.fail("GetFileChecksum should fail on non-existent file");
      } catch (IOException e) {
        assertTrue(e.getMessage().startsWith(
          "Null block locations, mostly because non-existent file"));
      }

      for(int n = 0; n < attempts; n++) {
        //generate random data
        final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
        RAN.nextBytes(data);
        System.out.println("data.length=" + data.length);
  
        //write data to a file
        final Path foo = new Path(dir, "foo" + n);
        {
          final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
               (short)3, block_size);
          if (!triggerBlockRecovery) {
            out.write(data);
          } else {
            // We have 4 total datanodes and write to a file of replication 3.
            // By restarting two data nodes, we make sure that we restarted
            // at least 1 but not all the 3 datanodes holding a replica.
            //
            out.write(data, 0, data.length / 4);
            out.sync();
            cluster.restartDataNode(0);

            out.write(data, data.length / 4, data.length / 2 - data.length / 4);
            out.sync();
            cluster.restartDataNode(1);

            out.write(data, data.length / 2, data.length - data.length / 2);
          }
          out.close();
        }
        
        if (triggerBlockRecovery && waitForScannerRebuild) {
          // Wait for all the datanode's block scanner to update the CRC information
          //
          LocatedBlocks locations = ((DistributedFileSystem) hdfs).dfs.namenode
              .getBlockLocations(foo.toString(), 0, Long.MAX_VALUE);
          assertEquals(1, locations.locatedBlockCount());
          LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
          int nsId = cluster.getNameNode().getNamespaceID();
          
          Set<Integer> portSet = new HashSet<Integer>();
          for (DatanodeInfo dnInfo : locatedblock.getLocations()) {
            portSet.add(dnInfo.getPort());
          }
          FSDataset fsd = null;
          for (DataNode dn : cluster.getDataNodes()) {
            if (portSet.contains(dn.getPort())) {
              fsd = (FSDataset) (dn.data);
              TestCase.assertNotNull(fsd);
              DatanodeBlockInfo binfo = fsd.getDatanodeBlockInfo(nsId,
                  locatedblock.getBlock());
              TestCase.assertNotNull(binfo);
              long waitTimeLeft = 20000;
              while (true) {
                if (binfo.hasBlockCrcInfo()) {
                  break;
                } else {
                  if (waitTimeLeft > 0) {
                    waitTimeLeft -= 100;
                    System.out.println("Sleep while waiting for datanode " + dn
                        + " block scanner to rebuild block CRC for: "
                        + locatedblock.getBlock());
                    Thread.sleep(100);
                  } else {
                    TestCase.fail();
                  }
                }
              }
            }
          }
          
        }
        // compute data CRC
        DataChecksum checksum = DataChecksum.newDataChecksum(
            DataChecksum.CHECKSUM_CRC32, 1);
        checksum.update(data, 0, data.length);
        
        //compute checksum
        final int crc = hdfs.getFileCrc(foo);
        System.out.println("crc=" + crc);
        
        TestCase.assertEquals((int) checksum.getValue(), crc);
      }
    } finally {
      DataBlockScannerSet.TIME_SLEEP_BETWEEN_SCAN = 5000;      

      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

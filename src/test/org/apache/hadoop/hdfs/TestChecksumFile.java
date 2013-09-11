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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ClientAdapter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

public class TestChecksumFile extends junit.framework.TestCase {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short) 1;
  byte[] inBytes = "something".getBytes();
  byte[] inBytes2 = "another".getBytes();

  static void checkMetaInfo(int namespaceId, Block b, InterDatanodeProtocol idp)
      throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(namespaceId, b, idp, null);
  }

  public void testRecoverZeroChecksumFile() throws Exception {
    MiniDFSCluster cluster = createMiniCluster();
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = false;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/zeroSizeFile";
      Path filepath = new Path(filestr);
      FSDataOutputStream out = dfs.create(filepath, true);

      // force creating data pipeline
      Method nextBlockOutputStreamMethod = DFSOutputStream.class
          .getDeclaredMethod("nextBlockOutputStream", String.class);
      nextBlockOutputStreamMethod.setAccessible(true);
      DatanodeInfo[] nodes = (DatanodeInfo[]) nextBlockOutputStreamMethod
          .invoke(out.getWrappedStream(), dfs.dfs.getClientName());

      // get data node
      DataNode datanode = cluster.getDataNode(nodes[0].getIpcPort());
      assertTrue(datanode != null);

      // verifies checksum file is of length 0
      LocatedBlockWithMetaInfo locatedblock = TestInterDatanodeProtocol
          .getLastLocatedBlock(dfs.dfs.namenode, filestr);
      Block lastblock = locatedblock.getBlock();
      DataNode.LOG.info("newblocks=" + lastblock);
      BlockPathInfo blockPathInfo = datanode.getBlockPathInfo(lastblock);
      String blockPath = blockPathInfo.getBlockPath();
      String metaPath = blockPathInfo.getMetaPath();

      File f = new File(blockPath);
      File meta = new File(metaPath);
      assertEquals(0, f.length());
      // set the checksum file to 0
      meta.delete();
      DataOutputStream outs = new DataOutputStream(new FileOutputStream(
          metaPath, false));
      outs.close();

      // issue recovery and makit e sure it succeeds.
      int numTries = 500;
      for (int idxTry = 0; idxTry < numTries; idxTry++) {
        boolean success = dfs.recoverLease(filepath);
        if (success) {
          break;
        } else if (idxTry == numTries - 1) {
          TestCase.fail("Recovery lease failed");
        } else {
          Thread.sleep(10);
        }
      }

      // make sure the meta file is still empty
      locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.namenode, filestr);
      Block newBlock = locatedblock.getBlock();
      blockPathInfo = datanode.getBlockPathInfo(newBlock);
      assertEquals(0, blockPathInfo.getNumBytes());
      metaPath = blockPathInfo.getMetaPath();
      meta = new File(metaPath);
      assertEquals(0, meta.length());

      // make sure the file can be opened and read.
      InputStream in = dfs.open(new Path(filestr), 8);
      TestCase.assertEquals(-1, in.read()); // EOF
      in.close();
    } finally {
      cluster.shutdown();
    }
  }

  public void testMissingChecksumFile() throws Exception {
    MiniDFSCluster cluster = createMiniCluster();
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = false;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/testMissingChecksumFile";
      DFSTestUtil.creatFileAndWriteSomething(dfs, filestr, (short)2);

      BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr,
          cluster, dfs.dfs);
      String metaPath = blockPathInfo.getMetaPath();

      // Delete the checksum file
      File meta = new File(metaPath);
      meta.delete();

      // The file can be read
      InputStream in = dfs.open(new Path(filestr));
      try {
        TestCase.assertEquals(inBytes.length, in.read(inBytes));
        TestCase.assertEquals(0, in.available());
      } finally {
        in.close();
      }
    } finally {
      cluster.shutdown();
    }

  }

  public void testCorruptChecksumFile() throws Exception {
    MiniDFSCluster cluster = createMiniCluster();
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = false;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/testCorruptChecksumFile";
      DFSTestUtil.creatFileAndWriteSomething(dfs, filestr, (short)2);

      BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr,
          cluster, dfs.dfs);
      String metaPath = blockPathInfo.getMetaPath();

      // Populate the checksum file
      File meta = new File(metaPath);
      meta.delete();
      OutputStream metaOut = new FileOutputStream(metaPath);
      try {
        metaOut.write(inBytes);
      } finally {
        metaOut.close();
      }

      // Exception when read from the file
      InputStream in = dfs.open(new Path(filestr));
      try {
        TestCase.assertEquals(inBytes.length, in.read(inBytes));
        TestCase.fail();
      } catch (IOException e) {
      } finally {
        in.close();
      }
    } finally {
      cluster.shutdown();
    }

  }

  public void testDisableReadChecksum() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    // Not sending checksum
    conf.setBoolean("dfs.datanode.read.ignore.checksum", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = false;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/testDisableReadChecksum";
      DFSTestUtil.creatFileAndWriteSomething(dfs, filestr, (short)2);

      BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr,
          cluster, dfs.dfs);
      String metaPath = blockPathInfo.getMetaPath();

      // Pollute the checksum file
      File meta = new File(metaPath);
      meta.delete();
      OutputStream metaOut = new FileOutputStream(metaPath);
      try {
        metaOut.write(inBytes);
      } finally {
        metaOut.close();
      }

      // File can be read without any exception
      InputStream in = dfs.open(new Path(filestr));
      try {
        TestCase.assertEquals(inBytes.length, in.read(inBytes));
        TestCase.assertEquals(0, in.available());
      } finally {
        in.close();
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  public void testCorruptInlineChecksumFile() throws Exception {
    MiniDFSCluster cluster = createMiniCluster();
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = true;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/testCorruptInlineChecksumFile";
      DFSTestUtil.creatFileAndWriteSomething(dfs, filestr, (short)2);

      BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr, cluster, dfs.dfs);
      String blockPath = blockPathInfo.getBlockPath();

      // Populate the checksum file
      RandomAccessFile blockOut = new RandomAccessFile(blockPath, "rw");
      try {
        blockOut.seek(DataChecksum.getChecksumHeaderSize());
        blockOut.write(inBytes2);
      } finally {
        blockOut.close();
      }

      // Exception when read from the file
      InputStream in = dfs.open(new Path(filestr));
      try {
        TestCase.assertEquals(inBytes.length, in.read(inBytes));
        TestCase.fail();
      } catch (IOException e) {
      } finally {
        in.close();
      }
    } finally {
      cluster.shutdown();
    }

  }

  public void testDisableReadInlineChecksum() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    // Not sending checksum
    conf.setBoolean("dfs.datanode.read.ignore.checksum", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      cluster.waitActive();
      cluster.getDataNodes().get(0).useInlineChecksum = true;

      // create a file
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      String filestr = "/testDisableReadInlineChecksum";
      DFSTestUtil.creatFileAndWriteSomething(dfs, filestr, (short)2);

      BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr, cluster, dfs.dfs);
      String blockPath = blockPathInfo.getBlockPath();

      // Populate the checksum file
      RandomAccessFile blockOut = new RandomAccessFile(blockPath, "rw");
      try {
        blockOut.seek(DataChecksum.getChecksumHeaderSize());
        blockOut.write(inBytes2);
      } finally {
        blockOut.close();
      }

      // File can be read without any exception
      InputStream in = dfs.open(new Path(filestr));
      try {
        TestCase.assertEquals(inBytes.length, in.read(inBytes));
        TestCase.assertEquals(0, in.available());
      } finally {
        in.close();
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  
  private MiniDFSCluster createMiniCluster() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    return new MiniDFSCluster(conf, 1, true, null);
  }
  

}

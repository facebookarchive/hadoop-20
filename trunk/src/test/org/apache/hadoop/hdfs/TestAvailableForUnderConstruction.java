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
import java.io.InputStream;
import java.io.RandomAccessFile;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;

/** This class implements some of tests posted in HADOOP-2658. */
public class TestAvailableForUnderConstruction extends junit.framework.TestCase {
  static final long BLOCK_SIZE = 64 * 1024;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 5;

  private static Configuration conf;
  private static int buffersize;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  public static Test suite() {
    return new TestSetup(new TestSuite(TestAvailableForUnderConstruction.class)) {
      protected void setUp() throws java.lang.Exception {
        AppendTestUtil.LOG.info("setUp()");
        conf = new Configuration();
        conf.setInt("io.bytes.per.checksum", 512);
        conf.setBoolean("dfs.support.append", true);
        buffersize = conf.getInt("io.file.buffer.size", 4096);
        cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
        fs = (DistributedFileSystem) cluster.getFileSystem();
      }

      protected void tearDown() throws Exception {
        AppendTestUtil.LOG.info("tearDown()");
        if (fs != null)
          fs.close();
        if (cluster != null)
          cluster.shutdown();
      }
    };
  }

  public void testUpdateAvailable() throws Exception {
    final Path p = new Path("/TC1/foo");
    System.out.println("p=" + p);

    final int len1 = (int) BLOCK_SIZE / 4 - 3;
    FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
        BLOCK_SIZE);
    FSDataInputStream in = fs.open(p);
    int available;

    available = in.available();
    TestCase.assertEquals(0, available);

    AppendTestUtil.write(out, 0, len1);
    out.sync();

    available = in.available();
    TestCase.assertEquals(len1, available);
    
    long i = -1;
    for(i++; i < len1; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }

    available = in.available();
    TestCase.assertEquals(0, available);

    final int len2 = (int) BLOCK_SIZE / 2 + 3;
    AppendTestUtil.write(out, len1, len2);
    out.sync();
    available = in.available();
    TestCase.assertEquals(len2, available);
    for(; i < len1 + len2; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    // test available update until end of the block
    final int len3 = (int) BLOCK_SIZE / 2;
    AppendTestUtil.write(out, len1 + len2, len3);
    out.sync();
    available = in.available();
    TestCase.assertEquals((int) BLOCK_SIZE / 2, available);
    for(; i < len1 + len2 + len3; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    final int len4 = (int)BLOCK_SIZE / 4 - 7;
    AppendTestUtil.write(out, len1 + len2 + len3, len4);
    out.sync();
    available = in.available();
    for(; i < len1 + len2 + len3 + len4; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    
    final int len5 = 2;
    AppendTestUtil.write(out, len1 + len2 + len3 + len4, len5);
    TestCase.assertTrue(in.isUnderConstruction());

    out.sync();

    available = in.available();
    TestCase.assertEquals(len5, available);
    for(; i < len1 + len2 + len3 + len4 + len5; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    out.close();

    available = in.available();
    TestCase.assertEquals(0, available);
    TestCase.assertFalse(in.isUnderConstruction());
    
    in.close();

    // b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2 + len3 + len4 + len5);
  }
}

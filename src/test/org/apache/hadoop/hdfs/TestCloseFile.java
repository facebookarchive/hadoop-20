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

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestCloseFile {
  private MiniDFSCluster cluster;
  private Configuration conf;
  private static int BLOCK_SIZE = 1024;
  private static int BLOCKS = 20;
  private static int FILE_LEN = BLOCK_SIZE * BLOCKS;
  private FileSystem fs;
  private static Random random = new Random();
  private static volatile boolean pass = true;
  private static Log LOG = LogFactory.getLog(TestCloseFile.class);
  private static int CLOSE_FILE_TIMEOUT = 20000;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean("dfs.permissions", false);
    conf.setInt("dfs.client.closefile.timeout", CLOSE_FILE_TIMEOUT);
    conf.setInt("dfs.close.replication.min", 2);
    conf.setInt("dfs.replication.pending.timeout.sec", 1);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCloseReplicationWithNoInValidReplicas() throws Exception {
    testCloseReplication(false);
  }
  
  @Test
  public void testCloseReplicationWithOneInValidReplicas() throws Exception {
    testCloseReplication(true);
  }
  
  /**
   * Test that close.replication.min of 2 works
   * @throws Exception
   */
  private void testCloseReplication(boolean restartDN)
  throws Exception {
    String file = "/testRestartDatanodeNode";

    // Create a file with a replication factor of 2 and write data.
    final FSDataOutputStream out = fs.create(new Path(file), (short)2);
    byte[] buffer = new byte[FILE_LEN];
    random.nextBytes(buffer);
    out.write(buffer);
    ((DFSOutputStream) out.getWrappedStream()).sync();

    // take down one datanode in the pipeline
    DatanodeInfo[] locs = cluster.getNameNode().getBlockLocations(
        file, 0, FILE_LEN).getLocatedBlocks().get(0).getLocations();
    assertEquals(2, locs.length);
    DatanodeInfo loc = locs[0];
    DataNodeProperties dnprop = cluster.stopDataNode(loc.getName());
    cluster.getNameNode().namesystem.removeDatanode(loc);
    out.write(buffer);
    ((DFSOutputStream) out.getWrappedStream()).sync();
    // the pipeline length drops to 1
    assertEquals(1, (((DFSOutputStream) out.getWrappedStream()).getNumCurrentReplicas()));
    
    if (restartDN) {
      // restart introduces an invalid replica
      cluster.restartDataNode(dnprop);
      // the number of targets becomes 2 with 1 invalid replica
      locs = cluster.getNameNode().getBlockLocations(
          file, 0, FILE_LEN).getLocatedBlocks().get(0).getLocations();
      assertEquals(2, locs.length);
    }
    
    //  now close the file
    Thread closeThread = new Thread() {
      public void run() {
        try {
          out.close();
          pass = true;
          LOG.info("File closed");
        } catch (Exception e) {
          pass = false;
        }
      }
    };
    closeThread.run();
    
    // wait until the last block is replicated and the file is closed
    closeThread.join(CLOSE_FILE_TIMEOUT);
    assertTrue(pass);
  }
  
  public void testRestartNameNode(boolean waitSafeMode) throws Exception {
    String file = "/testRestartNameNode" + waitSafeMode;

    // Create a file and write data.
    FSDataOutputStream out = fs.create(new Path(file));
    String clientName = ((DistributedFileSystem) fs).getClient().getClientName();
    byte[] buffer = new byte[FILE_LEN];
    random.nextBytes(buffer);
    out.write(buffer);
    ((DFSOutputStream) out.getWrappedStream()).sync();

    // Now shutdown the namenode and try to close the file.
    cluster.shutdownNameNode(0);
    Thread closeThread = new CloseThread(out, file, clientName);
    closeThread.start();
    Thread.sleep(CLOSE_FILE_TIMEOUT / 4);

    // Restart the namenode and verify the close file worked.
    if (!waitSafeMode) {
      cluster.restartNameNode(0, new String[]{}, false);
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } else {
      cluster.restartNameNode(0);
    }
    closeThread.join(5000);
    assertTrue(pass);
  }

  @Test
  public void testRestartNameNodeWithSafeMode() throws Exception {
    testRestartNameNode(true);
  }

  @Test
  public void testRestartNameNodeWithoutSafeMode() throws Exception {
    testRestartNameNode(false);
  }

  private class CloseThread extends Thread {
    private final FSDataOutputStream out;
    private final String file;
    private final String clientName;

    public CloseThread(FSDataOutputStream out, String file, String clientName) {
      this.out = out;
      this.file = file;
      this.clientName = clientName;
    }

    public void run() {
      try {
        out.close();
      } catch (Exception e) {
        LOG.warn("Close failed", e);
        while (true) {
          try {
            Thread.sleep(1000);
            cluster.getNameNode().recoverLease(file, clientName);
            break;
          } catch (SafeModeException se) {
            LOG.warn("Retrying lease recovery for failed close", se);
          } catch (Exception ee) {
            LOG.warn("Lease recovery failed", ee);
            pass = false;
          }
        }
      }
    }
  }
}


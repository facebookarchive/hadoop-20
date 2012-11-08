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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;


/**
 * This class tests the listCorruptFileBlocks API.
 * We create 3 files; intentionally delete their blocks
 * Use listCorruptFileBlocks to validate that we get the list of corrupt
 * files/blocks; also test the "paging" support by calling the API
 * with a block # from a previous call and validate that the subsequent
 * blocks/files are also returned.
 */
public class TestListCorruptFileBlocks {
  static Log LOG = LogFactory.getLog(TestListCorruptFileBlocks.class);

  @Test
  public void testListCorruptFilesCorruptedBlock() throws Exception {
    testListCorruptFilesCorruptedBlockInternal(false);
  }

  @Test
  public void testListCorruptFilesCorruptedBlockInlineChecksum() throws Exception {
    testListCorruptFilesCorruptedBlockInternal(true);
  }
  
  private boolean shouldSelectFile(String fileName) {
    return fileName.startsWith("blk_") && fileName.indexOf("_", "blk_".length()) > 0;
  }
  
  /** check if nn.getCorruptFiles() returns a file that has corrupted blocks */
  private void testListCorruptFilesCorruptedBlockInternal(boolean inlineChecksum)
      throws Exception {
    MiniDFSCluster cluster = null;
    Random random = new Random();
    
    try {
      Configuration conf = new Configuration();
      conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans directories
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000); // datanode sends block reports
      conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil("testCorruptFilesCorruptedBlock", 2, 1, 512);
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.
        getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Namenode has " + badFiles.size()
          + " corrupt files. Expecting None.", badFiles.size() == 0);

      // Now deliberately corrupt one block
      File data_dir = cluster.getBlockDirectory("data1"); 
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (shouldSelectFile(blocks[idx].getName())) {
          //
          // shorten .meta file
          //
          RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
          FileChannel channel = file.getChannel();
          long position = channel.size() - 2;
          int length = 2;
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          channel.write(ByteBuffer.wrap(buffer), position);
          file.close();
          LOG.info("Deliberately corrupting file " + blocks[idx].getName() +
              " at offset " + position + " length " + length);

          // read all files to trigger detection of corrupted replica
          try {
            util.checkFiles(fs, "/srcdat10");
            // we should get a ChecksumException or a BlockMissingException
          } catch (BlockMissingException e) {
            System.out.println("Received BlockMissingException as expected.");
          } catch (ChecksumException e) {
            System.out.println("Received ChecksumException as expected.");
          } catch (IOException e) {
            assertTrue("Corrupted replicas not handled properly. Expecting BlockMissingException " +
                " but received IOException " + e, false);
          }
          break;
        }
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = namenode.getNamesystem().listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);
      util.cleanup(fs, "/srcdat10");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testListCorruptFileBlocksInSafeMode() throws Exception {
    testListCorruptFileBlocksInSafeModeInternal(false);
  }

  @Test
  public void testListCorruptFileBlocksInSafeModeInlineChecksum()
      throws Exception {
    testListCorruptFileBlocksInSafeModeInternal(true);
  }

  
  /**
   * Check that listCorruptFileBlocks works while the namenode is still in
   * safemode.
   */
  private void testListCorruptFileBlocksInSafeModeInternal(
      boolean inlineChecksum) throws Exception {
    MiniDFSCluster cluster = null;
    Random random = new Random();

    try {
      Configuration conf = new Configuration();
      // datanode scans directories
      conf.setInt("dfs.datanode.directoryscan.interval", 1);
      // datanode sends block reports
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000);
      // do not maintain access times
      conf.setLong("dfs.access.time.precision", 0);
      // never leave safemode automatically
      conf.setFloat("dfs.safemode.threshold.pct",
                    1.5f);
      // start populating repl queues immediately 
      conf.setFloat("dfs.namenode.replqueue.threshold-pct",
                    0f);
      conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
      cluster = new MiniDFSCluster(conf, 1, true, null, false);
      cluster.getNameNode().
        setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil("testListCorruptFileBlocksInSafeMode",
                                         2, 1, 512);
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = 
        cluster.getNameNode().getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Namenode has " + badFiles.size()
          + " corrupt files. Expecting None.", badFiles.size() == 0);

      // Now deliberately corrupt one block
      File data_dir = cluster.getBlockDirectory("data1"); 
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
                 (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (shouldSelectFile(blocks[idx].getName())) {
          //
          // shorten .meta file
          //
          RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
          FileChannel channel = file.getChannel();
          long position = channel.size() - 2;
          int length = 2;
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          channel.write(ByteBuffer.wrap(buffer), position);
          file.close();
          LOG.info("Deliberately corrupting file " + blocks[idx].getName() +
              " at offset " + position + " length " + length);

          // read all files to trigger detection of corrupted replica
          try {
            util.checkFiles(fs, "/srcdat10");
          } catch (BlockMissingException e) {
            System.out.println("Received BlockMissingException as expected.");
          } catch (ChecksumException e) {
            System.out.println("Received ChecksumException as expected.");
          } catch (IOException e) {
            assertTrue("Corrupted replicas not handled properly. " +
                       "Expecting BlockMissingException " +
                       " but received IOException " + e, false);
          }
          break;
        }
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = cluster.getNameNode().getNamesystem().
        listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);
 
      // restart namenode
      cluster.restartNameNode(0);
      fs = cluster.getFileSystem();

      // wait until replication queues have been initialized
      while (!cluster.getNameNode().namesystem.isPopulatingReplQueues()) {
        try {
          LOG.info("waiting for replication queues");
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }
      }

      
      // read all files to trigger detection of corrupted replica
      try {
        util.checkFiles(fs, "/srcdat10");
      } catch (BlockMissingException e) {
        System.out.println("Received BlockMissingException as expected.");
      } catch (ChecksumException e) {
        System.out.println("Received ChecksumException as expected.");
      } catch (IOException e) {
        assertTrue("Corrupted replicas not handled properly. " +
                   "Expecting BlockMissingException " +
                   " but received IOException " + e, false);
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = cluster.getNameNode().getNamesystem().
        listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);

      // check that we are still in safe mode
      assertTrue("Namenode is not in safe mode", 
                 cluster.getNameNode().isInSafeMode());

      // now leave safe mode so that we can clean up
      cluster.getNameNode().
        setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);

      util.cleanup(fs, "/srcdat10");
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cluster != null) {
        cluster.shutdown(); 
      }
    }
  }

  @Test
  public void testListCorruptFileBlocksInSafeModeNotPopulated() throws Exception {
    testListCorruptFileBlocksInSafeModeNotPopulatedInternal(false);
  }

  @Test
  public void testListCorruptFileBlocksInSafeModeNotPopulatedInlineChecksum()
      throws Exception {
    testListCorruptFileBlocksInSafeModeNotPopulatedInternal(true);
  }
  
  /**
   * Check that listCorruptFileBlocks throws an exception if we try to run it
   * before the underreplicated block queues have been populated.
   */
  private void testListCorruptFileBlocksInSafeModeNotPopulatedInternal(
      boolean inlineChecksum) throws Exception {
    MiniDFSCluster cluster = null;
    Random random = new Random();

    try {
      Configuration conf = new Configuration();
      // datanode scans directories
      conf.setInt("dfs.datanode.directoryscan.interval", 1);
      // datanode sends block reports
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000);
      // do not maintain access times
      conf.setLong("dfs.access.time.precision", 0);
      // never leave safemode automatically
      conf.setFloat("dfs.safemode.threshold.pct",
                    10f);
      conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
      // never populate repl queues
      // conf.setFloat("dfs.namenode.replqueue.threshold-pct",
      //              1.5f);
      cluster = new MiniDFSCluster(conf, 1, true, null, false);
      cluster.getNameNode().
        setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util =
        new DFSTestUtil("testListCorruptFileBlocksInSafeModeNotPopulated",
                        2, 1, 512);
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = 
        cluster.getNameNode().getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Namenode has " + badFiles.size()
          + " corrupt files. Expecting None.", badFiles.size() == 0);

      // Now deliberately corrupt one block
      File data_dir = cluster.getBlockDirectory("data1");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
                 (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (shouldSelectFile(blocks[idx].getName())) {
          //
          // shorten .meta file
          //
          RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
          FileChannel channel = file.getChannel();
          long position = channel.size() - 2;
          int length = 2;
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          channel.write(ByteBuffer.wrap(buffer), position);
          file.close();
          LOG.info("Deliberately corrupting file " + blocks[idx].getName() +
              " at offset " + position + " length " + length);

          // read all files to trigger detection of corrupted replica
          try {
            util.checkFiles(fs, "/srcdat10");
          } catch (BlockMissingException e) {
            System.out.println("Received BlockMissingException as expected.");
          } catch (ChecksumException e) {
            System.out.println("Received ChecksumException as expected.");
          } catch (IOException e) {
            assertTrue("Corrupted replicas not handled properly. " +
                       "Expecting BlockMissingException " +
                       " but received IOException " + e, false);
          }
          break;
        }
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = cluster.getNameNode().getNamesystem().
        listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);
 
      // restart namenode
      cluster.restartNameNode(0);
      fs = cluster.getFileSystem();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }

      // read all files to trigger detection of corrupted replica
      try {
        util.checkFiles(fs, "/srcdat10");
      } catch (BlockMissingException e) {
        System.out.println("Received BlockMissingException as expected.");
      } catch (ChecksumException e) {
        System.out.println("Received ChecksumException as expected.");
      } catch (IOException e) {
        assertTrue("Corrupted replicas not handled properly. " +
                   "Expecting BlockMissingException " +
                   " but received IOException " + e, false);
      }

      // try to get corrupt files
      // this should cause an exception because the repl queues have not
      // been initialized
      try {
        cluster.getNameNode().getNamesystem().
          listCorruptFileBlocks("/", null);
        fail("Expected IOException.");
      } catch (IOException e) {
        assertTrue("Received wrong type of IOException",
                   e.getMessage().
                   equals("Cannot run listCorruptFileBlocks because " +
                          "replication queues have not been initialized."));
        LOG.info("Received IOException as expected.");
      }

      // check that we are still in safe mode
      assertTrue("Namenode is not in safe mode", 
                 cluster.getNameNode().isInSafeMode());
      // check that repl queues have not been initialized
      assertFalse("Namenode has initialized replication queues", 
                 cluster.getNameNode().namesystem.isPopulatingReplQueues());

      // now leave safe mode so that we can clean up
      cluster.getNameNode().
        setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);

      util.cleanup(fs, "/srcdat10");
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cluster != null) {
        cluster.shutdown(); 
      }
    }
  }
  
  /**
   * deliberately remove blocks from a file and validate the list-corrupt-file-blocks API
   */
  @Test
  public void testListCorruptFileBlocks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000);
    conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans
                                                           // directories
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil util = new DFSTestUtil("testGetCorruptFiles", 3, 1, 1024);
      util.createFiles(fs, "/corruptData");

      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks =
        namenode.getNamesystem().listCorruptFileBlocks("/corruptData", null);
      int numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      // delete the blocks
      for (int i = 0; i < 8; i++) {
        File data_dir = cluster.getBlockDirectory("data" + (i + 1));
        File[] blocks = data_dir.listFiles();
        if (blocks == null)
          continue;
        // assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
        // (blocks.length > 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          if (!blocks[idx].getName().startsWith("blk_")) {
            continue;
          }
          LOG.info("Deliberately removing file " + blocks[idx].getName());
          assertTrue("Cannot remove file.", blocks[idx].delete());
          // break;
        }
      }

      int count = 0;
      corruptFileBlocks = namenode.getNamesystem().
        listCorruptFileBlocks("/corruptData", null);
      numCorrupt = corruptFileBlocks.size();
      while (numCorrupt < 3) {
        Thread.sleep(1000);
        corruptFileBlocks = namenode.getNamesystem().
            listCorruptFileBlocks("/corruptData", null);
        numCorrupt = corruptFileBlocks.size();
        count++;
        if (count > 30)
          break;
      }
      // Validate we get all the corrupt files
      LOG.info("Namenode has bad files. " + numCorrupt);
      assertTrue(numCorrupt == 3);
      // test the paging here

      FSNamesystem.CorruptFileBlockInfo[] cfb = corruptFileBlocks
          .toArray(new FSNamesystem.CorruptFileBlockInfo[0]);
      // now get the 2nd and 3rd file that is corrupt
      String[] cookie = new String[]{"1"};
      Collection<FSNamesystem.CorruptFileBlockInfo> nextCorruptFileBlocks = 
        namenode.getNamesystem().
        listCorruptFileBlocks("/corruptData", cookie);
      FSNamesystem.CorruptFileBlockInfo[] ncfb = nextCorruptFileBlocks
          .toArray(new FSNamesystem.CorruptFileBlockInfo[0]);
      numCorrupt = nextCorruptFileBlocks.size();
      assertTrue(numCorrupt == 2);
      assertTrue(ncfb[0].block.getBlockName()
          .equalsIgnoreCase(cfb[1].block.getBlockName()));

      corruptFileBlocks = namenode.getNamesystem().
        listCorruptFileBlocks("/corruptData", cookie);
      numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      // Do a listing on a dir which doesn't have any corrupt blocks and
      // validate
      util.createFiles(fs, "/goodData");
      corruptFileBlocks = namenode.getNamesystem().
        listCorruptFileBlocks("/goodData", null);
      numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      util.cleanup(fs, "/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static int countPaths(RemoteIterator<Path> iter) throws IOException {
    int i = 0;
    while (iter.hasNext()) {
      LOG.info("PATH: " + iter.next().toUri().getPath());
      i++;
    }
    return i;
  }

  /**
   * test listCorruptFileBlocks in DistributedFileSystem
   */
  @Test
  public void testListCorruptFileBlocksDFS() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000);
    conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans
                                                           // directories
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      DFSTestUtil util = new DFSTestUtil("testGetCorruptFiles", 3, 1, 1024);
      util.createFiles(fs, "/corruptData");

      final NameNode namenode = cluster.getNameNode();
      RemoteIterator<Path> corruptFileBlocks = 
        dfs.listCorruptFileBlocks(new Path("/corruptData"));
      int numCorrupt = countPaths(corruptFileBlocks);
      assertTrue(numCorrupt == 0);
      // delete the blocks
      for (int i = 0; i < 8; i++) {
        File data_dir = cluster.getBlockDirectory("data" + (i + 1));
        File[] blocks = data_dir.listFiles();
        if (blocks == null)
          continue;
        // assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
        // (blocks.length > 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          if (!blocks[idx].getName().startsWith("blk_")) {
            continue;
          }
          LOG.info("Deliberately removing file " + blocks[idx].getName());
          assertTrue("Cannot remove file.", blocks[idx].delete());
          // break;
        }
      }

      int count = 0;
      corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("/corruptData"));
      numCorrupt = countPaths(corruptFileBlocks);
      while (numCorrupt < 3) {
        Thread.sleep(1000);
        corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("/corruptData"));
        numCorrupt = countPaths(corruptFileBlocks);
        count++;
        if (count > 30)
          break;
      }
      // Validate we get all the corrupt files
      LOG.info("Namenode has bad files. " + numCorrupt);
      assertTrue(numCorrupt == 3);

      util.cleanup(fs, "/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
    
  /**
   * Test if NN.listCorruptFiles() returns the right number of results.
   * Also, test that DFS.listCorruptFileBlocks can make multiple successive
   * calls.
   */
  @Test
  public void testMaxCorruptFiles() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setInt("dfs.datanode.directoryscan.interval", 15); // datanode scans directories
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000); // datanode sends block reports
      final int maxCorruptFileBlocks = 20;
      conf.setInt("dfs.corruptfilesreturned.max", maxCorruptFileBlocks);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = cluster.getFileSystem();

      // create maxCorruptFileBlocks * 3 files with one block each
      DFSTestUtil util = new DFSTestUtil("testMaxCorruptFiles", 
          maxCorruptFileBlocks * 3, 1, 512);
      util.createFiles(fs, "/srcdat2", (short) 1);
      util.waitReplication(fs, "/srcdat2", (short) 1);

      // verify that there are no bad blocks.
      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.
        getNamesystem().listCorruptFileBlocks("/srcdat2", null);
      assertTrue("Namenode has " + badFiles.size() + " corrupt files. Expecting none.",
          badFiles.size() == 0);

      // Now deliberately remove blocks from all files
      for (int i=0; i<8; i++) {
        File data_dir = cluster.getBlockDirectory("data" +(i+1));
        File[] blocks = data_dir.listFiles();
        if (blocks == null)
          continue;

        for (int idx = 0; idx < blocks.length; idx++) {
          if (!blocks[idx].getName().startsWith("blk_")) {
            continue;
          }
          assertTrue("Cannot remove file.", blocks[idx].delete());
        }
      }

      badFiles = namenode.getNamesystem().
        listCorruptFileBlocks("/srcdat2", null);
        
       while (badFiles.size() < maxCorruptFileBlocks) {
        LOG.info("# of corrupt files is: " + badFiles.size());
        Thread.sleep(10000);
        badFiles = namenode.getNamesystem().
          listCorruptFileBlocks("/srcdat2", null);
      }
       badFiles = namenode.getNamesystem().
         listCorruptFileBlocks("/srcdat2", null); 
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting " + 
          maxCorruptFileBlocks + ".",
          badFiles.size() == maxCorruptFileBlocks);

      CorruptFileBlockIterator iter = (CorruptFileBlockIterator)
        fs.listCorruptFileBlocks(new Path("/srcdat2"));
      int corruptPaths = countPaths(iter);
      assertTrue("Expected more than " + maxCorruptFileBlocks +
                 " corrupt file blocks but got " + corruptPaths,
                 corruptPaths > maxCorruptFileBlocks);
      assertTrue("Iterator should have made more than 1 call but made " +
                 iter.getCallsMade(),
                 iter.getCallsMade() > 1);

      util.cleanup(fs, "/srcdat2");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

}

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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Exchanger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestDirectoryScannerDelta {
  final static Log LOG = LogFactory.getLog(TestDirectoryScannerDelta.class);

  private static final int REMOVE_BLOCK_FILES = 17;
  private static final int REMOVE_META_FILES = 23;
  private static final int REMOVE_BOTH_FILES = 19;
  private static final int REMOVE_FROM_VOLUME_MAP = 27;

  class FileAndBlockId {
    final Block block;
    final String fileName;
    final long originalGenStamp;

    public FileAndBlockId(Block blk, String fname) {
      this.block = blk;
      this.fileName = fname;
      this.originalGenStamp = -1;
    }

    public FileAndBlockId(Block blk, String fname, long gs) {
      this.block = blk;
      this.fileName = fname;
      this.originalGenStamp = gs;
    }
  }

  class ParallelInjectionHandler extends InjectionHandler {
    private boolean firstRunLoop = false;
    private final Set<InjectionEvent> events;

    public ParallelInjectionHandler(InjectionEvent... events) {
      this.events = EnumSet.copyOf(Arrays.asList(events));
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED) {
        firstRunLoop = true;
      }
      if (!firstRunLoop) {
        return;
      }

      if (events.contains(event)) {
        startParallelInjection((InjectionEvent) event);
        stopParallelInjection((InjectionEvent) event);
      }
    }
  }

  MiniDFSCluster cluster;
  int nsid;
  DataNode dn;
  DirectoryScanner scanner;
  FSDatasetDelta delta;
  FSDataset data;
  FileSystem fs;
  static volatile int filesCreated = 0;

  static Exchanger<InjectionEventI> exchanger = new Exchanger<InjectionEventI>();

  static void startParallelInjection(InjectionEventI evt) {
    try {
      assertTrue(exchanger.exchange(evt).equals(evt));
      LOG.info(evt.toString() + " start");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  static void stopParallelInjection(InjectionEvent evt) {
    try {
      assertTrue(exchanger.exchange(evt).equals(evt));
      LOG.info(evt.toString() + " stop");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void createFile(FileSystem fs, String fileName)
      throws IOException {
    Path file = new Path(fs.getHomeDirectory(), fileName);
    FSDataOutputStream output = fs.create(file);
    output.writeChars(fileName);
    output.close();
  }

  private static void removeFile(FileSystem fs, String fileName)
      throws IOException {
    Path file = new Path(fs.getHomeDirectory(), fileName);
    fs.delete(file, false);
  }

  private static String firstLine(File f) throws IOException {
    BufferedReader bf = null;
    try {
      bf = new BufferedReader(new FileReader(f));
      return bf.readLine();
    } finally {
      if (bf != null) {
        bf.close();
      }
    }
  }

  private void setUp(boolean inlineChecksums) {
    LOG.info("setting up!");
    exchanger = new Exchanger<InjectionEventI>();
    InjectionHandler.clear();
    Configuration CONF = new Configuration();
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setInt("dfs.datanode.directoryscan.interval", 1);
    CONF.setBoolean("dfs.use.inline.checksum", inlineChecksums);

    try {
      cluster = new MiniDFSCluster(CONF, 1, true, null);
      cluster.waitActive();

      dn = cluster.getDataNodes().get(0);
      nsid = dn.getAllNamespaces()[0];
      scanner = dn.directoryScanner;
      data = (FSDataset) dn.data;
      Field f = DirectoryScanner.class.getDeclaredField("delta");
      f.setAccessible(true);
      delta = (FSDatasetDelta) f.get(scanner);

      fs = cluster.getFileSystem();
    } catch (Exception e) {
      e.printStackTrace();
      fail("setup failed");
    }
  }

  @After
  public void tearDown() {
    LOG.info("Tearing down");
    InjectionHandler.clear();
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Tests if delta gets updates during scan and doesn't get any during all
   * other time
   * */
  @Test
  public void testDeltaLockingAndReleasing() throws Exception {
    testDeltaLockingAndReleasing(false);
    testDeltaLockingAndReleasing(true);
  }

  private void testDeltaLockingAndReleasing(boolean ic) throws Exception {
    setUp(ic);
    try {
      InjectionHandler.set(new ParallelInjectionHandler(
          InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED,
          InjectionEvent.DIRECTORY_SCANNER_BEFORE_FILE_SCAN,
          InjectionEvent.DIRECTORY_SCANNER_FINISHED));

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);
      createFile(fs, "file1.txt");
      assertEquals(0, delta.size(nsid));
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_BEFORE_FILE_SCAN);
      createFile(fs, "file2.txt");
      assertEquals(1, delta.size(nsid));
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_BEFORE_FILE_SCAN);

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
      createFile(fs, "file3.txt");
      assertEquals(1, delta.size(nsid));
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @Test
  public void testBlocksInMemoryOnly() throws Exception {
    testBlocksInMemoryOnly(false);
    testBlocksInMemoryOnly(true);
  }

  private void testBlocksInMemoryOnly(boolean ic) throws Exception {
    setUp(ic);
    try {
      InjectionHandler.set(new ParallelInjectionHandler(
          InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED,
          InjectionEvent.DIRECTORY_SCANNER_FINISHED));
      // let's make a bunch of files here
      for (int i = 0; i < 100; i++) {
        createFile(fs, "file" + i);
      }
      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);
      FSDataset fds = (FSDataset) dn.data;
      List<Block> blocksToBeRemoved = new LinkedList<Block>();
      for (DatanodeBlockInfo bi : getBlockInfos(fds, nsid)) {
        blocksToBeRemoved.add(bi.getBlock());
        assertTrue(bi.getBlockDataFile().getFile().delete());
        // for inline files it does not exist
        BlockWithChecksumFileWriter.getMetaFile(
            bi.getBlockDataFile().getFile(), bi.getBlock()).delete();
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
      for (Block b : blocksToBeRemoved) {
        assertNull(fds.volumeMap.get(nsid, b));
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  static List<Block> getBlocks(FSDataset fds, int nsid) {
    List<Block> blocks = new ArrayList<Block>();
    for (DatanodeBlockInfo dbi : getBlockInfos(fds, nsid)) {
      blocks.add(dbi.getBlock());
    }
    return blocks;
  }

  static LinkedList<DatanodeBlockInfo> getBlockInfos(FSDataset fds, int nsid) {
    NamespaceMap nm = fds.volumeMap.getNamespaceMap(nsid);
    int numBuckets = nm.getNumBucket();
    LinkedList<DatanodeBlockInfo> blocks = new LinkedList<DatanodeBlockInfo>();
    for (int i = 0; i < numBuckets; i++) {
      blocks.addAll(nm.getBucket(i).getBlockInfosForTesting());
    }
    return blocks;
  }

  @Test
  public void testBlocksOnDiskOnly() throws Exception {
    testBlocksOnDiskOnly(false);
    testBlocksOnDiskOnly(true);
  }

  private void testBlocksOnDiskOnly(boolean ic) throws Exception {
    setUp(ic);
    try {
      InjectionHandler.set(new ParallelInjectionHandler(
          InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED,
          InjectionEvent.DIRECTORY_SCANNER_FINISHED));
      // let's make a bunch of files here
      for (int i = 0; i < 100; i++) {
        createFile(fs, "file" + i);
      }
      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);
      FSDataset fds = (FSDataset) dn.data;
      List<Block> blocksToBeAdded = new LinkedList<Block>();
      for (Block b : getBlocks(fds, nsid)) {
        blocksToBeAdded.add(b);
      }
      for (Block b : blocksToBeAdded) {
        fds.volumeMap.remove(nsid, b);
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
      for (Block b : blocksToBeAdded) {
        assertNotNull(fds.volumeMap.get(nsid, b));
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @Test
  public void testDeltaBehaviour() throws Exception {
    testDeltaBehaviour(false);
    testDeltaBehaviour(true);
  }

  private void incInlineFileGenStamp(File f) {
    File parent = f.getParentFile();
    String name = f.getName();
    String[] parts = name.split("_");
    assertEquals(6, parts.length);
    long newStamp = Long.parseLong(parts[2]) + 1;
    parts[2] = "" + newStamp;
    String newName = parts[0];
    for (int i = 1; i < 6; i++) {
      newName += "_" + parts[i];
    }
    f.renameTo(new File(parent, newName));
  }

  private void testDeltaBehaviour(boolean ic) throws Exception {
    setUp(ic);
    try {
      InjectionHandler.set(new ParallelInjectionHandler(
          InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED,
          InjectionEvent.DIRECTORY_SCANNER_AFTER_FILE_SCAN,
          InjectionEvent.DIRECTORY_SCANNER_AFTER_DIFF,
          InjectionEvent.DIRECTORY_SCANNER_FINISHED));
      // let's make a bunch of files here
      for (int i = 0; i < 100; i++) {
        createFile(fs, "file" + i);
      }
      FSDataset fds = (FSDataset) dn.data;
      LinkedList<DatanodeBlockInfo> blockInfos = getBlockInfos(fds, nsid);

      // now lets corrupt some files
      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);
      LinkedList<FileAndBlockId> blocksToBeRemoved = new LinkedList<FileAndBlockId>();
      for (int i = 0; i < REMOVE_BLOCK_FILES; i++) {
        DatanodeBlockInfo bi = blockInfos.removeFirst();
        String fileName = firstLine(bi.getBlockDataFile().getFile());
        blocksToBeRemoved.add(new FileAndBlockId(bi.getBlock(), fileName));
        assertTrue(bi.getBlockDataFile().getFile().delete());
      }

      for (int i = 0; i < REMOVE_BOTH_FILES; i++) {
        DatanodeBlockInfo bi = blockInfos.removeFirst();
        String fileName = firstLine(bi.getBlockDataFile().getFile());
        blocksToBeRemoved.add(new FileAndBlockId(bi.getBlock(), fileName));
        assertTrue(bi.getBlockDataFile().getFile().delete());
        if (!ic) {
          assertTrue(BlockWithChecksumFileWriter.getMetaFile(
              bi.getBlockDataFile().getFile(), bi.getBlock()).delete());
        }
      }

      LinkedList<FileAndBlockId> blocksToBeUpdated = new LinkedList<FileAndBlockId>();
      for (int i = 0; i < REMOVE_META_FILES; i++) {
        DatanodeBlockInfo bi = blockInfos.removeFirst();
        String fileName = firstLine(bi.getBlockDataFile().getFile());
        blocksToBeUpdated.add(new FileAndBlockId(bi.getBlock(), fileName, bi
            .getBlock().getGenerationStamp()));
        if (!ic) {
          assertTrue(BlockWithChecksumFileWriter.getMetaFile(
              bi.getBlockDataFile().getFile(), bi.getBlock()).delete());
        } else {
          incInlineFileGenStamp(bi.getBlockDataFile().getFile());
        }
      }

      LinkedList<FileAndBlockId> blocksToBeAdded = new LinkedList<FileAndBlockId>();
      for (int i = 0; i < REMOVE_FROM_VOLUME_MAP; i++) {
        DatanodeBlockInfo bi = blockInfos.removeFirst();
        String fileName = firstLine(bi.getBlockDataFile().getFile());
        blocksToBeAdded.add(new FileAndBlockId(bi.getBlock(), fileName));
        fds.volumeMap.remove(nsid, bi.getBlock());
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_NOT_STARTED);

      // Now messing up with delta a bit
      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_AFTER_FILE_SCAN);
      messWithDelta(blocksToBeRemoved, blocksToBeUpdated, blocksToBeAdded);
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_AFTER_FILE_SCAN);

      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_AFTER_DIFF);
      messWithDelta(blocksToBeRemoved, blocksToBeUpdated, blocksToBeAdded);
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_AFTER_DIFF);

      // Checking results
      startParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
      for (FileAndBlockId f : blocksToBeAdded) {
        assertNotNull(fds.volumeMap.get(nsid, f.block));
      }
      for (FileAndBlockId f : blocksToBeRemoved) {
        assertNull(fds.volumeMap.get(nsid, f.block));
      }
      if (!ic) {
        // for inline checksums, the generation stamp is in the datafilename
        for (FileAndBlockId f : blocksToBeUpdated) {
          assertEquals(Block.GRANDFATHER_GENERATION_STAMP,
              fds.volumeMap.get(nsid, f.block).getBlock().getGenerationStamp());
        }
      } else {
        for (FileAndBlockId f : blocksToBeUpdated) {
          assertEquals(f.originalGenStamp + 1, fds.volumeMap.get(nsid, f.block)
              .getBlock().getGenerationStamp());
        }
      }
      stopParallelInjection(InjectionEvent.DIRECTORY_SCANNER_FINISHED);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
      InjectionHandler.clear();
    }
  }

  private void messWithDelta(LinkedList<FileAndBlockId> blocksToBeRemoved,
      LinkedList<FileAndBlockId> blocksToBeUpdated,
      LinkedList<FileAndBlockId> blocksToBeAdded) throws IOException {
    for (int i = 0, n = blocksToBeAdded.size() / 4; i < n; i++) {
      FileAndBlockId f = blocksToBeAdded.removeFirst();
      if (i % 2 == 0) {
        delta.addBlock(nsid, f.block);
      } else {
        delta.removeBlock(nsid, f.block);
      }
    }

    for (int i = 0, n = blocksToBeRemoved.size() / 4; i < n; i++) {
      FileAndBlockId f = blocksToBeAdded.removeFirst();
      if (i % 2 == 0) {
        delta.addBlock(nsid, f.block);
      } else {
        delta.removeBlock(nsid, f.block);
      }
    }

    for (int i = 0, n = blocksToBeUpdated.size() / 4; i < n; i++) {
      FileAndBlockId f = blocksToBeUpdated.removeFirst();
      removeFile(fs, f.fileName);
    }
  }
}

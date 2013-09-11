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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumReader.GenStampAndChecksum;
import org.junit.Test;

public class TestDatanodeFilesFormat {

  public static final Log LOG = LogFactory
      .getLog(TestDatanodeFilesFormat.class);

  // blk_1000 - old format block
  final static String bname = Block.BLOCK_FILE_PREFIX + "1000";
  // blk_1000_2000.meta
  final static String cname = Block.BLOCK_FILE_PREFIX + "1000_2000.meta";
  // blk_1000_2000_2_1_512
  final static String iname = Block.BLOCK_FILE_PREFIX + "1000_2000_2_1_512";

  @Test
  public void testSeparateFiles() {
    String[] filenames = new String[2];

    assertTrue(Block.isSeparateChecksumBlockFilename(bname));
    assertFalse(Block.isSeparateChecksumBlockFilename(cname));
    assertFalse(Block.isSeparateChecksumBlockFilename(iname));

    filenames[0] = bname;
    filenames[1] = cname;

    // parse generation stamp
    long genStamp = BlockWithChecksumFileReader
        .getGenerationStampFromSeperateChecksumFile(filenames, filenames[0]);
    assertEquals(2000, genStamp);
  }

  @Test
  public void testToDelete() {
    String pref = FSDataset.DELETE_FILE_EXT;
    // old format
    assertFalse(Block.isSeparateChecksumBlockFilename(pref + bname));
    assertFalse(Block.isSeparateChecksumBlockFilename(pref + cname));
    assertFalse(Block.isSeparateChecksumBlockFilename(pref + iname));

    // inline format
    assertFalse(Block.isInlineChecksumBlockFilename(pref + bname));
    assertFalse(Block.isInlineChecksumBlockFilename(pref + cname));
    assertFalse(Block.isInlineChecksumBlockFilename(pref + iname));
  }

  @Test
  public void testInlineFiles() throws Exception {

    assertFalse(Block.isInlineChecksumBlockFilename(bname));
    assertFalse(Block.isInlineChecksumBlockFilename(cname));
    assertTrue(Block.isInlineChecksumBlockFilename(iname));

    // parse filename
    GenStampAndChecksum sac = BlockInlineChecksumReader
        .getGenStampAndChecksumFromInlineChecksumFile(iname);
    assertEquals(2000, sac.generationStamp);
    assertEquals(512, sac.bytesPerChecksum);
    assertEquals(1, sac.checksumType);

    // try to parse old format
    checkErrorParseInlineFile(bname);
    checkErrorParseInlineFile(cname);
    String longiname = Block.BLOCK_FILE_PREFIX + "1000_2000_2_1_512_00";
    String shortiname = Block.BLOCK_FILE_PREFIX + "1000_2000_2_1";
    checkErrorParseInlineFile(longiname);
    checkErrorParseInlineFile(shortiname);

  }

  @Test
  public void testBlockReport() throws Exception {
    File baseDir = new File(MiniDFSCluster.getBaseDirectory(new Configuration()), "test");
    FileUtil.fullyDelete(baseDir);
    baseDir.mkdirs();

    // block files (2 valid blocks)
    createFile(baseDir, bname);
    createFile(baseDir, cname);
    createFile(baseDir, iname);
    
    // toDelete. files (invalid)
    String pref = FSDataset.DELETE_FILE_EXT;
    createFile(baseDir, pref + bname);
    createFile(baseDir, pref + cname);
    createFile(baseDir, pref + iname);
    
    // sth else (invalid)
    createFile(baseDir, "something");
 
    File blockFiles[] = baseDir.listFiles();
    String[] blockFilesNames = FSDataset.getFileNames(blockFiles);
    
    List<Block> list = new ArrayList<Block>();
    for (int i = 0; i < blockFiles.length; i++) {
      Block b = FSDataset.getBlockFromNames(blockFiles, blockFilesNames, i);
      if (b != null) {
        LOG.info("Found block: " + b);
        list.add(b);
      }
    }
    assertEquals(2, list.size());
    for(Block b : list) {
      assertEquals(1000, b.getBlockId());
      assertEquals(2000, b.getGenerationStamp());
      assertEquals(0, b.getNumBytes());
    }

  }
  
  private void createFile(File baseDir, String name) throws IOException {
    File f = new File(baseDir, name);
    f.delete();
    f.createNewFile();
  }

  private void checkErrorParseInlineFile(String name) {
    try {
      BlockInlineChecksumReader
          .getGenStampAndChecksumFromInlineChecksumFile(bname);
      fail("should fail here");
    } catch (Exception e) {
      LOG.info("Expected exception", e);
    }
  }

}

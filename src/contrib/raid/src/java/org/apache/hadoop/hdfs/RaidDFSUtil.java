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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.Codec;

public abstract class RaidDFSUtil {
  public static final String[] codes = new String[] {"xor", "rs"};
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.RaidDFSUtil");
  /**
   * Returns the corrupt blocks in a file.
   */
  public static List<LocatedBlock> corruptBlocksInFile(
    DistributedFileSystem dfs, String path, long offset, long length)
  throws IOException {
    List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
    LocatedBlocks locatedBlocks =
      getBlockLocations(dfs, path, offset, length);
    for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
      if (b.isCorrupt() ||
         (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
        corrupt.add(b);
      }
    }
    return corrupt;
  }

  public static LocatedBlocks getBlockLocations(
    DistributedFileSystem dfs, String path, long offset, long length)
    throws IOException {
    return dfs.getClient().namenode.getBlockLocations(path, offset, length);
  }
  
  public static long getCRC(FileSystem fs, Path p) throws IOException {
    CRC32 crc = new CRC32();
    FSDataInputStream stm = fs.open(p);
    int b;
    while ((b = stm.read())>=0) {
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }
  
  public static void cleanUp(FileSystem fileSys, Path dir) throws IOException {
    if (fileSys.exists(dir)) {
      fileSys.delete(dir, true);
    }
  }

  public static void reportCorruptBlocks(FileSystem fs, Path file, int[] idxs,
      long blockSize) throws IOException {
    FSDataInputStream in = fs.open(file);
    try {
      for (int idx: idxs) {
        long offset = idx * blockSize;
        LOG.info("Reporting corrupt block " + file + ":" + offset);
        in.seek(offset);
        try {
          in.readFully(new byte[(int)blockSize]);
          fail("Expected exception not thrown for " + file + ":" + offset);
        } catch (org.apache.hadoop.fs.ChecksumException e) {
        } catch (org.apache.hadoop.fs.BlockMissingException bme) {
        }
      }
    } finally {
      in.close();
    }
  }
}
